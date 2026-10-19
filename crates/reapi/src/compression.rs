//! CAS payload compression and independently negotiated gRPC compression.
//!
//! Hashes/sizes always describe uncompressed CAS content. Never enable outgoing
//! gRPC compression on a ByteStream that already carries a compressed blob.
use std::{
    io::{self, Write},
    sync::atomic::{AtomicU8, Ordering},
};
use tonic::{codec::CompressionEncoding, metadata::MetadataMap};
use zstd::stream::raw::Operation;

/// Per-client transport capability, distinct from REAPI compressor capabilities.
/// An unknown peer receives identity requests; response codecs can be advertised
/// immediately. Only an explicit peer advertisement enables compressed requests.
#[derive(Default)]
pub struct GrpcCompression(AtomicU8);
impl GrpcCompression {
    pub fn observe(&self, metadata: &MetadataMap) {
        let mut selected = 0;
        for value in metadata.get_all("grpc-accept-encoding").iter() {
            if let Ok(value) = value.to_str() {
                for name in value.split(',').map(str::trim) {
                    if name.eq_ignore_ascii_case("gzip") {
                        selected = 1;
                    }
                }
            }
        }
        if selected != 0 {
            self.0.store(selected, Ordering::Release);
        }
    }
    pub fn request_encoding(&self) -> Option<CompressionEncoding> {
        match self.0.load(Ordering::Acquire) {
            1 => Some(CompressionEncoding::Gzip),
            _ => None,
        }
    }
    /// Services on one origin can accept different codecs. Retry only explicit
    /// unsupported-codec errors, never method failures or authorization errors.
    pub fn retry_identity(
        &self,
        status: &tonic::Status,
        attempted: Option<CompressionEncoding>,
    ) -> bool {
        if attempted != Some(CompressionEncoding::Gzip)
            || status.code() != tonic::Code::Unimplemented
        {
            return false;
        }
        let mut advertised = false;
        for value in status.metadata().get_all("grpc-accept-encoding").iter() {
            let Ok(value) = value.to_str() else {
                return false;
            };
            advertised = true;
            if value
                .split(',')
                .any(|name| name.trim().eq_ignore_ascii_case("gzip"))
            {
                return false;
            }
        }
        if advertised {
            self.0.store(0, Ordering::Release);
        }
        advertised
    }
}

/// Fast compression for already-bounded batch items, not whole large blobs.
pub fn encode_zstd(data: &[u8]) -> io::Result<Vec<u8>> {
    let mut encoder = zstd::stream::write::Encoder::new(Vec::new(), 1)?;
    encoder.include_checksum(true)?;
    encoder.write_all(data)?;
    encoder.finish()
}

/// Incremental decoder with a fixed output buffer and at most an 8 MiB window.
/// Concatenated frames are valid (including independently compressed CDC chunks).
/// Callers must bound wire bytes/deadline and enforce decoded size before writes.
pub struct ZstdDecoder {
    decoder: zstd::stream::raw::Decoder<'static>,
    complete: bool,
    saw_input: bool,
    output: Box<[u8; 64 * 1024]>,
}
/// One bounded decode step. Async consumers should yield between steps.
pub struct DecodeStep {
    pub consumed: usize,
    pub needs_drain: bool,
}
impl ZstdDecoder {
    pub fn new() -> io::Result<Self> {
        let mut decoder = zstd::stream::raw::Decoder::new()?;
        decoder.set_parameter(zstd::stream::raw::DParameter::WindowLogMax(23))?;
        Ok(Self {
            decoder,
            complete: false,
            saw_input: false,
            output: Box::new([0; 64 * 1024]),
        })
    }
    pub fn decode(&mut self, mut input: &[u8], output: &mut impl Write) -> io::Result<()> {
        let mut drain = false;
        while !input.is_empty() || drain {
            let step = self.decode_step(input, output)?;
            input = &input[step.consumed..];
            drain = step.needs_drain;
        }
        Ok(())
    }
    /// Emit at most 64 KiB plaintext so async callers can enforce deadlines even
    /// when one small wire message expands to many gigabytes.
    pub fn decode_step(&mut self, input: &[u8], output: &mut impl Write) -> io::Result<DecodeStep> {
        let status = self.decoder.run_on_buffers(input, self.output.as_mut())?;
        self.saw_input |= status.bytes_read != 0;
        self.complete = status.remaining == 0;
        output.write_all(&self.output[..status.bytes_written])?;
        if status.bytes_read == 0 && status.bytes_written == 0 && !input.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "ZSTD made no progress",
            ));
        }
        Ok(DecodeStep {
            consumed: status.bytes_read,
            needs_drain: status.bytes_written == self.output.len() && !self.complete,
        })
    }
    /// EOF must coincide with a complete frame, even if all expected plaintext
    /// has already been emitted. A digest alone does not detect a missing footer.
    pub fn finish(&self) -> io::Result<()> {
        if self.saw_input && self.complete {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "incomplete ZSTD frame",
            ))
        }
    }
}

/// Convenience for bounded batch/test payloads; streaming consumers use decoder.
pub fn decode_zstd(data: &[u8], max_output: u64) -> io::Result<Vec<u8>> {
    struct Bounded {
        bytes: Vec<u8>,
        limit: u64,
    }
    impl Write for Bounded {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            let len = (self.bytes.len() as u64)
                .checked_add(bytes.len() as u64)
                .ok_or_else(|| io::Error::other("decoded length overflow"))?;
            if len > self.limit {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "decoded size exceeds limit",
                ));
            }
            self.bytes.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut output = Bounded {
        bytes: Vec::new(),
        limit: max_output,
    };
    let mut decoder = ZstdDecoder::new()?;
    decoder.decode(data, &mut output)?;
    decoder.finish()?;
    Ok(output.bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fragmented_concatenated_frames_preserve_content() {
        let first = vec![b'a'; 200_000];
        let second = b"separate CDC frame";
        let mut frames = encode_zstd(&first).unwrap();
        frames.extend(encode_zstd(&[]).unwrap());
        frames.extend(encode_zstd(second).unwrap());
        let expected = [first.as_slice(), second].concat();
        for fragment_size in [1, 3, 17] {
            let mut decoder = ZstdDecoder::new().unwrap();
            let mut decoded = Vec::new();
            for part in frames.chunks(fragment_size) {
                decoder.decode(part, &mut decoded).unwrap();
            }
            decoder.finish().unwrap();
            assert_eq!(decoded, expected);
        }
    }

    #[test]
    fn missing_footer_corruption_garbage_and_expansion_are_rejected() {
        let content = vec![b'x'; 200_000];
        let encoded = encode_zstd(&content).unwrap();
        // A checksum footer can be truncated after every plaintext byte emerged.
        let mut decoder = ZstdDecoder::new().unwrap();
        let mut output = Vec::new();
        decoder
            .decode(&encoded[..encoded.len() - 1], &mut output)
            .unwrap();
        assert_eq!(output, content);
        assert!(decoder.finish().is_err());
        let mut corrupt = encoded.clone();
        *corrupt.last_mut().unwrap() ^= 1;
        assert!(decode_zstd(&corrupt, content.len() as u64).is_err());
        let mut garbage = encoded.clone();
        garbage.push(0xef);
        assert!(decode_zstd(&garbage, content.len() as u64).is_err());
        assert!(decode_zstd(&encoded, content.len() as u64 - 1).is_err());
        assert!(decode_zstd(&[], 0).is_err());
        assert_eq!(decode_zstd(&encode_zstd(&[]).unwrap(), 0).unwrap(), b"");
        // Valid empty frame advertising a 16 MiB window, above our policy cap.
        let large_window = [0x28, 0xb5, 0x2f, 0xfd, 0x00, 0x70, 0x01, 0x00, 0x00];
        assert_eq!(
            zstd::stream::decode_all(large_window.as_slice()).unwrap(),
            b""
        );
        assert!(decode_zstd(&large_window, 0).is_err());
    }

    #[test]
    fn grpc_requests_require_an_explicit_peer_advertisement() {
        let compression = GrpcCompression::default();
        assert!(compression.request_encoding().is_none());
        let mut metadata = MetadataMap::new();
        metadata.insert("grpc-accept-encoding", "identity, gzip".parse().unwrap());
        compression.observe(&metadata);
        assert_eq!(
            compression.request_encoding(),
            Some(CompressionEncoding::Gzip)
        );
        metadata.insert(
            "grpc-accept-encoding",
            "identity,zstd,gzip".parse().unwrap(),
        );
        compression.observe(&metadata);
        assert_eq!(
            compression.request_encoding(),
            Some(CompressionEncoding::Gzip)
        );

        assert!(!compression.retry_identity(
            &tonic::Status::unimplemented("no splice"),
            Some(CompressionEncoding::Gzip)
        ));
        let mut refusal = tonic::Status::permission_denied("denied");
        refusal
            .metadata_mut()
            .insert("grpc-accept-encoding", "identity".parse().unwrap());
        assert!(!compression.retry_identity(&refusal, Some(CompressionEncoding::Gzip)));
        let mut refusal = tonic::Status::unimplemented("codec unsupported");
        refusal
            .metadata_mut()
            .insert("grpc-accept-encoding", "identity".parse().unwrap());
        assert!(compression.retry_identity(&refusal, Some(CompressionEncoding::Gzip)));
        assert!(compression.request_encoding().is_none());
        assert!(!compression.retry_identity(&refusal, None));
    }
}
