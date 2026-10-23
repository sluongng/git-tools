//! Bound protobuf collection allocation before asking Prost to construct objects.
use crate::Limits;
use bytes::Buf;
use prost::Message;
use std::marker::PhantomData;
use tinytree_proto::v1 as p;
use tonic::{
    Request, Response, Status, Streaming,
    codec::{Codec, CompressionEncoding, DecodeBuf, Decoder, EncodeBuf, Encoder},
    transport::Channel,
};
#[derive(Clone)]
pub(crate) struct Client {
    inner: tonic::client::Grpc<Channel>,
    limits: Limits,
    decode: usize,
}
impl Client {
    pub fn new(channel: Channel, limits: Limits) -> Self {
        let decode = limits.max_batch_bytes;
        Self {
            inner: tonic::client::Grpc::new(channel),
            limits,
            decode,
        }
    }
    pub fn max_decoding_message_size(mut self, n: usize) -> Self {
        self.decode = n;
        self.inner = self.inner.max_decoding_message_size(n);
        self
    }
    pub fn max_encoding_message_size(mut self, n: usize) -> Self {
        self.inner = self.inner.max_encoding_message_size(n);
        self
    }
    pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
        self.inner = self.inner.send_compressed(encoding);
        self
    }
    pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
        self.inner = self.inner.accept_compressed(encoding);
        self
    }
    async fn unary<T: Message + Send + 'static, U: Message + Default + Send + 'static>(
        &mut self,
        request: Request<T>,
        path: &'static str,
        shape: Shape,
    ) -> Result<Response<U>, Status> {
        self.inner
            .ready()
            .await
            .map_err(|_| Status::unavailable("native gRPC transport not ready"))?;
        self.inner
            .unary(
                request,
                http::uri::PathAndQuery::from_static(path),
                BoundedCodec::<T, U>::new(shape, self.limits.clone(), self.decode),
            )
            .await
    }
    pub async fn open_repository(
        &mut self,
        r: Request<p::OpenRepositoryRequest>,
    ) -> Result<Response<p::RepositoryView>, Status> {
        self.unary(r, "/tinytree.v1.Repository/OpenRepository", Shape::View)
            .await
    }
    pub async fn get_commit_graph(
        &mut self,
        r: Request<p::GetCommitGraphRequest>,
    ) -> Result<Response<p::CommitGraphPage>, Status> {
        self.unary(r, "/tinytree.v1.Repository/GetCommitGraph", Shape::Graph)
            .await
    }
    pub async fn find_missing_objects(
        &mut self,
        r: Request<p::FindMissingObjectsRequest>,
    ) -> Result<Response<p::MissingObjects>, Status> {
        self.unary(
            r,
            "/tinytree.v1.Repository/FindMissingObjects",
            Shape::Missing,
        )
        .await
    }
    pub async fn publish_reference(
        &mut self,
        r: Request<p::PublishReferenceRequest>,
    ) -> Result<Response<p::Publication>, Status> {
        self.unary(
            r,
            "/tinytree.v1.Repository/PublishReference",
            Shape::Publication,
        )
        .await
    }
    pub async fn get_publication(
        &mut self,
        r: Request<p::GetPublicationRequest>,
    ) -> Result<Response<p::Publication>, Status> {
        self.unary(
            r,
            "/tinytree.v1.Repository/GetPublication",
            Shape::Publication,
        )
        .await
    }
    pub async fn batch_read_objects(
        &mut self,
        r: Request<p::BatchReadObjectsRequest>,
    ) -> Result<Response<Streaming<p::ObjectResult>>, Status> {
        self.inner
            .ready()
            .await
            .map_err(|_| Status::unavailable("native gRPC transport not ready"))?;
        self.inner
            .server_streaming(
                r,
                http::uri::PathAndQuery::from_static("/tinytree.v1.Repository/BatchReadObjects"),
                BoundedCodec::<p::BatchReadObjectsRequest, p::ObjectResult>::new(
                    Shape::MetadataResult,
                    self.limits.clone(),
                    self.decode,
                ),
            )
            .await
    }
    pub async fn read_file(
        &mut self,
        r: Request<p::ReadFileRequest>,
    ) -> Result<Response<Streaming<p::FileFrame>>, Status> {
        self.inner
            .ready()
            .await
            .map_err(|_| Status::unavailable("native gRPC transport not ready"))?;
        self.inner
            .server_streaming(
                r,
                http::uri::PathAndQuery::from_static("/tinytree.v1.Repository/ReadFile"),
                BoundedCodec::<p::ReadFileRequest, p::FileFrame>::new(
                    Shape::FileFrame,
                    self.limits.clone(),
                    self.decode,
                ),
            )
            .await
    }
    pub async fn write_object(
        &mut self,
        r: impl tonic::IntoStreamingRequest<Message = p::WriteFrame>,
    ) -> Result<Response<p::WriteReceipt>, Status> {
        self.inner
            .ready()
            .await
            .map_err(|_| Status::unavailable("native gRPC transport not ready"))?;
        self.inner
            .client_streaming(
                r.into_streaming_request(),
                http::uri::PathAndQuery::from_static("/tinytree.v1.Repository/WriteObject"),
                BoundedCodec::<p::WriteFrame, p::WriteReceipt>::new(
                    Shape::Receipt,
                    self.limits.clone(),
                    self.decode,
                ),
            )
            .await
    }
}
#[derive(Clone, Copy)]
enum Shape {
    View,
    Reference,
    Descriptor,
    Key,
    Graph,
    Entry,
    Missing,
    MetadataResult,
    ObjectData,
    ItemError,
    FileFrame,
    Chunk,
    Receipt,
    Publication,
}
struct BoundedCodec<T, U> {
    shape: Shape,
    limits: Limits,
    cap: usize,
    _type: PhantomData<(T, U)>,
}
impl<T, U> BoundedCodec<T, U> {
    fn new(shape: Shape, limits: Limits, cap: usize) -> Self {
        Self {
            shape,
            limits,
            cap,
            _type: PhantomData,
        }
    }
}
impl<T: Message + Send + 'static, U: Message + Default + Send + 'static> Codec
    for BoundedCodec<T, U>
{
    type Encode = T;
    type Decode = U;
    type Encoder = WireEncoder<T>;
    type Decoder = WireDecoder<U>;
    fn encoder(&mut self) -> Self::Encoder {
        WireEncoder(PhantomData)
    }
    fn decoder(&mut self) -> Self::Decoder {
        WireDecoder {
            shape: self.shape,
            limits: self.limits.clone(),
            cap: self.cap,
            _type: PhantomData,
        }
    }
}
struct WireEncoder<T>(PhantomData<T>);
impl<T: Message> Encoder for WireEncoder<T> {
    type Item = T;
    type Error = Status;
    fn encode(&mut self, item: T, buf: &mut EncodeBuf<'_>) -> Result<(), Status> {
        item.encode(buf)
            .map_err(|_| Status::internal("native request encoding failed"))
    }
}
struct WireDecoder<U> {
    shape: Shape,
    limits: Limits,
    cap: usize,
    _type: PhantomData<U>,
}
impl<U: Message + Default> Decoder for WireDecoder<U> {
    type Item = U;
    type Error = Status;
    fn decode(&mut self, buf: &mut DecodeBuf<'_>) -> Result<Option<U>, Status> {
        let bytes = buf.copy_to_bytes(buf.remaining());
        let mut budget = if matches!(self.shape, Shape::Graph) {
            self.cap
        } else {
            self.cap.saturating_mul(2)
        };
        charge(&mut budget, std::mem::size_of::<U>())?;
        scan(&bytes, self.shape, &self.limits, &mut budget, 0)?;
        U::decode(bytes).map(Some).map_err(|_| invalid())
    }
}
fn invalid() -> Status {
    Status::data_loss("native protobuf response has an invalid bounded shape")
}
fn exhausted() -> Status {
    Status::resource_exhausted("native protobuf response exceeds collection allocation budget")
}
fn charge(budget: &mut usize, n: usize) -> Result<(), Status> {
    *budget = budget.checked_sub(n).ok_or_else(exhausted)?;
    Ok(())
}
fn varint(bytes: &[u8], position: &mut usize) -> Result<u64, Status> {
    let mut value = 0u64;
    for shift in (0..70).step_by(7) {
        let byte = *bytes.get(*position).ok_or_else(invalid)?;
        *position += 1;
        if shift == 63 && byte > 1 {
            return Err(invalid());
        }
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(invalid())
}
fn scan(
    bytes: &[u8],
    shape: Shape,
    limits: &Limits,
    budget: &mut usize,
    depth: usize,
) -> Result<(), Status> {
    if depth > 8 {
        return Err(invalid());
    }
    let mut position = 0;
    let mut counts = [0usize; 16];
    let mut fields = 0u32;
    while position < bytes.len() {
        let tag = varint(bytes, &mut position)?;
        let number = u32::try_from(tag >> 3).map_err(|_| invalid())?;
        let wire = (tag & 7) as u8;
        if number == 0 {
            return Err(invalid());
        }
        let data = match wire {
            0 => {
                let start = position;
                varint(bytes, &mut position)?;
                &bytes[start..position]
            }
            1 => {
                let end = position
                    .checked_add(8)
                    .filter(|end| *end <= bytes.len())
                    .ok_or_else(invalid)?;
                let data = &bytes[position..end];
                position = end;
                data
            }
            2 => {
                let length =
                    usize::try_from(varint(bytes, &mut position)?).map_err(|_| invalid())?;
                let end = position
                    .checked_add(length)
                    .filter(|end| *end <= bytes.len())
                    .ok_or_else(invalid)?;
                let data = &bytes[position..end];
                position = end;
                data
            }
            5 => {
                let end = position
                    .checked_add(4)
                    .filter(|end| *end <= bytes.len())
                    .ok_or_else(invalid)?;
                let data = &bytes[position..end];
                position = end;
                data
            }
            _ => return Err(invalid()),
        };
        let rule = rule(shape, number, limits);
        if let Some((expected, child, maximum, allocation)) = rule {
            if wire != expected {
                return Err(invalid());
            }
            let index = number as usize;
            if index >= counts.len() {
                return Err(invalid());
            }
            counts[index] += 1;
            if counts[index] > maximum {
                return Err(exhausted());
            }
            fields |= 1u32 << number;
            if wire == 2 {
                if let Some(child) = child {
                    charge(budget, allocation)?;
                    scan(data, child, limits, budget, depth + 1)?;
                } else {
                    let length_limit = allocation;
                    if data.len() > length_limit {
                        return Err(exhausted());
                    }
                    charge(budget, data.len().saturating_mul(2))?;
                    if matches!(shape, Shape::Key) && number == 2 && data.len() != 32 {
                        return Err(invalid());
                    }
                    if matches!(shape, Shape::Descriptor) && number == 2 && data.len() != 32 {
                        return Err(invalid());
                    }
                    if matches!((shape, number), (Shape::Reference, 1)) && data.is_empty() {
                        return Err(invalid());
                    }
                }
            }
        }
    }
    let required = match shape {
        Shape::Reference => 1 << 1,
        Shape::Descriptor => (1 << 1) | (1 << 2) | (1 << 4),
        Shape::Key => (1 << 1) | (1 << 2),
        Shape::Entry => 1 << 1,
        Shape::MetadataResult => (1 << 2),
        Shape::ObjectData => (1 << 1) | (1 << 2),
        Shape::Receipt => (1 << 1) | (1 << 2),
        Shape::Publication => (1 << 1) | (1 << 2),
        _ => 0,
    };
    if fields & required != required {
        return Err(invalid());
    }
    if matches!(shape, Shape::MetadataResult)
        && ((counts[3] > 0) as u8 + (counts[4] > 0) as u8) != 1
    {
        return Err(invalid());
    }
    if matches!(shape, Shape::FileFrame) && ((counts[1] > 0) as u8 + (counts[2] > 0) as u8) != 1 {
        return Err(invalid());
    }
    Ok(())
}
// (wire type, nested shape, field occurrences, length/collection allocation)
fn rule(shape: Shape, n: u32, l: &Limits) -> Option<(u8, Option<Shape>, usize, usize)> {
    let scalar = || Some((0, None, 1, 0));
    let text = |max| Some((2, None, 1, max));
    let nested = |child| Some((2, Some(child), 1, 0));
    let repeated = |child, count, allocation| Some((2, Some(child), count, allocation));
    match (shape, n) {
        (Shape::View, 1) => text(128),
        (Shape::View, 2 | 3 | 5 | 6 | 7) => scalar(),
        (Shape::View, 4) => repeated(
            Shape::Reference,
            l.max_batch_items,
            4 * std::mem::size_of::<p::Reference>(),
        ),
        (Shape::Reference, 1) => text(4096),
        (Shape::Reference, 2) => scalar(),
        (Shape::Reference, 3) => nested(Shape::Descriptor),
        (Shape::Descriptor, 1) => nested(Shape::Key),
        (Shape::Descriptor, 2) => text(32),
        (Shape::Descriptor, 3 | 4) => scalar(),
        (Shape::Key, 1) => scalar(),
        (Shape::Key, 2) => text(32),
        (Shape::Graph, 1) => repeated(
            Shape::Entry,
            l.max_batch_items,
            4 * std::mem::size_of::<p::CommitGraphEntry>(),
        ),
        (Shape::Graph, 2) => text(4096),
        (Shape::Entry, 1) => nested(Shape::Descriptor),
        (Shape::Entry, 2) => repeated(
            Shape::Key,
            l.max_graph_objects,
            4 * std::mem::size_of::<p::ObjectKey>(),
        ),
        (Shape::Entry, 3) => text(l.max_metadata_bytes),
        (Shape::Missing, 1) => repeated(
            Shape::Key,
            l.max_batch_items,
            4 * std::mem::size_of::<p::ObjectKey>(),
        ),
        (Shape::MetadataResult, 1) => scalar(),
        (Shape::MetadataResult, 2) => nested(Shape::Key),
        (Shape::MetadataResult, 3) => nested(Shape::ObjectData),
        (Shape::MetadataResult, 4) => nested(Shape::ItemError),
        (Shape::ObjectData, 1) => nested(Shape::Descriptor),
        (Shape::ObjectData, 2) => text(l.max_metadata_bytes),
        (Shape::ItemError, 1) => scalar(),
        (Shape::ItemError, 2) => text(4096),
        (Shape::FileFrame, 1) => nested(Shape::Descriptor),
        (Shape::FileFrame, 2) => nested(Shape::Chunk),
        (Shape::Chunk, 1) => scalar(),
        (Shape::Chunk, 2) => text(l.max_frame_bytes),
        (Shape::Receipt, 1) => nested(Shape::Descriptor),
        (Shape::Receipt, 2) => text(512),
        (Shape::Receipt, 3) => scalar(),
        (Shape::Publication, 1) => text(4096),
        (Shape::Publication, 2) => nested(Shape::Reference),
        _ => None,
    }
}
