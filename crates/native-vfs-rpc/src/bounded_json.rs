//! Allocation-free structural preflight before serde constructs domain vectors.
//! The ordinary JSON parser may use a bounded per-string scratch buffer, but no
//! collection of decoded labels/terms is allocated by this visitor.
use crate::Limits;
use serde::de::{self, DeserializeSeed, MapAccess, SeqAccess, Visitor};
use std::{cell::Cell, fmt};
const MAX_TERMS: usize = 1023;
const MAX_LABEL_BYTES: usize = 256 * 1024;
#[derive(Clone, Copy)]
enum Shape<'a> {
    Value,
    Terms,
    Term,
    Labels,
    Label(&'a Cell<usize>),
    Bytes(usize),
    Text(usize),
}
struct Seed<'a> {
    shape: Shape<'a>,
    limits: &'a Limits,
}
impl<'de> DeserializeSeed<'de> for Seed<'_> {
    type Value = ();
    fn deserialize<D: de::Deserializer<'de>>(self, decoder: D) -> Result<(), D::Error> {
        decoder.deserialize_any(self)
    }
}
fn invalid<E: de::Error>() -> E {
    E::custom("projection JSON shape exceeds its predecode bounds")
}
struct Reject;
impl<'de> DeserializeSeed<'de> for Reject {
    type Value = ();
    fn deserialize<D: de::Deserializer<'de>>(self, _: D) -> Result<(), D::Error> {
        Err(invalid())
    }
}
#[derive(Clone, Copy)]
enum Key {
    Value,
    Terms,
    Labels,
    Operation,
    Contents,
    Digest,
    Incarnation,
    Repository,
    Mount,
    Request,
    Path,
    HostPath,
    Kind,
    Message,
}
impl<'de> DeserializeSeed<'de> for Key {
    type Value = Key;
    fn deserialize<D: de::Deserializer<'de>>(self, decoder: D) -> Result<Key, D::Error> {
        decoder.deserialize_identifier(self)
    }
}
impl<'de> Visitor<'de> for Key {
    type Value = Key;
    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a projection object key")
    }
    fn visit_str<E: de::Error>(self, key: &str) -> Result<Key, E> {
        Ok(match key {
            "terms" => Key::Terms,
            "labels" => Key::Labels,
            "operation" => Key::Operation,
            "contents" => Key::Contents,
            "digest" | "fingerprint" => Key::Digest,
            "incarnation" => Key::Incarnation,
            "repository" => Key::Repository,
            "mount" => Key::Mount,
            "request_id" => Key::Request,
            "path" | "directory" => Key::Path,
            "root" | "state_path" => Key::HostPath,
            "kind" => Key::Kind,
            "message" => Key::Message,
            _ => Key::Value,
        })
    }
}
impl<'de> Visitor<'de> for Seed<'_> {
    type Value = ();
    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a bounded projection domain value")
    }
    fn visit_bool<E: de::Error>(self, _: bool) -> Result<(), E> {
        Ok(())
    }
    fn visit_i64<E: de::Error>(self, _: i64) -> Result<(), E> {
        Ok(())
    }
    fn visit_u64<E: de::Error>(self, _: u64) -> Result<(), E> {
        Ok(())
    }
    fn visit_f64<E: de::Error>(self, _: f64) -> Result<(), E> {
        Ok(())
    }
    fn visit_unit<E: de::Error>(self) -> Result<(), E> {
        Ok(())
    }
    fn visit_none<E: de::Error>(self) -> Result<(), E> {
        Ok(())
    }
    fn visit_str<E: de::Error>(self, value: &str) -> Result<(), E> {
        match self.shape {
            Shape::Text(max) if value.len() > max => return Err(invalid()),
            Shape::Label(bytes) => {
                let total = bytes.get().checked_add(value.len()).ok_or_else(invalid)?;
                if total > MAX_LABEL_BYTES {
                    return Err(invalid());
                }
                bytes.set(total);
            }
            _ => {}
        }
        Ok(())
    }
    fn visit_string<E: de::Error>(self, value: String) -> Result<(), E> {
        self.visit_str(&value)
    }
    fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<(), M::Error> {
        while let Some(key) = map.next_key_seed(Key::Value)? {
            let shape = match key {
                Key::Value => Shape::Value,
                Key::Terms => Shape::Terms,
                Key::Labels => Shape::Labels,
                Key::Operation => Shape::Bytes(128),
                Key::Contents => Shape::Bytes(
                    self.limits
                        .max_policy_bytes
                        .min(self.limits.max_frame_bytes),
                ),
                Key::Digest => Shape::Bytes(32),
                Key::Incarnation => Shape::Bytes(16),
                Key::Repository => Shape::Text(256),
                Key::Mount => Shape::Text(128),
                Key::Request => Shape::Text(128),
                Key::Path => Shape::Text(4096),
                Key::HostPath => Shape::Text(32768),
                Key::Kind => Shape::Text(64),
                Key::Message => Shape::Text(4096),
            };
            map.next_value_seed(Seed {
                shape,
                limits: self.limits,
            })?;
        }
        Ok(())
    }
    fn visit_seq<S: SeqAccess<'de>>(self, mut sequence: S) -> Result<(), S::Error> {
        let label_bytes = Cell::new(0usize);
        let (limit, element) = match self.shape {
            Shape::Terms => (MAX_TERMS, Shape::Term),
            Shape::Term => (32, Shape::Value),
            Shape::Labels => (MAX_TERMS, Shape::Label(&label_bytes)),
            Shape::Bytes(max) => (max, Shape::Value),
            // All legitimate arrays have explicit bounded field shapes. Reject
            // positional struct encodings and unknown arrays before serde can
            // select a sequence-based struct/enum conversion.
            _ => return Err(invalid()),
        };
        for _ in 0..limit {
            if sequence
                .next_element_seed(Seed {
                    shape: element,
                    limits: self.limits,
                })?
                .is_none()
            {
                return Ok(());
            }
        }
        // Reject the first excessive element before deserializing its contents.
        sequence.next_element_seed(Reject)?;
        Ok(())
    }
}
pub(crate) fn check(payload: &[u8], limits: &Limits) -> Result<(), tonic::Status> {
    let mut decoder = serde_json::Deserializer::from_slice(payload);
    Seed {
        shape: Shape::Value,
        limits,
    }
    .deserialize(&mut decoder)
    .and_then(|_| decoder.end())
    .map_err(|_| {
        tonic::Status::invalid_argument("projection frame exceeds structural decode limits")
    })
}
