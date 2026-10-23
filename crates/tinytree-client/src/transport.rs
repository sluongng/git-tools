//! Bound protobuf collection allocation before asking Prost to construct objects.
use crate::Limits;
use bytes::Buf;
use prost::Message;
use std::{any::Any, marker::PhantomData, sync::Arc};
type PhysicalOwner = Arc<dyn Any + Send + Sync>;
pub(crate) const QUERY_CLOSE_CAP: usize = 1024;
use tinytree_proto::{query_v1 as q, v1 as p};
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
    owner: Option<PhysicalOwner>,
}
impl Client {
    pub fn new(channel: Channel, limits: Limits) -> Self {
        let decode = limits.max_batch_bytes;
        Self {
            inner: tonic::client::Grpc::new(channel),
            limits,
            decode,
            owner: None,
        }
    }
    pub fn with_owner(mut self, owner: PhysicalOwner) -> Self {
        self.owner = Some(owner);
        self
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
                BoundedCodec::<T, U>::new(shape, self.limits.clone(), self.decode)
                    .with_optional_owner(self.owner.clone()),
            )
            .await
    }
    pub(crate) async fn cloud_unary<
        T: Message + Send + 'static,
        U: Message + Default + Send + 'static,
    >(
        &mut self,
        request: Request<T>,
        path: &'static str,
        shape: Shape,
        limits: &crate::CloudLimits,
        cap: usize,
    ) -> Result<Response<U>, Status> {
        self.limits.max_batch_items = limits.max_lookup_items;
        self.limits.max_metadata_bytes = limits.max_manifest_bytes;
        self.limits.max_frame_bytes = limits.max_chunk_bytes;
        self.decode = cap;
        self.inner = self
            .inner
            .clone()
            .max_decoding_message_size(cap)
            .max_encoding_message_size(
                limits.max_manifest_bytes.max(limits.max_object_bytes) + 128 * 1024,
            );
        self.unary(request, path, shape).await
    }
    pub(crate) async fn cloud_read(
        &mut self,
        request: Request<tinytree_proto::cloud_v1::ReadObjectRequest>,
        limits: &crate::CloudLimits,
    ) -> Result<Response<Streaming<tinytree_proto::cloud_v1::ObjectChunk>>, Status> {
        let cap = limits.max_chunk_bytes + 1024;
        self.inner = self
            .inner
            .clone()
            .max_decoding_message_size(cap)
            .max_encoding_message_size(4096);
        self.limits.max_frame_bytes = limits.max_chunk_bytes;
        self.inner
            .ready()
            .await
            .map_err(|_| Status::unavailable("native gRPC transport not ready"))?;
        self.inner
            .server_streaming(
                request,
                http::uri::PathAndQuery::from_static(
                    "/tinytree.cloud.v1.WorkspaceCloud/ReadObject",
                ),
                BoundedCodec::<
                    tinytree_proto::cloud_v1::ReadObjectRequest,
                    tinytree_proto::cloud_v1::ObjectChunk,
                >::new(Shape::CloudChunk, self.limits.clone(), cap)
                .with_optional_owner(self.owner.clone()),
            )
            .await
    }
    pub async fn open_scope(
        &mut self,
        request: impl tonic::IntoStreamingRequest<Message = q::OpenScopeFrame>,
        owner: PhysicalOwner,
    ) -> Result<Response<q::QueryScope>, Status> {
        self.inner
            .ready()
            .await
            .map_err(|_| Status::unavailable("native gRPC transport not ready"))?;
        self.inner
            .client_streaming(
                request.into_streaming_request(),
                http::uri::PathAndQuery::from_static(
                    "/tinytree.query.v1.RepositoryQuery/OpenScope",
                ),
                BoundedCodec::<q::OpenScopeFrame, q::QueryScope>::new(
                    Shape::QueryScope,
                    self.limits.clone(),
                    self.decode,
                )
                .with_owner(owner),
            )
            .await
    }
    pub async fn fork_scope(
        &mut self,
        request: impl tonic::IntoStreamingRequest<Message = q::ForkScopeFrame>,
        owner: PhysicalOwner,
    ) -> Result<Response<q::QueryScope>, Status> {
        self.inner
            .ready()
            .await
            .map_err(|_| Status::unavailable("native gRPC transport not ready"))?;
        self.inner
            .client_streaming(
                request.into_streaming_request(),
                http::uri::PathAndQuery::from_static(
                    "/tinytree.query.v1.RepositoryQuery/ForkScope",
                ),
                BoundedCodec::<q::ForkScopeFrame, q::QueryScope>::new(
                    Shape::QueryScope,
                    self.limits.clone(),
                    self.decode,
                )
                .with_owner(owner),
            )
            .await
    }
    pub async fn close_scope(
        &mut self,
        r: Request<q::CloseScopeRequest>,
    ) -> Result<Response<q::CloseScopeResponse>, Status> {
        self.unary(
            r,
            "/tinytree.query.v1.RepositoryQuery/CloseScope",
            Shape::QueryEmpty,
        )
        .await
    }
    pub async fn close_query(
        &mut self,
        request: Request<q::CloseQueryRequest>,
        owner: PhysicalOwner,
    ) -> Result<Response<q::CloseQueryResponse>, Status> {
        // The transport/decompress cap must match cleanup admission, not the
        // much larger ordinary metadata-page response cap.
        let mut grpc = self
            .clone()
            .max_decoding_message_size(QUERY_CLOSE_CAP)
            .max_encoding_message_size(QUERY_CLOSE_CAP);
        grpc.inner
            .ready()
            .await
            .map_err(|_| Status::unavailable("native gRPC transport not ready"))?;
        grpc.inner
            .unary(
                request,
                http::uri::PathAndQuery::from_static(
                    "/tinytree.query.v1.RepositoryQuery/CloseQuery",
                ),
                BoundedCodec::<q::CloseQueryRequest, q::CloseQueryResponse>::new(
                    Shape::QueryEmpty,
                    self.limits.clone(),
                    QUERY_CLOSE_CAP,
                )
                .with_owner(owner),
            )
            .await
    }
    pub async fn query_facts(
        &mut self,
        r: Request<q::QueryFactsRequest>,
    ) -> Result<Response<q::GraphFacts>, Status> {
        self.unary(
            r,
            "/tinytree.query.v1.RepositoryQuery/QueryFacts",
            Shape::QueryFacts,
        )
        .await
    }
    pub async fn resolve_commit_prefix(
        &mut self,
        r: Request<q::ResolveCommitPrefixRequest>,
    ) -> Result<Response<q::CommitPrefixResult>, Status> {
        self.unary(
            r,
            "/tinytree.query.v1.RepositoryQuery/ResolveCommitPrefix",
            Shape::CommitPrefix,
        )
        .await
    }
    pub async fn resolve_change_prefix(
        &mut self,
        r: Request<q::ResolveChangePrefixRequest>,
    ) -> Result<Response<q::ChangePrefixResult>, Status> {
        self.unary(
            r,
            "/tinytree.query.v1.RepositoryQuery/ResolveChangePrefix",
            Shape::ChangePrefix,
        )
        .await
    }
    pub async fn shortest_prefix(
        &mut self,
        r: Request<q::ShortestPrefixRequest>,
    ) -> Result<Response<q::ShortestPrefixResult>, Status> {
        self.unary(
            r,
            "/tinytree.query.v1.RepositoryQuery/ShortestPrefix",
            Shape::ShortestPrefix,
        )
        .await
    }
    pub async fn evaluate(
        &mut self,
        request: Request<q::EvaluateRequest>,
        owner: PhysicalOwner,
    ) -> Result<Response<q::EvaluatePage>, Status> {
        let shape = match q::Projection::try_from(request.get_ref().projection) {
            Ok(q::Projection::Ids) => Shape::IdsPage,
            Ok(q::Projection::Graph) => Shape::GraphPage,
            Ok(q::Projection::Canonical) => Shape::CanonicalPage,
            _ => return Err(Status::invalid_argument("invalid query projection")),
        };
        self.inner
            .ready()
            .await
            .map_err(|_| Status::unavailable("native gRPC transport not ready"))?;
        self.inner
            .unary(
                request,
                http::uri::PathAndQuery::from_static("/tinytree.query.v1.RepositoryQuery/Evaluate"),
                BoundedCodec::<q::EvaluateRequest, q::EvaluatePage>::new(
                    shape,
                    self.limits.clone(),
                    self.decode,
                )
                .with_owner(owner),
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
                )
                .with_optional_owner(self.owner.clone()),
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
                )
                .with_optional_owner(self.owner.clone()),
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
                )
                .with_optional_owner(self.owner.clone()),
            )
            .await
    }
}
#[derive(Clone, Copy)]
pub(crate) enum Shape {
    QueryScope,
    ScopeKey,
    QueryFacts,
    FactAnswer,
    CommitSet,
    CommitId,
    ChangeId,
    CommitPrefix,
    ChangePrefix,
    CommitMatch,
    ChangeMatch,
    ChangeTarget,
    ShortestPrefix,
    QueryEmpty,
    CloudId,
    CloudDigest,
    CloudDescriptor,
    CloudReceipt,
    CloudSelections,
    CloudSelection,
    CloudChunk,
    CloudWorkspace,
    CloudPublication,
    CloudPublicationResult,
    CloudVersion,
    IdsPage,
    IdsRow,
    GraphPage,
    CanonicalPage,
    GraphRow,
    CanonicalRow,
    SelectedEdge,
    EdgeTarget,
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
    owner: Option<PhysicalOwner>,
}
impl<T, U> BoundedCodec<T, U> {
    fn new(shape: Shape, limits: Limits, cap: usize) -> Self {
        Self {
            shape,
            limits,
            cap,
            _type: PhantomData,
            owner: None,
        }
    }
    fn with_optional_owner(mut self, owner: Option<PhysicalOwner>) -> Self {
        self.owner = owner;
        self
    }
    fn with_owner(mut self, owner: PhysicalOwner) -> Self {
        self.owner = Some(owner);
        self
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
        WireEncoder(PhantomData, self.owner.clone())
    }
    fn decoder(&mut self) -> Self::Decoder {
        WireDecoder {
            shape: self.shape,
            limits: self.limits.clone(),
            cap: self.cap,
            _type: PhantomData,
            _owner: self.owner.clone(),
        }
    }
}
// The encoder lives in Tonic's physical HTTP body after its fused source stream
// reaches EOF. Keep owned buffers and admission there through final body drop.
struct WireEncoder<T>(PhantomData<T>, Option<PhysicalOwner>);
impl<T: Message> Encoder for WireEncoder<T> {
    type Item = T;
    type Error = Status;
    fn encode(&mut self, item: T, buf: &mut EncodeBuf<'_>) -> Result<(), Status> {
        let _owner = &self.1;
        item.encode(buf)
            .map_err(|_| Status::internal("native request encoding failed"))
    }
}
struct WireDecoder<U> {
    shape: Shape,
    limits: Limits,
    cap: usize,
    _type: PhantomData<U>,
    _owner: Option<PhysicalOwner>,
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
        let mut edges = 0usize;
        scan(&bytes, self.shape, &self.limits, &mut budget, &mut edges, 0)?;
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
    edges: &mut usize,
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
        // This client surface requests IDS only. Reject graph/canonical payloads
        // before Prost can allocate them, even if empty/default-valued.
        if matches!(shape, Shape::IdsRow) && matches!(number, 3 | 4) {
            return Err(invalid());
        }
        if matches!(shape, Shape::GraphRow) && number == 4 {
            return Err(invalid());
        }
        if matches!(shape, Shape::SelectedEdge) {
            if !(1..=3).contains(&number) {
                return Err(invalid());
            }
        }
        if matches!(shape, Shape::GraphRow | Shape::CanonicalRow) && number == 3 {
            *edges = edges.checked_add(1).ok_or_else(exhausted)?;
            if *edges > limits.max_query_edges {
                return Err(exhausted());
            }
        }
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
            if wire == 0
                && matches!(
                    (shape, number),
                    (Shape::FactAnswer, 1 | 2 | 5)
                        | (Shape::ChangeTarget, 2)
                        | (Shape::IdsPage | Shape::GraphPage | Shape::CanonicalPage, 6)
                        | (Shape::CloudChunk, 4)
                )
            {
                let mut offset = 0;
                if varint(data, &mut offset)? > 1 {
                    return Err(invalid());
                }
            }
            if wire == 2 {
                if let Some(child) = child {
                    charge(budget, allocation)?;
                    scan(data, child, limits, budget, edges, depth + 1)?;
                } else {
                    let length_limit = allocation;
                    if data.len() > length_limit {
                        return Err(exhausted());
                    }
                    charge(budget, data.len().saturating_mul(2))?;
                    let exact = match (shape, number) {
                        (Shape::QueryScope, 2)
                        | (Shape::IdsPage | Shape::GraphPage | Shape::CanonicalPage, 1)
                        | (Shape::ScopeKey, 2)
                        | (Shape::CommitId, 1)
                        | (Shape::QueryFacts, 2) => Some(32),
                        (Shape::QueryScope, 3)
                        | (Shape::IdsPage | Shape::GraphPage | Shape::CanonicalPage, 2)
                        | (Shape::ChangeId, 1)
                        | (Shape::QueryFacts, 3) => Some(16),
                        _ => None,
                    };
                    if exact.is_some_and(|n| data.len() != n) {
                        return Err(invalid());
                    }
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
        Shape::CloudId => (1 << 1) | (1 << 2),
        Shape::CloudDigest => 1 << 1,
        Shape::CloudDescriptor | Shape::CloudReceipt | Shape::CloudPublication => {
            (1 << 1) | (1 << 2)
        }
        Shape::CloudChunk | Shape::CloudWorkspace => 1 << 1,
        Shape::CloudVersion => (1 << 1) | (1 << 3),
        Shape::IdsPage | Shape::GraphPage | Shape::CanonicalPage => (1 << 1) | (1 << 2) | (1 << 6),
        Shape::IdsRow | Shape::GraphRow | Shape::CanonicalRow => (1 << 1) | (1 << 2),
        Shape::QueryScope => (1 << 1) | (1 << 2) | (1 << 3) | (1 << 5),
        Shape::ScopeKey => (1 << 1) | (1 << 2),
        Shape::QueryFacts => (1 << 2) | (1 << 3),
        Shape::CommitId
        | Shape::ChangeId
        | Shape::CommitMatch
        | Shape::ChangeMatch
        | Shape::EdgeTarget => 1 << 1,
        Shape::ChangeTarget => (1 << 1) | (1 << 2),
        Shape::ShortestPrefix => 1 << 1,
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
    let oneof_fields = match shape {
        Shape::FactAnswer => 5,
        Shape::SelectedEdge => 3,
        Shape::CommitPrefix | Shape::ChangePrefix => 3,
        _ => 0,
    };
    if oneof_fields > 0 && counts[1..=oneof_fields].iter().sum::<usize>() != 1 {
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
        (Shape::CloudId, 1)
        | (Shape::CloudDigest, 2)
        | (Shape::CloudReceipt, 3)
        | (Shape::CloudChunk, 2 | 4)
        | (Shape::CloudWorkspace, 2)
        | (Shape::CloudVersion, 2) => scalar(),
        (Shape::CloudId, 2) => text(64),
        (Shape::CloudDigest, 1) => text(32),
        (Shape::CloudDescriptor, 1) => nested(Shape::CloudId),
        (Shape::CloudDescriptor, 2) => nested(Shape::CloudDigest),
        (Shape::CloudReceipt | Shape::CloudSelection | Shape::CloudChunk, 1) => {
            nested(Shape::CloudDescriptor)
        }
        (Shape::CloudReceipt, 2) => text(64),
        (Shape::CloudSelections, 1) => repeated(
            Shape::CloudSelection,
            l.max_batch_items,
            4 * std::mem::size_of::<tinytree_proto::cloud_v1::ObjectSelection>(),
        ),
        (Shape::CloudChunk, 3) => text(l.max_frame_bytes),
        (Shape::CloudWorkspace, 1 | 3) | (Shape::CloudVersion, 1 | 3 | 4) => text(32),
        (Shape::CloudPublication, 1) => text(256),
        (Shape::CloudPublication, 2) => nested(Shape::CloudWorkspace),
        (Shape::CloudPublicationResult, 1) => nested(Shape::CloudPublication),
        (Shape::CloudVersion, 5) => text(l.max_metadata_bytes),
        (Shape::GraphPage, 4) => repeated(
            Shape::GraphRow,
            l.max_batch_items,
            4 * std::mem::size_of::<q::GraphRow>(),
        ),
        (Shape::CanonicalPage, 4) => repeated(
            Shape::CanonicalRow,
            l.max_batch_items,
            4 * std::mem::size_of::<q::GraphRow>(),
        ),
        (Shape::GraphRow | Shape::CanonicalRow, 1) => nested(Shape::CommitId),
        (Shape::GraphRow | Shape::CanonicalRow, 2) => nested(Shape::ChangeId),
        (Shape::GraphRow | Shape::CanonicalRow, 3) => repeated(
            Shape::SelectedEdge,
            l.max_query_edges,
            4 * std::mem::size_of::<q::SelectedEdge>(),
        ),
        (Shape::CanonicalRow, 4) => nested(Shape::ObjectData),
        (Shape::SelectedEdge, 1..=3) => nested(Shape::EdgeTarget),
        (Shape::EdgeTarget, 1) => nested(Shape::CommitId),
        (Shape::IdsPage | Shape::GraphPage | Shape::CanonicalPage, 1) => text(32),
        (Shape::IdsPage | Shape::GraphPage | Shape::CanonicalPage, 2) => text(16),
        (Shape::IdsPage | Shape::GraphPage | Shape::CanonicalPage, 3 | 6) => scalar(),
        (Shape::IdsPage, 4) => repeated(
            Shape::IdsRow,
            l.max_batch_items,
            4 * std::mem::size_of::<q::GraphRow>(),
        ),
        (Shape::IdsPage | Shape::GraphPage | Shape::CanonicalPage, 5) => text(4096),
        (Shape::IdsRow, 1) => nested(Shape::CommitId),
        (Shape::IdsRow, 2) => nested(Shape::ChangeId),
        (Shape::QueryScope, 1) => nested(Shape::ScopeKey),
        (Shape::QueryScope, 2)
        | (Shape::ScopeKey, 2)
        | (Shape::CommitId, 1)
        | (Shape::QueryFacts, 2) => text(32),
        (Shape::QueryScope, 3) | (Shape::ChangeId, 1) | (Shape::QueryFacts, 3) => text(16),
        (Shape::QueryScope, 4 | 5 | 7) | (Shape::ShortestPrefix, 1 | 2) => scalar(),
        (Shape::ScopeKey, 1) => text(128),
        (Shape::QueryFacts, 1) => repeated(
            Shape::FactAnswer,
            l.max_batch_items,
            4 * std::mem::size_of::<q::GraphFactAnswer>(),
        ),
        (Shape::FactAnswer, 1 | 2 | 5) | (Shape::ChangeTarget, 2) => scalar(),
        (Shape::FactAnswer, 3 | 4) => nested(Shape::CommitSet),
        (Shape::CommitSet, 1) => repeated(
            Shape::CommitId,
            l.max_batch_items,
            4 * std::mem::size_of::<q::CommitId>(),
        ),
        (Shape::CommitPrefix, 1 | 3) | (Shape::ChangePrefix, 1 | 3) => nested(Shape::QueryEmpty),
        (Shape::CommitPrefix, 2) => nested(Shape::CommitMatch),
        (Shape::ChangePrefix, 2) => nested(Shape::ChangeMatch),
        (Shape::CommitMatch, 1) | (Shape::ChangeTarget, 1) => nested(Shape::CommitId),
        (Shape::ChangeMatch, 1) => nested(Shape::ChangeId),
        (Shape::ChangeMatch, 2) => repeated(
            Shape::ChangeTarget,
            l.max_batch_items,
            4 * std::mem::size_of::<q::ChangeTarget>(),
        ),
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        pin::Pin,
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        task::{Context, Poll},
    };
    use tonic::{codec::EncodeBody, codegen::Body};

    struct OwnerProbe(Arc<AtomicUsize>);
    impl Drop for OwnerProbe {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    struct Source {
        frame: Option<q::OpenScopeFrame>,
        dropped: Arc<AtomicBool>,
        _owner: PhysicalOwner,
    }
    impl tonic::codegen::tokio_stream::Stream for Source {
        type Item = Result<q::OpenScopeFrame, Status>;
        fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            Poll::Ready(self.frame.take().map(Ok))
        }
    }
    impl Drop for Source {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn physical_encoded_body_owns_admission_after_fused_source_eof() {
        let drops = Arc::new(AtomicUsize::new(0));
        let source_dropped = Arc::new(AtomicBool::new(false));
        let slots = Arc::new(tokio::sync::Semaphore::new(1));
        let slot = slots.clone().acquire_owned().await.unwrap();
        let owner: PhysicalOwner = Arc::new((OwnerProbe(drops.clone()), slot));
        let source = Source {
            frame: Some(q::OpenScopeFrame {
                frame: Some(q::open_scope_frame::Frame::ScopeEnd(q::ScopeEnd::default())),
            }),
            dropped: source_dropped.clone(),
            _owner: owner.clone(),
        };
        let mut codec = BoundedCodec::<q::OpenScopeFrame, q::QueryScope>::new(
            Shape::QueryScope,
            Limits::default(),
            1024,
        )
        .with_owner(owner.clone());
        let encoder = codec.encoder();
        drop(codec);
        drop(owner);
        let mut body = Box::pin(EncodeBody::new_client(
            encoder,
            source,
            Some(CompressionEncoding::Zstd),
            Some(1024),
        ));
        let mut frames = 0;
        while let Some(frame) =
            futures_util::future::poll_fn(|cx| body.as_mut().poll_frame(cx)).await
        {
            assert!(!frame.unwrap().into_data().unwrap().is_empty());
            frames += 1;
        }
        assert!(frames > 0);
        // This is the real Tonic Fuse boundary: the source no longer exists,
        // while the physical body still owns its encoder and admitted buffers.
        assert!(source_dropped.load(Ordering::SeqCst));
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert!(
            slots.clone().try_acquire_owned().is_err(),
            "physical body must keep the sole admitted slot after source EOF"
        );
        drop(body);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert!(slots.try_acquire_owned().is_ok());
    }
}
