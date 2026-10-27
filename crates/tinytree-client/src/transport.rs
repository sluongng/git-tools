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
    channel: Channel,
    inner: tonic::client::Grpc<Channel>,
    limits: Limits,
    decode: usize,
    owner: Option<PhysicalOwner>,
}
impl Client {
    pub fn new(channel: Channel, limits: Limits) -> Self {
        let decode = limits.max_batch_bytes;
        Self {
            inner: tonic::client::Grpc::new(channel.clone()),
            channel,
            limits,
            decode,
            owner: None,
        }
    }
    /// Small replies use no compression: a decoded-byte cap alone does not
    /// bound an advertised compression window. Keep the same channel and owner.
    pub fn uncompressed(mut self) -> Self {
        self.inner =
            tonic::client::Grpc::new(self.channel.clone()).max_decoding_message_size(self.decode);
        self
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
    pub(crate) async fn landing_unary<T: Message + Send + 'static>(
        &mut self,
        request: Request<T>,
        path: &'static str,
        limits: &crate::LandingLimits,
    ) -> Result<Response<tinytree_proto::landing_v1::LandingResponse>, Status> {
        self.limits.max_batch_items = limits.max_rewrites;
        self.limits.max_metadata_bytes = limits.max_conflict_path_bytes;
        self.decode = limits.max_response_bytes;
        self.inner = self
            .inner
            .clone()
            .max_decoding_message_size(self.decode)
            .max_encoding_message_size(16 * 1024);
        self.unary(request, path, Shape::LandingResponse).await
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
        request_bound: usize,
    ) -> Result<Response<U>, Status> {
        self.limits.max_batch_items = limits.max_lookup_items;
        self.limits.max_metadata_bytes = limits.max_manifest_bytes;
        self.limits.max_frame_bytes = limits.max_chunk_bytes;
        self.decode = cap;
        self.inner = self
            .inner
            .clone()
            .max_decoding_message_size(cap)
            .max_encoding_message_size(request_bound);
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
    pub(crate) async fn cloud_representation_read(
        &mut self,
        request: Request<tinytree_proto::cloud_v1::ReadObjectRepresentationRequest>,
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
                    "/tinytree.cloud.v1.WorkspaceCloud/ReadObjectRepresentation",
                ),
                BoundedCodec::<
                    tinytree_proto::cloud_v1::ReadObjectRepresentationRequest,
                    tinytree_proto::cloud_v1::ObjectChunk,
                >::new(Shape::CloudChunk, self.limits.clone(), cap)
                .with_optional_owner(self.owner.clone()),
            )
            .await
    }
    pub(crate) async fn cloud_index_read(
        &mut self,
        request: Request<tinytree_proto::cloud_v1::ReadIndexArtifactRequest>,
        limits: &crate::CloudLimits,
    ) -> Result<Response<Streaming<tinytree_proto::cloud_v1::IndexArtifactChunk>>, Status> {
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
                    "/tinytree.cloud.v1.WorkspaceCloud/ReadIndexArtifact",
                ),
                BoundedCodec::<
                    tinytree_proto::cloud_v1::ReadIndexArtifactRequest,
                    tinytree_proto::cloud_v1::IndexArtifactChunk,
                >::new(Shape::CloudIndexChunk, self.limits.clone(), cap)
                .with_optional_owner(self.owner.clone()),
            )
            .await
    }
    pub(crate) async fn cloud_capture_read(
        &mut self,
        request: Request<tinytree_proto::cloud_v1::ReadCaptureRequest>,
        limits: &crate::CloudLimits,
    ) -> Result<Response<Streaming<tinytree_proto::cloud_v1::CaptureChunk>>, Status> {
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
                    "/tinytree.cloud.v1.WorkspaceCloud/ReadCapture",
                ),
                BoundedCodec::<
                    tinytree_proto::cloud_v1::ReadCaptureRequest,
                    tinytree_proto::cloud_v1::CaptureChunk,
                >::new(Shape::CloudCaptureChunk, self.limits.clone(), cap)
                .with_optional_owner(self.owner.clone()),
            )
            .await
    }
    pub(crate) async fn cloud_native_read(
        &mut self,
        request: Request<tinytree_proto::cloud_v1::ReadNativeClosureRequest>,
    ) -> Result<Response<Streaming<tinytree_proto::cloud_v1::NativeClosureChunk>>, Status> {
        let cap = self.limits.max_frame_bytes.saturating_add(1024);
        self.inner = self
            .inner
            .clone()
            .max_decoding_message_size(cap)
            .max_encoding_message_size(self.limits.max_request_bytes);
        self.inner
            .ready()
            .await
            .map_err(|_| Status::unavailable("native gRPC transport not ready"))?;
        self.inner
            .server_streaming(
                request,
                http::uri::PathAndQuery::from_static(
                    "/tinytree.cloud.v1.WorkspaceCloud/ReadNativeClosure",
                ),
                BoundedCodec::<
                    tinytree_proto::cloud_v1::ReadNativeClosureRequest,
                    tinytree_proto::cloud_v1::NativeClosureChunk,
                >::new(Shape::CloudNativeChunk, self.limits.clone(), cap)
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
    pub async fn open_cloud_scope(
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
                    "/tinytree.query.v1.RepositoryQuery/OpenCloudScope",
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
    pub async fn search(
        &mut self,
        request: Request<q::SearchRequest>,
    ) -> Result<Response<q::SearchResult>, Status> {
        self.unary(
            request,
            "/tinytree.query.v1.RepositoryQuery/Search",
            Shape::Search,
        )
        .await
    }
    pub async fn path_history(
        &mut self,
        request: Request<q::PathHistoryRequest>,
    ) -> Result<Response<q::PathHistoryResult>, Status> {
        self.unary(
            request,
            "/tinytree.query.v1.RepositoryQuery/PathHistory",
            Shape::PathHistory,
        )
        .await
    }
    pub async fn open_annotation_frontier(
        &mut self,
        request: Request<q::OpenAnnotationFrontierRequest>,
    ) -> Result<Response<q::AnnotationFrontier>, Status> {
        self.unary(
            request,
            "/tinytree.query.v1.RepositoryQuery/OpenAnnotationFrontier",
            Shape::AnnotationFrontier,
        )
        .await
    }
    pub async fn open_annotation_frontier_paged(
        &mut self,
        request: Request<q::OpenAnnotationFrontierPagedRequest>,
    ) -> Result<Response<q::AnnotationFrontierPage>, Status> {
        self.unary(
            request,
            "/tinytree.query.v1.RepositoryQuery/OpenAnnotationFrontierPaged",
            Shape::AnnotationFrontierPage,
        )
        .await
    }
    pub async fn continue_annotation_frontier(
        &mut self,
        request: Request<q::ContinueAnnotationFrontierRequest>,
    ) -> Result<Response<q::AnnotationFrontierPage>, Status> {
        self.unary(
            request,
            "/tinytree.query.v1.RepositoryQuery/ContinueAnnotationFrontier",
            Shape::AnnotationFrontierPage,
        )
        .await
    }
    pub async fn read_annotation_input(
        &mut self,
        request: Request<q::ReadAnnotationInputRequest>,
    ) -> Result<Response<q::AnnotationInput>, Status> {
        self.unary(
            request,
            "/tinytree.query.v1.RepositoryQuery/ReadAnnotationInput",
            Shape::AnnotationInput,
        )
        .await
    }
    pub async fn close_annotation_frontier(
        &mut self,
        request: Request<q::CloseAnnotationFrontierRequest>,
    ) -> Result<Response<q::CloseAnnotationFrontierResponse>, Status> {
        self.unary(
            request,
            "/tinytree.query.v1.RepositoryQuery/CloseAnnotationFrontier",
            Shape::QueryEmpty,
        )
        .await
    }
    pub async fn blame(
        &mut self,
        request: Request<q::BlameRequest>,
    ) -> Result<Response<q::BlameResult>, Status> {
        self.unary(
            request,
            "/tinytree.query.v1.RepositoryQuery/Blame",
            Shape::Blame,
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
    pub async fn get_capabilities(
        &mut self,
        r: Request<p::GetCapabilitiesRequest>,
    ) -> Result<Response<p::Capabilities>, Status> {
        self.unary(
            r,
            "/tinytree.v1.Repository/GetCapabilities",
            Shape::Capabilities,
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
    Capabilities,
    FeatureCapability,
    LandingResponse,
    LandingJob,
    LandingCandidate,
    LandingRewrite,
    LandingReceipt,
    LandingReference,
    Search,
    SearchHit,
    PathHistory,
    AnnotationFrontier,
    AnnotationFrontierPage,
    AnnotationUsage,
    AnnotationLimits,
    AnnotationNode,
    AnnotationPathValue,
    AnnotationInput,
    PathHistoryEntry,
    HistoryPathState,
    Blame,
    BlameRange,
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
    CloudWorkspacePage,
    CloudVersionPage,
    CloudVersionSummary,
    CloudPublication,
    CloudPublicationResult,
    CloudVersion,
    CloudNativeAuthority,
    CloudNativeChunk,
    CloudIndexId,
    CloudIndexDescriptor,
    CloudIndexReceipt,
    CloudIndexSelections,
    CloudIndexSelection,
    CloudIndexChunk,
    CloudCaptureDescriptor,
    CloudCaptureChunk,
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
        if (matches!(shape, Shape::GraphRow | Shape::CanonicalRow) && number == 3)
            || (matches!(shape, Shape::AnnotationNode) && number == 2)
        {
            *edges = edges.checked_add(1).ok_or_else(exhausted)?;
            if *edges > limits.max_query_edges {
                return Err(exhausted());
            }
        }
        if wire == 0
            && matches!(
                (shape, number),
                (Shape::Capabilities, 1 | 4)
                    | (Shape::FeatureCapability, 1)
                    | (Shape::AnnotationFrontier, 4 | 8 | 9)
                    | (Shape::AnnotationNode, 3)
                    | (Shape::AnnotationLimits, 1..=10)
                    | (Shape::AnnotationFrontierPage, 7..=11)
                    | (Shape::HistoryPathState, 1)
            )
        {
            let mut offset = 0;
            let value = varint(data, &mut offset)?;
            let valid = match (shape, number) {
                (Shape::Capabilities, _) => value == 1,
                (Shape::AnnotationFrontier, 4) => matches!(value, 1 | 2 | 3),
                (Shape::FeatureCapability, _) => (1..=14).contains(&value),
                (Shape::AnnotationFrontier, 8) => (0..=7).contains(&value),
                (Shape::AnnotationFrontier, 9) => value <= u32::MAX as u64,
                (Shape::AnnotationLimits, _) => value > 0 && value <= usize::MAX as u64,
                (Shape::AnnotationNode, 3) => value <= 1,
                (Shape::AnnotationFrontierPage, 7..=9) => value > 0 && value <= u32::MAX as u64,
                (Shape::AnnotationFrontierPage, 10) => (1..=4).contains(&value),
                (Shape::AnnotationFrontierPage, 11) => value <= u32::MAX as u64,
                (Shape::HistoryPathState, 1) => (1..=4).contains(&value),
                _ => false,
            };
            if !valid {
                return Err(invalid());
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
                    (Shape::FeatureCapability, 2 | 3)
                        | (Shape::AnnotationFrontierPage, 4)
                        | (Shape::LandingJob, 3)
                        | (Shape::FactAnswer, 1 | 2 | 5)
                        | (Shape::ChangeTarget, 2)
                        | (Shape::IdsPage | Shape::GraphPage | Shape::CanonicalPage, 6)
                        | (
                            Shape::CloudChunk | Shape::CloudIndexChunk | Shape::CloudCaptureChunk,
                            4
                        )
                        | (Shape::ShortestPrefix, 3)
                        | (Shape::HistoryPathState, 4)
                        | (Shape::AnnotationNode, 6)
                        | (Shape::CloudNativeChunk, 4)
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
                        (Shape::LandingJob, 1)
                        | (Shape::LandingCandidate, 1 | 2)
                        | (Shape::LandingRewrite, 1 | 2)
                        | (Shape::LandingReceipt, 3)
                        | (Shape::QueryScope, 2 | 8)
                        | (Shape::IdsPage | Shape::GraphPage | Shape::CanonicalPage, 1)
                        | (Shape::ScopeKey, 2)
                        | (Shape::CommitId, 1)
                        | (Shape::QueryFacts, 2)
                        | (Shape::PathHistory | Shape::Blame | Shape::Search, 1)
                        | (Shape::SearchHit, 2) => Some(32),
                        (Shape::LandingRewrite, 3)
                        | (Shape::QueryScope, 3)
                        | (Shape::IdsPage | Shape::GraphPage | Shape::CanonicalPage, 2)
                        | (Shape::ChangeId, 1)
                        | (Shape::QueryFacts, 3)
                        | (Shape::PathHistory | Shape::Blame | Shape::Search, 2) => Some(16),
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
        Shape::Capabilities => (1 << 1) | (1 << 2) | (1 << 4),
        Shape::FeatureCapability => 1 << 1,
        Shape::LandingResponse => (1 << 1) | (1 << 2) | (1 << 3),
        Shape::LandingJob => (1 << 1) | (1 << 2) | (1 << 4) | (1 << 5) | (1 << 6) | (1 << 10),
        Shape::LandingCandidate => (1 << 1) | (1 << 2),
        Shape::LandingRewrite => (1 << 1) | (1 << 2) | (1 << 3),
        Shape::LandingReceipt => (1 << 1) | (1 << 3),
        Shape::LandingReference => (1 << 1) | (1 << 2) | (1 << 3),
        Shape::Search => {
            (1 << 1) | (1 << 2) | (1 << 3) | (1 << 6) | (1 << 8) | (1 << 9) | (1 << 14)
        }
        Shape::SearchHit => (1 << 1) | (1 << 2) | (1 << 3) | (1 << 5) | (1 << 6) | (1 << 9),
        Shape::PathHistory | Shape::Blame => {
            (1 << 1) | (1 << 2) | (1 << 3) | (1 << 4) | (1 << 5) | (1 << 6)
        }
        Shape::PathHistoryEntry => (1 << 1) | (1 << 2) | (1 << 3),
        Shape::AnnotationFrontier => {
            (1 << 1) | (1 << 2) | (1 << 3) | (1 << 4) | (1 << 5) | (1 << 6) | (1 << 7)
        }
        Shape::AnnotationFrontierPage => {
            (1 << 1) | (1 << 6) | (1 << 7) | (1 << 8) | (1 << 9) | (1 << 10)
        }
        Shape::AnnotationLimits => (1 << 11) - 2,
        Shape::AnnotationNode => (1 << 1) | (1 << 5) | (1 << 6),
        Shape::AnnotationPathValue => 1 << 1,
        Shape::AnnotationInput => (1 << 1) | (1 << 2) | (1 << 3) | (1 << 4),
        Shape::HistoryPathState => 1 << 1,
        Shape::CloudId | Shape::CloudIndexId => (1 << 1) | (1 << 2),
        Shape::CloudDigest => 1 << 1,
        Shape::CloudDescriptor
        | Shape::CloudReceipt
        | Shape::CloudPublication
        | Shape::CloudIndexDescriptor
        | Shape::CloudIndexReceipt
        | Shape::CloudCaptureDescriptor => (1 << 1) | (1 << 2),
        Shape::CloudChunk | Shape::CloudWorkspace | Shape::CloudIndexChunk => 1 << 1,
        Shape::CloudVersion | Shape::CloudVersionSummary => (1 << 1) | (1 << 3),
        Shape::CloudVersionPage => 1 << 1,
        Shape::CloudNativeAuthority => (1 << 2) | (1 << 3) | (1 << 4),
        Shape::CloudNativeChunk | Shape::CloudCaptureChunk => (1 << 1) | (1 << 5),
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
        (Shape::LandingResponse, 1) => nested(Shape::LandingJob),
        (Shape::LandingResponse, 2 | 3) => text(128),
        (Shape::LandingJob, 1) => text(32),
        (Shape::LandingJob, 2 | 3 | 4 | 10) => scalar(),
        (Shape::LandingJob, 5) => text(64),
        (Shape::LandingJob, 6) => text(128),
        (Shape::LandingJob, 7) => nested(Shape::LandingCandidate),
        (Shape::LandingJob, 8) => nested(Shape::LandingReceipt),
        (Shape::LandingJob, 9) => text(l.max_metadata_bytes),
        (Shape::LandingCandidate, 1 | 2) => text(32),
        (Shape::LandingCandidate, 3) | (Shape::LandingReceipt, 2) => repeated(
            Shape::LandingRewrite,
            l.max_batch_items,
            std::mem::size_of::<tinytree_proto::landing_v1::Rewrite>(),
        ),
        (Shape::LandingRewrite, 1 | 2) => text(32),
        (Shape::LandingRewrite, 3) => text(16),
        (Shape::LandingReceipt, 1) => nested(Shape::LandingReference),
        (Shape::LandingReceipt, 3) => text(32),
        (Shape::LandingReference, 1) => text(512),
        (Shape::LandingReference, 2) => scalar(),
        (Shape::LandingReference, 3) => nested(Shape::Descriptor),
        (Shape::Search, 1 | 4 | 5) => text(32),
        (Shape::Search, 2) => text(16),
        (Shape::Search, 3) => nested(Shape::CommitId),
        (Shape::Search, 6) => text(1024),
        (Shape::Search, 7) => text(4096),
        (Shape::Search, 8 | 9 | 12 | 13 | 14) => scalar(),
        (Shape::Search, 10) => repeated(
            Shape::SearchHit,
            l.max_batch_items,
            4 * std::mem::size_of::<q::SearchHit>(),
        ),
        (Shape::Search, 11) => text(4096),
        (Shape::SearchHit, 1) => text(4096),
        (Shape::SearchHit, 2) => text(32),
        (Shape::SearchHit, 3..=8) => scalar(),
        (Shape::SearchHit, 9) => text(1280),
        (Shape::AnnotationFrontier, 1 | 2) | (Shape::AnnotationInput, 1) => text(32),
        (Shape::AnnotationFrontier, 3) => text(16),
        (Shape::AnnotationFrontier, 4 | 7 | 8 | 9) => scalar(),
        (Shape::AnnotationFrontier, 5) => repeated(
            Shape::CommitId,
            l.max_graph_objects,
            4 * std::mem::size_of::<q::CommitId>(),
        ),
        (Shape::AnnotationFrontier, 6) => text(4096),
        (Shape::AnnotationFrontier, 10) => repeated(
            Shape::AnnotationNode,
            l.max_graph_objects.saturating_add(1),
            4 * std::mem::size_of::<q::AnnotationNode>(),
        ),
        (Shape::AnnotationFrontier, 11) => nested(Shape::AnnotationLimits),
        (Shape::AnnotationFrontierPage, 1) => nested(Shape::AnnotationFrontier),
        (Shape::AnnotationFrontierPage, 2) => text(32),
        (Shape::AnnotationFrontierPage, 3 | 4 | 5 | 7..=11) => scalar(),
        (Shape::AnnotationFrontierPage, 6) => nested(Shape::AnnotationUsage),
        (Shape::AnnotationFrontierPage, 12) => nested(Shape::Blame),
        (Shape::AnnotationUsage, 1..=4) => scalar(),
        (Shape::AnnotationLimits, 1..=10) => scalar(),
        (Shape::AnnotationNode, 1) | (Shape::AnnotationInput, 2) => nested(Shape::CommitId),
        (Shape::AnnotationNode, 2) => repeated(
            Shape::CommitId,
            l.max_query_edges,
            4 * std::mem::size_of::<q::CommitId>(),
        ),
        (Shape::AnnotationNode, 3 | 4 | 6) => scalar(),
        (Shape::AnnotationNode, 5) | (Shape::AnnotationInput, 3) => {
            nested(Shape::AnnotationPathValue)
        }
        (Shape::AnnotationPathValue, 1) => nested(Shape::HistoryPathState),
        (Shape::AnnotationPathValue, 2) => text(32),
        (Shape::AnnotationInput, 4) => nested(Shape::Descriptor),
        (Shape::AnnotationInput, 5) => {
            text(l.max_file_bytes.min(l.max_batch_bytes as u64) as usize)
        }
        (Shape::PathHistory | Shape::Blame, 1) => text(32),
        (Shape::PathHistory | Shape::Blame, 2) => text(16),
        (Shape::PathHistory | Shape::Blame, 3) => nested(Shape::CommitId),
        (Shape::PathHistory | Shape::Blame, 4) => text(4096),
        (Shape::PathHistory | Shape::Blame, 5 | 6 | 7) => scalar(),
        (Shape::PathHistory, 8) => repeated(
            Shape::PathHistoryEntry,
            l.max_graph_objects,
            4 * std::mem::size_of::<q::PathHistoryEntry>(),
        ),
        (Shape::PathHistory, 9) | (Shape::Blame, 8) => nested(Shape::CommitId),
        (Shape::PathHistoryEntry, 1) => nested(Shape::CommitId),
        (Shape::PathHistoryEntry, 2 | 3) => nested(Shape::HistoryPathState),
        (Shape::HistoryPathState, 1 | 3 | 4) => scalar(),
        (Shape::HistoryPathState, 2) => text(32),
        (Shape::Blame, 9) => nested(Shape::Descriptor),
        (Shape::Blame, 10) => text(l.max_file_bytes.min(l.max_batch_bytes as u64) as usize),
        (Shape::Blame, 11) => repeated(
            Shape::BlameRange,
            l.max_query_edges,
            4 * std::mem::size_of::<q::BlameRange>(),
        ),
        (Shape::BlameRange, 1 | 2 | 4) => scalar(),
        (Shape::BlameRange, 3) => nested(Shape::CommitId),
        (Shape::CloudCaptureDescriptor, 1) | (Shape::CloudCaptureChunk, 5) => text(32),
        (Shape::CloudCaptureDescriptor, 2) => nested(Shape::CloudDigest),
        (Shape::CloudCaptureChunk, 1) => nested(Shape::CloudCaptureDescriptor),
        (Shape::CloudCaptureChunk, 2 | 4) => scalar(),
        (Shape::CloudCaptureChunk, 3) => text(l.max_frame_bytes),
        (Shape::CloudIndexId, 1)
        | (Shape::CloudIndexReceipt, 3)
        | (Shape::CloudIndexChunk, 2 | 4) => scalar(),
        (Shape::CloudIndexId, 2) => text(32),
        (Shape::CloudIndexDescriptor, 1) => nested(Shape::CloudIndexId),
        (Shape::CloudIndexDescriptor, 2) => nested(Shape::CloudDigest),
        (Shape::CloudIndexReceipt | Shape::CloudIndexSelection | Shape::CloudIndexChunk, 1) => {
            nested(Shape::CloudIndexDescriptor)
        }
        (Shape::CloudIndexReceipt, 2) => text(64),
        (Shape::CloudIndexSelections, 1) => repeated(
            Shape::CloudIndexSelection,
            l.max_batch_items,
            4 * std::mem::size_of::<tinytree_proto::cloud_v1::IndexArtifactSelection>(),
        ),
        (Shape::CloudIndexChunk, 3) => text(l.max_frame_bytes),
        (Shape::Capabilities, 1 | 4) | (Shape::FeatureCapability, 1 | 2 | 3) => scalar(),
        (Shape::Capabilities, 2) => text(128),
        (Shape::Capabilities, 3) => repeated(
            Shape::FeatureCapability,
            14,
            4 * std::mem::size_of::<p::FeatureCapability>(),
        ),
        (Shape::CloudWorkspacePage, 1) => repeated(
            Shape::CloudWorkspace,
            128,
            4 * std::mem::size_of::<tinytree_proto::cloud_v1::Workspace>(),
        ),
        (Shape::CloudWorkspacePage, 2) | (Shape::CloudVersionPage, 3) => text(74),
        (Shape::CloudVersionPage, 1) => nested(Shape::CloudWorkspace),
        (Shape::CloudVersionPage, 2) => repeated(
            Shape::CloudVersionSummary,
            128,
            4 * std::mem::size_of::<tinytree_proto::cloud_v1::VersionSummary>(),
        ),
        (Shape::CloudVersionPage, 4) | (Shape::CloudVersionSummary, 2) => scalar(),
        (Shape::CloudVersionSummary, 1 | 3 | 4) => text(32),
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
        (Shape::CloudVersion, 6) => nested(Shape::CloudNativeAuthority),
        (Shape::CloudNativeAuthority, 1) | (Shape::CloudNativeChunk, 2 | 4) => scalar(),
        (Shape::CloudNativeAuthority, 2 | 3) | (Shape::CloudNativeChunk, 5) => text(32),
        (Shape::CloudNativeAuthority, 4) => nested(Shape::CloudDigest),
        (Shape::CloudNativeChunk, 1) => nested(Shape::Descriptor),
        (Shape::CloudNativeChunk, 3) => text(l.max_frame_bytes),
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
        (Shape::QueryScope, 2 | 8)
        | (Shape::ScopeKey, 2)
        | (Shape::CommitId, 1)
        | (Shape::QueryFacts, 2) => text(32),
        (Shape::QueryScope, 3) | (Shape::ChangeId, 1) | (Shape::QueryFacts, 3) => text(16),
        (Shape::QueryScope, 4 | 5 | 7) | (Shape::ShortestPrefix, 1 | 2 | 3) => scalar(),
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
        (Shape::CommitPrefix | Shape::ChangePrefix | Shape::ShortestPrefix, 4) => text(32),
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

    #[test]
    fn capability_wire_rejects_invalid_scalars_and_feature_counts_before_prost() {
        let valid = p::Capabilities {
            schema_version: 1,
            repository_id: "example".into(),
            features: vec![p::FeatureCapability {
                feature: 1,
                supported: true,
                authorized: true,
            }],
            root_readiness: 1,
        }
        .encode_to_vec();
        let check = |bytes: &[u8]| {
            scan(
                bytes,
                Shape::Capabilities,
                &Limits::default(),
                &mut 65536,
                &mut 0,
                0,
            )
        };
        check(&valid).unwrap();
        // Raw nested fields exercise values generated bool/enum setters cannot
        // produce, including a u32-truncating scalar before Prost decoding.
        for (name, nested) in [
            ("nonboolean support", vec![8, 1, 16, 2]),
            ("next unknown feature", vec![8, 15]),
            ("unknown feature", vec![8, 99]),
            (
                "u32 truncating feature",
                vec![8, 0x81, 0x80, 0x80, 0x80, 0x10],
            ),
        ] {
            let mut bytes = vec![8, 1, 18, 1, b'r', 26, nested.len() as u8];
            bytes.extend(nested);
            bytes.extend([32, 1]);
            assert!(check(&bytes).is_err(), "{name}");
        }
        let too_many = p::Capabilities {
            schema_version: 1,
            repository_id: "example".into(),
            features: vec![
                p::FeatureCapability {
                    feature: 1,
                    supported: false,
                    authorized: false
                };
                15
            ],
            root_readiness: 1,
        }
        .encode_to_vec();
        assert!(check(&too_many).is_err());
    }
    #[test]
    fn annotation_wire_rejects_scalar_aliases_before_prost_narrowing() {
        fn scalar(number: u8, mut value: u64) -> Vec<u8> {
            let mut bytes = vec![number << 3];
            while value >= 128 {
                bytes.push((value as u8 & 127) | 128);
                value >>= 7;
            }
            bytes.push(value as u8);
            bytes
        }
        let root = q::AnnotationNode {
            revision: Some(q::CommitId { value: vec![0; 32] }),
            parents: Vec::new(),
            source_layer: 0,
            topological_key: 0,
            value: Some(q::AnnotationPathValue {
                state: Some(q::HistoryPathState {
                    kind: 1,
                    native_id: Vec::new(),
                    size_bytes: 0,
                    executable: false,
                }),
                copy_id: Vec::new(),
            }),
            affects_path: Some(false),
        };
        let frontier = q::AnnotationFrontier {
            limits: None,
            context: vec![1; 32],
            scope_digest: vec![2; 32],
            index_epoch: vec![3; 16],
            version: 1,
            anchors: vec![q::CommitId { value: vec![0; 32] }],
            path: "file".into(),
            expires_at_unix_seconds: 1,
            coverage: 1,
            scanned_commits: 0,
            nodes: vec![root.clone()],
        };
        let check =
            |bytes: &[u8], shape| scan(bytes, shape, &Limits::default(), &mut 65536, &mut 0, 0);
        check(&frontier.encode_to_vec(), Shape::AnnotationFrontier).unwrap();
        for (name, field, value) in [
            ("version alias", 4, (1u64 << 32) + 1),
            ("coverage alias", 8, (1u64 << 32) + 1),
            ("scan-count alias", 9, 1u64 << 32),
        ] {
            let mut base = frontier.clone();
            match field {
                4 => base.version = 0,
                8 => base.coverage = 0,
                9 => base.scanned_commits = 0,
                _ => unreachable!(),
            }
            let mut bytes = base.encode_to_vec();
            bytes.extend(scalar(field, value));
            // These normalize into otherwise valid values under generated Prost.
            let decoded = q::AnnotationFrontier::decode(bytes.as_slice()).unwrap();
            assert_eq!(
                (decoded.version, decoded.coverage, decoded.scanned_commits),
                (1, 1, 0),
                "{name}"
            );
            assert_eq!(
                check(&bytes, Shape::AnnotationFrontier).unwrap_err().code(),
                tonic::Code::DataLoss,
                "{name}"
            );
        }
        let mut bytes = root.encode_to_vec();
        bytes.extend(scalar(3, 1u64 << 32));
        assert_eq!(
            q::AnnotationNode::decode(bytes.as_slice())
                .unwrap()
                .source_layer,
            0
        );
        assert_eq!(
            check(&bytes, Shape::AnnotationNode).unwrap_err().code(),
            tonic::Code::DataLoss
        );
        let mut bytes = q::HistoryPathState {
            kind: 0,
            native_id: vec![4; 32],
            size_bytes: 0,
            executable: false,
        }
        .encode_to_vec();
        bytes.extend(scalar(1, (1u64 << 32) + 2));
        assert_eq!(
            q::HistoryPathState::decode(bytes.as_slice()).unwrap().kind,
            2
        );
        assert_eq!(
            check(&bytes, Shape::HistoryPathState).unwrap_err().code(),
            tonic::Code::DataLoss
        );
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

#[cfg(test)]
mod landing_wire_tests {
    use super::*;
    use http_body::{Body, Frame};
    use std::{
        convert::Infallible,
        pin::Pin,
        sync::atomic::{AtomicUsize, Ordering},
        task::{Context, Poll},
    };
    struct Once(Option<bytes::Bytes>);
    impl Body for Once {
        type Data = bytes::Bytes;
        type Error = Infallible;
        fn poll_frame(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
            Poll::Ready(self.0.take().map(|bytes| Ok(Frame::data(bytes))))
        }
    }
    struct Physical {
        drops: Arc<AtomicUsize>,
        _permit: tokio::sync::OwnedSemaphorePermit,
    }
    impl Drop for Physical {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }
    fn grpc_zstd(window_log: u8, payload: &[u8], rle: Option<usize>) -> bytes::Bytes {
        let mut frame = vec![0x28, 0xb5, 0x2f, 0xfd, 0, (window_log - 10) << 3];
        let size = rle.unwrap_or(payload.len());
        let block = ((size as u32) << 3) | if rle.is_some() { 3 } else { 1 };
        frame.extend_from_slice(&block.to_le_bytes()[..3]);
        frame.extend_from_slice(if rle.is_some() {
            &payload[..1]
        } else {
            payload
        });
        let mut grpc = vec![1];
        grpc.extend_from_slice(&(frame.len() as u32).to_be_bytes());
        grpc.extend(frame);
        grpc.into()
    }
    #[tokio::test]
    async fn landing_zstd_advertised_windows_and_ratio_keep_physical_reservation() {
        use tinytree_proto::landing_v1 as l;
        let valid = l::LandingResponse {
            repository_id: "example".into(),
            idempotency_key: "wire-shape".into(),
            job: Some(l::LandingJob {
                id: vec![1; 32],
                phase: l::Phase::Intent as i32,
                roots_retained: true,
                expires_at_seconds: 1,
                profile_name: "checks".into(),
                profile_version: "v1".into(),
                preparation: l::PreparationAvailability::ProfileConfigured as i32,
                ..Default::default()
            }),
            ..Default::default()
        }
        .encode_to_vec();
        for (name, log, payload, rle, accepted) in [
            ("maximum decoder window", 27, valid.as_slice(), None, true),
            ("over decoder window", 28, valid.as_slice(), None, false),
            (
                "high ratio decoded byte cap",
                27,
                &b"x"[..],
                Some(128 * 1024),
                false,
            ),
        ] {
            let slots = Arc::new(tokio::sync::Semaphore::new(
                crate::client::landing::ZSTD_WORKSPACE_BYTES,
            ));
            let drops = Arc::new(AtomicUsize::new(0));
            let owner = Arc::new(Physical {
                drops: drops.clone(),
                _permit: slots
                    .clone()
                    .acquire_many_owned(crate::client::landing::ZSTD_WORKSPACE_BYTES as u32)
                    .await
                    .unwrap(),
            });
            let mut codec = BoundedCodec::<l::GetLandingRequest, l::LandingResponse>::new(
                Shape::LandingResponse,
                Limits::default(),
                4096,
            )
            .with_owner(owner.clone());
            let decoder = codec.decoder();
            drop(codec);
            drop(owner);
            let mut stream = Streaming::new_response(
                decoder,
                Once(Some(grpc_zstd(log, payload, rle))),
                http::StatusCode::OK,
                Some(CompressionEncoding::Zstd),
                Some(4096),
            );
            let result = stream.message().await;
            assert_eq!(result.is_ok(), accepted, "{name}: {result:?}");
            assert_eq!(
                drops.load(Ordering::SeqCst),
                0,
                "{name}: decoder released owner early"
            );
            assert_eq!(
                slots.available_permits(),
                0,
                "{name}: physical work escaped accounting"
            );
            drop(stream);
            assert_eq!(drops.load(Ordering::SeqCst), 1);
            assert_eq!(
                slots.available_permits(),
                crate::client::landing::ZSTD_WORKSPACE_BYTES
            );
        }
    }
    #[tokio::test]
    async fn uncompressed_small_query_rejects_compression_before_workspace_and_keeps_owner() {
        let reply = q::ShortestPrefixResult {
            kind: q::PrefixKind::CommitPrefix as i32,
            hex_length: 1,
            ..Default::default()
        }
        .encode_to_vec();
        let mut plain = vec![0];
        plain.extend_from_slice(&(reply.len() as u32).to_be_bytes());
        plain.extend_from_slice(&reply);
        for (name, body, accepted) in [
            ("bounded plaintext", bytes::Bytes::from(plain), true),
            (
                "unsolicited compressed reply",
                grpc_zstd(10, &reply, None),
                false,
            ),
            (
                "unsolicited huge decoder window",
                grpc_zstd(28, &reply, None),
                false,
            ),
            (
                "unsolicited high ratio reply",
                grpc_zstd(10, b"x", Some(128 * 1024)),
                false,
            ),
        ] {
            let slots = Arc::new(tokio::sync::Semaphore::new(1));
            let drops = Arc::new(AtomicUsize::new(0));
            let owner = Arc::new(Physical {
                drops: drops.clone(),
                _permit: slots.clone().acquire_owned().await.unwrap(),
            });
            let mut codec = BoundedCodec::<q::ShortestPrefixRequest, q::ShortestPrefixResult>::new(
                Shape::ShortestPrefix,
                Limits::default(),
                1024,
            )
            .with_owner(owner.clone());
            let decoder = codec.decoder();
            drop(codec);
            drop(owner);
            // No negotiated compression: compressed frames are rejected before
            // a decompressor can inspect a peer-controlled window or ratio.
            let mut stream = Streaming::new_response(
                decoder,
                Once(Some(body)),
                http::StatusCode::OK,
                None,
                Some(1024),
            );
            let result = stream.message().await;
            assert_eq!(result.is_ok(), accepted, "{name}: {result:?}");
            assert_eq!(
                drops.load(Ordering::SeqCst),
                0,
                "{name}: early owner release"
            );
            assert_eq!(slots.available_permits(), 0);
            drop(stream);
            assert_eq!(drops.load(Ordering::SeqCst), 1);
            assert_eq!(slots.available_permits(), 1);
        }
    }
    #[test]
    fn landing_nested_rewrites_are_bounded_before_prost() {
        use tinytree_proto::landing_v1 as l;
        let rewrite = l::Rewrite {
            old_commit: vec![1; 32],
            new_commit: vec![2; 32],
            change_id: vec![3; 16],
        };
        let wire = l::LandingResponse {
            repository_id: "example".into(),
            idempotency_key: "wire-shape".into(),
            job: Some(l::LandingJob {
                id: vec![1; 32],
                phase: 2,
                roots_retained: true,
                expires_at_seconds: 1,
                profile_name: "checks".into(),
                profile_version: "v1".into(),
                preparation: 1,
                candidate: Some(l::Candidate {
                    head: vec![2; 32],
                    root_tree: vec![4; 32],
                    rewrites: vec![rewrite; 129],
                }),
                ..Default::default()
            }),
            ..Default::default()
        }
        .encode_to_vec();
        let mut limits = Limits::default();
        limits.max_batch_items = 128;
        let mut budget = 256 * 1024;
        let mut edges = 0;
        assert_eq!(
            scan(
                &wire,
                Shape::LandingResponse,
                &limits,
                &mut budget,
                &mut edges,
                0
            )
            .unwrap_err()
            .code(),
            tonic::Code::ResourceExhausted
        );
    }
}
