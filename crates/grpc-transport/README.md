# grpc-transport · library

Independent operation/idle deadlines for gRPC response bodies, including peers
that stop consuming HTTP/2 data. `ResponseBudget::new` transfers owned resource
guards from response extensions to the body and releases them on completion,
cancellation or timeout. Shutdown does not force-close unrelated connections.
