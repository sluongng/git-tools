use native_broker::LocalEndpoint;
#[test]
fn local_pipe_namespace_rejects_remote_and_ambiguous_names() {
    for name in [
        r"\\host\pipe\broker",
        r"\\?\pipe\broker",
        r"\\.\pipe\",
        r"\\.\pipe\..",
        r"\\.\pipe\nested\broker",
        r"\\.\pipe\broker/other",
        "\\\\.\\pipe\\broker\0suffix",
    ] {
        assert!(
            LocalEndpoint::WindowsNamedPipe(name.into())
                .validate()
                .is_err(),
            "{name:?}"
        );
    }
    assert!(
        LocalEndpoint::WindowsNamedPipe(r"\\.\pipe\native-broker.example_1".into())
            .validate()
            .is_ok()
    );
    assert!(
        LocalEndpoint::WindowsNamedPipe(format!(r"\\.\pipe\{}", "a".repeat(129)))
            .validate()
            .is_err()
    );
}
#[test]
fn remote_pipe_input_fails_without_a_caller_tokio_runtime() {
    let error = futures::executor::block_on(native_broker::BrokerControlClient::connect_local(
        LocalEndpoint::WindowsNamedPipe(r"\\remote\pipe\broker".into()),
        "example",
        native_broker::Config::default(),
    ))
    .unwrap_err();
    assert_eq!(error.code(), native_broker::ControlCode::InvalidArgument);
}
#[cfg(unix)]
#[test]
fn unsupported_native_transport_never_falls_back_to_a_network_listener() {
    let error = futures::executor::block_on(native_broker::BrokerControlClient::connect_local(
        LocalEndpoint::WindowsNamedPipe(r"\\.\pipe\native-broker-example".into()),
        "example",
        native_broker::Config::default(),
    ))
    .unwrap_err();
    assert_eq!(error.code(), native_broker::ControlCode::Unimplemented);
}
