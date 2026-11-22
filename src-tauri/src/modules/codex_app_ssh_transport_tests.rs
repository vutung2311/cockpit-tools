use super::*;
use tokio::net::TcpListener;

async fn exchange(response: Value) -> Result<Value, String> {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
        let Message::Text(text) = socket.next().await.unwrap().unwrap() else {
            panic!("expected command");
        };
        let command: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(command["method"], "Runtime.evaluate");
        assert_eq!(command["params"]["awaitPromise"], true);
        assert_eq!(command["params"]["expression"], "fixture reconnect");
        socket
            .send(Message::Text(response.to_string().into()))
            .await
            .unwrap();
    });
    let result = evaluate(&url, "fixture reconnect", std::future::pending()).await;
    server.await.unwrap();
    result
}

#[tokio::test]
async fn reconnect_uses_one_shot_promise_evaluation_without_installing_future_hooks() {
    let value = exchange(json!({"id":1,"result":{"result":{"value":{"ok":true}}}}))
        .await
        .unwrap();
    assert_eq!(value["ok"], true);
}

#[tokio::test]
async fn native_script_exceptions_are_not_reported_as_success() {
    assert_eq!(
        exchange(json!({"id":1,"result":{"exceptionDetails":{"text":"fixture failure"}}}))
            .await
            .unwrap_err(),
        "bridge_failed"
    );
}

#[tokio::test]
async fn destroyed_startup_pages_are_retryable_but_script_errors_are_not() {
    let reason = exchange(json!({"id":1,"error":{
        "code":-32000,"message":"Execution context was destroyed."}}))
    .await
    .unwrap_err();
    assert_eq!(reason, "bridge_closed");
    assert!(retryable(&reason));
    let reason = exchange(json!({"id":1,"result":{
        "exceptionDetails":{"text":"fixture script failure"}}}))
    .await
    .unwrap_err();
    assert!(!retryable(&reason));
}

#[tokio::test]
async fn a_closed_renderer_socket_can_be_replaced_by_a_new_native_page() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
        socket.next().await.unwrap().unwrap();
        socket.close(None).await.unwrap();
    });
    let reason = evaluate(&url, "fixture reconnect", std::future::pending())
        .await
        .unwrap_err();
    server.await.unwrap();
    assert_eq!(reason, "bridge_closed");
    assert!(retryable(&reason));
    let value = exchange(json!({"id":1,"result":{"result":{"value":{"ok":true}}}}))
        .await
        .unwrap();
    assert_eq!(value["ok"], true);
}

#[tokio::test]
async fn superseded_account_cancels_a_pending_native_refresh() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let (sent, received) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
        socket.next().await.unwrap().unwrap();
        sent.send(()).unwrap();
        // Dropping the cancelled request releases the socket as well.
        let _ = socket.next().await;
    });
    let value = evaluate(&url, "fixture reconnect", async {
        received.await.unwrap();
    })
    .await;
    assert_eq!(value.unwrap_err(), "ssh_sync_superseded");
    tokio::time::timeout(std::time::Duration::from_secs(1), server)
        .await
        .unwrap()
        .unwrap();
}
