use super::*;

#[tokio::test]
async fn model_refresh_targets_the_new_instance_after_registration() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
        for providers in [
            json!([]),
            json!([{
                "instanceId": "parallel-ag", "enabled": true, "installed": true, "status": "ready"
            }]),
        ] {
            let frame = socket.next().await.unwrap().unwrap().into_text().unwrap();
            let request: Value = serde_json::from_str(&frame).unwrap();
            assert_eq!(request["tag"], "server.refreshProviders");
            assert_eq!(request["payload"]["instanceId"], "parallel-ag");
            assert_eq!(request["payload"]["refreshModels"], true);
            let response = json!({ "_tag": "Exit", "requestId": request["id"],
                "exit": { "_tag": "Success", "value": { "providers": providers } } });
            socket
                .send(Message::Text(response.to_string().into()))
                .await
                .unwrap();
        }
    });
    let (socket, _) = connect_async(format!("ws://{address}")).await.unwrap();
    let mut control = ProviderControl {
        socket,
        sequence: 0,
    };
    control.refresh_models("parallel-ag").await.unwrap();
    server.await.unwrap();
}

#[test]
fn reload_keeps_the_enabled_provider_and_other_environment_settings() {
    let preserved = json!({"name": "USER_SETTING", "value": "example", "sensitive": false});
    let entry = json!({
        "driver": "codex", "displayName": "Work", "enabled": true,
        "config": { "homePath": "example-home" },
        "environment": [preserved, { "name": GENERATION_ENV, "value": "old" }],
    });
    let reloaded = with_generation(entry.clone(), "new").unwrap();
    for field in ["driver", "displayName", "enabled", "config"] {
        assert_eq!(reloaded[field], entry[field]);
    }
    assert_eq!(
        reloaded["environment"],
        json!([
            preserved, { "name": GENERATION_ENV, "value": "new", "sensitive": false },
        ])
    );
    assert!(has_generation(&reloaded, "new"));
    assert!(!has_generation(&reloaded, "old"));
}
