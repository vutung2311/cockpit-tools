use super::*;
use tokio::net::UnixListener;
use tokio_tungstenite::accept_async;

#[test]
fn matching_email_cannot_hide_a_different_workspace_or_user() {
    let expected = CodexOfficialOAuthIdentity {
        email: "same@example.invalid".into(),
        account_id: Some("workspace-b".into()),
        user_id: Some("user-a".into()),
        organization_id: None,
    };
    let outcome = |workspace, user| {
        compare_workspace(
            &expected,
            &json!({
                "account": {"type":"chatgpt","email":"same@example.invalid"},
                "workspaceRouting":{"chatgptAccountId":workspace},
                "userId":user,
            }),
        )
    };
    assert!(matches!(
        outcome(Some("workspace-b"), Some("user-a")),
        Ok(RestartOutcome::WorkspaceConfirmed)
    ));
    assert!(matches!(
        outcome(None, Some("user-a")),
        Ok(RestartOutcome::WorkspaceUnconfirmed)
    ));
    assert!(matches!(
        outcome(Some("workspace-b"), None),
        Ok(RestartOutcome::WorkspaceUnconfirmed)
    ));
    assert_eq!(
        outcome(Some("workspace-a"), Some("user-a"))
            .err()
            .as_deref(),
        Some(WORKSPACE_MISMATCH)
    );
    assert_eq!(
        outcome(Some("workspace-b"), Some("user-b"))
            .err()
            .as_deref(),
        Some(WORKSPACE_MISMATCH)
    );
}

#[tokio::test]
async fn requires_the_daemon_owner_and_checks_all_loaded_pages() {
    for (status, cursor, owner_matches, allowed) in [
        ("idle", Value::Null, true, true),
        ("active", Value::Null, true, false),
        ("notLoaded", Value::Null, true, false),
        ("idle", json!("more"), true, false),
        ("idle", Value::Null, false, false),
    ] {
        let fixture = tempfile::Builder::new()
            .prefix("cda")
            .tempdir_in("/tmp")
            .unwrap();
        let path = fixture.path().join(super::super::CONTROL_SOCKET);
        std::fs::create_dir(path.parent().unwrap()).unwrap();
        let listener = UnixListener::bind(path).unwrap();
        let rpc = tokio::spawn(async move {
            let (peer, _) = listener.accept().await.unwrap();
            let socket = accept_async(peer).await;
            if !owner_matches {
                assert!(
                    socket.is_err(),
                    "a different PID must receive no WebSocket request"
                );
                return;
            }
            let mut socket = socket.unwrap();
            let mut round = 0;
            while let Some(Ok(Message::Text(message))) = socket.next().await {
                let rpc: Value = serde_json::from_str(&message).unwrap();
                let result = match rpc["method"].as_str().unwrap() {
                    "initialize" => json!({}),
                    "initialized" => continue,
                    "thread/loaded/list" => {
                        round += 1;
                        if round == 1 {
                            assert!(rpc["params"]["cursor"].is_null());
                            json!({"data":["first"], "nextCursor":"more"})
                        } else {
                            assert_eq!(rpc["params"]["cursor"], "more");
                            json!({"data":["second"], "nextCursor":cursor})
                        }
                    }
                    "thread/read" => {
                        assert_eq!(rpc["params"]["includeTurns"], false);
                        let thread = rpc["params"]["threadId"].as_str().unwrap();
                        let state = if thread == "first" { "idle" } else { status };
                        json!({"thread":{"id":thread, "status":{"type":state}}})
                    }
                    method => panic!("non-observation RPC: {method}"),
                };
                socket
                    .send(Message::Text(
                        json!({
                            "id": rpc["id"], "result": result
                        })
                        .to_string()
                        .into(),
                    ))
                    .await
                    .unwrap();
            }
            assert_eq!(round, 2);
        });
        let pid = if owner_matches { std::process::id() } else { 0 };
        assert_eq!(managed_daemon_is_idle(fixture.path(), pid).await, allowed);
        tokio::time::timeout(Duration::from_secs(1), rpc)
            .await
            .unwrap()
            .unwrap();
    }
}
