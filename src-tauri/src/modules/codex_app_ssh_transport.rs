//! Evaluate once and await the promise; never register a navigation hook.
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio_tungstenite::{connect_async, tungstenite::Message};

pub(super) fn retryable(reason: &str) -> bool {
    matches!(reason, "bridge_unavailable" | "bridge_closed")
}

pub(super) async fn evaluate(
    url: &str,
    script: &str,
    cancelled: impl std::future::Future<Output = ()>,
) -> Result<Value, String> {
    tokio::select! {
        result = evaluate_once(url, script) => result,
        _ = cancelled => Err("ssh_sync_superseded".into()),
    }
}

async fn evaluate_once(url: &str, script: &str) -> Result<Value, String> {
    let (mut socket, _) = connect_async(url).await.map_err(|_| "bridge_unavailable")?;
    socket
        .send(Message::Text(
            json!({"id":1,"method":"Runtime.evaluate","params":{
                "expression":script,"returnByValue":true,"awaitPromise":true
            }})
            .to_string()
            .into(),
        ))
        .await
        .map_err(|_| "bridge_failed")?;
    while let Some(message) = socket.next().await {
        let Ok(Message::Text(text)) = message else {
            continue;
        };
        let value: Value = serde_json::from_str(&text).map_err(|_| "bridge_invalid_response")?;
        if value["id"] != 1 {
            continue;
        }
        if value["error"]["message"].as_str().is_some_and(|message| {
            message.contains("Execution context was destroyed")
                || message.contains("Cannot find context")
        }) {
            return Err("bridge_closed".into());
        }
        if value.get("error").is_some() || value.pointer("/result/exceptionDetails").is_some() {
            return Err("bridge_failed".into());
        }
        return value
            .pointer("/result/result/value")
            .cloned()
            .ok_or_else(|| "bridge_invalid_response".into());
    }
    Err("bridge_closed".into())
}

#[cfg(test)]
#[path = "codex_app_ssh_transport_tests.rs"]
mod tests;
