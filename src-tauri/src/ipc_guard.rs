// 应用自定义命令的来源校验。
//
// 应用未声明 AppManifest，Tauri 不会对自定义命令做 ACL 校验，外部页面（OAuth 窗口、
// WorkBuddy 窗口等 WebviewUrl::External）同样能拿到 `__TAURI_INTERNALS__` 并调用
// `export_*_accounts` 等返回凭据的命令。这里只放行加载应用自身前端的 webview。

use tauri::ipc::Invoke;
use tauri::{Runtime, Webview};

fn is_trusted_app_url(url: &tauri::Url) -> bool {
    match url.scheme() {
        "tauri" => url.host_str() == Some("localhost"),
        "http" | "https" => match url.host_str() {
            Some("tauri.localhost") => true,
            Some("localhost") | Some("127.0.0.1") => cfg!(debug_assertions),
            _ => false,
        },
        _ => false,
    }
}

fn is_trusted_webview<R: Runtime>(webview: &Webview<R>) -> bool {
    webview
        .url()
        .map(|url| is_trusted_app_url(&url))
        .unwrap_or(false)
}

pub fn guard<R, F>(handler: F) -> impl Fn(Invoke<R>) -> bool + Send + Sync + 'static
where
    R: Runtime,
    F: Fn(Invoke<R>) -> bool + Send + Sync + 'static,
{
    move |invoke: Invoke<R>| {
        if !is_trusted_webview(invoke.message.webview_ref()) {
            let label = invoke.message.webview_ref().label().to_string();
            let command = invoke.message.command().to_string();
            crate::modules::logger::log_warn(&format!(
                "[IpcGuard] 拒绝非应用来源的命令调用: webview={}, command={}",
                label, command
            ));
            invoke
                .resolver
                .reject(format!("Command {} not allowed from this origin", command));
            return true;
        }
        handler(invoke)
    }
}

#[cfg(test)]
mod tests {
    use super::is_trusted_app_url;

    fn check(raw: &str) -> bool {
        is_trusted_app_url(&tauri::Url::parse(raw).unwrap())
    }

    #[test]
    fn trusts_bundled_app_origins() {
        assert!(check("tauri://localhost/"));
        assert!(check("http://tauri.localhost/index.html"));
        assert!(check("https://tauri.localhost/"));
    }

    #[test]
    fn rejects_remote_origins() {
        assert!(!check("https://www.workbuddy.cn/growth"));
        assert!(!check("https://auth.openai.com/authorize"));
        assert!(!check("https://tauri.localhost.evil.com/"));
        assert!(!check("tauri://evil/"));
        assert!(!check("about:blank"));
    }
}
