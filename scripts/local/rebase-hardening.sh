#!/usr/bin/env bash
# Rebase the local token-safety hardening branch onto upstream, re-check that each
# guard is still wired in, and list upstream changes to security-relevant code so
# they can be reviewed before running the new build.
#
# Usage: scripts/local/rebase-hardening.sh            (defaults to origin/main)
#        UPSTREAM=origin/some-branch scripts/local/rebase-hardening.sh
#        SKIP_CARGO=1 scripts/local/rebase-hardening.sh (skip cargo check/test)
set -euo pipefail

BRANCH="${BRANCH:-hardening/token-safety}"
UPSTREAM="${UPSTREAM:-origin/main}"
REMOTE="${UPSTREAM%%/*}"

cd "$(git rev-parse --show-toplevel)"

if [ -n "$(git status --porcelain)" ]; then
  echo "error: working tree has uncommitted changes; commit or stash them first" >&2
  exit 1
fi

git fetch --quiet "$REMOTE"
git switch --quiet "$BRANCH"

old_base="$(git merge-base HEAD "$UPSTREAM")"
new_base="$(git rev-parse "$UPSTREAM")"

if [ "$old_base" = "$new_base" ]; then
  echo "Already on top of $UPSTREAM ($(git rev-parse --short "$new_base")); nothing to rebase."
else
  echo "Rebasing $BRANCH: $(git rev-parse --short "$old_base") -> $(git rev-parse --short "$new_base")"
  if ! git rebase "$UPSTREAM"; then
    echo >&2
    echo "Rebase stopped on a conflict. Resolve it, then run 'git rebase --continue'," >&2
    echo "or 'git rebase --abort' to go back. Re-run this script afterwards." >&2
    exit 2
  fi
fi

# Each hardening change must still be present after the rebase.
fail=0
require() {
  local file="$1" pattern="$2" what="$3"
  if ! grep -qF -- "$pattern" "$file"; then
    echo "MISSING: $what ($file)" >&2
    fail=1
  fi
}
require src-tauri/src/lib.rs 'ipc_guard::guard(tauri::generate_handler![' "IPC origin guard around invoke_handler"
require src-tauri/src/modules/announcement.rs 'if !REMOTE_SPONSOR_ROUTE_REWRITE_ENABLED' "remote sponsor base-URL rewrite disabled"
require src-tauri/src/modules/external_import.rs 'payload.activate = false;' "deep links cannot auto-activate"
require src-tauri/src/lib.rs '?<redacted>' "deep-link query redacted in logs"
require src-tauri/src/models/codex_local_access.rs 'CodexLocalAccessScope::Localhost' "legacy local-access configs default to localhost"
require src-tauri/src/modules/web_report.rs 'token == "change-this-token"' "web report refuses default token"
require src-tauri/src/modules/websocket.rs 'accept_hdr_async(stream, check_ws_origin)' "WebSocket Origin check"
require src-tauri/src/modules/config.rs 'write_secret_string_atomic(&status_path' "server.json written 0600"
require src-tauri/src/modules/account.rs 'restrict_data_dir_permissions(&data_dir)' "data dir 0700"
require src-tauri/src/modules/kiro_oauth.rs 'normalize_idc_region(Some(region.as_str()))' "Kiro refresh region validated"
require src-tauri/src/modules/webdav_sync.rs 'ensure_secure_transport(&normalized_base_url)' "WebDAV https-only"
require src-tauri/src/modules/diagnostics.rs 'BEARER_SCHEME_RE' "Sentry bearer redaction"
require src/pages/ApiKeyFunPage.tsx 'isApiKeyFunHostUrl(remoteBaseUrl)' "APIKEY.FUN base URL pinned to apikey.fan"
if [ "$fail" -ne 0 ]; then
  echo "error: some hardening changes were lost in the rebase; restore them before building" >&2
  exit 3
fi
echo "All hardening changes present."

if [ "$old_base" != "$new_base" ]; then
  echo
  echo "== Upstream changes to security-relevant code since last rebase =="
  git --no-pager diff --stat "$old_base" "$new_base" -- \
    src-tauri/src/lib.rs \
    src-tauri/build.rs \
    src-tauri/tauri.conf.json \
    src-tauri/capabilities \
    src-tauri/src/modules/announcement.rs \
    src-tauri/src/modules/sponsor_route_sync.rs \
    src-tauri/src/modules/remote_config.rs \
    src-tauri/src/modules/external_import.rs \
    src-tauri/src/modules/websocket.rs \
    src-tauri/src/modules/web_report.rs \
    src-tauri/src/modules/diagnostics.rs \
    src-tauri/src/modules/webdav_sync.rs \
    src-tauri/src/modules/secure_account_storage.rs \
    announcements.json remote-config.json \
    .github/workflows package.json
  echo
  echo "== New upstream lines that open remote pages, bind all interfaces, or fetch remote config =="
  git --no-pager diff -U0 "$old_base" "$new_base" -- src-tauri/src crates sidecars/cockpit-cliproxy/*.go src \
    | grep -nE '^\+.*(WebviewUrl::External|0\.0\.0\.0|raw\.githubusercontent\.com|baseUrlAliases|base_url_aliases|option_env!|dangerouslySetInnerHTML)' \
    || echo "(none)"
fi

if [ "${SKIP_CARGO:-0}" != "1" ]; then
  echo
  echo "== cargo check + hardening tests =="
  cargo check -p cockpit-tools --tests
  cargo test -p cockpit-tools --lib -- ipc_guard origin_tests webdav diagnostics external_import
fi

echo
echo "Done. $BRANCH is on top of $UPSTREAM."
