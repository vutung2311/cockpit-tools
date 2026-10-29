//! Bounded, read-only dialog transport. The regular injection poll remains a
//! fallback, but clicks and completed reads need not wait for its next tick.
use super::*;
use std::sync::Arc;
use tokio::sync::mpsc;

const BINDING: &str = "__cockpitUsageRequest";
const TARGET_LIMIT: usize = 8;
const PAYLOAD_LIMIT: usize = 2048;

#[derive(Clone, Copy)]
struct ConnectionTiming {
    timeout: Duration,
    heartbeat: Duration,
    health: Duration,
}

impl Default for ConnectionTiming {
    fn default() -> Self {
        Self {
            timeout: CDP_CONNECT_TIMEOUT,
            heartbeat: Duration::from_secs(5),
            health: Duration::from_millis(250),
        }
    }
}

enum Event {
    Targets(Vec<String>),
    Fallback(Vec<dialog_data::DialogQuery>),
    Ready(String, u64),
    Query(String, u64, Option<dialog_data::DialogQuery>),
    Closed(String, u64),
}

pub(super) struct LiveDialogBridge {
    events: mpsc::Sender<Event>,
    ready: Arc<Mutex<HashSet<String>>>,
    latest: Arc<Mutex<Value>>,
    task: tokio::task::JoinHandle<()>,
}

impl LiveDialogBridge {
    pub(super) fn new(profile: &Path) -> Self {
        Self::with_reader(
            profile,
            Arc::new(dialog_data::read_dialog_data),
            ConnectionTiming::default(),
        )
    }

    fn with_reader(
        profile: &Path,
        reader: InjectionDialogReader,
        timing: ConnectionTiming,
    ) -> Self {
        let (events, receiver) = mpsc::channel(32);
        let ready = Arc::new(Mutex::new(HashSet::new()));
        let latest = Arc::new(Mutex::new(json!({})));
        let task = tokio::spawn(run(
            profile.to_path_buf(),
            events.clone(),
            receiver,
            ready.clone(),
            latest.clone(),
            reader,
            timing,
        ));
        Self {
            events,
            ready,
            latest,
            task,
        }
    }

    pub(super) fn collect(&self) -> Value {
        self.latest
            .lock()
            .map(|state| state.clone())
            .unwrap_or_else(|_| json!({}))
    }

    pub(super) fn request(&self, queries: Vec<dialog_data::DialogQuery>) {
        let _ = self.events.try_send(Event::Fallback(queries));
    }

    pub(super) fn reconcile(&self, targets: &[CdpTarget], port: u16) {
        let urls = targets
            .iter()
            .filter(|target| is_codex_app_target(target))
            .filter_map(|target| target.websocket_url.as_ref())
            .filter(|url| is_safe_cdp_websocket_url(url, port))
            .take(TARGET_LIMIT)
            .cloned()
            .collect();
        let _ = self.events.try_send(Event::Targets(urls));
    }

    pub(super) fn ready(&self, target: &CdpTarget) -> bool {
        target
            .websocket_url
            .as_ref()
            .is_some_and(|url| self.ready.lock().is_ok_and(|ready| ready.contains(url)))
    }
}

impl Drop for LiveDialogBridge {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct Target {
    generation: u64,
    replies: mpsc::Sender<Value>,
    abort: tokio::task::AbortHandle,
    query: Option<dialog_data::DialogQuery>,
    delivered: Option<Value>,
}

async fn run(
    profile: PathBuf,
    events: mpsc::Sender<Event>,
    mut receiver: mpsc::Receiver<Event>,
    ready: Arc<Mutex<HashSet<String>>>,
    latest: Arc<Mutex<Value>>,
    reader: InjectionDialogReader,
    timing: ConnectionTiming,
) {
    let mut sockets = JoinSet::new();
    let mut targets: HashMap<String, Target> = HashMap::new();
    let mut poller = InjectionDialogPoller::with_reader(&profile, reader);
    let mut fallback = Vec::new();
    let mut generation = 0u64;
    let mut tick = tokio::time::interval(Duration::from_millis(75));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            event = receiver.recv() => match event {
                None => break,
                Some(Event::Fallback(queries)) => fallback = queries,
                Some(Event::Targets(urls)) => {
                    targets.retain(|url, target| {
                        if urls.contains(url) { return true; }
                        target.abort.abort();
                        if let Ok(mut ready) = ready.lock() { ready.remove(url); }
                        false
                    });
                    for url in urls {
                        if targets.contains_key(&url) || targets.len() >= TARGET_LIMIT { continue; }
                        generation = generation.wrapping_add(1);
                        let current = generation;
                        let (replies, inbox) = mpsc::channel(2);
                        let target_url = url.clone();
                        let sender = events.clone();
                        let abort = sockets.spawn(async move {
                            connection(&target_url, current, sender.clone(), inbox, timing).await;
                            let _ = sender.send(Event::Closed(target_url, current)).await;
                        });
                        targets.insert(url, Target {generation:current,replies,abort,query:None,delivered:None});
                    }
                }
                Some(Event::Ready(url, generation)) => {
                    if targets.get(&url).is_some_and(|target| target.generation == generation) {
                        if let Ok(mut ready) = ready.lock() { ready.insert(url); }
                    }
                }
                Some(Event::Closed(url, generation)) => {
                    if targets.get(&url).is_some_and(|target| target.generation == generation) {
                        targets.remove(&url);
                        if let Ok(mut ready) = ready.lock() { ready.remove(&url); }
                    }
                }
                Some(Event::Query(url, generation, query)) => {
                    if let Some(target) = targets.get_mut(&url).filter(|target| target.generation == generation) {
                        if target.query != query { target.delivered = None; }
                        target.query = query;
                    }
                }
            },
            _ = tick.tick(), if !poller.tasks.is_empty() => {},
        }
        while sockets.try_join_next().is_some() {}
        // Both transports share one poller, including deduplication, deadlines
        // and the two physical blocking-worker slots during handovers.
        poller.request(
            targets
                .values()
                .filter_map(|target| target.query.clone())
                .chain(fallback.iter().cloned())
                .collect(),
        );
        let state = poller.collect();
        if let Ok(mut latest) = latest.lock() {
            if *latest != state {
                *latest = state.clone();
            }
        }
        for target in targets.values_mut() {
            let Some(query) = &target.query else {
                continue;
            };
            let Some(value) = state.get(&query.request_token) else {
                continue;
            };
            if value.get("loading").and_then(Value::as_bool) == Some(true)
                || target.delivered.as_ref() == Some(value)
            {
                continue;
            }
            if target.replies.try_send(value.clone()).is_ok() {
                target.delivered = Some(value.clone());
            }
        }
    }
}

fn binding_query(value: &Value) -> Option<Option<dialog_data::DialogQuery>> {
    if value.get("method")?.as_str()? != "Runtime.bindingCalled"
        || value.pointer("/params/name")?.as_str()? != BINDING
    {
        return None;
    }
    let payload = value.pointer("/params/payload")?.as_str()?;
    if payload.len() > PAYLOAD_LIMIT {
        return None;
    }
    let query: Value = serde_json::from_str(payload).ok()?;
    if query.is_null() {
        return Some(None);
    }
    dialog_data::parse_query(&query).map(Some)
}

fn delivery_script(value: &Value) -> String {
    // Serialize data as a JS value, never interpolate it into a quoted string.
    format!("(()=>{{const r=window.__cockpitCodexInjection;if(!r||!r.quotaDetailsOpen)return;const v={value};if(r.dialogState?.pending?.requestToken!==v.requestToken)return;r.usageState=r.usageState||{{}};r.usageState.__dialog={{[v.requestToken]:v}};r.render?.();}})()")
}

async fn connection(
    url: &str,
    generation: u64,
    events: mpsc::Sender<Event>,
    mut replies: mpsc::Receiver<Value>,
    timing: ConnectionTiming,
) {
    let Ok(Ok((mut socket, _))) = timeout(timing.timeout, connect_async(url)).await else {
        return;
    };
    for command in [
        json!({"id":1,"method":"Runtime.enable"}),
        json!({"id":2,"method":"Runtime.addBinding","params":{"name":BINDING}}),
    ] {
        if !matches!(
            timeout(
                timing.timeout,
                socket.send(Message::Text(command.to_string().into()))
            )
            .await,
            Ok(Ok(()))
        ) {
            return;
        }
    }
    let mut connected = false;
    let mut setup_timeout = Box::pin(tokio::time::sleep(timing.timeout));
    let mut command_id = 3u64;
    let mut acknowledgements: HashMap<u64, Instant> = HashMap::new();
    let mut heartbeat = tokio::time::interval(timing.heartbeat);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut health = tokio::time::interval(timing.health);
    loop {
        tokio::select! {
            _ = &mut setup_timeout, if !connected => return,
            _ = health.tick() => {
                if acknowledgements.values().any(|deadline|Instant::now()>=*deadline) { return; }
            }
            _ = heartbeat.tick(), if connected => {
                let command = json!({"id":command_id,"method":"Runtime.evaluate","params":{"expression":"true","returnByValue":true}});
                acknowledgements.insert(command_id,Instant::now()+timing.timeout);
                command_id=command_id.wrapping_add(1);
                if !matches!(timeout(timing.timeout,socket.send(Message::Text(command.to_string().into()))).await,Ok(Ok(()))) { return; }
            }
            response = replies.recv() => {
                let Some(response) = response else { return; };
                let command = json!({"id":command_id,"method":"Runtime.evaluate","params":{"expression":delivery_script(&response),"returnByValue":true}});
                acknowledgements.insert(command_id,Instant::now()+timing.timeout);
                command_id = command_id.wrapping_add(1);
                if !matches!(timeout(timing.timeout, socket.send(Message::Text(command.to_string().into()))).await, Ok(Ok(()))) { return; }
            }
            message = socket.next() => {
                let Some(Ok(message)) = message else { return; };
                match message {
                    Message::Ping(payload) => { if !matches!(timeout(timing.timeout,socket.send(Message::Pong(payload))).await,Ok(Ok(()))) { return; } }
                    Message::Close(_) => return,
                    Message::Text(text) if text.len() <= 65536 => {
                        let Ok(value) = serde_json::from_str::<Value>(&text) else { continue; };
                        if let Some(id)=value.get("id").and_then(Value::as_u64) {
                            if acknowledgements.remove(&id).is_some()
                                && (value.get("error").is_some() || value.pointer("/result/exceptionDetails").is_some()) { return; }
                        }
                        if value.get("id").and_then(Value::as_u64) == Some(2) {
                            if value.get("error").is_some() { return; }
                            connected = true;
                            let _ = events.send(Event::Ready(url.into(), generation)).await;
                            // Pick up an already-open dialog when the connection starts/reconnects.
                            let script = format!("(()=>{{const r=window.__cockpitCodexInjection;const q=r?.dialogState?.pending;if(r?.quotaDetailsOpen&&q)window.{BINDING}(JSON.stringify(q));}})()");
                            acknowledgements.insert(0,Instant::now()+timing.timeout);
                            if !matches!(timeout(timing.timeout,socket.send(Message::Text(json!({"id":0,"method":"Runtime.evaluate","params":{"expression":script}}).to_string().into()))).await,Ok(Ok(()))) { return; }
                        } else if let Some(query) = binding_query(&value) {
                            if events.try_send(Event::Query(url.into(), generation, query)).is_err() { return; }
                        }
                    }
                    _ => {},
                }
            }
        }
    }
}


#[cfg(test)]
#[path = "codex_app_injection_live_tests.rs"]
mod tests;
