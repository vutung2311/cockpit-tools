    use super::*;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Condvar,
    };

    enum Control {
        Event(Value),
        Silent,
        Error,
        Exception,
        Close,
    }

    struct MockCdp {
        port: u16,
        url: String,
        peers: Arc<Mutex<Vec<mpsc::Sender<Control>>>>,
        commands: mpsc::UnboundedReceiver<(usize, Value)>,
        task: tokio::task::JoinHandle<()>,
    }

    impl Drop for MockCdp {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    impl MockCdp {
        async fn new() -> Self {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = listener.local_addr().unwrap().port();
            let peers = Arc::new(Mutex::new(Vec::new()));
            let server_peers = peers.clone();
            let (commands, receiver) = mpsc::unbounded_channel();
            let task = tokio::spawn(async move {
                let mut connections = JoinSet::new();
                loop {
                    tokio::select! {
                        accepted=listener.accept()=> {
                            let Ok((stream,_))=accepted else {return;};
                            let peers=server_peers.clone();
                            let commands=commands.clone();
                            connections.spawn(async move {
                                let mut socket=tokio_tungstenite::accept_async(stream).await.unwrap();
                                let (sender,mut controls)=mpsc::channel(16);
                                let index={let mut peers=peers.lock().unwrap();let index=peers.len();peers.push(sender);index};
                                let mut silent=false;
                                let mut fail_next=None;
                                loop {
                                    tokio::select! {
                                        control=controls.recv()=>match control {
                                            Some(Control::Event(value))=>if socket.send(Message::Text(value.to_string().into())).await.is_err(){return;},
                                            Some(Control::Silent)=>silent=true,
                                            Some(Control::Error)=>fail_next=Some(false),
                                            Some(Control::Exception)=>fail_next=Some(true),
                                            Some(Control::Close)|None=>{let _=socket.close(None).await;return;},
                                        },
                                        message=socket.next()=>{
                                            let Some(Ok(Message::Text(text)))=message else {return;};
                                            let value:Value=serde_json::from_str(&text).unwrap();
                                            let _=commands.send((index,value.clone()));
                                            if silent {continue;}
                                            let Some(id)=value["id"].as_u64() else {continue;};
                                            let failure=if value["method"]=="Runtime.evaluate" {fail_next.take()} else {None};
                                            let reply=match failure {
                                                Some(false)=>json!({"id":id,"error":{"message":"fixture protocol error"}}),
                                                Some(true)=>json!({"id":id,"result":{"exceptionDetails":{"text":"fixture exception"}}}),
                                                None=>json!({"id":id,"result":{"result":{"type":"boolean","value":true}}}),
                                            };
                                            if socket.send(Message::Text(reply.to_string().into())).await.is_err(){return;}
                                        }
                                    }
                                }
                            });
                        },
                        _=connections.join_next(), if !connections.is_empty()=>{},
                    }
                }
            });
            Self {
                port,
                url: format!("ws://127.0.0.1:{port}/devtools/page/fixture"),
                peers,
                commands: receiver,
                task,
            }
        }

        fn target(&self) -> CdpTarget {
            CdpTarget {
                target_id: "fixture".into(),
                target_type: "page".into(),
                url: "app://-/fixture".into(),
                websocket_url: Some(self.url.clone()),
            }
        }

        async fn send(&self, peer: usize, control: Control) {
            let sender = self
                .peers
                .lock()
                .unwrap()
                .get(peer)
                .cloned()
                .expect("mock connection exists");
            timeout(Duration::from_secs(1), sender.send(control))
                .await
                .unwrap()
                .unwrap();
        }

        async fn query(&self, peer: usize, query: &dialog_data::DialogQuery) {
            self.send(
                peer,
                Control::Event(json!({"method":"Runtime.bindingCalled","params":{
                    "name":BINDING,"payload":serde_json::to_string(query).unwrap()
                }})),
            )
            .await;
        }

        async fn delivery(&mut self, token: &str) -> Value {
            timeout(Duration::from_secs(2), async {
                loop {
                    let (_, command) = self.commands.recv().await.expect("mock server stays alive");
                    if command
                        .pointer("/params/expression")
                        .and_then(Value::as_str)
                        .is_some_and(|script| script.contains("const v=") && script.contains(token))
                    {
                        return command;
                    }
                }
            })
            .await
            .expect("live result must arrive without fallback polling")
        }
    }

    fn timing() -> ConnectionTiming {
        ConnectionTiming {
            timeout: Duration::from_millis(300),
            heartbeat: Duration::from_millis(100),
            health: Duration::from_millis(10),
        }
    }

    fn query(token: &str) -> dialog_data::DialogQuery {
        dialog_data::parse_query(&json!({"requestToken":token,"threadId":null,
            "scope":"service","period":"today","cursor":null,"view":"logs","binding":null}))
        .unwrap()
    }

    fn result(query: &dialog_data::DialogQuery) -> Value {
        json!({"requestToken":query.request_token,"threadId":query.thread_id,"binding":query.binding,
            "scope":query.scope,"period":query.period,"cursor":query.cursor,"view":query.view,
            "available":true,"requests":{"rows":[{"id":"1","model":"fixture-model"}],"nextCursor":null}})
    }

    async fn ready(bridge: &LiveDialogBridge, target: &CdpTarget, expected: bool) {
        timeout(Duration::from_secs(2), async {
            while bridge.ready(target) != expected {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .expect("live transport readiness must settle promptly");
    }

    #[tokio::test]
    async fn live_mock_cdp_delivers_click_results_without_waiting_for_poll_tick() {
        let mut server = MockCdp::new().await;
        let bridge = LiveDialogBridge::with_reader(
            Path::new("unused-fixture"),
            Arc::new(|_, query| Ok(result(query))),
            timing(),
        );
        let target = server.target();
        bridge.reconcile(&[target], server.port);
        ready(&bridge, &server.target(), true).await;
        let query = query("direct-click");
        server.query(0, &query).await;
        let delivery = server.delivery(&query.request_token).await;
        assert_eq!(delivery["method"], "Runtime.evaluate");
        assert!(delivery["params"]["expression"]
            .as_str()
            .unwrap()
            .contains("fixture-model"));
        assert_eq!(bridge.collect()["direct-click"], result(&query));
    }

    #[tokio::test]
    async fn live_and_fallback_deduplicate_tokens_and_share_two_physical_workers() {
        let mut server = MockCdp::new().await;
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let running = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let calls = Arc::new(Mutex::new(HashMap::<String, usize>::new()));
        let (reader_gate, reader_running, reader_peak, reader_calls) =
            (gate.clone(), running.clone(), peak.clone(), calls.clone());
        let reader: InjectionDialogReader = Arc::new(move |_, query| {
            let current = reader_running.fetch_add(1, Ordering::SeqCst) + 1;
            reader_peak.fetch_max(current, Ordering::SeqCst);
            *reader_calls
                .lock()
                .unwrap()
                .entry(query.request_token.clone())
                .or_default() += 1;
            let (lock, condition) = &*reader_gate;
            let released = lock.lock().unwrap();
            let (released, _) = condition
                .wait_timeout_while(released, Duration::from_secs(2), |released| !*released)
                .unwrap();
            reader_running.fetch_sub(1, Ordering::SeqCst);
            if !*released {
                return Err("fixture gate timed out".into());
            }
            Ok(result(query))
        });
        let bridge = LiveDialogBridge::with_reader(Path::new("unused-fixture"), reader, timing());
        bridge.reconcile(&[server.target()], server.port);
        ready(&bridge, &server.target(), true).await;
        server.query(0, &query("shared-token")).await;
        timeout(Duration::from_secs(1), async {
            while running.load(Ordering::SeqCst) < 1 {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .unwrap();
        bridge.request(vec![
            query("shared-token"),
            query("fallback-2"),
            query("queued-3"),
        ]);
        timeout(Duration::from_secs(1), async {
            while running.load(Ordering::SeqCst) < 2 {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(calls.lock().unwrap()["shared-token"], 1);
        bridge.request(vec![query("replacement-4")]);
        tokio::time::sleep(Duration::from_millis(25)).await;
        assert_eq!(peak.load(Ordering::SeqCst), 2);
        assert!(!calls.lock().unwrap().contains_key("replacement-4"));
        *gate.0.lock().unwrap() = true;
        gate.1.notify_all();
        server.delivery("shared-token").await;
        timeout(Duration::from_secs(1), async {
            loop {
                if bridge
                    .collect()
                    .get("replacement-4")
                    .is_some_and(|value| value["available"] == true)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(peak.load(Ordering::SeqCst), 2);
        assert_eq!(calls.lock().unwrap()["shared-token"], 1);
        assert!(!calls.lock().unwrap().contains_key("queued-3"));
    }

    #[tokio::test]
    async fn live_silent_connection_times_out_and_fallback_remains_usable() {
        let mut server = MockCdp::new().await;
        let bridge = LiveDialogBridge::with_reader(
            Path::new("unused-fixture"),
            Arc::new(|_, query| Ok(result(query))),
            timing(),
        );
        bridge.reconcile(&[server.target()], server.port);
        ready(&bridge, &server.target(), true).await;
        server.send(0, Control::Silent).await;
        server.query(0, &query("unacknowledged")).await;
        server.delivery("unacknowledged").await;
        ready(&bridge, &server.target(), false).await;
        bridge.request(vec![query("fallback-after-timeout")]);
        timeout(Duration::from_secs(1), async {
            loop {
                if bridge
                    .collect()
                    .get("fallback-after-timeout")
                    .is_some_and(|value| value["available"] == true)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn live_protocol_errors_and_exceptions_clear_ready_before_reconnect() {
        for control in [Control::Error, Control::Exception] {
            let server = MockCdp::new().await;
            let bridge = LiveDialogBridge::with_reader(
                Path::new("unused-fixture"),
                Arc::new(|_, query| Ok(result(query))),
                timing(),
            );
            bridge.reconcile(&[server.target()], server.port);
            ready(&bridge, &server.target(), true).await;
            server.send(0, control).await;
            server.query(0, &query("failed-delivery")).await;
            ready(&bridge, &server.target(), false).await;
        }
    }

    #[tokio::test]
    async fn live_reconnect_rejects_old_generation_queries_and_close_events() {
        let mut server = MockCdp::new().await;
        let calls = Arc::new(Mutex::new(Vec::<String>::new()));
        let reader_calls = calls.clone();
        let bridge = LiveDialogBridge::with_reader(
            Path::new("unused-fixture"),
            Arc::new(move |_, query| {
                reader_calls
                    .lock()
                    .unwrap()
                    .push(query.request_token.clone());
                Ok(result(query))
            }),
            timing(),
        );
        bridge.reconcile(&[server.target()], server.port);
        ready(&bridge, &server.target(), true).await;
        server.send(0, Control::Close).await;
        ready(&bridge, &server.target(), false).await;
        bridge.reconcile(&[server.target()], server.port);
        ready(&bridge, &server.target(), true).await;
        for event in [
            Event::Query(server.url.clone(), 1, Some(query("stale-generation"))),
            Event::Ready(server.url.clone(), 1),
            Event::Closed(server.url.clone(), 1),
        ] {
            bridge.events.send(event).await.unwrap();
        }
        server.query(1, &query("new-generation")).await;
        server.delivery("new-generation").await;
        assert!(bridge.ready(&server.target()));
        assert!(!calls
            .lock()
            .unwrap()
            .iter()
            .any(|token| token == "stale-generation"));
        assert!(bridge.collect().get("stale-generation").is_none());
    }

    #[test]
    fn live_queries_are_bounded_validated_and_cannot_request_paths() {
        let event = |payload: Value| json!({"method":"Runtime.bindingCalled","params":{"name":BINDING,"payload":payload.to_string()}});
        assert!(matches!(binding_query(&event(Value::Null)), Some(None)));
        let valid = json!({"requestToken":"live-1","threadId":null,"scope":"service","period":"today","cursor":null,"view":"logs","binding":null});
        assert!(matches!(
            binding_query(&event(valid.clone())),
            Some(Some(_))
        ));
        let mut invalid = valid;
        invalid["threadId"] = json!("../../auth.json");
        assert!(binding_query(&event(invalid)).is_none());
        assert!(binding_query(&event(json!("x".repeat(PAYLOAD_LIMIT)))).is_none());
        assert!(binding_query(&json!({"method":"Other","params":{}})).is_none());
    }

    #[test]
    fn live_results_require_the_current_open_dialog_and_request_token() {
        let script = delivery_script(&json!({"requestToken":"token","requests":{"rows":[]}}));
        assert!(script.contains("!r.quotaDetailsOpen"));
        assert!(script.contains("pending?.requestToken!==v.requestToken"));
        assert!(script.contains("r.render?.()"));
        assert!(!script.contains("fetch("));
    }
