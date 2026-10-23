#![cfg(unix)]

use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use armada_telegram_bridge::attachments::PrivateTempFile;
use armada_telegram_bridge::domain::{
    AttachmentLocator, AttachmentRef, BridgeEvent, BridgeReactionUpdate, Mutation, ReactionChange,
    ReactionEntry, RouteId, SenderIdentity, Source, TelegramChatId, TelegramThreadId,
};
use armada_telegram_bridge::rich_text::TelegramMessage;
use armada_telegram_bridge::rich_text::{TelegramEntity, TelegramEntityKind};
use armada_telegram_bridge::telegram::{
    CaptionText, InboundTelegramMessage, PollReport, TelegramAcceptError, TelegramAdapter,
    TelegramError, TelegramPort, TelegramRoute, TelegramUpdateSink,
};
use armada_telegram_bridge::test_support::FakeTelegramPort;
use async_trait::async_trait;
use serde_json::{Value, json};

const TOKEN: &str = "123456:TEST_ONLY_SECRET_TOKEN";
const BOT_ID: u64 = 123_456;
const CHAT_ID: i64 = -1_001_234;

fn route(thread_id: Option<u32>) -> TelegramRoute {
    TelegramRoute::new(
        RouteId::try_from("alpha").expect("route ID should be valid"),
        TelegramChatId::try_from(CHAT_ID).expect("chat ID should be valid"),
        thread_id.map(|id| TelegramThreadId::try_from(id).expect("thread ID should be valid")),
    )
}

#[derive(Clone, Debug)]
struct Response {
    status: u16,
    body: String,
    delay: Duration,
}

impl Response {
    #[allow(clippy::needless_pass_by_value)]
    fn json(body: Value) -> Self {
        Self {
            status: 200,
            body: body.to_string(),
            delay: Duration::ZERO,
        }
    }

    #[allow(clippy::needless_pass_by_value)]
    fn status(status: u16, body: Value) -> Self {
        Self {
            status,
            body: body.to_string(),
            delay: Duration::ZERO,
        }
    }

    fn raw(body: impl Into<String>) -> Self {
        Self {
            status: 200,
            body: body.into(),
            delay: Duration::ZERO,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RecordedRequest {
    method: String,
    body: Value,
}

struct ScriptedServer {
    base_url: String,
    requests: Arc<Mutex<Vec<RecordedRequest>>>,
    thread: Option<thread::JoinHandle<()>>,
}

impl ScriptedServer {
    fn start(responses: Vec<Response>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("test listener should bind");
        let address = listener.local_addr().expect("listener address should load");
        let requests = Arc::new(Mutex::new(Vec::new()));
        let thread_requests = Arc::clone(&requests);
        let thread = thread::spawn(move || {
            for response in responses {
                let (mut stream, _) = listener.accept().expect("scripted request should arrive");
                let mut request = Vec::new();
                let mut buffer = [0_u8; 4096];
                let header_end;
                loop {
                    let read = stream
                        .read(&mut buffer)
                        .expect("request should be readable");
                    assert!(read > 0, "request ended before headers");
                    request.extend_from_slice(&buffer[..read]);
                    if let Some(position) = request.windows(4).position(|part| part == b"\r\n\r\n")
                    {
                        header_end = position + 4;
                        break;
                    }
                }
                let headers = String::from_utf8_lossy(&request[..header_end]).into_owned();
                let content_length = headers
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().expect("valid content length"))
                    })
                    .unwrap_or(0);
                while request.len() < header_end + content_length {
                    let read = stream.read(&mut buffer).expect("body should be readable");
                    assert!(read > 0, "request ended before body");
                    request.extend_from_slice(&buffer[..read]);
                }

                let request_line = headers.lines().next().expect("request line should exist");
                let path = request_line
                    .split_whitespace()
                    .nth(1)
                    .expect("request path should exist");
                let method = path
                    .rsplit('/')
                    .next()
                    .expect("API method should exist")
                    .to_owned();
                let body = if content_length == 0 {
                    Value::Null
                } else if headers
                    .to_ascii_lowercase()
                    .contains("content-type: application/json")
                {
                    serde_json::from_slice(&request[header_end..header_end + content_length])
                        .expect("request JSON should parse")
                } else {
                    Value::String(
                        String::from_utf8_lossy(&request[header_end..header_end + content_length])
                            .into_owned(),
                    )
                };
                thread_requests
                    .lock()
                    .expect("request lock should work")
                    .push(RecordedRequest { method, body });

                thread::sleep(response.delay);
                let reason = match response.status {
                    200 => "OK",
                    400 => "Bad Request",
                    429 => "Too Many Requests",
                    503 => "Service Unavailable",
                    _ => "Response",
                };
                let wire = format!(
                    "HTTP/1.1 {} {}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                    response.status,
                    reason,
                    response.body.len(),
                    response.body
                );
                let _ = stream.write_all(wire.as_bytes());
            }
        });
        Self {
            base_url: format!("http://{address}"),
            requests,
            thread: Some(thread),
        }
    }

    fn requests(&self) -> Vec<RecordedRequest> {
        self.requests
            .lock()
            .expect("request lock should work")
            .clone()
    }
}

impl Drop for ScriptedServer {
    fn drop(&mut self) {
        if let Some(thread) = self.thread.take() {
            thread.join().expect("scripted server should stop cleanly");
        }
    }
}

fn adapter(server: &ScriptedServer, routes: Vec<TelegramRoute>) -> TelegramAdapter {
    TelegramAdapter::with_api_base(
        &server.base_url,
        TOKEN,
        BOT_ID,
        true,
        false,
        routes,
        Duration::from_secs(2),
    )
    .expect("adapter should build")
}

#[derive(Default)]
struct RecordingSink {
    accepted: Vec<InboundTelegramMessage>,
    reactions: Vec<(BridgeReactionUpdate, u64)>,
    reject_next: bool,
}

#[async_trait]
impl TelegramUpdateSink for RecordingSink {
    async fn accept(&mut self, message: InboundTelegramMessage) -> Result<(), TelegramAcceptError> {
        if self.reject_next {
            self.reject_next = false;
            return Err(TelegramAcceptError);
        }
        self.accepted.push(message);
        Ok(())
    }

    async fn accept_reaction(
        &mut self,
        update: BridgeReactionUpdate,
        next_offset: u64,
    ) -> Result<(), TelegramAcceptError> {
        self.reactions.push((update, next_offset));
        Ok(())
    }
}

#[tokio::test]
async fn reaction_updates_are_explicitly_requested_and_normalized_with_identity() {
    let server = ScriptedServer::start(vec![Response::json(json!({"ok":true,"result":[{
        "update_id":90,
        "message_reaction":{
            "chat":{"id":CHAT_ID,"type":"supergroup"},
            "message_id":190,
            "user":{"id":13,"is_bot":false,"first_name":"Alice","username":"alice"},
            "date":1_700_000_001,
            "old_reaction":[],
            "new_reaction":[
                {"type":"emoji","emoji":"👍"},
                {"type":"custom_emoji","custom_emoji_id":"unsupported"}
            ]
        }
    }]}))]);
    let mut adapter = adapter(&server, vec![route(None)]);
    let mut sink = RecordingSink::default();

    let report = adapter
        .poll_once(&mut sink)
        .await
        .expect("reaction poll succeeds");

    assert_eq!(report.accepted, 1);
    assert_eq!(sink.reactions.len(), 1);
    let (reaction, next_offset) = &sink.reactions[0];
    assert_eq!(*next_offset, 91);
    assert_eq!(reaction.source_event_id, "telegram:reaction:90");
    assert_eq!(
        reaction.target_source_event_id,
        format!("telegram:{CHAT_ID}:190")
    );
    assert!(matches!(
        &reaction.change,
        ReactionChange::ActorSelection { reactor_id, display_name: Some(name), emojis }
            if reactor_id == "telegram-user:13" && name == "@alice" && emojis == &["👍"]
    ));
    assert_eq!(
        server.requests()[0].body["allowed_updates"],
        json!([
            "message",
            "edited_message",
            "message_reaction",
            "message_reaction_count"
        ])
    );
}

#[tokio::test]
async fn anonymous_reaction_counts_are_normalized_as_a_complete_snapshot() {
    let server = ScriptedServer::start(vec![Response::json(json!({"ok":true,"result":[{
        "update_id":91,
        "message_reaction_count":{
            "chat":{"id":CHAT_ID,"type":"supergroup"},
            "message_id":190,
            "date":1_700_000_002,
            "reactions":[
                {"type":{"type":"emoji","emoji":"❤️"},"total_count":3},
                {"type":{"type":"custom_emoji","custom_emoji_id":"unsupported"},"total_count":2}
            ]
        }
    }]}))]);
    let mut adapter = adapter(&server, vec![route(None)]);
    let mut sink = RecordingSink::default();

    adapter
        .poll_once(&mut sink)
        .await
        .expect("aggregate reaction poll succeeds");

    let (reaction, next_offset) = &sink.reactions[0];
    assert_eq!(*next_offset, 92);
    assert!(matches!(
        &reaction.change,
        ReactionChange::Snapshot { entries }
            if entries == &[ReactionEntry {
                reactor_id: "telegram-anonymous:❤️".to_owned(),
                display_name: None,
                emoji: "❤️".to_owned(),
                count: 3,
            }]
    ));
}

#[tokio::test]
async fn get_me_must_match_the_configured_bot_identity() {
    let server = ScriptedServer::start(vec![
        Response::json(json!({"ok":true,"result":{"id":999,"is_bot":true,"first_name":"Wrong"}})),
        Response::json(
            json!({"ok":true,"result":{"id":BOT_ID,"is_bot":true,"first_name":"Bridge","username":"bridge_bot"}}),
        ),
    ]);
    let mut adapter = adapter(&server, vec![route(None)]);

    assert!(matches!(
        adapter.verify_identity().await,
        Err(TelegramError::IdentityMismatch {
            expected: BOT_ID,
            actual: 999
        })
    ));
    let identity = adapter
        .verify_identity()
        .await
        .expect("matching identity should pass");
    assert_eq!(identity.id, BOT_ID);
    assert_eq!(identity.username.as_deref(), Some("bridge_bot"));
}

#[tokio::test]
async fn polling_filters_exact_scope_and_preserves_stable_sender_identities() {
    let mut formatted_user = update(
        5,
        CHAT_ID,
        Some(42),
        user(13, false, "alice"),
        None,
        "A😀 user",
    );
    formatted_user["message"]["entities"] = json!([
        {"type":"bold","offset":1,"length":2},
        {"type":"custom_emoji","offset":1,"length":2,"custom_emoji_id":"ignored"}
    ]);
    let updates = json!({"ok":true,"result":[
        update(1, CHAT_ID - 1, Some(42), user(10, false, "wrong"), None, "wrong chat"),
        update(2, CHAT_ID, Some(41), user(11, false, "wrongtopic"), None, "wrong topic"),
        update(3, CHAT_ID, Some(42), user(BOT_ID, true, "selfbot"), None, "self"),
        update(4, CHAT_ID, Some(42), user(12, true, "otherbot"), None, "other bot"),
        formatted_user,
        update(6, CHAT_ID, Some(42), user(1_087_968_824, true, "GroupAnonymousBot"), Some(sender_chat(CHAT_ID, "Anonymous admin")), "anonymous"),
        update(7, CHAT_ID, Some(42), Value::Null, Some(sender_chat(-100_999, "News channel")), "sender chat")
    ]});
    let server = ScriptedServer::start(vec![Response::json(updates)]);
    let mut adapter = adapter(&server, vec![route(Some(42))]);
    let mut sink = RecordingSink::default();

    let report = adapter
        .poll_once(&mut sink)
        .await
        .expect("poll should succeed");

    assert_eq!(
        report,
        PollReport {
            observed: 7,
            accepted: 3,
            ignored: 4,
            next_offset: Some(8)
        }
    );
    assert_eq!(sink.accepted.len(), 3);
    assert!(matches!(
        &sink.accepted[0].event.sender,
        SenderIdentity::Telegram { user_id: Some(13), sender_chat_id: None, username: Some(name), .. } if name == "alice"
    ));
    assert!(matches!(
        &sink.accepted[1].event.sender,
        SenderIdentity::Telegram { user_id: None, sender_chat_id: Some(id), .. } if *id == CHAT_ID
    ));
    assert!(matches!(
        &sink.accepted[2].event.sender,
        SenderIdentity::Telegram {
            user_id: None,
            sender_chat_id: Some(-100_999),
            ..
        }
    ));
    assert_eq!(sink.accepted[0].next_offset, 6);
    let rich = sink.accepted[0]
        .event
        .rich_text
        .as_ref()
        .expect("Telegram formatting is normalized");
    assert_eq!(&rich.text[rich.spans[0].start..rich.spans[0].end], "😀");
    assert_eq!(
        sink.accepted[0].event.source_event_id,
        format!("telegram:{CHAT_ID}:105")
    );
}

#[tokio::test]
async fn update_offset_advances_only_after_durable_acceptance() {
    let response = Response::json(json!({"ok":true,"result":[
        update(20, CHAT_ID, None, user(13, false, "alice"), None, "retry me")
    ]}));
    let server = ScriptedServer::start(vec![response.clone(), response]);
    let mut adapter = adapter(&server, vec![route(None)]);
    let mut sink = RecordingSink {
        reject_next: true,
        ..RecordingSink::default()
    };

    assert!(matches!(
        adapter.poll_once(&mut sink).await,
        Err(TelegramError::AcceptanceFailed)
    ));
    assert_eq!(adapter.next_offset(), None);
    adapter
        .poll_once(&mut sink)
        .await
        .expect("retry should be accepted");
    assert_eq!(adapter.next_offset(), Some(21));
    assert_eq!(sink.accepted.len(), 1);

    let requests = server.requests();
    assert_eq!(requests[0].body.get("offset"), None);
    assert_eq!(requests[1].body.get("offset"), None);
}

#[tokio::test]
async fn media_groups_release_available_items_in_batch_order() {
    let mut first = update(
        30,
        CHAT_ID,
        None,
        user(13, false, "alice"),
        None,
        "first caption",
    );
    first["message"]["media_group_id"] = json!("album-1");
    first["message"]["photo"] = json!([
        {"file_id":"small","file_unique_id":"same-small","file_size":10,"width":10,"height":10},
        {"file_id":"large","file_unique_id":"same-large","file_size":20,"width":20,"height":20}
    ]);
    let mut second = update(31, CHAT_ID, None, user(13, false, "alice"), None, "");
    second["message"]["media_group_id"] = json!("album-1");
    second["message"]["photo"] = json!([
        {"file_id":"second","file_unique_id":"same-second","file_size":15,"width":15,"height":15}
    ]);
    let server = ScriptedServer::start(vec![Response::json(
        json!({"ok":true,"result":[first, second]}),
    )]);
    let mut adapter = adapter(&server, vec![route(None)]);
    let mut sink = RecordingSink::default();

    let report = adapter
        .poll_once(&mut sink)
        .await
        .expect("album poll works");
    assert_eq!(report.accepted, 2);
    assert_eq!(
        sink.accepted[0].event.attachments[0].source_id,
        "same-large"
    );
    assert_eq!(
        (
            sink.accepted[0].event.attachments[0].media_group_index,
            sink.accepted[0].event.attachments[0].media_group_total,
        ),
        (Some(1), Some(2))
    );
    assert_eq!(
        (
            sink.accepted[1].event.attachments[0].media_group_index,
            sink.accepted[1].event.attachments[0].media_group_total,
        ),
        (Some(2), Some(2))
    );
}

#[tokio::test]
async fn out_of_order_updates_cannot_advance_past_unaccepted_work() {
    let server = ScriptedServer::start(vec![Response::json(json!({"ok":true,"result":[
        update(21, CHAT_ID - 1, None, user(14, false, "ignored"), None, "later"),
        update(20, CHAT_ID, None, user(13, false, "alice"), None, "earlier")
    ]}))]);
    let mut adapter = adapter(&server, vec![route(None)]);
    let mut sink = RecordingSink::default();

    assert!(matches!(
        adapter.poll_once(&mut sink).await,
        Err(TelegramError::MalformedResponse)
    ));
    assert_eq!(adapter.next_offset(), None);
    assert_eq!(sink.accepted.len(), 0);
}

#[tokio::test]
async fn bot_api_failures_have_distinct_retry_classifications() {
    async fn one(response: Response, timeout: Duration) -> TelegramError {
        let server = ScriptedServer::start(vec![response]);
        let mut adapter = TelegramAdapter::with_api_base(
            &server.base_url,
            TOKEN,
            BOT_ID,
            true,
            false,
            vec![route(None)],
            timeout,
        )
        .expect("adapter should build");
        adapter
            .verify_identity()
            .await
            .expect_err("script should fail")
    }

    assert!(matches!(
        one(
            Response::status(
                429,
                json!({"ok":false,"error_code":429,"parameters":{"retry_after":17}})
            ),
            Duration::from_secs(1)
        )
        .await,
        TelegramError::RateLimited {
            retry_after_seconds: 17
        }
    ));
    assert!(matches!(
        one(
            Response::status(503, json!({"ok":false})),
            Duration::from_secs(1)
        )
        .await,
        TelegramError::Retryable { status: 503 }
    ));
    assert!(matches!(
        one(
            Response::status(400, json!({"ok":false})),
            Duration::from_secs(1)
        )
        .await,
        TelegramError::Terminal { status: 400 }
    ));
    assert!(matches!(
        one(
            Response {
                status: 200,
                body: "not-json".to_owned(),
                delay: Duration::ZERO
            },
            Duration::from_secs(1)
        )
        .await,
        TelegramError::MalformedResponse
    ));
    assert!(matches!(
        one(
            Response {
                status: 200,
                body: "{}".to_owned(),
                delay: Duration::from_millis(100)
            },
            Duration::from_millis(10)
        )
        .await,
        TelegramError::Timeout
    ));
}

#[tokio::test]
async fn group_migration_pauses_the_route_without_rewriting_its_chat_id() {
    let new_chat_id = -1_009_999;
    let migration = json!({"ok":true,"result":[{"update_id":30,"message":{
        "message_id":130,"date":1,"chat":{"id":CHAT_ID,"type":"group"},
        "migrate_to_chat_id":new_chat_id
    }}]});
    let server = ScriptedServer::start(vec![Response::json(migration)]);
    let mut adapter = adapter(&server, vec![route(None)]);
    let mut sink = RecordingSink::default();

    let error = adapter
        .poll_once(&mut sink)
        .await
        .expect_err("migration should stop intake");
    assert!(matches!(
        error,
        TelegramError::ChatMigrated { old_chat_id: CHAT_ID, new_chat_id: id, .. } if id == new_chat_id
    ));
    assert!(adapter.route_is_paused(&RouteId::try_from("alpha").expect("valid route")));
    assert_eq!(adapter.next_offset(), None);
    assert!(matches!(
        adapter
            .send_text(&RouteId::try_from("alpha").expect("valid route"), "blocked")
            .await,
        Err(TelegramError::RoutePaused { .. })
    ));
}

#[tokio::test]
async fn outbound_text_enforces_limits_protection_and_secret_safe_recording() {
    let server = ScriptedServer::start(vec![Response::json(json!({
        "ok":true,"result":{"message_id":777,"date":1,"chat":{"id":CHAT_ID,"type":"supergroup"}}
    }))]);
    let mut adapter = adapter(&server, vec![route(Some(42))]);
    let route_id = RouteId::try_from("alpha").expect("valid route");
    let text = "x".repeat(4096);

    let entities = [TelegramEntity {
        kind: TelegramEntityKind::Bold,
        offset: 0,
        length: 1,
        url: None,
        language: None,
    }];
    let receipt = adapter
        .send_rich_text(&route_id, &text, &entities)
        .await
        .expect("maximum text should send");
    assert_eq!(receipt.message_id, 777);
    assert_eq!(
        receipt.destination_message_id,
        format!("telegram:{CHAT_ID}:777")
    );
    assert!(matches!(
        adapter.send_text(&route_id, &"x".repeat(4097)).await,
        Err(TelegramError::TextTooLong { maximum: 4096, .. })
    ));
    assert!(CaptionText::try_from("x".repeat(1024)).is_ok());
    assert!(matches!(
        CaptionText::try_from("x".repeat(1025)),
        Err(TelegramError::CaptionTooLong { maximum: 1024, .. })
    ));

    let requests = server.requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].method, "sendMessage");
    assert_eq!(requests[0].body["chat_id"], CHAT_ID);
    assert_eq!(requests[0].body["message_thread_id"], 42);
    assert_eq!(requests[0].body["protect_content"], true);
    assert_eq!(requests[0].body["text"], text);
    assert_eq!(requests[0].body["entities"][0]["type"], "bold");
    assert!(!format!("{requests:?}").contains(TOKEN));
    assert!(!format!("{:?}", TelegramError::Timeout).contains(TOKEN));
}

#[tokio::test]
async fn replies_and_edits_are_normalized_without_confusing_inline_quotes() {
    let mut reply = update(80, CHAT_ID, None, user(13, false, "alice"), None, "answer");
    reply["message"]["reply_to_message"] = json!({
        "message_id":170,
        "date":1_699_999_999_i64,
        "chat":{"id":CHAT_ID,"type":"supergroup"},
        "text":"original\u{202e} text"
    });
    let inline = update(
        81,
        CHAT_ID,
        None,
        user(14, false, "bob"),
        None,
        "> inline quote",
    );
    let mut edit = update(82, CHAT_ID, None, user(13, false, "alice"), None, "fixed");
    let edited_message = edit
        .as_object_mut()
        .expect("update object")
        .remove("message")
        .expect("message exists");
    edit["edited_message"] = edited_message;
    let server = ScriptedServer::start(vec![Response::json(
        json!({"ok":true,"result":[reply, inline, edit]}),
    )]);
    let mut adapter = adapter(&server, vec![route(None)]);
    let mut sink = RecordingSink::default();

    adapter
        .poll_once(&mut sink)
        .await
        .expect("updates normalize");
    assert_eq!(sink.accepted.len(), 3);
    let reply = &sink.accepted[0].event;
    assert_eq!(
        server.requests()[0].body["allowed_updates"],
        json!([
            "message",
            "edited_message",
            "message_reaction",
            "message_reaction_count"
        ])
    );
    assert_eq!(
        reply
            .reply_to
            .as_ref()
            .expect("native reply target")
            .source_event_id,
        format!("telegram:{CHAT_ID}:170")
    );
    assert_eq!(reply.reply_quote.as_deref(), Some("original text"));
    assert!(sink.accepted[1].event.reply_to.is_none());
    assert_eq!(sink.accepted[1].event.mutation, Mutation::Create);
    let edit = &sink.accepted[2].event;
    assert_eq!(edit.mutation, Mutation::Edit);
    assert_eq!(
        edit.mutation_target
            .as_ref()
            .expect("edit target")
            .source_event_id,
        format!("telegram:{CHAT_ID}:182")
    );
    assert_eq!(
        edit.source_event_id,
        format!("telegram:edit:{CHAT_ID}:182:82")
    );
}

#[tokio::test]
async fn native_reply_edit_and_delete_are_scoped_and_missing_is_idempotent() {
    let server = ScriptedServer::start(vec![
        Response::json(json!({
            "ok":true,"result":{"message_id":900,"date":1,"chat":{"id":CHAT_ID,"type":"supergroup"}}
        })),
        Response::status(
            400,
            json!({"ok":false,"error_code":400,"description":"Bad Request: message is not modified"}),
        ),
        Response::status(
            400,
            json!({"ok":false,"error_code":400,"description":"Bad Request: message to delete not found"}),
        ),
        Response::status(
            400,
            json!({"ok":false,"error_code":400,"description":"Bad Request: message can't be edited"}),
        ),
    ]);
    let mut adapter = adapter(&server, vec![route(None)]);
    let route_id = RouteId::try_from("alpha").expect("valid route");
    let target = format!("telegram:{CHAT_ID}:700");

    adapter
        .send_reply(&route_id, &target, "reply", &[])
        .await
        .expect("native reply sends");
    adapter
        .edit_message(&route_id, &target, "fixed", &[])
        .await
        .expect("missing edit resolves idempotently");
    adapter
        .delete_message(&route_id, &target)
        .await
        .expect("missing delete resolves idempotently");
    assert!(matches!(
        adapter.edit_message(&route_id, &target, "fixed", &[]).await,
        Err(TelegramError::Terminal { status: 400 })
    ));
    assert!(matches!(
        adapter.delete_message(&route_id, "telegram:-999:700").await,
        Err(TelegramError::InvalidMessageReference)
    ));

    let requests = server.requests();
    assert_eq!(
        requests
            .iter()
            .map(|request| request.method.as_str())
            .collect::<Vec<_>>(),
        [
            "sendMessage",
            "editMessageText",
            "deleteMessage",
            "editMessageText"
        ]
    );
    assert_eq!(requests[0].body["reply_parameters"]["message_id"], 700);
    assert_eq!(requests[1].body["message_id"], 700);
    assert_eq!(requests[2].body["message_id"], 700);
}

#[tokio::test]
async fn attachments_stream_through_get_file_and_multipart_upload_with_caption() {
    let server = ScriptedServer::start(vec![
        Response::json(json!({
            "ok":true,
            "result":{"file_id":"download-id","file_unique_id":"stable-id","file_size":9,"file_path":"documents/file.pdf"}
        })),
        Response::raw("%PDF-1.7"),
        Response::json(json!({
            "ok":true,"result":{"message_id":778,"date":1,"chat":{"id":CHAT_ID,"type":"supergroup"}}
        })),
    ]);
    let mut adapter = adapter(&server, vec![route(Some(42))]);
    let root = tempfile::tempdir().expect("temp root");
    let staged = PrivateTempFile::new(root.path(), Some("report.pdf")).expect("private staging");
    let attachment = AttachmentRef {
        source_id: "stable-id".to_owned(),
        file_name: Some("report.pdf".to_owned()),
        media_type: Some("image/jpeg".to_owned()),
        size_bytes: 9,
        locator: AttachmentLocator::Telegram {
            file_id: "download-id".to_owned(),
        },
        media_group_id: None,
        media_group_index: None,
        media_group_total: None,
    };
    adapter
        .download_attachment(&attachment, staged.path(), 9)
        .await
        .expect("download is streamed within the bound");
    let metadata = staged.inspect(9).expect("download is inspected");
    assert_eq!(metadata.media_type, "application/pdf");
    let caption = TelegramMessage {
        text: "✈️ report".to_owned(),
        entities: vec![TelegramEntity {
            kind: TelegramEntityKind::Bold,
            offset: 3,
            length: 6,
            url: None,
            language: None,
        }],
    };
    let receipt = adapter
        .send_attachment(
            &RouteId::try_from("alpha").expect("valid route"),
            staged.path(),
            &metadata,
            Some(&caption),
        )
        .await
        .expect("document uploads");
    assert_eq!(receipt.message_id, 778);

    let requests = server.requests();
    assert_eq!(
        requests
            .iter()
            .map(|request| request.method.as_str())
            .collect::<Vec<_>>(),
        ["getFile", "file.pdf", "sendDocument"]
    );
    let multipart = requests[2].body.as_str().expect("multipart captured");
    assert!(multipart.contains("report.pdf"));
    assert!(multipart.contains("caption_entities"));
    assert!(multipart.contains("protect_content"));
    assert!(!multipart.contains(TOKEN));
}

#[tokio::test]
async fn specialized_media_rejection_falls_back_to_document() {
    let server = ScriptedServer::start(vec![
        Response::status(400, json!({"ok":false,"error_code":400})),
        Response::json(json!({
            "ok":true,"result":{"message_id":779,"date":1,"chat":{"id":CHAT_ID,"type":"supergroup"}}
        })),
    ]);
    let mut adapter = adapter(&server, vec![route(None)]);
    let root = tempfile::tempdir().expect("temp root");
    let staged = PrivateTempFile::new(root.path(), Some("image.png")).expect("private staging");
    std::fs::write(staged.path(), b"\x89PNG\r\n\x1a\nbytes").expect("fixture writes");
    let metadata = staged.inspect(100).expect("image is inspected");

    adapter
        .send_attachment(
            &RouteId::try_from("alpha").expect("valid route"),
            staged.path(),
            &metadata,
            None,
        )
        .await
        .expect("document fallback succeeds");

    assert_eq!(
        server
            .requests()
            .iter()
            .map(|request| request.method.as_str())
            .collect::<Vec<_>>(),
        ["sendPhoto", "sendDocument"]
    );
}

#[tokio::test]
async fn reusable_port_contract_passes_for_fake_and_http_adapter() {
    async fn contract(port: &mut dyn TelegramPort) {
        let identity = port
            .verify_identity()
            .await
            .expect("identity should verify");
        assert_eq!(identity.id, BOT_ID);
        let route_id = RouteId::try_from("alpha").expect("valid route");
        let access = port
            .verify_route_access(&route_id)
            .await
            .expect("route should be accessible");
        assert_eq!(access.chat_id, CHAT_ID);
        let mut sink = RecordingSink::default();
        let report = port
            .poll_once(&mut sink)
            .await
            .expect("poll should succeed");
        assert_eq!(report.accepted, 1);
        assert_eq!(report.next_offset, Some(51));
        assert_eq!(sink.accepted[0].event.body, "contract inbound");
        assert_eq!(sink.accepted[0].event.route_id, route_id);
        let receipt = port
            .send_text(&route_id, "contract outbound")
            .await
            .expect("send should succeed");
        assert!(receipt.message_id > 0);
        assert!(receipt.destination_message_id.starts_with("telegram:"));
    }

    let inbound = InboundTelegramMessage {
        event: BridgeEvent {
            source: Source::Telegram,
            source_event_id: format!("telegram:{CHAT_ID}:150"),
            route_id: RouteId::try_from("alpha").expect("valid route"),
            sender: SenderIdentity::Telegram {
                user_id: Some(13),
                sender_chat_id: None,
                username: Some("alice".to_owned()),
                display_name: Some("alice".to_owned()),
            },
            timestamp_ms: 1_700_000_000_000,
            body: "contract inbound".to_owned(),
            rich_text: None,
            attachments: Vec::new(),
            reply_to: None,
            reply_quote: None,
            mutation_target: None,
            mutation: Mutation::Create,
            expires_at_ms: None,
        },
        next_offset: 51,
    };
    let mut fake = FakeTelegramPort::new(
        armada_telegram_bridge::telegram::TelegramBotIdentity {
            id: BOT_ID,
            username: Some("bridge_bot".to_owned()),
        },
        vec![route(None)],
    );
    fake.push_inbound(inbound);
    contract(&mut fake).await;
    assert_eq!(fake.sent().len(), 1);

    let server = ScriptedServer::start(vec![
        Response::json(
            json!({"ok":true,"result":{"id":BOT_ID,"is_bot":true,"first_name":"Bridge","username":"bridge_bot"}}),
        ),
        Response::json(
            json!({"ok":true,"result":{"id":CHAT_ID,"type":"supergroup","title":"Private test"}}),
        ),
        Response::json(json!({"ok":true,"result":[
            update(50, CHAT_ID, None, user(13, false, "alice"), None, "contract inbound")
        ]})),
        Response::json(
            json!({"ok":true,"result":{"message_id":777,"date":1,"chat":{"id":CHAT_ID,"type":"supergroup"}}}),
        ),
    ]);
    let mut http = adapter(&server, vec![route(None)]);
    contract(&mut http).await;
}

fn user(id: u64, is_bot: bool, username: &str) -> Value {
    json!({"id":id,"is_bot":is_bot,"first_name":username,"username":username})
}

fn sender_chat(id: i64, title: &str) -> Value {
    json!({"id":id,"type":"channel","title":title})
}

fn update(
    update_id: u64,
    chat_id: i64,
    thread_id: Option<u32>,
    from: Value,
    sender_chat: Option<Value>,
    text: &str,
) -> Value {
    let mut message = json!({
        "message_id": update_id + 100,
        "date": 1_700_000_000_i64,
        "chat": {"id":chat_id,"type":"supergroup"},
        "text": text
    });
    if !from.is_null() {
        message["from"] = from;
    }
    if let Some(sender_chat) = sender_chat {
        message["sender_chat"] = sender_chat;
    }
    if let Some(thread_id) = thread_id {
        message["message_thread_id"] = json!(thread_id);
    }
    json!({"update_id":update_id,"message":message})
}
