#![no_main]

use armada_telegram_bridge::rich_text::{
    TelegramEntity, TelegramEntityKind, from_telegram, parse_armada_markdown,
    render_armada_markdown, split_for_telegram, to_telegram,
};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // Match a bounded message-sized parser surface, not unbounded allocator stress.
    if data.len() > 65_536 { return; }
    let text = String::from_utf8_lossy(data);
    let rich = parse_armada_markdown(&text);
    let _ = render_armada_markdown(&rich);
    for chunk in split_for_telegram(&rich, 4096) {
        let message = to_telegram(&chunk);
        assert!(message.text.chars().count() <= 4096);
        assert!(from_telegram(&message.text, &message.entities).is_ok());
    }
    if data.len() >= 8 {
        let offset = u32::from_le_bytes(data[..4].try_into().unwrap());
        let length = u32::from_le_bytes(data[4..8].try_into().unwrap());
        let entity = TelegramEntity { kind: TelegramEntityKind::Bold, offset, length, url: None, language: None };
        let _ = from_telegram(&text, &[entity]);
    }
});
