use armada_telegram_bridge::rich_text::{
    RichSpan, RichStyle, RichText, TelegramEntity, TelegramEntityKind, from_telegram,
    parse_armada_markdown, prepend_plain, render_armada_markdown, split_for_telegram, to_telegram,
};

fn entity(kind: TelegramEntityKind, offset: u32, length: u32) -> TelegramEntity {
    TelegramEntity {
        kind,
        offset,
        length,
        url: None,
        language: None,
    }
}

#[test]
fn armada_markdown_golden_styles_become_semantic_spans() {
    let parsed = parse_armada_markdown(
        "**bold** *italic* __under__ ~~strike~~ ||secret|| `code`\n> quote\n# heading\n- list\n```rust\nlet x = 1;\n```\n[label](https://example.com)",
    );

    assert_eq!(
        parsed.text,
        "bold italic under strike secret code\nquote\nheading\n- list\nlet x = 1;\nlabel"
    );
    for expected in [
        RichStyle::Bold,
        RichStyle::Italic,
        RichStyle::Underline,
        RichStyle::Strikethrough,
        RichStyle::Spoiler,
        RichStyle::Code,
        RichStyle::Quote,
        RichStyle::Pre {
            language: Some("rust".to_owned()),
        },
        RichStyle::Link {
            url: "https://example.com".to_owned(),
        },
    ] {
        assert!(parsed.spans.iter().any(|span| span.style == expected));
    }
}

#[test]
fn telegram_offsets_use_utf16_but_internal_ranges_use_utf8() {
    let text = "A😀e\u{301} אב";
    let parsed = from_telegram(
        text,
        &[
            entity(TelegramEntityKind::Bold, 1, 2),
            entity(TelegramEntityKind::Italic, 3, 2),
            entity(TelegramEntityKind::Underline, 6, 2),
        ],
    )
    .expect("valid UTF-16 entity boundaries");
    assert_eq!(
        &parsed.text[parsed.spans[0].start..parsed.spans[0].end],
        "😀"
    );
    assert_eq!(
        &parsed.text[parsed.spans[1].start..parsed.spans[1].end],
        "e\u{301}"
    );
    assert_eq!(
        &parsed.text[parsed.spans[2].start..parsed.spans[2].end],
        "אב"
    );
    assert_eq!(to_telegram(&parsed).entities[0].offset, 1);
    assert!(from_telegram(text, &[entity(TelegramEntityKind::Bold, 2, 1)]).is_err());
}

#[test]
fn unsafe_links_and_directional_controls_cannot_spoof_output() {
    let text = "click safe\u{202e}name";
    let entities = [TelegramEntity {
        kind: TelegramEntityKind::TextLink,
        offset: 0,
        length: 5,
        url: Some("javascript:alert(1)".to_owned()),
        language: None,
    }];
    let parsed = from_telegram(text, &entities).expect("entity coordinates are valid");
    assert_eq!(parsed.text, "click safename");
    assert_eq!(parsed.spans, Vec::new());
    assert_eq!(render_armada_markdown(&parsed), "click safename");
}

#[test]
fn crossings_are_resolved_deterministically_and_nested_ranges_remain_valid() {
    let rich = RichText {
        text: "abcdef".to_owned(),
        spans: vec![
            RichSpan {
                start: 0,
                end: 4,
                style: RichStyle::Bold,
            },
            RichSpan {
                start: 2,
                end: 6,
                style: RichStyle::Italic,
            },
            RichSpan {
                start: 1,
                end: 3,
                style: RichStyle::Underline,
            },
        ],
    };
    let telegram = to_telegram(&rich);
    assert_eq!(telegram.entities.len(), 2);
    assert_eq!(telegram.entities[0].kind, TelegramEntityKind::Bold);
    assert_eq!(telegram.entities[1].kind, TelegramEntityKind::Underline);
}

#[test]
fn splitting_is_grapheme_safe_and_rebases_every_entity() {
    let family = "👨‍👩‍👧‍👦";
    let text = format!("first paragraph\n\n{family} second paragraph");
    let rich = RichText {
        spans: vec![RichSpan {
            start: 0,
            end: text.len(),
            style: RichStyle::Bold,
        }],
        text,
    };
    let chunks = split_for_telegram(&rich, 17);
    assert!(chunks.len() >= 2);
    assert_eq!(chunks.concat_text(), rich.text);
    for chunk in chunks {
        assert!(chunk.text.chars().count() <= 17 || chunk.text == family);
        let rendered = to_telegram(&chunk);
        from_telegram(&rendered.text, &rendered.entities).expect("rebased entities stay valid");
    }
}

#[test]
fn plain_text_round_trips_except_documented_sanitization() {
    for sample in [
        "plain",
        "emoji 😀 and combining e\u{301}",
        "asterisk \\* and brackets \\[x\\]",
        "العربية עברית",
    ] {
        let original = RichText::plain(sample);
        let markdown = render_armada_markdown(&original);
        assert_eq!(parse_armada_markdown(&markdown).text, sample);
    }

    let attributed = prepend_plain(
        "header\n",
        &RichText {
            text: "body".to_owned(),
            spans: vec![RichSpan {
                start: 0,
                end: 4,
                style: RichStyle::Bold,
            }],
        },
    );
    assert_eq!(attributed.spans[0].start, "header\n".len());
}

#[test]
fn broad_valid_unicode_samples_never_break_rendering_or_entity_ranges() {
    let sample: String = (0..=0x10_ffff)
        .step_by(997)
        .filter_map(char::from_u32)
        .filter(|character| {
            !character.is_control()
                && !matches!(
                    character,
                    '\u{061c}'
                        | '\u{200b}'
                        | '\u{200e}'..='\u{200f}'
                        | '\u{202a}'..='\u{202e}'
                        | '\u{2060}'..='\u{206f}'
                        | '\u{feff}'
                )
        })
        .collect();
    let rich = RichText {
        text: sample.clone(),
        spans: vec![RichSpan {
            start: 0,
            end: sample.len(),
            style: RichStyle::Bold,
        }],
    };

    let telegram = to_telegram(&rich);
    assert_eq!(telegram.text, sample);
    from_telegram(&telegram.text, &telegram.entities)
        .expect("every generated Telegram entity remains valid");
    let markdown = render_armada_markdown(&RichText::plain(sample.clone()));
    assert_eq!(parse_armada_markdown(&markdown).text, sample);
}

#[test]
fn adversarial_markdown_nesting_is_bounded_and_remains_readable() {
    let source = format!("{}text{}", "**".repeat(10_000), "**".repeat(10_000));
    let parsed = parse_armada_markdown(&source);

    assert!(parsed.text.contains("text"));
    assert!(parsed.spans.len() <= 32);
}

trait ConcatText {
    fn concat_text(&self) -> String;
}

impl ConcatText for Vec<RichText> {
    fn concat_text(&self) -> String {
        self.iter().map(|chunk| chunk.text.as_str()).collect()
    }
}
