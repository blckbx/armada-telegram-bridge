//! Transport-neutral rich text and deterministic transport conversions.

use std::cmp::Reverse;
use std::error::Error;
use std::fmt::{self, Display, Formatter};

use serde::{Deserialize, Serialize};
use unicode_segmentation::UnicodeSegmentation;

/// A visible UTF-8 string with byte-aligned formatting spans.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RichText {
    /// Visible, formatting-free text.
    pub text: String,
    /// Normalized formatting ranges into `text`.
    pub spans: Vec<RichSpan>,
}

impl RichText {
    /// Creates unformatted rich text.
    #[must_use]
    pub fn plain(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            spans: Vec::new(),
        }
    }
}

/// One half-open UTF-8 byte range carrying a style.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RichSpan {
    /// Inclusive byte offset.
    pub start: usize,
    /// Exclusive byte offset.
    pub end: usize,
    /// Semantic style.
    pub style: RichStyle,
}

/// Formatting semantics supported by both bridge transports where possible.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RichStyle {
    /// Strong emphasis.
    Bold,
    /// Emphasis.
    Italic,
    /// Underlined text.
    Underline,
    /// Struck text.
    Strikethrough,
    /// Spoiler/concealed text.
    Spoiler,
    /// Inline code.
    Code,
    /// Fenced code, optionally with a language hint.
    Pre {
        /// A conservative ASCII language identifier.
        language: Option<String>,
    },
    /// A safe explicit hyperlink.
    Link {
        /// An `http`, `https`, or `mailto` URL.
        url: String,
    },
    /// A quoted block.
    Quote,
}

/// Telegram Bot API entity type.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TelegramEntityKind {
    /// Bold entity.
    Bold,
    /// Italic entity.
    Italic,
    /// Underline entity.
    Underline,
    /// Strikethrough entity.
    Strikethrough,
    /// Spoiler entity.
    Spoiler,
    /// Inline code entity.
    Code,
    /// Preformatted block entity.
    Pre,
    /// Link whose target is supplied in `url`.
    TextLink,
    /// Link whose target is the visible entity text.
    Url,
    /// Block quote entity.
    Blockquote,
    /// Expandable block quote entity.
    ExpandableBlockquote,
}

/// Telegram entity using Bot API UTF-16 code-unit offsets.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TelegramEntity {
    /// Entity kind.
    #[serde(rename = "type")]
    pub kind: TelegramEntityKind,
    /// UTF-16 code-unit offset.
    pub offset: u32,
    /// UTF-16 code-unit length.
    pub length: u32,
    /// Explicit target for `text_link`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    /// Optional language for `pre`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub language: Option<String>,
}

/// A Telegram-ready text message.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TelegramMessage {
    /// Visible text.
    pub text: String,
    /// Valid UTF-16 entities into `text`.
    pub entities: Vec<TelegramEntity>,
}

/// Invalid inbound formatting coordinates.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RichTextError;

impl Display for RichTextError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("rich-text entity has an invalid UTF-16 range")
    }
}

impl Error for RichTextError {}

/// Parses the conservative Markdown subset used by Armada/Concord.
#[must_use]
pub fn parse_armada_markdown(source: &str) -> RichText {
    let source = normalize_newlines(source);
    let mut output = String::with_capacity(source.len());
    let mut spans = Vec::new();
    let mut lines = source.split_inclusive('\n').peekable();

    while let Some(line_with_newline) = lines.next() {
        let has_newline = line_with_newline.ends_with('\n');
        let line = line_with_newline
            .strip_suffix('\n')
            .unwrap_or(line_with_newline);
        if let Some(after_fence) = line.strip_prefix("```") {
            let language = valid_language(after_fence.trim());
            let start = output.len();
            let mut closed = false;
            for code_line in lines.by_ref() {
                let code_has_newline = code_line.ends_with('\n');
                let code = code_line.strip_suffix('\n').unwrap_or(code_line);
                if code == "```" {
                    closed = true;
                    break;
                }
                output.push_str(code);
                if code_has_newline {
                    output.push('\n');
                }
            }
            if output.ends_with('\n') {
                output.pop();
            }
            if output.len() > start {
                spans.push(RichSpan {
                    start,
                    end: output.len(),
                    style: RichStyle::Pre { language },
                });
            }
            if !closed && has_newline {
                output.push('\n');
            }
            if lines.peek().is_some() {
                output.push('\n');
            }
            continue;
        }

        let (content, block_style) = strip_block_prefix(line);
        let start = output.len();
        parse_inline(content, &mut output, &mut spans);
        if let Some(style) = block_style
            && output.len() > start
        {
            spans.push(RichSpan {
                start,
                end: output.len(),
                style,
            });
        }
        if has_newline {
            output.push('\n');
        }
    }
    if source.is_empty() {
        return RichText::plain("");
    }
    normalize_rich_text(RichText {
        text: output,
        spans,
    })
}

/// Converts Telegram UTF-16 entities into the bridge's UTF-8 byte spans.
///
/// # Errors
///
/// Returns an error when an entity starts or ends inside a UTF-16 surrogate
/// pair, exceeds the message, or overflows an offset calculation.
pub fn from_telegram(text: &str, entities: &[TelegramEntity]) -> Result<RichText, RichTextError> {
    let mut spans = Vec::new();
    for entity in entities {
        let end = entity
            .offset
            .checked_add(entity.length)
            .ok_or(RichTextError)?;
        let start_byte = utf16_to_byte(text, entity.offset).ok_or(RichTextError)?;
        let end_byte = utf16_to_byte(text, end).ok_or(RichTextError)?;
        if start_byte == end_byte {
            continue;
        }
        let visible = &text[start_byte..end_byte];
        let style =
            match entity.kind {
                TelegramEntityKind::Bold => Some(RichStyle::Bold),
                TelegramEntityKind::Italic => Some(RichStyle::Italic),
                TelegramEntityKind::Underline => Some(RichStyle::Underline),
                TelegramEntityKind::Strikethrough => Some(RichStyle::Strikethrough),
                TelegramEntityKind::Spoiler => Some(RichStyle::Spoiler),
                TelegramEntityKind::Code => Some(RichStyle::Code),
                TelegramEntityKind::Pre => Some(RichStyle::Pre {
                    language: entity.language.as_deref().and_then(valid_language),
                }),
                TelegramEntityKind::TextLink => entity
                    .url
                    .as_deref()
                    .filter(|url| safe_url(url))
                    .map(|url| RichStyle::Link {
                        url: url.to_owned(),
                    }),
                TelegramEntityKind::Url => safe_url(visible).then(|| RichStyle::Link {
                    url: visible.to_owned(),
                }),
                TelegramEntityKind::Blockquote | TelegramEntityKind::ExpandableBlockquote => {
                    Some(RichStyle::Quote)
                }
            };
        if let Some(style) = style {
            spans.push(RichSpan {
                start: start_byte,
                end: end_byte,
                style,
            });
        }
    }
    Ok(sanitize_rich_text(normalize_rich_text(RichText {
        text: text.to_owned(),
        spans,
    })))
}

/// Converts bridge spans to Telegram's UTF-16 entity coordinates.
#[must_use]
pub fn to_telegram(rich: &RichText) -> TelegramMessage {
    let rich = sanitize_rich_text(normalize_rich_text(rich.clone()));
    let entities = rich
        .spans
        .iter()
        .filter_map(|span| {
            let offset = u32::try_from(rich.text[..span.start].encode_utf16().count()).ok()?;
            let length =
                u32::try_from(rich.text[span.start..span.end].encode_utf16().count()).ok()?;
            let (kind, url, language) = match &span.style {
                RichStyle::Bold => (TelegramEntityKind::Bold, None, None),
                RichStyle::Italic => (TelegramEntityKind::Italic, None, None),
                RichStyle::Underline => (TelegramEntityKind::Underline, None, None),
                RichStyle::Strikethrough => (TelegramEntityKind::Strikethrough, None, None),
                RichStyle::Spoiler => (TelegramEntityKind::Spoiler, None, None),
                RichStyle::Code => (TelegramEntityKind::Code, None, None),
                RichStyle::Pre { language } => (TelegramEntityKind::Pre, None, language.clone()),
                RichStyle::Link { url } if safe_url(url) => {
                    (TelegramEntityKind::TextLink, Some(url.clone()), None)
                }
                RichStyle::Link { .. } => return None,
                RichStyle::Quote => (TelegramEntityKind::Blockquote, None, None),
            };
            Some(TelegramEntity {
                kind,
                offset,
                length,
                url,
                language,
            })
        })
        .collect();
    TelegramMessage {
        text: rich.text,
        entities,
    }
}

/// Renders bridge rich text to safe Armada Markdown.
#[must_use]
pub fn render_armada_markdown(rich: &RichText) -> String {
    let rich = sanitize_rich_text(normalize_rich_text(rich.clone()));
    let mut output = String::with_capacity(rich.text.len() + rich.spans.len() * 4);
    for boundary in 0..=rich.text.len() {
        let mut closing: Vec<&RichSpan> = rich
            .spans
            .iter()
            .filter(|span| span.end == boundary)
            .collect();
        closing.sort_by_key(|span| (Reverse(span.start), Reverse(style_rank(&span.style))));
        for span in closing {
            output.push_str(&close_marker(&span.style));
        }
        let mut opening: Vec<&RichSpan> = rich
            .spans
            .iter()
            .filter(|span| span.start == boundary)
            .collect();
        opening.sort_by_key(|span| (Reverse(span.end), style_rank(&span.style)));
        for span in opening {
            output.push_str(&open_marker(&span.style));
        }
        if boundary == rich.text.len() || !rich.text.is_char_boundary(boundary) {
            continue;
        }
        let Some(character) = rich.text[boundary..].chars().next() else {
            continue;
        };
        let in_code = rich.spans.iter().any(|span| {
            span.start <= boundary
                && boundary < span.end
                && matches!(span.style, RichStyle::Code | RichStyle::Pre { .. })
        });
        if (in_code && character == '`') || (!in_code && markdown_special(character)) {
            output.push('\\');
        }
        output.push(character);
    }
    output
}

/// Prepends an unformatted attribution string and shifts all spans.
#[must_use]
pub fn prepend_plain(prefix: &str, rich: &RichText) -> RichText {
    let prefix = sanitize_visible(prefix);
    let shift = prefix.len();
    let mut text = prefix;
    text.push_str(&rich.text);
    RichText {
        text,
        spans: rich
            .spans
            .iter()
            .map(|span| RichSpan {
                start: span.start + shift,
                end: span.end + shift,
                style: span.style.clone(),
            })
            .collect(),
    }
}

/// Splits text without breaking a grapheme and clips spans into each chunk.
#[must_use]
pub fn split_for_telegram(rich: &RichText, maximum_characters: usize) -> Vec<RichText> {
    let rich = sanitize_rich_text(normalize_rich_text(rich.clone()));
    if rich.text.is_empty() || maximum_characters == 0 {
        return Vec::new();
    }
    let mut chunks = Vec::new();
    let mut start = 0;
    while start < rich.text.len() {
        let remainder = &rich.text[start..];
        if remainder.chars().count() <= maximum_characters {
            chunks.push(slice_rich_text(&rich, start, rich.text.len()));
            break;
        }
        let hard_end = remainder
            .grapheme_indices(true)
            .scan(0usize, |characters, (offset, grapheme)| {
                *characters += grapheme.chars().count();
                (*characters <= maximum_characters).then_some(start + offset + grapheme.len())
            })
            .last()
            .unwrap_or_else(|| {
                remainder
                    .grapheme_indices(true)
                    .next()
                    .map_or(rich.text.len(), |(_, grapheme)| start + grapheme.len())
            });
        let candidate = &rich.text[start..hard_end];
        let relative = candidate
            .rfind("\n\n")
            .map(|offset| offset + 2)
            .or_else(|| candidate.rfind('\n').map(|offset| offset + 1))
            .or_else(|| {
                candidate
                    .char_indices()
                    .filter(|(_, character)| character.is_whitespace())
                    .map(|(offset, character)| offset + character.len_utf8())
                    .next_back()
            })
            .filter(|offset| *offset > 0)
            .unwrap_or(hard_end - start);
        let end = start + relative;
        chunks.push(slice_rich_text(&rich, start, end));
        start = end;
    }
    chunks
}

fn parse_inline(source: &str, output: &mut String, spans: &mut Vec<RichSpan>) {
    parse_inline_depth(source, output, spans, 0);
}

fn parse_inline_depth(source: &str, output: &mut String, spans: &mut Vec<RichSpan>, depth: usize) {
    const MAX_NESTING: usize = 32;
    if depth >= MAX_NESTING {
        output.push_str(source);
        return;
    }
    let mut index = 0;
    while index < source.len() {
        let remaining = &source[index..];
        if let Some(escaped) = remaining.strip_prefix('\\')
            && let Some(character) = escaped.chars().next()
        {
            output.push(character);
            index += 1 + character.len_utf8();
            continue;
        }
        if let Some(consumed) = parse_inline_link(remaining, output, spans, depth) {
            index += consumed;
            continue;
        }
        if let Some(consumed) = parse_bold_italic(remaining, output, spans, depth) {
            index += consumed;
            continue;
        }
        if let Some(consumed) = parse_delimited_style(remaining, output, spans, depth) {
            index += consumed;
            continue;
        }
        if let Some(consumed) = parse_inline_code(remaining, output, spans) {
            index += consumed;
            continue;
        }
        let Some(character) = remaining.chars().next() else {
            break;
        };
        output.push(character);
        index += character.len_utf8();
    }
}

fn parse_inline_link(
    source: &str,
    output: &mut String,
    spans: &mut Vec<RichSpan>,
    depth: usize,
) -> Option<usize> {
    if source.starts_with('[') {
        let (consumed, label, url) = markdown_link(source)?;
        let start = output.len();
        parse_inline_depth(label, output, spans, depth + 1);
        if safe_url(url) && output.len() > start {
            spans.push(RichSpan {
                start,
                end: output.len(),
                style: RichStyle::Link {
                    url: url.to_owned(),
                },
            });
        }
        return Some(consumed);
    }
    if source.starts_with('<') {
        let end = source.find('>')?;
        let url = &source[1..end];
        if safe_url(url) {
            let start = output.len();
            output.push_str(url);
            spans.push(RichSpan {
                start,
                end: output.len(),
                style: RichStyle::Link {
                    url: url.to_owned(),
                },
            });
            return Some(end + 1);
        }
    }
    None
}

fn parse_bold_italic(
    source: &str,
    output: &mut String,
    spans: &mut Vec<RichSpan>,
    depth: usize,
) -> Option<usize> {
    let inner = source.strip_prefix("***")?;
    let close = find_unescaped(inner, "***").filter(|close| *close > 0)?;
    let start = output.len();
    parse_inline_depth(&inner[..close], output, spans, depth + 1);
    let end = output.len();
    spans.push(RichSpan {
        start,
        end,
        style: RichStyle::Bold,
    });
    spans.push(RichSpan {
        start,
        end,
        style: RichStyle::Italic,
    });
    Some(close + 6)
}

fn parse_delimited_style(
    source: &str,
    output: &mut String,
    spans: &mut Vec<RichSpan>,
    depth: usize,
) -> Option<usize> {
    for (marker, style) in [
        ("**", RichStyle::Bold),
        ("__", RichStyle::Underline),
        ("~~", RichStyle::Strikethrough),
        ("||", RichStyle::Spoiler),
        ("*", RichStyle::Italic),
        ("_", RichStyle::Italic),
    ] {
        let Some(inner) = source.strip_prefix(marker) else {
            continue;
        };
        let Some(close) = find_unescaped(inner, marker).filter(|close| *close > 0) else {
            continue;
        };
        let start = output.len();
        parse_inline_depth(&inner[..close], output, spans, depth + 1);
        if output.len() > start {
            spans.push(RichSpan {
                start,
                end: output.len(),
                style,
            });
        }
        return Some(marker.len() + close + marker.len());
    }
    None
}

fn parse_inline_code(
    source: &str,
    output: &mut String,
    spans: &mut Vec<RichSpan>,
) -> Option<usize> {
    let inner = source.strip_prefix('`')?;
    let close = find_unescaped(inner, "`").filter(|close| *close > 0)?;
    let start = output.len();
    output.push_str(&inner[..close].replace("\\`", "`"));
    spans.push(RichSpan {
        start,
        end: output.len(),
        style: RichStyle::Code,
    });
    Some(close + 2)
}

fn strip_block_prefix(line: &str) -> (&str, Option<RichStyle>) {
    if let Some(content) = line.strip_prefix("> ") {
        return (content, Some(RichStyle::Quote));
    }
    for marker in ["###### ", "##### ", "#### ", "### ", "## ", "# "] {
        if let Some(content) = line.strip_prefix(marker) {
            return (content, Some(RichStyle::Bold));
        }
    }
    (line, None)
}

fn markdown_link(source: &str) -> Option<(usize, &str, &str)> {
    let label_end = source.find("](")?;
    let rest = &source[label_end + 2..];
    let url_end = rest.find(')')?;
    Some((
        label_end + 2 + url_end + 1,
        &source[1..label_end],
        &rest[..url_end],
    ))
}

fn find_unescaped(source: &str, marker: &str) -> Option<usize> {
    let mut search = 0;
    while let Some(relative) = source[search..].find(marker) {
        let position = search + relative;
        let slashes = source[..position]
            .bytes()
            .rev()
            .take_while(|byte| *byte == b'\\')
            .count();
        if slashes % 2 == 0 {
            return Some(position);
        }
        search = position + marker.len();
    }
    None
}

fn normalize_rich_text(mut rich: RichText) -> RichText {
    rich.spans.retain(|span| {
        span.start < span.end
            && span.end <= rich.text.len()
            && rich.text.is_char_boundary(span.start)
            && rich.text.is_char_boundary(span.end)
            && !matches!(&span.style, RichStyle::Link { url } if !safe_url(url))
    });
    rich.spans
        .sort_by_key(|span| (span.start, Reverse(span.end), style_rank(&span.style)));
    let mut accepted: Vec<RichSpan> = Vec::with_capacity(rich.spans.len());
    for candidate in rich.spans {
        let duplicate = accepted.iter().any(|span| span == &candidate);
        let crossing = accepted.iter().any(|span| {
            (span.start < candidate.start && candidate.start < span.end && span.end < candidate.end)
                || (candidate.start < span.start
                    && span.start < candidate.end
                    && candidate.end < span.end)
        });
        let code_overlap = accepted.iter().any(|span| {
            spans_overlap(span, &candidate) && (is_code(&span.style) || is_code(&candidate.style))
        });
        if !duplicate && !crossing && !code_overlap {
            accepted.push(candidate);
        }
    }
    rich.spans = accepted;
    rich
}

fn sanitize_rich_text(rich: RichText) -> RichText {
    let mut text = String::with_capacity(rich.text.len());
    let mut byte_map = vec![0usize; rich.text.len() + 1];
    for (offset, character) in rich.text.char_indices() {
        byte_map[offset] = text.len();
        if character == '\n'
            || character == '\t'
            || (!character.is_control() && !is_bidi(character))
        {
            text.push(character);
        }
        byte_map[offset + character.len_utf8()] = text.len();
    }
    let spans = rich
        .spans
        .into_iter()
        .filter_map(|span| {
            let start = *byte_map.get(span.start)?;
            let end = *byte_map.get(span.end)?;
            (start < end).then_some(RichSpan {
                start,
                end,
                style: span.style,
            })
        })
        .collect();
    normalize_rich_text(RichText { text, spans })
}

fn sanitize_visible(value: &str) -> String {
    value
        .chars()
        .filter(|character| {
            *character == '\n'
                || *character == '\t'
                || (!character.is_control() && !is_bidi(*character))
        })
        .collect()
}

fn slice_rich_text(rich: &RichText, start: usize, end: usize) -> RichText {
    RichText {
        text: rich.text[start..end].to_owned(),
        spans: rich
            .spans
            .iter()
            .filter_map(|span| {
                let clipped_start = span.start.max(start);
                let clipped_end = span.end.min(end);
                (clipped_start < clipped_end).then(|| RichSpan {
                    start: clipped_start - start,
                    end: clipped_end - start,
                    style: span.style.clone(),
                })
            })
            .collect(),
    }
}

fn utf16_to_byte(text: &str, target: u32) -> Option<usize> {
    let mut units = 0u32;
    for (offset, character) in text.char_indices() {
        if units == target {
            return Some(offset);
        }
        units = units.checked_add(u32::try_from(character.len_utf16()).ok()?)?;
        if units > target {
            return None;
        }
    }
    (units == target).then_some(text.len())
}

fn normalize_newlines(source: &str) -> String {
    source.replace("\r\n", "\n").replace('\r', "\n")
}

fn valid_language(value: &str) -> Option<String> {
    (!value.is_empty()
        && value.len() <= 32
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'-' | b'_' | b'#')))
    .then(|| value.to_owned())
}

fn safe_url(url: &str) -> bool {
    let Some((scheme, remainder)) = url.split_once(':') else {
        return false;
    };
    matches!(
        scheme.to_ascii_lowercase().as_str(),
        "http" | "https" | "mailto"
    ) && !remainder.is_empty()
        && !url
            .chars()
            .any(|character| character.is_control() || is_bidi(character))
}

const fn is_bidi(character: char) -> bool {
    matches!(
        character,
        '\u{061c}'
            | '\u{200b}'
            | '\u{200e}'..='\u{200f}'
            | '\u{202a}'..='\u{202e}'
            | '\u{2060}'..='\u{206f}'
            | '\u{feff}'
    )
}

fn spans_overlap(left: &RichSpan, right: &RichSpan) -> bool {
    left.start < right.end && right.start < left.end
}

const fn is_code(style: &RichStyle) -> bool {
    matches!(style, RichStyle::Code | RichStyle::Pre { .. })
}

const fn style_rank(style: &RichStyle) -> u8 {
    match style {
        RichStyle::Pre { .. } => 0,
        RichStyle::Code => 1,
        RichStyle::Quote => 2,
        RichStyle::Link { .. } => 3,
        RichStyle::Bold => 4,
        RichStyle::Italic => 5,
        RichStyle::Underline => 6,
        RichStyle::Strikethrough => 7,
        RichStyle::Spoiler => 8,
    }
}

fn open_marker(style: &RichStyle) -> String {
    match style {
        RichStyle::Bold => "**".to_owned(),
        RichStyle::Italic => "_".to_owned(),
        RichStyle::Underline => "__".to_owned(),
        RichStyle::Strikethrough => "~~".to_owned(),
        RichStyle::Spoiler => "||".to_owned(),
        RichStyle::Code => "`".to_owned(),
        RichStyle::Pre { language } => format!("```{}\n", language.as_deref().unwrap_or("")),
        RichStyle::Link { .. } => "[".to_owned(),
        RichStyle::Quote => "> ".to_owned(),
    }
}

fn close_marker(style: &RichStyle) -> String {
    match style {
        RichStyle::Bold => "**".to_owned(),
        RichStyle::Italic => "_".to_owned(),
        RichStyle::Underline => "__".to_owned(),
        RichStyle::Strikethrough => "~~".to_owned(),
        RichStyle::Spoiler => "||".to_owned(),
        RichStyle::Code => "`".to_owned(),
        RichStyle::Pre { .. } => "\n```".to_owned(),
        RichStyle::Link { url } => format!("]({})", url.replace('(', "%28").replace(')', "%29")),
        RichStyle::Quote => String::new(),
    }
}

const fn markdown_special(character: char) -> bool {
    matches!(
        character,
        '\\' | '`' | '*' | '_' | '~' | '|' | '[' | ']' | '(' | ')' | '<' | '>' | '#'
    )
}
