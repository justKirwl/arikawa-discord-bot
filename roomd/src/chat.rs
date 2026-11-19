//! Validation and parsing of client `chat_send` and `reaction` messages.
//!
//! The WebSocket handler passes the already-decoded JSON object fields. This
//! module only enforces the protocol §3.4 and §3.5 constraints and returns
//! typed values; on failure it returns the `bad_message` error code for the
//! handler to answer with.

/// A validated `chat_send` message, ready to broadcast as a `chat` event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChatMessage {
    pub user_id: String,
    pub text: String,
    pub client_msg_id: Option<String>,
}

/// A validated `reaction` message, ready to broadcast as a `reaction` event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reaction {
    pub user_id: String,
    pub emoji: String,
}

/// Error code returned when a required field is missing, has the wrong type or
/// violates a protocol length limit.
const BAD_MESSAGE: &str = "bad_message";

/// Validates the fields of a `chat_send` message.
///
/// `text` must be a string whose trimmed form is 1–500 characters. When
/// present, `client_msg_id` must be a non-empty string of at most 64
/// characters.
pub fn parse_chat(
    fields: &serde_json::Map<String, serde_json::Value>,
    user_id: &str,
) -> Result<ChatMessage, &'static str> {
    let text = fields
        .get("text")
        .and_then(serde_json::Value::as_str)
        .ok_or(BAD_MESSAGE)?
        .trim();
    let text_len = text.chars().count();
    if !(1..=500).contains(&text_len) {
        return Err(BAD_MESSAGE);
    }

    let client_msg_id = match fields.get("client_msg_id") {
        Some(value) => {
            let id = value.as_str().ok_or(BAD_MESSAGE)?;
            if id.is_empty() || id.chars().count() > 64 {
                return Err(BAD_MESSAGE);
            }
            Some(id.to_owned())
        }
        None => None,
    };

    Ok(ChatMessage {
        user_id: user_id.to_owned(),
        text: text.to_owned(),
        client_msg_id,
    })
}

/// Validates the fields of a `reaction` message.
///
/// `emoji` must be a string whose trimmed form is non-empty and at most 32
/// UTF-8 bytes.
pub fn parse_reaction(
    fields: &serde_json::Map<String, serde_json::Value>,
    user_id: &str,
) -> Result<Reaction, &'static str> {
    let emoji = fields
        .get("emoji")
        .and_then(serde_json::Value::as_str)
        .ok_or(BAD_MESSAGE)?
        .trim();
    // shortcut: "single emoji" is enforced as a byte-length limit only;
    // upgrade to a grapheme-aware check when exactly one emoji must be checked.
    if emoji.is_empty() || emoji.len() > 32 {
        return Err(BAD_MESSAGE);
    }

    Ok(Reaction {
        user_id: user_id.to_owned(),
        emoji: emoji.to_owned(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chat_fields(text: serde_json::Value) -> serde_json::Map<String, serde_json::Value> {
        let mut fields = serde_json::Map::new();
        fields.insert("text".to_owned(), text);
        fields
    }

    fn reaction_fields(emoji: serde_json::Value) -> serde_json::Map<String, serde_json::Value> {
        let mut fields = serde_json::Map::new();
        fields.insert("emoji".to_owned(), emoji);
        fields
    }

    fn json_string(value: &str) -> serde_json::Value {
        serde_json::Value::String(value.to_owned())
    }

    #[test]
    fn chat_accepts_minimal_valid_text() {
        let message = parse_chat(&chat_fields(json_string("hi")), "u-1").expect("valid chat");

        assert_eq!(message.user_id, "u-1");
        assert_eq!(message.text, "hi");
        assert_eq!(message.client_msg_id, None);
    }

    #[test]
    fn chat_trims_text_for_validation_and_storage() {
        let message = parse_chat(&chat_fields(json_string("  hi \n")), "u-1").expect("valid chat");

        assert_eq!(message.text, "hi");
    }

    #[test]
    fn chat_rejects_empty_text() {
        assert_eq!(
            parse_chat(&chat_fields(json_string("")), "u-1").unwrap_err(),
            BAD_MESSAGE
        );
    }

    #[test]
    fn chat_rejects_whitespace_only_text() {
        assert_eq!(
            parse_chat(&chat_fields(json_string("   \t\n")), "u-1").unwrap_err(),
            BAD_MESSAGE
        );
    }

    #[test]
    fn chat_accepts_500_chars_and_rejects_501() {
        let at_limit = "a".repeat(500);
        let over_limit = "a".repeat(501);

        let message = parse_chat(&chat_fields(json_string(&at_limit)), "u-1").expect("500 chars");
        assert_eq!(message.text.len(), 500);
        assert_eq!(
            parse_chat(&chat_fields(json_string(&over_limit)), "u-1").unwrap_err(),
            BAD_MESSAGE
        );
    }

    #[test]
    fn chat_rejects_missing_or_non_string_text() {
        assert_eq!(
            parse_chat(&serde_json::Map::new(), "u-1").unwrap_err(),
            BAD_MESSAGE
        );
        assert_eq!(
            parse_chat(&chat_fields(serde_json::Value::Null), "u-1").unwrap_err(),
            BAD_MESSAGE
        );
        assert_eq!(
            parse_chat(&chat_fields(serde_json::json!(42)), "u-1").unwrap_err(),
            BAD_MESSAGE
        );
    }

    #[test]
    fn chat_accepts_64_char_client_msg_id_and_rejects_65() {
        let at_limit = "c".repeat(64);
        let over_limit = "c".repeat(65);
        let mut fields = chat_fields(json_string("hi"));
        fields.insert("client_msg_id".to_owned(), json_string(&at_limit));

        let message = parse_chat(&fields, "u-1").expect("64 char id");
        assert_eq!(message.client_msg_id.as_deref(), Some(at_limit.as_str()));

        fields.insert("client_msg_id".to_owned(), json_string(&over_limit));
        assert_eq!(parse_chat(&fields, "u-1").unwrap_err(), BAD_MESSAGE);
    }

    #[test]
    fn chat_rejects_empty_or_non_string_client_msg_id() {
        let mut fields = chat_fields(json_string("hi"));

        fields.insert("client_msg_id".to_owned(), json_string(""));
        assert_eq!(parse_chat(&fields, "u-1").unwrap_err(), BAD_MESSAGE);

        fields.insert("client_msg_id".to_owned(), serde_json::Value::Null);
        assert_eq!(parse_chat(&fields, "u-1").unwrap_err(), BAD_MESSAGE);
    }

    #[test]
    fn reaction_trims_emoji_for_validation_and_storage() {
        let reaction =
            parse_reaction(&reaction_fields(json_string(" 🍿 ")), "u-1").expect("valid reaction");

        assert_eq!(reaction.user_id, "u-1");
        assert_eq!(reaction.emoji, "🍿");
    }

    #[test]
    fn reaction_accepts_32_bytes_and_rejects_33() {
        // "🍿" is 4 UTF-8 bytes: eight of them are exactly 32 bytes.
        let at_limit = "🍿".repeat(8);
        let over_limit = format!("{at_limit}a");
        assert_eq!(at_limit.len(), 32);
        assert_eq!(over_limit.len(), 33);

        let reaction =
            parse_reaction(&reaction_fields(json_string(&at_limit)), "u-1").expect("32 bytes");
        assert_eq!(reaction.emoji, at_limit);
        assert_eq!(
            parse_reaction(&reaction_fields(json_string(&over_limit)), "u-1").unwrap_err(),
            BAD_MESSAGE
        );
    }

    #[test]
    fn reaction_rejects_empty_whitespace_and_non_string_emoji() {
        assert_eq!(
            parse_reaction(&reaction_fields(json_string("")), "u-1").unwrap_err(),
            BAD_MESSAGE
        );
        assert_eq!(
            parse_reaction(&reaction_fields(json_string("  ")), "u-1").unwrap_err(),
            BAD_MESSAGE
        );
        assert_eq!(
            parse_reaction(&serde_json::Map::new(), "u-1").unwrap_err(),
            BAD_MESSAGE
        );
        assert_eq!(
            parse_reaction(&reaction_fields(serde_json::json!(7)), "u-1").unwrap_err(),
            BAD_MESSAGE
        );
    }
}
