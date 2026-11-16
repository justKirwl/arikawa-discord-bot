//! Room-token verification (protocol §12).
//!
//! Tokens are short-lived JWTs signed by the Go bot with Ed25519 (`EdDSA`).
//! `roomd` only needs the public key: it never calls the bot to validate a join.

use base64::Engine;
use base64::engine::general_purpose::{STANDARD_NO_PAD, URL_SAFE_NO_PAD};
use jsonwebtoken::errors::ErrorKind;
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Clock skew allowed when checking `exp`, per protocol §12.
pub const JWT_LEEWAY_SECS: u64 = 30;

/// Role of a participant inside a room.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    Host,
    Viewer,
}

impl Role {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Host => "host",
            Self::Viewer => "viewer",
        }
    }
}

/// Claims issued by the Go bot (protocol §12). Unknown claims are ignored.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Claims {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub iss: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sub: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub guild_id: Option<String>,
    pub room_id: String,
    pub user_id: String,
    pub role: Role,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub iat: Option<u64>,
    pub exp: u64,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum AuthError {
    #[error("invalid token")]
    Invalid,
    #[error("token expired")]
    Expired,
    #[error("invalid JWT configuration: {0}")]
    Config(String),
}

/// Verifies room JWTs with a single Ed25519 public key.
#[derive(Debug)]
pub struct JwtAuth {
    key: DecodingKey,
    validation: Validation,
    key_id: Option<String>,
}

impl JwtAuth {
    /// `public_key` is a base64(URL-safe) encoded raw 32-byte Ed25519 key, i.e.
    /// the JWK `x` component. When `issuer` is set the `iss` claim is required.
    pub fn new(
        public_key: &str,
        issuer: Option<String>,
        key_id: Option<String>,
    ) -> Result<Self, AuthError> {
        let raw = decode_public_key(public_key)?;
        let canonical = URL_SAFE_NO_PAD.encode(raw);
        let key = DecodingKey::from_ed_components(&canonical)
            .map_err(|error| AuthError::Config(error.to_string()))?;

        let mut validation = Validation::new(Algorithm::EdDSA);
        validation.leeway = JWT_LEEWAY_SECS;
        // The protocol defines no audience; do not reject tokens that carry one.
        validation.validate_aud = false;

        if let Some(issuer) = issuer.as_deref().map(str::trim).filter(|v| !v.is_empty()) {
            validation.set_issuer(&[issuer]);
            validation.required_spec_claims.insert("iss".to_owned());
        }

        Ok(Self {
            key,
            validation,
            key_id: key_id
                .map(|value| value.trim().to_owned())
                .filter(|value| !value.is_empty()),
        })
    }

    /// Verifies signature, `exp`, issuer and, when configured, `kid`.
    pub fn verify(&self, token: &str) -> Result<Claims, AuthError> {
        let header = decode_header(token).map_err(|_| AuthError::Invalid)?;
        if header.alg != Algorithm::EdDSA {
            return Err(AuthError::Invalid);
        }
        if let Some(expected) = self.key_id.as_deref()
            && header.kid.as_deref() != Some(expected)
        {
            return Err(AuthError::Invalid);
        }

        let data = decode::<Claims>(token, &self.key, &self.validation).map_err(|error| {
            if matches!(error.kind(), ErrorKind::ExpiredSignature) {
                AuthError::Expired
            } else {
                AuthError::Invalid
            }
        })?;

        if data.claims.user_id.trim().is_empty() || data.claims.room_id.trim().is_empty() {
            return Err(AuthError::Invalid);
        }

        Ok(data.claims)
    }
}

fn decode_public_key(raw: &str) -> Result<[u8; 32], AuthError> {
    let trimmed = raw
        .trim()
        .trim_matches(|c| c == '"' || c == '\'')
        .trim_end_matches('=');

    for engine in [&URL_SAFE_NO_PAD, &STANDARD_NO_PAD] {
        if let Ok(bytes) = engine.decode(trimmed)
            && let Ok(key) = <[u8; 32]>::try_from(bytes)
        {
            return Ok(key);
        }
    }

    Err(AuthError::Config(
        "public key must be a base64 encoded 32-byte Ed25519 key".to_owned(),
    ))
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use ed25519_dalek::SigningKey;
    use jsonwebtoken::{EncodingKey, Header, encode};

    use super::*;

    const SEED: [u8; 32] = [7u8; 32];
    const OTHER_SEED: [u8; 32] = [9u8; 32];

    fn public_key(seed: [u8; 32]) -> String {
        URL_SAFE_NO_PAD.encode(SigningKey::from_bytes(&seed).verifying_key().to_bytes())
    }

    fn encoding_key(seed: [u8; 32]) -> EncodingKey {
        let mut pkcs8 = vec![
            0x30, 0x2e, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x04, 0x22,
            0x04, 0x20,
        ];
        pkcs8.extend_from_slice(&seed);
        EncodingKey::from_ed_der(&pkcs8)
    }

    fn now() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    fn claims(room_id: &str, exp: u64) -> Claims {
        Claims {
            iss: Some("cinema-bot".to_owned()),
            sub: Some("42".to_owned()),
            guild_id: Some("guild".to_owned()),
            room_id: room_id.to_owned(),
            user_id: "42".to_owned(),
            role: Role::Host,
            iat: Some(now()),
            exp,
        }
    }

    fn token(seed: [u8; 32], kid: &str, claims: &Claims) -> String {
        let mut header = Header::new(Algorithm::EdDSA);
        header.kid = Some(kid.to_owned());
        encode(&header, claims, &encoding_key(seed)).unwrap()
    }

    fn auth() -> JwtAuth {
        JwtAuth::new(
            &public_key(SEED),
            Some("cinema-bot".to_owned()),
            Some("dev-1".to_owned()),
        )
        .unwrap()
    }

    #[test]
    fn accepts_a_valid_token() {
        let claims = claims("room-1", now() + 120);

        let verified = auth().verify(&token(SEED, "dev-1", &claims)).unwrap();

        assert_eq!(verified.room_id, "room-1");
        assert_eq!(verified.user_id, "42");
        assert_eq!(verified.role, Role::Host);
    }

    #[test]
    fn rejects_an_expired_token() {
        let claims = claims("room-1", now() - JWT_LEEWAY_SECS - 1);

        let error = auth().verify(&token(SEED, "dev-1", &claims)).unwrap_err();

        assert_eq!(error, AuthError::Expired);
    }

    #[test]
    fn rejects_a_token_signed_by_another_key() {
        let claims = claims("room-1", now() + 120);

        let error = auth()
            .verify(&token(OTHER_SEED, "dev-1", &claims))
            .unwrap_err();

        assert_eq!(error, AuthError::Invalid);
    }

    #[test]
    fn rejects_an_unknown_key_id() {
        let claims = claims("room-1", now() + 120);

        let error = auth().verify(&token(SEED, "other", &claims)).unwrap_err();

        assert_eq!(error, AuthError::Invalid);
    }

    #[test]
    fn rejects_a_different_issuer() {
        let mut claims = claims("room-1", now() + 120);
        claims.iss = Some("someone-else".to_owned());

        let error = auth().verify(&token(SEED, "dev-1", &claims)).unwrap_err();

        assert_eq!(error, AuthError::Invalid);
    }

    #[test]
    fn rejects_a_missing_issuer() {
        let mut claims = claims("room-1", now() + 120);
        claims.iss = None;

        let error = auth().verify(&token(SEED, "dev-1", &claims)).unwrap_err();

        assert_eq!(error, AuthError::Invalid);
    }

    #[test]
    fn rejects_another_algorithm() {
        let claims = claims("room-1", now() + 120);
        let token = encode(
            &Header::new(Algorithm::HS256),
            &claims,
            &EncodingKey::from_secret(b"secret").unwrap(),
        )
        .unwrap();

        let error = auth().verify(&token).unwrap_err();

        assert_eq!(error, AuthError::Invalid);
    }

    #[test]
    fn rejects_empty_user_id() {
        let mut claims = claims("room-1", now() + 120);
        claims.user_id = "  ".to_owned();

        let error = auth().verify(&token(SEED, "dev-1", &claims)).unwrap_err();

        assert_eq!(error, AuthError::Invalid);
    }

    #[test]
    fn accepts_padded_standard_base64_keys() {
        use base64::engine::general_purpose::STANDARD;

        let key = STANDARD.encode(SigningKey::from_bytes(&SEED).verifying_key().to_bytes());

        assert!(JwtAuth::new(&key, None, None).is_ok());
    }

    #[test]
    fn rejects_keys_of_the_wrong_length() {
        let short = public_key(SEED);
        let error = JwtAuth::new(&short[..8], None, None).unwrap_err();

        assert!(matches!(error, AuthError::Config(_)));
    }
}
