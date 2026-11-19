use std::env;
use std::net::SocketAddr;
use std::time::Duration;

use thiserror::Error;

pub const ENV_GRPC_ADDR: &str = "ROOMD_GRPC_ADDR";
pub const ENV_HTTP_ADDR: &str = "ROOMD_HTTP_ADDR";
pub const ENV_LOG_FORMAT: &str = "ROOMD_LOG_FORMAT";
pub const ENV_LOG_FILTER: &str = "ROOMD_LOG_FILTER";
pub const ENV_JWT_PUBLIC_KEY: &str = "ROOMD_JWT_PUBLIC_KEY";
pub const ENV_JWT_ISSUER: &str = "ROOMD_JWT_ISSUER";
pub const ENV_JWT_KEY_ID: &str = "ROOMD_JWT_KEY_ID";
pub const ENV_ROOM_IDLE_TIMEOUT: &str = "ROOMD_ROOM_IDLE_TIMEOUT";

pub const DEFAULT_GRPC_ADDR: &str = "0.0.0.0:50051";
pub const DEFAULT_HTTP_ADDR: &str = "0.0.0.0:8080";
pub const DEFAULT_LOG_FILTER: &str = "info";
pub const DEFAULT_JWT_ISSUER: &str = "cinema-bot";
pub const DEFAULT_ROOM_IDLE_TIMEOUT: Duration = Duration::from_secs(300);

/// Runtime configuration, read from `ROOMD_*` environment variables.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    /// gRPC listener used by the Go bot.
    pub grpc_addr: SocketAddr,
    /// HTTP listener: health checks now, WebSocket and media segments later.
    pub http_addr: SocketAddr,
    pub log_format: LogFormat,
    /// Fallback filter used when `RUST_LOG` is not set.
    pub log_filter: String,
    /// Base64url Ed25519 public key used to verify room tokens.
    ///
    /// `/ws` rejects connections while this is unset.
    pub jwt_public_key: Option<String>,
    /// Required `iss` claim; `None` disables the issuer check.
    pub jwt_issuer: Option<String>,
    /// Required JWT header `kid`; `None` accepts any key id.
    pub jwt_key_id: Option<String>,
    /// How long an empty room is kept before the idle sweeper closes it.
    /// Overridden by `ROOMD_ROOM_IDLE_TIMEOUT` (integer seconds).
    pub room_idle_timeout: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogFormat {
    Pretty,
    Json,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ConfigError {
    #[error("{key} must be a socket address like 0.0.0.0:50051, got {value:?}")]
    InvalidAddr { key: &'static str, value: String },
    #[error("{key} must be one of: pretty, json; got {value:?}")]
    InvalidLogFormat { key: &'static str, value: String },
    #[error("{key} must be an integer number of seconds, got {value:?}")]
    InvalidRoomIdleTimeout { key: &'static str, value: String },
}

impl Config {
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_lookup(|key| env::var(key).ok())
    }

    /// Reads configuration through `lookup`, which keeps the parser testable
    /// without mutating process-wide environment variables.
    pub fn from_lookup<F>(lookup: F) -> Result<Self, ConfigError>
    where
        F: Fn(&str) -> Option<String>,
    {
        let grpc_addr = parse_addr(
            ENV_GRPC_ADDR,
            lookup(ENV_GRPC_ADDR).unwrap_or_else(|| DEFAULT_GRPC_ADDR.to_owned()),
        )?;
        let http_addr = parse_addr(
            ENV_HTTP_ADDR,
            lookup(ENV_HTTP_ADDR).unwrap_or_else(|| DEFAULT_HTTP_ADDR.to_owned()),
        )?;

        let log_format = match lookup(ENV_LOG_FORMAT) {
            None => LogFormat::Pretty,
            Some(raw) => match raw.trim().to_ascii_lowercase().as_str() {
                "pretty" | "text" => LogFormat::Pretty,
                "json" => LogFormat::Json,
                _ => {
                    return Err(ConfigError::InvalidLogFormat {
                        key: ENV_LOG_FORMAT,
                        value: raw,
                    });
                }
            },
        };

        let log_filter = lookup(ENV_LOG_FILTER)
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| DEFAULT_LOG_FILTER.to_owned());

        let jwt_public_key = lookup(ENV_JWT_PUBLIC_KEY)
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty());
        let jwt_issuer = match lookup(ENV_JWT_ISSUER) {
            None => Some(DEFAULT_JWT_ISSUER.to_owned()),
            Some(value) => {
                let value = value.trim().to_owned();
                (!value.is_empty()).then_some(value)
            }
        };
        let jwt_key_id = lookup(ENV_JWT_KEY_ID)
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty());

        let room_idle_timeout = match lookup(ENV_ROOM_IDLE_TIMEOUT) {
            None => DEFAULT_ROOM_IDLE_TIMEOUT,
            Some(raw) => {
                let secs: u64 =
                    raw.trim()
                        .parse()
                        .map_err(|_| ConfigError::InvalidRoomIdleTimeout {
                            key: ENV_ROOM_IDLE_TIMEOUT,
                            value: raw,
                        })?;
                Duration::from_secs(secs)
            }
        };

        Ok(Self {
            grpc_addr,
            http_addr,
            log_format,
            log_filter,
            jwt_public_key,
            jwt_issuer,
            jwt_key_id,
            room_idle_timeout,
        })
    }
}

fn parse_addr(key: &'static str, raw: String) -> Result<SocketAddr, ConfigError> {
    raw.trim()
        .parse()
        .map_err(|_| ConfigError::InvalidAddr { key, value: raw })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn lookup(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect();
        move |key| map.get(key).cloned()
    }

    #[test]
    fn defaults_are_used_when_nothing_is_set() {
        let config = Config::from_lookup(lookup(&[])).expect("defaults should be valid");

        assert_eq!(config.grpc_addr, "0.0.0.0:50051".parse().unwrap());
        assert_eq!(config.http_addr, "0.0.0.0:8080".parse().unwrap());
        assert_eq!(config.log_format, LogFormat::Pretty);
        assert_eq!(config.log_filter, DEFAULT_LOG_FILTER);
        assert_eq!(config.jwt_public_key, None);
        assert_eq!(config.jwt_issuer.as_deref(), Some(DEFAULT_JWT_ISSUER));
        assert_eq!(config.jwt_key_id, None);
        assert_eq!(config.room_idle_timeout, DEFAULT_ROOM_IDLE_TIMEOUT);
    }

    #[test]
    fn environment_overrides_are_applied() {
        let config = Config::from_lookup(lookup(&[
            (ENV_GRPC_ADDR, "127.0.0.1:7000"),
            (ENV_HTTP_ADDR, "127.0.0.1:7001"),
            (ENV_LOG_FORMAT, " JSON "),
            (ENV_LOG_FILTER, "warn,roomd=trace"),
            (ENV_JWT_PUBLIC_KEY, "  key  "),
            (ENV_JWT_ISSUER, "cinema-bot-test"),
            (ENV_JWT_KEY_ID, " k-1 "),
            (ENV_ROOM_IDLE_TIMEOUT, " 60 "),
        ]))
        .expect("overrides should be valid");

        assert_eq!(config.grpc_addr, "127.0.0.1:7000".parse().unwrap());
        assert_eq!(config.http_addr, "127.0.0.1:7001".parse().unwrap());
        assert_eq!(config.log_format, LogFormat::Json);
        assert_eq!(config.log_filter, "warn,roomd=trace");
        assert_eq!(config.jwt_public_key.as_deref(), Some("key"));
        assert_eq!(config.jwt_issuer.as_deref(), Some("cinema-bot-test"));
        assert_eq!(config.jwt_key_id.as_deref(), Some("k-1"));
        assert_eq!(config.room_idle_timeout, Duration::from_secs(60));
    }

    #[test]
    fn empty_jwt_issuer_disables_the_claim_check() {
        let config =
            Config::from_lookup(lookup(&[(ENV_JWT_ISSUER, " ")])).expect("empty issuer is valid");

        assert_eq!(config.jwt_issuer, None);
    }

    #[test]
    fn invalid_socket_address_is_rejected() {
        let error = Config::from_lookup(lookup(&[(ENV_GRPC_ADDR, "not-an-address")]))
            .expect_err("invalid address must fail");

        assert_eq!(
            error,
            ConfigError::InvalidAddr {
                key: ENV_GRPC_ADDR,
                value: "not-an-address".to_owned(),
            }
        );
    }

    #[test]
    fn invalid_log_format_is_rejected() {
        let error = Config::from_lookup(lookup(&[(ENV_LOG_FORMAT, "xml")]))
            .expect_err("invalid log format must fail");

        assert!(matches!(error, ConfigError::InvalidLogFormat { .. }));
    }
}
