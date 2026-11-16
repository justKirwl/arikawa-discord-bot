use std::env;
use std::net::SocketAddr;

use thiserror::Error;

pub const ENV_GRPC_ADDR: &str = "ROOMD_GRPC_ADDR";
pub const ENV_HTTP_ADDR: &str = "ROOMD_HTTP_ADDR";
pub const ENV_LOG_FORMAT: &str = "ROOMD_LOG_FORMAT";
pub const ENV_LOG_FILTER: &str = "ROOMD_LOG_FILTER";

pub const DEFAULT_GRPC_ADDR: &str = "0.0.0.0:50051";
pub const DEFAULT_HTTP_ADDR: &str = "0.0.0.0:8080";
pub const DEFAULT_LOG_FILTER: &str = "info";

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

        Ok(Self {
            grpc_addr,
            http_addr,
            log_format,
            log_filter,
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
    }

    #[test]
    fn environment_overrides_are_applied() {
        let config = Config::from_lookup(lookup(&[
            (ENV_GRPC_ADDR, "127.0.0.1:7000"),
            (ENV_HTTP_ADDR, "127.0.0.1:7001"),
            (ENV_LOG_FORMAT, " JSON "),
            (ENV_LOG_FILTER, "warn,roomd=trace"),
        ]))
        .expect("overrides should be valid");

        assert_eq!(config.grpc_addr, "127.0.0.1:7000".parse().unwrap());
        assert_eq!(config.http_addr, "127.0.0.1:7001".parse().unwrap());
        assert_eq!(config.log_format, LogFormat::Json);
        assert_eq!(config.log_filter, "warn,roomd=trace");
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
