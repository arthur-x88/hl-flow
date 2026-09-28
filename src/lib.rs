//! Public Hyperliquid market data: validate wire messages, stream, and replay.
//! No private keys, signing, order submission, or account access.

#![doc = include_str!("../README.md")]

pub mod candle;
pub mod client;
pub mod dedup;
pub mod info;
pub mod network;
pub mod protocol;

pub use hl_depth as primitives;

/// Errors from decoding, transport, configuration, and downstream delivery.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Invalid JSON or unexpected wire shape.
    #[error("invalid JSON: {0}")]
    Json(#[from] serde_json::Error),
    /// Public Hyperliquid info request failed (HTTP status, timeout, or TLS).
    #[error("Hyperliquid info request: {0}")]
    Http(#[from] reqwest::Error),
    /// A market value or book violates its invariants.
    #[error(transparent)]
    Market(#[from] primitives::Error),
    /// Transport failed or timed out.
    #[error("transport: {0}")]
    Transport(String),
    /// Hyperliquid explicitly rejected a request or returned an unexpected payload.
    #[error("Hyperliquid protocol: {0}")]
    Hyperliquid(String),
    /// Configuration is not usable.
    #[error("configuration: {0}")]
    Config(String),
    /// Bounded delivery is full. The caller must restart and treat the gap explicitly.
    #[error("consumer fell behind; feed stopped instead of silently dropping market data")]
    SlowConsumer,
    /// The receiver has been dropped.
    #[error("consumer closed")]
    Closed,
}
