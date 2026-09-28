//! Public Hyperliquid market data: validate wire messages, stream, and replay.
//! No private keys, signing, order submission, or account access.

#![doc = include_str!("../README.md")]

pub mod candle;
pub mod client;
pub mod dedup;
pub mod protocol;

pub use hyperliquid_primitives as primitives;

/// Errors from decoding, transport, configuration, and downstream delivery.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Invalid JSON or unexpected wire shape.
    #[error("invalid JSON: {0}")]
    Json(#[from] serde_json::Error),
    /// A market value or book violates its invariants.
    #[error(transparent)]
    Market(#[from] primitives::Error),
    /// Transport failed or timed out.
    #[error("transport: {0}")]
    Transport(String),
    /// Exchange explicitly rejected a request.
    #[error("exchange: {0}")]
    Exchange(String),
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
