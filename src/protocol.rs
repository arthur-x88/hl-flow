//! Shared decoding for recorded NDJSON and live text frames.

use crate::{
    primitives::{
        types::Coin,
        wire::{BookSnapshot, Trade},
    },
    Error,
};
use serde_json::{json, Value};

/// A validated market message or a transport/control event.
#[derive(Debug, Clone)]
pub enum Event {
    /// A connection was established and both subscriptions sent for every coin.
    Connected,
    /// Transport failed; invalidate cached books before awaiting fresh snapshots.
    Disconnected {
        /// Diagnostic supplied by the transport.
        reason: String,
        /// Number of retries already used.
        retries_used: u32,
    },
    /// Complete L2 snapshot (published depth only).
    Book(BookSnapshot),
    /// Validated positive-size prints. Use `TradeDeduper` before aggregation.
    Trades(Vec<Trade>),
    /// Exchange subscription acknowledgement.
    Subscribed,
    /// Application-level heartbeat response.
    Pong,
    /// A future or unrequested channel, deliberately not interpreted as market data.
    Ignored(String),
}

/// Build public `trades` and `l2Book` subscriptions, preserving the exact coin name.
pub fn subscriptions(coin: &Coin) -> [String; 2] {
    ["trades", "l2Book"].map(|kind| {
        json!({
            "method": "subscribe", "subscription": {"type": kind, "coin": coin.as_str()}
        })
        .to_string()
    })
}

/// Decode one complete WebSocket JSON text frame; never partially accept a trade batch.
pub fn decode(text: &str) -> Result<Event, Error> {
    let message: Value = serde_json::from_str(text)?;
    let channel = message
        .get("channel")
        .and_then(Value::as_str)
        .ok_or_else(|| Error::Exchange("frame is missing a string channel".into()))?;
    match channel {
        "trades" => {
            let trades: Vec<Trade> = serde_json::from_value(message["data"].clone())?;
            for trade in &trades {
                trade.validate()?;
            }
            Ok(Event::Trades(trades))
        }
        "l2Book" => {
            let book: BookSnapshot = serde_json::from_value(message["data"].clone())?;
            book.validate()?;
            Ok(Event::Book(book))
        }
        "subscriptionResponse" => Ok(Event::Subscribed),
        "pong" => Ok(Event::Pong),
        "error" => Err(Error::Exchange(message["data"].to_string())),
        other => Ok(Event::Ignored(other.to_owned())),
    }
}
