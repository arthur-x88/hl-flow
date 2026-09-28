//! Event-time OHLCV with explicit late-trade rejection and no synthetic empty bars.

use crate::{
    primitives::{types::Coin, wire::Trade, Decimal, Error as MarketError},
    Error,
};
use serde::Serialize;

/// A bar built from trade prints. The currently open bar is provisional.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Candle {
    /// Exchange coin identifier.
    pub coin: Coin,
    /// Inclusive UTC millisecond bucket start.
    pub start_ms: u64,
    /// First price.
    pub open: Decimal,
    /// Maximum price.
    pub high: Decimal,
    /// Minimum price.
    pub low: Decimal,
    /// Latest price.
    pub close: Decimal,
    /// Total base quantity.
    pub volume: Decimal,
    /// Number of accepted prints.
    pub trades: u64,
}

/// One coin and one fixed interval. Deduplicate prints before calling `push`.
pub struct CandleBuilder {
    coin: Coin,
    interval_ms: u64,
    last_time: Option<u64>,
    current: Option<Candle>,
}

impl CandleBuilder {
    /// Construct a builder with a positive interval.
    pub fn new(coin: Coin, interval_ms: u64) -> Result<Self, Error> {
        if interval_ms == 0 {
            return Err(Error::Config("bar interval must be positive".into()));
        }
        Ok(Self {
            coin,
            interval_ms,
            last_time: None,
            current: None,
        })
    }

    /// Add a nondecreasing-time trade, returning the previous bar on a bucket change.
    /// Rejected input and arithmetic overflow leave state intact.
    pub fn push(&mut self, trade: &Trade) -> Result<Option<Candle>, Error> {
        trade.validate()?;
        if trade.coin != self.coin {
            return Err(MarketError::Invalid("trade coin does not match candle".into()).into());
        }
        if let Some(last) = self.last_time {
            if trade.time < last {
                return Err(MarketError::Stale {
                    received: trade.time,
                    current: last,
                }
                .into());
            }
        }
        let start_ms = trade.time / self.interval_ms * self.interval_ms;
        let px = trade.px.value();
        if let Some(bar) = &mut self.current {
            if start_ms == bar.start_ms {
                let volume = bar
                    .volume
                    .checked_add(trade.sz.value())
                    .ok_or(MarketError::Overflow)?;
                let trades = bar.trades.checked_add(1).ok_or(MarketError::Overflow)?;
                bar.high = bar.high.max(px);
                bar.low = bar.low.min(px);
                bar.close = px;
                bar.volume = volume;
                bar.trades = trades;
                self.last_time = Some(trade.time);
                return Ok(None);
            }
        }
        let previous = self.current.replace(Candle {
            coin: self.coin.clone(),
            start_ms,
            open: px,
            high: px,
            low: px,
            close: px,
            volume: trade.sz.value(),
            trades: 1,
        });
        self.last_time = Some(trade.time);
        Ok(previous)
    }

    /// The provisional bar, which is not closed by a wall-clock timer.
    pub fn current(&self) -> Option<&Candle> {
        self.current.as_ref()
    }
}
