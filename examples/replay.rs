//! Executable examples and regression checks for the public market-data API.
use hyperliquid_stream::{
    candle::CandleBuilder,
    dedup::TradeDeduper,
    primitives::book::OrderBook,
    protocol::{decode, Event},
};
use std::collections::HashMap;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let input = match std::env::args().nth(1) {
        Some(path) => std::fs::read_to_string(path)?,
        None => include_str!("data/btc.ndjson").to_owned(),
    };
    let mut seen = TradeDeduper::new(10_000)?;
    let mut builders = HashMap::new();
    let mut books = HashMap::new();
    let mut accepted = 0;
    let mut duplicates = 0;
    for (index, line) in input.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let event = decode(line).map_err(|e| format!("line {}: {e}", index + 1))?;
        match event {
            Event::Trades(trades) => {
                for trade in trades {
                    if !seen.accept(&trade) {
                        duplicates += 1;
                        continue;
                    }
                    let builder = builders
                        .entry(trade.coin.clone())
                        .or_insert(CandleBuilder::new(trade.coin.clone(), 60_000)?);
                    if let Some(bar) = builder.push(&trade)? {
                        println!("closed_bar {}", serde_json::to_string(&bar)?);
                    }
                    accepted += 1;
                }
            }
            Event::Book(snapshot) => {
                let coin = snapshot.coin.clone();
                let book = books
                    .entry(coin.clone())
                    .or_insert(OrderBook::new(coin.clone()));
                book.replace(snapshot)?;
                println!(
                    "book {coin} spread={} mid={}",
                    book.spread().map_or("n/a".into(), |v| v.to_string()),
                    book.mid_price().map_or("n/a".into(), |v| v.to_string())
                );
            }
            _ => {}
        }
    }
    println!(
        "accepted_trades={accepted} duplicates={duplicates} open_bars={}",
        builders.len()
    );
    Ok(())
}
