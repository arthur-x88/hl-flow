# hl-flow

[![CI](https://github.com/arthur-x88/hl-flow/actions/workflows/ci.yml/badge.svg)](https://github.com/arthur-x88/hl-flow/actions/workflows/ci.yml)
[![MIT](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/arthur-x88/hl-flow/blob/main/LICENSE)

Public Hyperliquid market data in Rust: discover markets, stream trades and level-2
books (L2), or replay saved JSON frames. Supports spot, perpetuals, HIP-3 assets, and
HIP-4 outcomes on mainnet and testnet.

The client uses [hl-depth](https://github.com/arthur-x88/hl-depth) for decimal
types, metadata, and book validation. Access is read-only and needs no API key or
wallet.

## Quick start

Requires Rust 1.90 or newer.

```bash
git clone https://github.com/arthur-x88/hl-flow.git
cd hl-flow

# Replay the bundled synthetic fixture offline.
cargo run --locked --example replay

# Resolve BTC from metadata, then stream for 15 seconds.
cargo run --locked --example live -- BTC 15
```

The replay accepts three trades, suppresses one duplicate, and closes one
one-minute bar. It leaves the final bar open. To replay your own file:

```bash
cargo run --locked --example replay -- path/to/frames.ndjson
```

Each line must contain a complete Hyperliquid JSON text frame. Live data and
replay use the same decoder.

## Choose a market

List identifiers and precision before subscribing:

```bash
cargo run --locked --example markets -- mainnet
cargo run --locked --example markets -- mainnet xyz
cargo run --locked --example markets -- mainnet --outcomes
```

| Market | Example subscription coin |
| --- | --- |
| Perpetual | `BTC` |
| Spot | `@107` |
| HIP-3 perpetual | `xyz:XYZ100` |
| HIP-4 outcome side | `#12090` |

Use an identifier returned by the selected network. Examples can expire or be
unavailable, and mainnet and testnet have separate listings.

```bash
cargo run --locked --example live -- '@107' 15 mainnet
cargo run --locked --example live -- xyz:XYZ100 15 mainnet
cargo run --locked --example live -- '#12090' 15 mainnet
cargo run --locked --example live -- BTC 15 testnet
```

Quote `#` coins in your shell. The live example resolves metadata before opening
the socket: `meta` and `spotMeta` for spot/perps, or `outcomeMeta` for outcomes.
A `dex:COIN` selects that HIP-3 DEX. Unknown coins and delisted perps fail before
subscription. The duration starts after metadata discovery.

## Use as a library

Add these Git and runtime dependencies to `Cargo.toml`. The crate is distributed
through Git, with `hl-depth` pinned to a public revision.

```toml
[dependencies]
hl-flow = { git = "https://github.com/arthur-x88/hl-flow" }
tokio = { version = "1", features = ["full"] }
```

```rust,no_run
use hl_flow::{
    client::{Client, Config},
    primitives::types::Coin,
    protocol::Event,
};
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::new(Config::mainnet(vec![Coin::new("BTC")?]))?;
    let (tx, mut rx) = mpsc::channel::<Event>(256);
    let worker = tokio::spawn(client.run(tx));

    while let Some(event) = rx.recv().await {
        println!("{event:?}");
        // Consumers that keep a book must clear it on Event::Disconnected.
    }

    worker.await??; // Surface terminal errors, including SlowConsumer.
    Ok(())
}
```

`Client` subscribes to the coins supplied by the caller. To resolve an instrument
and its precision first, use `InfoClient`:

```rust,no_run
use hl_flow::{info::InfoClient, network::Network, primitives::types::Coin};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let info = InfoClient::new(Network::Mainnet)?;
    let catalog = info.catalog(None).await?;
    let coin = Coin::new("@107")?;
    let market = catalog.resolve(&coin).ok_or("market unavailable")?;
    if let Some(rules) = market.rules() {
        println!("{}: size precision {}", market.label(), rules.size_decimals());
    }
    Ok(())
}
```

Use the same `Network` for discovery and streaming. Refresh catalogs when needed;
metadata calls are separate requests and do not form an atomic snapshot.

## Connection and data handling

The client sends a JSON ping every 20 seconds, times out after 50 seconds without
a received frame, and gives each connection or write operation 10 seconds. It
allows five retries over a run, using capped exponential delays. Each connection
resends both subscriptions for every coin. `Connected` means the requests were
sent; acknowledgements and market data follow separately.

Consumers own their cached state. On `Disconnected`, clear books and mark the
affected candle interval incomplete. Reconnecting cannot recover every trade
missed during the gap. If the bounded queue fills, `run` returns `SlowConsumer`.
Monitor the worker's result and invalidate cached state on terminal errors too.
Dropping the receiver cancels the client.

| Component | Behavior |
| --- | --- |
| `protocol` | Decodes `trades` and `l2Book`; rejects an entire trade batch if any print is invalid. Unknown channels become `Ignored`; malformed payloads and server errors end the run. |
| `dedup` | Tracks `(time, coin, tid)` in a bounded FIFO window. Keep it across reconnects; an evicted key can be accepted again. |
| `candle` | Builds OHLCV in exchange-time order. A later nonempty bucket closes the current bar. Late trades return an error; gaps stay empty. |

The live example prints raw trades and replaces book snapshots. The replay
example also applies deduplication and candle aggregation. Account access and
order execution are outside this library.

## HIP-4 outcomes

`InfoClient::outcome_catalog()` resolves both sides of each listed outcome. For
outcome `1`, the market coins are `#10` and `#11`. The corresponding `+` token
names are rejected as subscription coins. Labels and resolution descriptions
come from metadata.

Outcome books and trades use the same decoding and aggregation APIs as other
markets. `Instrument::rules()` returns `None` for outcomes because `outcomeMeta`
does not expose `szDecimals`. Order precision, settlement, question-level
portfolio accounting, and split/merge/negate actions are not implemented.

## Checks

```bash
cargo fmt --check
cargo test --locked
cargo clippy --locked --all-targets -- -D warnings
cargo doc --locked --no-deps
```

CI runs on Linux and Windows without exchange access. Local servers test HTTP
metadata requests, WebSocket reconnects, resubscription, heartbeats, timeouts,
and queue overflow. The Rust examples above are compiled as doctests. Recorded
results, including mainnet and testnet checks, are in
[VALIDATION.md](https://github.com/arthur-x88/hl-flow/blob/main/VALIDATION.md).

For 0.1 users: replace `Config::endpoint` assignments with
`Config::new(Network::Mainnet, coins)` or `Config::testnet(coins)`. Read the URL
through `config.endpoint()`. `Error::Exchange` is now `Error::Hyperliquid`.
See the [changelog](https://github.com/arthur-x88/hl-flow/blob/main/CHANGELOG.md).

## Hyperliquid references

- [Subscriptions and payloads](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions)
- [Heartbeats](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/timeouts-and-heartbeats)
- [Perpetual metadata](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint/perpetuals)
- [Spot and outcome metadata](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint/spot)
- [HIP-4 markets](https://hyperliquid.gitbook.io/hyperliquid-docs/hyperliquid-improvement-proposals-hips/hip-4-outcome-markets)
- [Outcome identifiers](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/asset-ids)

## License

Released under the [MIT License](https://github.com/arthur-x88/hl-flow/blob/main/LICENSE).
The license file contains the terms and retained copyright notices, including
those for adapted code. This project is not affiliated with Hyperliquid.
