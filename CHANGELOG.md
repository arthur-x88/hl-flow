# Changelog

## 0.2.0 — 2026-09-28

- Replace arbitrary public WebSocket URLs with typed Hyperliquid mainnet/testnet selection.
- Add public metadata discovery, HIP-3 DEX selection, and a market-listing example.
- Resolve market identifiers and precision before the live example opens a feed.
- Share explicit TLS configuration across HTTP metadata and WebSocket transport.
- Rename the protocol error to `Hyperliquid`; keep local endpoint overrides in unit tests only.
- Add controlled HTTP tests for request bodies, failure responses, and metadata resolution.
- Add HIP-4 `outcomeMeta` discovery and outcome book/trade streaming on the selected network.
- Extend the live and market-listing examples with `#` coins and outcome catalogs.

## 0.1.0 — 2026-09-28

- Start a fresh history for the focused Hyperliquid adaptation.
- Add public trades and L2 subscriptions, validation, heartbeats, reconnects, and explicit gap events.
- Bound delivery and deduplication; add event-time OHLCV and NDJSON replay.
- Add live and synthetic examples, local WebSocket integration tests, and CI.
