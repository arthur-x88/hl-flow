# Validation — 2026-09-28

Local environment: Rust 1.90.0, Linux under WSL2.

| Check | Result |
| --- | --- |
| `cargo test --locked` | 7 pipeline tests, 7 local WebSocket tests, and 1 compiled README example passed |
| `cargo clippy --locked --all-targets -- -D warnings` | Passed |
| `cargo fmt --check` | Passed |
| `RUSTDOCFLAGS="-D warnings" cargo doc --locked --no-deps` | Passed |
| `cargo run --locked --example replay` | 3 accepted prints, 1 duplicate, 1 closed bar, 1 provisional bar |
| `cargo run --locked --example live -- BTC 12` | Connected to public mainnet; received 3 L2 snapshots and 61 trade prints; exited successfully |

The live probe only used public `trades` and `l2Book` subscriptions. It did not
access an account or submit any exchange action. The counts describe one short
run and are not throughput or availability measurements.

Reconnects, resubscription, heartbeat/pong, idle timeout, queue overflow,
cancellation, and malformed frames were verified with controlled local servers.
The short mainnet probe does not establish long-running feed completeness.

Linux and Windows automation results are available in the repository's Actions tab.
