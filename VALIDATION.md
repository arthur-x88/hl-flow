# Validation — 0.2.0 — 2026-09-28

Local environment: Rust 1.90.0, Linux under WSL2.

| Check | Result |
| --- | --- |
| `cargo test --locked` | 8 pipeline tests, 7 local WebSocket tests, 4 local HTTP tests, 1 network-selection test, and 2 compiled README examples passed |
| `cargo clippy --locked --all-targets -- -D warnings` | Passed |
| `cargo fmt --check` | Passed |
| `RUSTDOCFLAGS="-D warnings" cargo doc --locked --no-deps` | Passed |
| `cargo run --locked --example replay` | 3 accepted prints, 1 duplicate, 1 closed bar, 1 provisional bar |
| `cargo run --locked --example live -- BTC 8` | Resolved mainnet metadata; received 2 L2 snapshots and 47 trade prints; exited successfully |
| `cargo run --locked --example live -- '#12090' 8 mainnet` | Resolved HIP-4 metadata; received 2 L2 snapshots and 30 recent trade prints; exited successfully |
| `cargo run --locked --example live -- '#102170' 8 testnet` | Resolved HIP-4 metadata; received 3 empty L2 snapshots and 0 trade prints; exited successfully |

The live probe only used public `trades` and `l2Book` subscriptions. It did not
access an account or submit any exchange action. The counts describe one short
run and are not throughput or availability measurements.

Metadata discovery used only public `meta`, `spotMeta`, and `outcomeMeta`
requests. Trade counts include the recent-trade batch returned at subscription,
not just trades occurring during the probe. Outcome identifiers can disappear
from metadata between calls; examples reject unavailable markets before opening
the socket. HIP-4 order precision remains unavailable, as `outcomeMeta` does not
provide `szDecimals`.

Reconnects, resubscription, heartbeat/pong, idle timeout, queue overflow,
cancellation, and malformed frames were verified with controlled local servers.
The short mainnet probe does not establish long-running feed completeness.

Linux and Windows automation results are available in the repository's Actions tab.
