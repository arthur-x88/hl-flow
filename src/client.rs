//! Async transport adapted around bounded delivery and an explicit reconnect budget.

use crate::{
    primitives::types::Coin,
    protocol::{decode, subscriptions, Event},
    Error,
};
use futures_util::{SinkExt, StreamExt};
use std::time::Duration;
use tokio::{
    net::TcpStream,
    sync::mpsc,
    time::{self, Instant},
};
use tokio_tungstenite::{
    connect_async_tls_with_config, tungstenite::Message, Connector, MaybeTlsStream, WebSocketStream,
};

/// Mainnet public market-data endpoint.
pub const MAINNET: &str = "wss://api.hyperliquid.xyz/ws";
/// Testnet public market-data endpoint.
pub const TESTNET: &str = "wss://api.hyperliquid-testnet.xyz/ws";

/// Capped exponential delays, adapted from the original streaming reconnect policy.
#[derive(Debug, Clone, Copy)]
pub struct ReconnectPolicy {
    /// Total retries over the lifetime of `Client::run`, excluding the first connection.
    pub max_retries: u32,
    /// First delay, doubled after each failure.
    pub initial: Duration,
    /// Maximum delay.
    pub maximum: Duration,
}

impl Default for ReconnectPolicy {
    fn default() -> Self {
        Self {
            max_retries: 5,
            initial: Duration::from_millis(500),
            maximum: Duration::from_secs(30),
        }
    }
}

impl ReconnectPolicy {
    /// Delay for a zero-based retry index; saturates without floating-point arithmetic.
    pub fn delay(self, attempt: u32) -> Duration {
        self.initial
            .saturating_mul(2u32.checked_pow(attempt).unwrap_or(u32::MAX))
            .min(self.maximum)
    }
}

/// Configuration for public trades and L2 snapshots for up to 20 distinct coins.
#[derive(Debug, Clone)]
pub struct Config {
    /// Mainnet/testnet endpoint, or a local WebSocket endpoint for integration tests.
    pub endpoint: String,
    /// Exact coin identifiers.
    pub coins: Vec<Coin>,
    /// Explicit reconnect budget.
    pub reconnect: ReconnectPolicy,
    /// Application ping cadence (must be below the idle timeout).
    pub ping_interval: Duration,
    /// Maximum interval without receiving any frame, strictly below 60 seconds.
    pub idle_timeout: Duration,
    /// Timeout for connection establishment and each write.
    pub io_timeout: Duration,
}

impl Config {
    /// Mainnet defaults with a 20-second heartbeat and a 50-second idle timeout.
    pub fn mainnet(coins: Vec<Coin>) -> Self {
        Self {
            endpoint: MAINNET.into(),
            coins,
            reconnect: ReconnectPolicy::default(),
            ping_interval: Duration::from_secs(20),
            idle_timeout: Duration::from_secs(50),
            io_timeout: Duration::from_secs(10),
        }
    }

    fn validate(&self) -> Result<(), Error> {
        let unique: std::collections::HashSet<_> = self.coins.iter().collect();
        if self.coins.is_empty() || self.coins.len() > 20 || unique.len() != self.coins.len() {
            return Err(Error::Config("provide 1..=20 distinct coins".into()));
        }
        if !(self.endpoint.starts_with("wss://") || self.endpoint.starts_with("ws://")) {
            return Err(Error::Config("endpoint must use ws:// or wss://".into()));
        }
        if self.ping_interval.is_zero()
            || self.ping_interval >= self.idle_timeout
            || self.idle_timeout >= Duration::from_secs(60)
            || self.io_timeout.is_zero()
            || self.reconnect.initial.is_zero()
            || self.reconnect.maximum < self.reconnect.initial
        {
            return Err(Error::Config(
                "invalid heartbeat, timeout, or reconnect durations".into(),
            ));
        }
        Ok(())
    }
}

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Read-only client. Dropping the receiver cancels dial, receive, and retry waits.
pub struct Client {
    config: Config,
}

impl Client {
    /// Validate configuration before opening a connection.
    pub fn new(config: Config) -> Result<Self, Error> {
        config.validate()?;
        Ok(Self { config })
    }

    /// Run until the receiver closes, a terminal decoding/backpressure error occurs,
    /// or transport failures exhaust the retry budget. Monitor the returned result.
    pub async fn run(self, tx: mpsc::Sender<Event>) -> Result<(), Error> {
        let mut retries = 0;
        loop {
            let result = tokio::select! {
                _ = tx.closed() => return Ok(()),
                result = self.session(&tx) => result,
            };
            match result {
                Err(Error::Closed) => return Ok(()),
                Err(Error::Transport(reason)) => {
                    deliver(
                        &tx,
                        Event::Disconnected {
                            reason: reason.clone(),
                            retries_used: retries,
                        },
                    )?;
                    if retries == self.config.reconnect.max_retries {
                        return Err(Error::Transport(reason));
                    }
                    let delay = self.config.reconnect.delay(retries);
                    retries += 1;
                    tokio::select! { _ = tx.closed() => return Ok(()), _ = time::sleep(delay) => {} }
                }
                other => return other,
            }
        }
    }

    async fn send(&self, socket: &mut Socket, message: Message) -> Result<(), Error> {
        time::timeout(self.config.io_timeout, socket.send(message))
            .await
            .map_err(|_| Error::Transport("write timeout".into()))?
            .map_err(|e| Error::Transport(e.to_string()))
    }

    async fn session(&self, tx: &mpsc::Sender<Event>) -> Result<(), Error> {
        // Select a provider locally: embedding applications may enable more than one.
        let tls = rustls::ClientConfig::builder_with_provider(std::sync::Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .map_err(|e| Error::Config(e.to_string()))?
        .with_root_certificates(rustls::RootCertStore::from_iter(
            webpki_roots::TLS_SERVER_ROOTS.iter().cloned(),
        ))
        .with_no_client_auth();
        let (mut socket, _) = time::timeout(
            self.config.io_timeout,
            connect_async_tls_with_config(
                &self.config.endpoint,
                None,
                false,
                Some(Connector::Rustls(std::sync::Arc::new(tls))),
            ),
        )
        .await
        .map_err(|_| Error::Transport("connect timeout".into()))?
        .map_err(|e| Error::Transport(e.to_string()))?;
        for coin in &self.config.coins {
            for sub in subscriptions(coin) {
                self.send(&mut socket, Message::Text(sub)).await?;
            }
        }
        deliver(tx, Event::Connected)?;
        let mut ping = time::interval(self.config.ping_interval);
        ping.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
        ping.tick().await;
        let mut last_received = Instant::now();
        loop {
            tokio::select! {
                _ = time::sleep_until(last_received + self.config.idle_timeout) => return Err(Error::Transport("idle timeout".into())),
                _ = ping.tick() => self.send(&mut socket, Message::Text(r#"{"method":"ping"}"#.into())).await?,
                frame = socket.next() => {
                    last_received = Instant::now();
                    match frame {
                        Some(Ok(Message::Text(text))) => {
                            let event = decode(&text)?;
                            let valid_coin = match &event {
                                Event::Book(book) => self.config.coins.contains(&book.coin),
                                Event::Trades(trades) => trades.iter().all(|t| self.config.coins.contains(&t.coin)),
                                _ => true,
                            };
                            if !valid_coin { return Err(Error::Exchange("received an unrequested coin".into())); }
                            deliver(tx, event)?;
                        }
                        Some(Ok(Message::Ping(data))) => self.send(&mut socket, Message::Pong(data)).await?,
                        Some(Ok(Message::Pong(_) | Message::Frame(_))) => {},
                        Some(Ok(Message::Binary(_))) => return Err(Error::Exchange("unexpected binary market frame".into())),
                        Some(Ok(Message::Close(_))) | None => return Err(Error::Transport("connection closed".into())),
                        Some(Err(e)) => return Err(Error::Transport(e.to_string())),
                    }
                }
            }
        }
    }
}

fn deliver(tx: &mpsc::Sender<Event>, event: Event) -> Result<(), Error> {
    tx.try_send(event).map_err(|e| match e {
        mpsc::error::TrySendError::Full(_) => Error::SlowConsumer,
        mpsc::error::TrySendError::Closed(_) => Error::Closed,
    })
}
