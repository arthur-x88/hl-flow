//! Executable examples and regression checks for the public market-data API.
use crate::{
    client::{Client, Config, ReconnectPolicy},
    primitives::types::Coin,
    protocol::Event,
    Error,
};
use futures_util::{SinkExt, StreamExt};
use std::time::Duration;
use tokio::{net::TcpListener, sync::mpsc, time::timeout};
use tokio_tungstenite::{accept_async, tungstenite::Message};

const BOOK: &str = r#"{"channel":"l2Book","data":{"coin":"BTC","time":1700000000000,"levels":[[{"px":"99","sz":"1","n":1}],[{"px":"101","sz":"1","n":1}]]}}"#;

async fn setup() -> (TcpListener, Config) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = Config::mainnet(vec![Coin::new("BTC").unwrap()]);
    config.endpoint_override = Some(format!("ws://{}", listener.local_addr().unwrap()));
    config.reconnect = ReconnectPolicy {
        max_retries: 0,
        initial: Duration::from_millis(10),
        maximum: Duration::from_millis(50),
    };
    config.ping_interval = Duration::from_millis(30);
    config.idle_timeout = Duration::from_millis(150);
    config.io_timeout = Duration::from_millis(500);
    (listener, config)
}

#[tokio::test]
async fn reconnect_resends_both_subscriptions_and_marks_the_gap() {
    let (listener, mut config) = setup().await;
    config.reconnect.max_retries = 1;
    let server = tokio::spawn(async move {
        for _ in 0..2 {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(tcp).await.unwrap();
            for kind in ["trades", "l2Book"] {
                let msg = socket.next().await.unwrap().unwrap();
                let value: serde_json::Value =
                    serde_json::from_str(msg.to_text().unwrap()).unwrap();
                assert_eq!(value["subscription"]["type"], kind);
                assert_eq!(value["subscription"]["coin"], "BTC");
            }
            socket.send(Message::Text(BOOK.into())).await.unwrap();
            socket.close(None).await.unwrap();
        }
    });
    let (tx, mut rx) = mpsc::channel(32);
    let worker = tokio::spawn(Client::new(config).unwrap().run(tx));
    let mut connected = 0;
    let mut disconnected = 0;
    let mut books = 0;
    timeout(Duration::from_secs(3), async {
        while let Some(event) = rx.recv().await {
            match event {
                Event::Connected => connected += 1,
                Event::Disconnected { .. } => disconnected += 1,
                Event::Book(_) => books += 1,
                _ => {}
            }
        }
    })
    .await
    .unwrap();
    assert_eq!((connected, disconnected, books), (2, 2, 2));
    assert!(matches!(worker.await.unwrap(), Err(Error::Transport(_))));
    server.await.unwrap();
}

#[tokio::test]
async fn application_heartbeat_uses_json_ping_and_decodes_pong() {
    let (listener, config) = setup().await;
    let server = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(tcp).await.unwrap();
        for _ in 0..2 {
            socket.next().await.unwrap().unwrap();
        }
        let ping = socket.next().await.unwrap().unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(ping.to_text().unwrap()).unwrap(),
            serde_json::json!({"method":"ping"})
        );
        socket
            .send(Message::Text(r#"{"channel":"pong"}"#.into()))
            .await
            .unwrap();
        socket.close(None).await.unwrap();
    });
    let (tx, mut rx) = mpsc::channel(16);
    let worker = tokio::spawn(Client::new(config).unwrap().run(tx));
    let mut pong = false;
    timeout(Duration::from_secs(3), async {
        while let Some(event) = rx.recv().await {
            pong |= matches!(event, Event::Pong);
        }
    })
    .await
    .unwrap();
    assert!(pong);
    assert!(worker.await.unwrap().is_err());
    server.await.unwrap();
}

#[tokio::test]
async fn bounded_queue_overflow_is_terminal_and_visible() {
    let (listener, config) = setup().await;
    let server = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(tcp).await.unwrap();
        for _ in 0..2 {
            socket.next().await.unwrap().unwrap();
        }
        socket.send(Message::Text(BOOK.into())).await.unwrap();
    });
    let (tx, _rx) = mpsc::channel(1); // Connected fills the only slot.
    let result = timeout(Duration::from_secs(3), Client::new(config).unwrap().run(tx))
        .await
        .unwrap();
    assert!(matches!(result, Err(Error::SlowConsumer)));
    server.await.unwrap();
}

#[tokio::test]
async fn stalled_connection_times_out_even_while_pings_are_sent() {
    let (listener, config) = setup().await;
    let server = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let _socket = accept_async(tcp).await.unwrap();
        tokio::time::sleep(Duration::from_secs(5)).await;
    });
    let (tx, _rx) = mpsc::channel(16);
    let result = timeout(Duration::from_secs(3), Client::new(config).unwrap().run(tx))
        .await
        .unwrap();
    assert!(matches!(result, Err(Error::Transport(reason)) if reason == "idle timeout"));
    server.abort();
}

#[tokio::test]
async fn dropping_receiver_cancels_a_pending_handshake() {
    let (listener, config) = setup().await;
    let (tx, rx) = mpsc::channel(16);
    let worker = tokio::spawn(Client::new(config).unwrap().run(tx));
    let (_tcp, _) = timeout(Duration::from_secs(3), listener.accept())
        .await
        .unwrap()
        .unwrap();
    drop(rx);
    assert!(timeout(Duration::from_millis(300), worker)
        .await
        .unwrap()
        .unwrap()
        .is_ok());
}

#[tokio::test]
async fn malformed_data_is_terminal_instead_of_retrying_forever() {
    let (listener, config) = setup().await;
    let server = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(tcp).await.unwrap();
        for _ in 0..2 {
            socket.next().await.unwrap().unwrap();
        }
        socket
            .send(Message::Text("broken-json".into()))
            .await
            .unwrap();
    });
    let (tx, _rx) = mpsc::channel(16);
    assert!(matches!(
        timeout(Duration::from_secs(3), Client::new(config).unwrap().run(tx))
            .await
            .unwrap(),
        Err(Error::Json(_))
    ));
    server.await.unwrap();
}

#[test]
fn config_rejects_duplicate_coins_and_zero_timers() {
    let mut config = Config::mainnet(vec![Coin::new("BTC").unwrap(); 2]);
    assert!(Client::new(config.clone()).is_err());
    config.coins.pop();
    config.ping_interval = Duration::ZERO;
    assert!(Client::new(config).is_err());
    let policy = ReconnectPolicy::default();
    assert_eq!(policy.delay(0), Duration::from_millis(500));
    assert_eq!(policy.delay(2), Duration::from_secs(2));
    assert_eq!(policy.delay(u32::MAX), Duration::from_secs(30));
}
