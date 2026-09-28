//! Controlled HTTP checks for public Hyperliquid market discovery.
use super::*;
use serde_json::Value;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    task::JoinHandle,
};

async fn server(replies: Vec<(&'static str, &'static str)>) -> (String, JoinHandle<Vec<Value>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/info", listener.local_addr().unwrap());
    let worker = tokio::spawn(async move {
        tokio::time::timeout(Duration::from_secs(5), async move {
            let mut requests = Vec::new();
            for (status, body) in replies {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut bytes = Vec::new();
                let (header_end, length) = loop {
                    let mut chunk = [0; 1024];
                    let n = socket.read(&mut chunk).await.unwrap();
                    assert!(n > 0, "client closed before request headers");
                    bytes.extend_from_slice(&chunk[..n]);
                    if let Some(end) = bytes.windows(4).position(|p| p == b"\r\n\r\n") {
                        let headers = std::str::from_utf8(&bytes[..end]).unwrap();
                        assert!(headers.starts_with("POST /info HTTP/1.1"));
                        assert!(!headers.to_lowercase().contains("authorization:"));
                        let length: usize = headers.lines().find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            name.eq_ignore_ascii_case("content-length").then(|| value.trim().parse().unwrap())
                        }).unwrap();
                        break (end + 4, length);
                    }
                };
                while bytes.len() < header_end + length {
                    let mut chunk = [0; 1024];
                    let n = socket.read(&mut chunk).await.unwrap();
                    assert!(n > 0, "client closed before request body");
                    bytes.extend_from_slice(&chunk[..n]);
                }
                requests.push(serde_json::from_slice(&bytes[header_end..header_end + length]).unwrap());
                let response = format!("HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
                socket.write_all(response.as_bytes()).await.unwrap();
            }
            requests
        }).await.unwrap()
    });
    (endpoint, worker)
}

fn client(endpoint: String) -> InfoClient {
    let mut client = InfoClient::new(Network::Mainnet).unwrap();
    client.endpoint_override = Some(endpoint);
    client
}

#[tokio::test]
async fn catalog_posts_only_public_metadata_and_resolves_spot_precision() {
    let (endpoint, server) = server(vec![
        ("200 OK", r#"{"universe":[{"name":"BTC","szDecimals":5}]}"#),
        ("200 OK", r#"{"universe":[{"name":"@107","index":107,"tokens":[150,0]}],"tokens":[{"name":"USDC","index":0,"szDecimals":8},{"name":"HYPE","index":150,"szDecimals":2}]}"#),
    ]).await;
    let client = client(endpoint);
    assert_eq!(client.network(), Network::Mainnet);
    let catalog = client.catalog(None).await.unwrap();
    assert_eq!(
        catalog
            .resolve(&Coin::new("BTC").unwrap())
            .unwrap()
            .rules()
            .unwrap()
            .size_decimals(),
        5
    );
    assert_eq!(
        catalog
            .resolve(&Coin::new("@107").unwrap())
            .unwrap()
            .label(),
        "HYPE/USDC"
    );
    assert_eq!(
        server.await.unwrap(),
        vec![json!({"type":"meta"}), json!({"type":"spotMeta"})]
    );
}

#[tokio::test]
async fn hip3_dex_is_sent_explicitly_and_invalid_dex_rejected_before_http() {
    let (endpoint, server) = server(vec![(
        "200 OK",
        r#"{"universe":[{"name":"xyz:XYZ100","szDecimals":4}]}"#,
    )])
    .await;
    let client = client(endpoint);
    for invalid in ["", "xyz:XYZ100", "@107", "two words"] {
        assert!(client.perpetuals(Some(invalid)).await.is_err());
    }
    let meta = client.perpetuals(Some("xyz")).await.unwrap();
    assert_eq!(meta.universe[0].name.as_str(), "xyz:XYZ100");
    assert_eq!(
        server.await.unwrap(),
        vec![json!({"type":"meta","dex":"xyz"})]
    );
}

#[tokio::test]
async fn http_errors_redirects_and_malformed_metadata_are_not_silent_empty_catalogs() {
    for (status, body) in [
        ("429 Too Many Requests", "{}"),
        ("302 Found", "{}"),
        ("200 OK", "not json"),
        ("200 OK", "{}"),
    ] {
        let (endpoint, server) = server(vec![(status, body)]).await;
        assert!(client(endpoint).perpetuals(None).await.is_err());
        assert_eq!(server.await.unwrap().len(), 1);
    }
}

#[tokio::test]
async fn hip4_discovery_uses_outcome_meta_and_resolves_both_sides() {
    let (endpoint, server) = server(vec![("200 OK",r#"{"outcomes":[{"outcome":123,"name":"Synthetic event","description":"Test only","sideSpecs":[{"name":"Yes"},{"name":"No"}],"quoteToken":"USDC"}]}"#)]).await;
    let catalog = client(endpoint).outcome_catalog().await.unwrap();
    for coin in ["#1230", "#1231"] {
        let instrument = catalog.resolve(&Coin::new(coin).unwrap()).unwrap();
        assert_eq!(
            instrument.kind(),
            crate::primitives::market::MarketKind::Outcome
        );
        assert!(instrument.rules().is_none());
    }
    assert_eq!(server.await.unwrap(), vec![json!({"type":"outcomeMeta"})]);
}
