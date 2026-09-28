//! Executable examples and regression checks for the public market-data API.
use hyperliquid_stream::{
    client::{Client, Config, TESTNET},
    primitives::{book::OrderBook, types::Coin},
    protocol::Event,
};
use std::time::Duration;
use tokio::{sync::mpsc, time};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.len() > 3 {
        return Err("usage: live [COIN] [SECONDS] [mainnet|testnet]".into());
    }
    let coin = Coin::new(args.first().map_or("BTC", String::as_str))?;
    let seconds = args.get(1).map_or(Ok(15u64), |s| s.parse())?;
    if seconds == 0 || seconds > 3600 {
        return Err("duration must be 1..=3600 seconds".into());
    }
    let mut config = Config::mainnet(vec![coin.clone()]);
    match args.get(2).map(String::as_str) {
        None | Some("mainnet") => {}
        Some("testnet") => config.endpoint = TESTNET.into(),
        _ => return Err("network must be mainnet or testnet".into()),
    }
    eprintln!(
        "Public read-only feed: {} | coin={coin} | duration={seconds}s",
        config.endpoint
    );
    let (tx, mut rx) = mpsc::channel(256);
    let mut worker = tokio::spawn(Client::new(config)?.run(tx));
    let deadline = time::sleep(Duration::from_secs(seconds));
    tokio::pin!(deadline);
    let mut book = OrderBook::new(coin);
    let mut snapshots = 0;
    let mut trades = 0;
    loop {
        tokio::select! {
            result = &mut worker => { result??; break; },
            _ = &mut deadline => { worker.abort(); let _ = worker.await; break; },
            _ = tokio::signal::ctrl_c() => { worker.abort(); let _ = worker.await; break; },
            Some(event) = rx.recv() => match event {
                Event::Connected => eprintln!("connected; subscriptions sent"),
                Event::Disconnected { reason, retries_used } => { book.clear(); eprintln!("disconnected ({reason}); book invalidated; retries_used={retries_used}"); },
                Event::Book(snapshot) => {
                    let timestamp = snapshot.time;
                    if let Err(error) = book.replace(snapshot) { eprintln!("rejected book: {error}"); continue; }
                    snapshots += 1;
                    println!("L2 t={timestamp} spread={} mid={}", book.spread().map_or("n/a".into(), |v| v.to_string()), book.mid_price().map_or("n/a".into(), |v| v.to_string()));
                },
                Event::Trades(batch) => {
                    trades += batch.len();
                    for trade in batch { println!("TRADE t={} {:?} {} @ {} tid={}", trade.time, trade.side, trade.sz, trade.px, trade.tid); }
                },
                _ => {},
            },
        }
    }
    eprintln!("received snapshots={snapshots} trade_prints={trades}");
    if snapshots + trades == 0 {
        return Err("no market data received within the requested duration".into());
    }
    Ok(())
}
