//! Executable examples and regression checks for the public market-data API.
use hl_flow::{
    candle::CandleBuilder,
    dedup::TradeDeduper,
    primitives::{types::Coin, wire::Trade},
    protocol::{decode, subscriptions, Event},
};
use rust_decimal_macros::dec;
use serde_json::json;

fn trade(time: u64, tid: u64, px: &str, sz: &str) -> Trade {
    serde_json::from_value(json!({"coin":"BTC","side":"B","px":px,"sz":sz,"time":time,"tid":tid}))
        .unwrap()
}

#[test]
fn subscriptions_preserve_spot_and_dex_identifiers() {
    for coin in ["BTC", "@107", "xyz:XYZ100"] {
        for (subscription, kind) in subscriptions(&Coin::new(coin).unwrap())
            .into_iter()
            .zip(["trades", "l2Book"])
        {
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(&subscription).unwrap(),
                json!({"method":"subscribe","subscription":{"type":kind,"coin":coin}})
            );
        }
    }
}

#[test]
fn entire_batch_is_rejected_when_one_trade_is_invalid() {
    let good = serde_json::to_value(trade(1, 1, "10.01", "0.2")).unwrap();
    for (key, value) in [
        ("px", json!("-1")),
        ("sz", json!("0")),
        ("side", json!("X")),
        ("px", json!(10.1)),
        ("time", json!(-1)),
    ] {
        let mut bad = good.clone();
        bad[key] = value;
        assert!(
            decode(&json!({"channel":"trades","data":[good.clone(),bad]}).to_string()).is_err()
        );
    }
    let event =
        decode(&json!({"channel":"trades","data":[good],"future_field":true}).to_string()).unwrap();
    assert!(matches!(event, Event::Trades(t) if t.len() == 1));
}

#[test]
fn control_frames_unknown_channels_and_server_errors_are_distinct() {
    assert!(matches!(
        decode(r#"{"channel":"pong"}"#).unwrap(),
        Event::Pong
    ));
    assert!(matches!(
        decode(r#"{"channel":"subscriptionResponse","data":{}}"#).unwrap(),
        Event::Subscribed
    ));
    assert!(matches!(
        decode(r#"{"channel":"futureChannel","data":{}}"#).unwrap(),
        Event::Ignored(_)
    ));
    assert!(decode(r#"{"channel":"error","data":"bad coin"}"#).is_err());
    assert!(decode(r#"{"data":[]}"#).is_err());
    assert!(decode("not JSON").is_err());
    assert!(decode(r#"{"channel":"l2Book","data":{"coin":"BTC","time":1,"levels":[[{"px":"11","sz":"1","n":1}],[{"px":"10","sz":"1","n":1}]]}}"#).is_err());
}

#[test]
fn dedup_uses_time_coin_and_tid_and_bounds_memory() {
    let mut seen = TradeDeduper::new(2).unwrap();
    let first = trade(1, 7, "10", "1");
    assert!(seen.accept(&first));
    assert!(!seen.accept(&first));
    let mut other_coin = first.clone();
    other_coin.coin = Coin::new("ETH").unwrap();
    assert!(seen.accept(&other_coin));
    assert!(seen.accept(&trade(2, 7, "10", "1")));
    assert!(seen.accept(&first)); // the oldest key has now been evicted
    assert!(TradeDeduper::new(0).is_err());
}

#[test]
fn candles_use_exchange_time_and_close_only_on_next_nonempty_bucket() {
    let mut bars = CandleBuilder::new(Coin::new("BTC").unwrap(), 1000).unwrap();
    bars.push(&trade(1000, 1, "10", "0.1")).unwrap();
    bars.push(&trade(1200, 2, "12", "0.2")).unwrap();
    bars.push(&trade(1500, 3, "9", "0.3")).unwrap();
    let closed = bars.push(&trade(3000, 4, "11", "1")).unwrap().unwrap();
    assert_eq!(
        (
            closed.start_ms,
            closed.open,
            closed.high,
            closed.low,
            closed.close,
            closed.volume,
            closed.trades
        ),
        (1000, dec!(10), dec!(12), dec!(9), dec!(9), dec!(0.6), 3)
    );
    assert_eq!(bars.current().unwrap().start_ms, 3000);
    let before = bars.current().cloned();
    assert!(bars.push(&trade(2999, 5, "20", "1")).is_err());
    let mut wrong = trade(3001, 6, "20", "1");
    wrong.coin = Coin::new("ETH").unwrap();
    assert!(bars.push(&wrong).is_err());
    assert_eq!(bars.current(), before.as_ref());
    assert!(CandleBuilder::new(Coin::new("BTC").unwrap(), 0).is_err());
}

#[test]
fn replayed_duplicate_does_not_inflate_volume() {
    let mut seen = TradeDeduper::new(100).unwrap();
    let mut bars = CandleBuilder::new(Coin::new("BTC").unwrap(), 1000).unwrap();
    let t = trade(1000, 1, "10", "0.25");
    for print in [&t, &t] {
        if seen.accept(print) {
            bars.push(print).unwrap();
        }
    }
    assert_eq!(bars.current().unwrap().volume, dec!(0.25));
    assert_eq!(bars.current().unwrap().trades, 1);
}

#[test]
fn candle_overflow_does_not_mutate_state() {
    let mut bars = CandleBuilder::new(Coin::new("BTC").unwrap(), 1000).unwrap();
    bars.push(&trade(1, 1, "1", "79228162514264337593543950335"))
        .unwrap();
    let before = bars.current().cloned();
    assert!(bars.push(&trade(2, 2, "2", "1")).is_err());
    assert_eq!(bars.current(), before.as_ref());
}
