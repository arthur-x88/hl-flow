//! Inspect Hyperliquid metadata before choosing a subscription coin.
use hl_flow::{info::InfoClient, network::Network};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.len() > 2 {
        return Err("usage: markets [mainnet|testnet] [HIP3_DEX|--outcomes]".into());
    }
    let network: Network = args.first().map_or("mainnet", String::as_str).parse()?;
    let info = InfoClient::new(network)?;
    let catalog = if args.get(1).map(String::as_str) == Some("--outcomes") {
        info.outcome_catalog().await?
    } else {
        info.catalog(args.get(1).map(String::as_str)).await?
    };
    println!(
        "Hyperliquid {network}: {} listed market sides/pairs",
        catalog.instruments().len()
    );
    println!(
        "{:<18} {:<25} {:<12} {:>10} {:>10}",
        "COIN", "LABEL", "KIND", "SIZE DP", "PRICE DP"
    );
    for instrument in catalog.instruments() {
        println!(
            "{:<18} {:<25} {:<12?} {:>10} {:>10}",
            instrument.coin(),
            instrument.label(),
            instrument.kind(),
            instrument
                .rules()
                .map_or("n/a".into(), |rules| rules.size_decimals().to_string()),
            instrument
                .rules()
                .map_or("n/a".into(), |rules| rules.price_decimals().to_string())
        );
    }
    Ok(())
}
