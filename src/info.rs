//! Read-only Hyperliquid market discovery; no wallet or exchange SDK dependency.

use crate::{
    network::{tls_config, Network},
    primitives::{
        metadata::{MarketCatalog, PerpMeta, SpotMeta},
        outcome::OutcomeMeta,
        types::Coin,
    },
    Error,
};
use serde::de::DeserializeOwned;
use serde_json::json;
use std::time::Duration;

const MAX_METADATA_BYTES: usize = 8 * 1024 * 1024;

/// HTTP client restricted to public `meta`, `spotMeta`, and `outcomeMeta` requests.
/// Reuse a client to reuse its connections. A catalog is a snapshot, not a cache.
pub struct InfoClient {
    network: Network,
    http: reqwest::Client,
    #[cfg(test)]
    endpoint_override: Option<String>,
}

impl InfoClient {
    /// Construct a client with certificate verification, a ten-second request timeout,
    /// and redirects disabled. Endpoint selection is tied to the Hyperliquid network.
    pub fn new(network: Network) -> Result<Self, Error> {
        let http = reqwest::Client::builder()
            .use_preconfigured_tls(tls_config()?)
            .timeout(Duration::from_secs(10))
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        Ok(Self {
            network,
            http,
            #[cfg(test)]
            endpoint_override: None,
        })
    }

    /// The deployment queried by this client.
    pub fn network(&self) -> Network {
        self.network
    }

    /// Fetch perpetual metadata for the default DEX, or an explicit HIP-3 DEX.
    pub async fn perpetuals(&self, dex: Option<&str>) -> Result<PerpMeta, Error> {
        let mut request = json!({"type":"meta"});
        if let Some(dex) = dex {
            Coin::new(dex)?;
            if dex.contains([':', '/', '@']) {
                return Err(Error::Config(
                    "pass a DEX name such as xyz, not a coin or URL".into(),
                ));
            }
            request["dex"] = json!(dex);
        }
        self.request(request).await
    }

    /// Fetch spot pair and token metadata for this network.
    pub async fn spot(&self) -> Result<SpotMeta, Error> {
        self.request(json!({"type":"spotMeta"})).await
    }

    /// Fetch HIP-4 outcome specifications, including the two side labels.
    /// Availability is determined by the selected network's response.
    pub async fn outcomes(&self) -> Result<OutcomeMeta, Error> {
        self.request(json!({"type":"outcomeMeta"})).await
    }

    /// Discover both `#` subscription coins for every returned HIP-4 outcome.
    /// This requires only `outcomeMeta`; it does not guess order precision.
    pub async fn outcome_catalog(&self) -> Result<MarketCatalog, Error> {
        Ok(MarketCatalog::from_outcomes(&self.outcomes().await?)?)
    }

    /// Resolve active perps on the chosen DEX and spot pairs into one validated catalog.
    /// Separate metadata responses are not an atomic exchange snapshot.
    pub async fn catalog(&self, dex: Option<&str>) -> Result<MarketCatalog, Error> {
        let perps = self.perpetuals(dex).await?;
        let spot = self.spot().await?;
        Ok(MarketCatalog::new(&perps, &spot)?)
    }

    async fn request<T: DeserializeOwned>(&self, body: serde_json::Value) -> Result<T, Error> {
        let endpoint = self.network.info_url();
        #[cfg(test)]
        let endpoint = self.endpoint_override.as_deref().unwrap_or(endpoint);
        let mut response = self
            .http
            .post(endpoint)
            .json(&body)
            .send()
            .await?
            .error_for_status()?;
        if response.status().is_redirection() {
            return Err(Error::Hyperliquid(
                "metadata redirects are not accepted".into(),
            ));
        }
        if response
            .content_length()
            .is_some_and(|size| size > MAX_METADATA_BYTES as u64)
        {
            return Err(Error::Hyperliquid(
                "metadata exceeds the 8 MiB response limit".into(),
            ));
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await? {
            if chunk.len() > MAX_METADATA_BYTES.saturating_sub(bytes.len()) {
                return Err(Error::Hyperliquid(
                    "metadata exceeds the 8 MiB response limit".into(),
                ));
            }
            bytes.extend_from_slice(&chunk);
        }
        Ok(serde_json::from_slice(&bytes)?)
    }
}

#[cfg(test)]
mod tests;
