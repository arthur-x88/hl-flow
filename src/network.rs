//! The two supported Hyperliquid deployments. Endpoint URLs are not user-supplied.

use crate::Error;

/// Select Hyperliquid mainnet or testnet for both HTTP metadata and WebSocket data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Network {
    /// Production public data.
    #[default]
    Mainnet,
    /// Hyperliquid's separate test deployment and asset universe.
    Testnet,
}

impl Network {
    /// Public WebSocket endpoint for this network.
    pub const fn websocket_url(self) -> &'static str {
        match self {
            Self::Mainnet => "wss://api.hyperliquid.xyz/ws",
            Self::Testnet => "wss://api.hyperliquid-testnet.xyz/ws",
        }
    }

    /// Public read-only information endpoint for this network.
    pub const fn info_url(self) -> &'static str {
        match self {
            Self::Mainnet => "https://api.hyperliquid.xyz/info",
            Self::Testnet => "https://api.hyperliquid-testnet.xyz/info",
        }
    }
}

impl std::str::FromStr for Network {
    type Err = Error;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "mainnet" => Ok(Self::Mainnet),
            "testnet" => Ok(Self::Testnet),
            _ => Err(Error::Config("network must be mainnet or testnet".into())),
        }
    }
}

impl std::fmt::Display for Network {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Mainnet => "mainnet",
            Self::Testnet => "testnet",
        })
    }
}

pub(crate) fn tls_config() -> Result<rustls::ClientConfig, Error> {
    // A local provider avoids ambiguity when embedding in applications with another provider.
    Ok(
        rustls::ClientConfig::builder_with_provider(std::sync::Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .map_err(|e| Error::Config(e.to_string()))?
        .with_root_certificates(rustls::RootCertStore::from_iter(
            webpki_roots::TLS_SERVER_ROOTS.iter().cloned(),
        ))
        .with_no_client_auth(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_hyperliquid_deployments_can_be_selected() {
        assert_eq!(
            "mainnet".parse::<Network>().unwrap().info_url(),
            "https://api.hyperliquid.xyz/info"
        );
        assert_eq!(
            "testnet".parse::<Network>().unwrap().websocket_url(),
            "wss://api.hyperliquid-testnet.xyz/ws"
        );
        for invalid in [
            "",
            "production",
            "https://example.com",
            "ws://127.0.0.1:8080",
        ] {
            assert!(invalid.parse::<Network>().is_err());
        }
    }
}
