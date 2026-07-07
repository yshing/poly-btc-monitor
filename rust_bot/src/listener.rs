use std::collections::HashMap;

use alloy_primitives::{Address, B256};
use alloy_provider::{Provider, ProviderBuilder, RootProvider};
use alloy_rpc_types::{Filter, Log};
use alloy_transport_http::Http;
use anyhow::{Context, Result};
use reqwest::Client;

use crate::decoder::{decode_v2_order_filled, CTF_EXCHANGE_V2, ORDER_FILLED_V2_TOPIC};

pub type AlloyProvider = RootProvider<Http<Client>>;

pub struct ChainListener {
    provider: AlloyProvider,
    contract: Address,
    topic0: B256,
}

impl ChainListener {
    pub fn new(rpc_url: &str) -> Result<Self> {
        let url = rpc_url.parse()?;
        let provider = ProviderBuilder::new().on_http(url);
        let contract: Address = CTF_EXCHANGE_V2.parse()?;
        let topic0: B256 = ORDER_FILLED_V2_TOPIC.parse()?;

        Ok(Self {
            provider,
            contract,
            topic0,
        })
    }

    pub async fn latest_block(&self) -> Result<u64> {
        let num = self
            .provider
            .get_block_number()
            .await
            .context("failed to get latest block number")?;
        Ok(num)
    }

    pub async fn fetch_order_filled_logs(
        &self,
        from_block: u64,
        to_block: u64,
    ) -> Result<Vec<DecodedLog>> {
        let filter = Filter::new()
            .address(self.contract)
            .event_signature(self.topic0)
            .from_block(from_block)
            .to_block(to_block);

        let logs = self
            .provider
            .get_logs(&filter)
            .await
            .context("failed to fetch logs")?;

        let mut decoded = Vec::with_capacity(logs.len());
        for log in logs {
            let tx_hash = log.transaction_hash.unwrap_or_default();
            let log_index = log.log_index.unwrap_or(0);
            let block_number = log.block_number.unwrap_or(0);
            let topics: Vec<B256> = log.topics().to_vec();
            let data = log.data().data.to_vec();

            match decode_v2_order_filled(tx_hash, log_index, block_number, &topics, &data) {
                Ok(trade) => decoded.push(DecodedLog { raw: log, trade }),
                Err(e) => tracing::debug!("failed to decode log: {}", e),
            }
        }

        Ok(decoded)
    }
}

pub struct DecodedLog {
    pub raw: Log,
    pub trade: crate::decoder::DecodedTrade,
}

pub fn build_wallet_set(wallets: &[crate::db::TargetWallet]) -> HashMap<String, crate::db::TargetWallet> {
    wallets
        .iter()
        .map(|w| (w.wallet.to_lowercase(), w.clone()))
        .collect()
}
