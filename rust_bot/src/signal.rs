use std::collections::HashMap;

use crate::db::{Market, TargetWallet};
use crate::decoder::DecodedTrade;
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct Signal {
    pub timestamp: String,
    pub block_number: u64,
    pub tx_hash: String,
    pub log_index: u64,
    pub wallet: String,
    pub role: String, // maker or taker
    pub market_slug: String,
    pub outcome: String,
    pub direction: String, // LONG or SHORT
    pub side: String,      // BUY or SELL
    pub size: f64,
    pub price: f64,
    pub usd_amount: f64,
    pub wallet_rank: i32,
    pub wallet_confidence: String,
    pub recommended_follow_size: f64,
    pub suggested_copy_size: f64,
}

pub struct SignalBuilder {
    pub follow_fraction: f64,
}

impl SignalBuilder {
    pub fn new(follow_fraction: f64) -> Self {
        Self { follow_fraction }
    }

    pub fn build(
        &self,
        trade: &DecodedTrade,
        role: &str,
        wallet: &TargetWallet,
        markets: &HashMap<String, Market>,
    ) -> Option<Signal> {
        let market = markets.get(&trade.token_id)?;

        let outcome = if trade.token_id == market.token_one {
            market.outcome_one.clone()
        } else {
            market.outcome_two.clone()
        };

        let is_buy = if role == "maker" {
            trade.side == 0
        } else {
            trade.side == 1
        };

        let direction = if is_buy { "LONG" } else { "SHORT" };
        let side = if is_buy { "BUY" } else { "SELL" };

        let suggested_copy_size =
            wallet.recommended_follow_size * self.follow_fraction * (trade.price.max(0.01));

        Some(Signal {
            timestamp: chrono::Utc::now().to_rfc3339(),
            block_number: trade.block_number,
            tx_hash: trade.tx_hash.clone(),
            log_index: trade.log_index,
            wallet: wallet.wallet.clone(),
            role: role.to_string(),
            market_slug: market.slug.clone(),
            outcome,
            direction: direction.to_string(),
            side: side.to_string(),
            size: trade.share_size,
            price: trade.price,
            usd_amount: trade.usd_amount,
            wallet_rank: wallet.rank,
            wallet_confidence: wallet.confidence.clone(),
            recommended_follow_size: wallet.recommended_follow_size,
            suggested_copy_size: suggested_copy_size.max(5.0).min(100.0),
        })
    }
}
