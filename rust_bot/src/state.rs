use std::collections::HashMap;
use std::fs;
use std::path::Path;

use anyhow::Result;
use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct BotState {
    pub last_processed_block: u64,
}

pub fn load_state(path: &str) -> Result<BotState> {
    if !Path::new(path).exists() {
        return Ok(BotState::default());
    }
    let content = fs::read_to_string(path)?;
    let state = serde_json::from_str(&content)?;
    Ok(state)
}

pub fn save_state(path: &str, state: &BotState) -> Result<()> {
    let content = serde_json::to_string_pretty(state)?;
    fs::write(path, content)?;
    Ok(())
}

pub fn update_wallet_map(
    map: &mut HashMap<String, crate::db::TargetWallet>,
    wallets: &[crate::db::TargetWallet],
) {
    map.clear();
    for w in wallets {
        map.insert(w.wallet.to_lowercase(), w.clone());
    }
}

pub fn dedup_key(signal: &crate::signal::Signal) -> String {
    format!("{}:{}:{}", signal.block_number, signal.wallet, signal.market_slug)
}
