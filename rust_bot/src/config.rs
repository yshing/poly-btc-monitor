use std::env;
use anyhow::{Context, Result};

#[derive(Debug, Clone)]
pub struct Config {
    pub polygon_rpc_url: String,
    pub db_path: String,
    pub poll_interval_seconds: u64,
    pub signal_refresh_minutes: u64,
    pub min_confidence: String,
    pub top_n_wallets: usize,
    pub telegram_bot_token: Option<String>,
    pub telegram_chat_id: Option<String>,
    pub webhook_url: Option<String>,
    pub confirmations: u64,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        dotenvy::dotenv().ok();

        Ok(Self {
            polygon_rpc_url: env::var("POLYGON_RPC_URL")
                .unwrap_or_else(|_| "https://polygon.drpc.org".to_string()),
            db_path: env::var("DB_PATH").unwrap_or_else(|_| "data/polymarket.db".to_string()),
            poll_interval_seconds: env::var("POLL_INTERVAL_SECONDS")
                .unwrap_or_else(|_| "5".to_string())
                .parse()
                .context("POLL_INTERVAL_SECONDS must be a number")?,
            signal_refresh_minutes: env::var("SIGNAL_REFRESH_MINUTES")
                .unwrap_or_else(|_| "5".to_string())
                .parse()
                .context("SIGNAL_REFRESH_MINUTES must be a number")?,
            min_confidence: env::var("MIN_CONFIDENCE").unwrap_or_else(|_| "MEDIUM".to_string()),
            top_n_wallets: env::var("TOP_N_WALLETS")
                .unwrap_or_else(|_| "20".to_string())
                .parse()
                .context("TOP_N_WALLETS must be a number")?,
            telegram_bot_token: env::var("TELEGRAM_BOT_TOKEN").ok(),
            telegram_chat_id: env::var("TELEGRAM_CHAT_ID").ok(),
            webhook_url: env::var("WEBHOOK_URL").ok(),
            confirmations: env::var("CONFIRMATIONS")
                .unwrap_or_else(|_| "5".to_string())
                .parse()
                .context("CONFIRMATIONS must be a number")?,
        })
    }

    pub fn confidence_rank(&self) -> u8 {
        match self.min_confidence.to_uppercase().as_str() {
            "LOW" => 1,
            "MEDIUM" => 2,
            "HIGH" => 3,
            _ => 2,
        }
    }
}
