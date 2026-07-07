use std::collections::HashMap;
use std::time::Duration;

use anyhow::Result;
use tokio::time::interval;
use tracing::{info, warn};

mod config;
mod db;
mod decoder;
mod listener;
mod notifier;
mod signal;
mod state;

use config::Config;
use db::{Db, TargetWallet};
use listener::ChainListener;
use notifier::Notifier;
use signal::SignalBuilder;
use state::{load_state, save_state, update_wallet_map};

const STATE_FILE: &str = "rust_bot_state.json";

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let config = Config::from_env()?;
    info!("starting poly_copy_bot with config: {:?}", config);

    let db = Db::new(&config.db_path)?;
    let listener = ChainListener::new(&config.polygon_rpc_url)?;
    let notifier = Notifier::new(
        config.telegram_bot_token.clone(),
        config.telegram_chat_id.clone(),
        config.webhook_url.clone(),
    );
    let signal_builder = SignalBuilder::new(0.15);

    let mut state = load_state(STATE_FILE)?;
    if state.last_processed_block == 0 {
        let latest = listener.latest_block().await?;
        state.last_processed_block = latest.saturating_sub(config.confirmations);
        save_state(STATE_FILE, &state)?;
        info!("starting from block {}", state.last_processed_block);
    }

    let mut wallets: Vec<TargetWallet> = Vec::new();
    let mut wallet_map: HashMap<String, TargetWallet> = HashMap::new();
    let mut markets: HashMap<String, db::Market> = HashMap::new();

    let mut refresh_interval = interval(Duration::from_secs(
        config.signal_refresh_minutes * 60,
    ));
    refresh_interval.tick().await;

    refresh_data(&db, &config, &mut wallets, &mut wallet_map, &mut markets,
    )
    .await?;

    let mut poll_interval = interval(Duration::from_secs(config.poll_interval_seconds));
    let mut refresh_interval = interval(Duration::from_secs(
        config.signal_refresh_minutes * 60,
    ));

    loop {
        tokio::select! {
            _ = poll_interval.tick() => {
                if let Err(e) = scan(
                    &listener, &db, &config, &notifier, &signal_builder,
                    &wallet_map, &markets, &mut state,
                ).await {
                    warn!("scan failed: {}", e);
                }
            }
            _ = refresh_interval.tick() => {
                if let Err(e) = refresh_data(
                    &db, &config, &mut wallets, &mut wallet_map, &mut markets,
                ).await {
                    warn!("failed to refresh data: {}", e);
                }
            }
        }
    }
}

async fn scan(
    listener: &ChainListener,
    _db: &Db,
    config: &Config,
    notifier: &Notifier,
    signal_builder: &SignalBuilder,
    wallet_map: &HashMap<String, TargetWallet>,
    markets: &HashMap<String, db::Market>,
    state: &mut state::BotState,
) -> Result<()> {
    let latest = listener.latest_block().await?;
    let safe_block = latest.saturating_sub(config.confirmations);
    if safe_block <= state.last_processed_block {
        return Ok(());
    }

    let from_block = state.last_processed_block + 1;
    info!(
        "scanning blocks {} - {} (latest {})",
        from_block, safe_block, latest
    );

    let logs = listener.fetch_order_filled_logs(from_block, safe_block).await?;

    let mut seen = std::collections::HashSet::new();
    for decoded in logs {
        let trade = decoded.trade;

        for (role, wallet_addr) in [("maker", &trade.maker), ("taker", &trade.taker)] {
            if let Some(wallet) = wallet_map.get(wallet_addr) {
                if let Some(signal) = signal_builder.build(&trade, role, wallet, markets) {
                    let key = state::dedup_key(&signal);
                    if seen.insert(key) {
                        notifier.notify(&signal).await?;
                    }
                }
            }
        }
    }

    state.last_processed_block = safe_block;
    save_state(STATE_FILE, state)?;
    Ok(())
}

async fn refresh_data(
    db: &Db,
    config: &Config,
    wallets: &mut Vec<TargetWallet>,
    wallet_map: &mut HashMap<String, TargetWallet>,
    markets: &mut HashMap<String, db::Market>,
) -> Result<()> {
    *wallets = db.load_targets(config.confidence_rank(), config.top_n_wallets)?;
    update_wallet_map(wallet_map, wallets);
    *markets = db.load_markets()?;
    info!(
        "refreshed {} target wallets and {} active market tokens",
        wallets.len(),
        markets.len()
    );
    Ok(())
}
