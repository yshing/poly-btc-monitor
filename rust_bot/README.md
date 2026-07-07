# Rust Copy-Trading Bot

Observational Phase-1 bot for Polymarket BTC Up/Down 5m markets.

## Setup

```bash
cd rust_bot
cp .env.example .env
# Edit .env with your settings
cargo run --release
```

## Configuration

```bash
POLYGON_RPC_URL=https://polygon.drpc.org
DB_PATH=data/polymarket.db
POLL_INTERVAL_SECONDS=5
SIGNAL_REFRESH_MINUTES=5
MIN_CONFIDENCE=MEDIUM
TOP_N_WALLETS=20
CONFIRMATIONS=5

# Optional notifications
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=
WEBHOOK_URL=
```

## What it does

1. Loads target wallets from `copy_trade_signals` in the SQLite DB.
2. Polls Polygon for new `OrderFilled` events on the Polymarket CTF V2 exchange.
3. Decodes each trade to determine direction, market, and size.
4. Logs a structured signal when a target wallet trades.
5. Optionally sends Telegram messages or webhook payloads.

## Safety

This bot does **not** execute trades. It only observes and notifies.
