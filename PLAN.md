# Polymarket BTC Up/Down 5m Copy-Trading Bot Plan

## 1. Goal

Turn the historical on-chain PnL analysis into a live **signal observation bot** that:
- Watches the wallets identified by `copy_trade_signals`
- Detects their new trades on Polymarket BTC Up/Down 5m markets in real time
- Decodes direction, market, size, and price
- Logs and notifies the operator of actionable copy-trade opportunities

This plan targets **Phase 1 (observation)**. Semi-automatic execution is Phase 2 and can be added later.

---

## 2. Why Phase 1 First

- Historical data is only one week; live behaviour must be validated.
- 5-minute markets are extremely latency-sensitive.
- Target wallets may be market makers, not directional traders.
- Polymarket requires deposits, API keys, and signature management before execution.
- Manual review prevents catastrophic losses while the strategy is proven.

---

## 3. Architecture

```textn+----------------+     +------------------+     +------------------+
|  SQLite DB     |     |  Polygon RPC     |     |  Operator        |
| (signals,      |---->|  OrderFilled     |---->|  (Telegram /     |
|  markets)      |     |  logs watcher    |     |  logs / web)     |
+----------------+     +------------------+     +------------------+
        |                       |
        v                       v
+----------------+     +------------------+
|  Target wallet |     |  Decode + map    |
|  loader        |     |  token -> market |
+----------------+     +------------------+
```

---

## 4. Components

### 4.1 Wallet loader
- Reads `copy_trade_signals` from `data/polymarket.db`.
- Loads the top N wallets to watch.
- Refreshes periodically so the bot follows the latest plan.

### 4.2 Chain listener
- Polls Polygon RPC for `OrderFilled` events on `CTF_EXCHANGE_V2`.
- Supports both:
  - **Polling mode** (default): asks for logs every N seconds.
  - **WebSocket mode** (future): subscribe to logs if RPC supports it.
- Tracks the last processed block to avoid gaps.

### 4.3 Event decoder
- Decodes V2 `OrderFilled` event:
  - `side`: 0 = maker BUY, 1 = maker SELL
  - `tokenId`: identifies Up or Down token
  - `makerAmountFilled`, `takerAmountFilled`
  - `maker`, `taker` addresses
- Computes price and share size using the same logic as the Python analyzer.

### 4.4 Market mapper
- Loads active BTC Up/Down 5m markets from SQLite.
- Maps `tokenId` to `Up` / `Down` outcome.
- Falls back to a minimal in-memory cache if DB is not available.

### 4.5 Signal generator
- For each event, if `maker` or `taker` is a target wallet:
  - Determine the traded outcome (Up/Down)
  - Determine the wallet's net direction (BUY = long, SELL = short)
  - Compute recommended follow size based on the plan's `recommended_follow_size`
  - Emit a structured signal

### 4.6 Notifier
- Console logger with structured JSON.
- Optional Telegram bot message.
- Optional webhook for Discord/Slack.

### 4.7 Risk guard (Phase 1 light version)
- Skip signals for wallets whose trailing 20-market win rate has dropped below 50% (requires updating `wallet_stats` periodically).
- Skip duplicate signals within the same block for the same wallet.

---

## 5. Data Flow

1. Bot starts.
2. Load configuration from `.env`.
3. Connect to SQLite and load target wallets.
4. Determine start block (latest on-chain block minus a small buffer).
5. Loop:
   - Fetch logs from `start_block` to `latest_block`.
   - Decode each log.
   - If wallet is in watch list, build and emit signal.
   - Sleep until next poll.
   - Refresh watch list every 5 minutes.

---

## 6. Configuration (`.env`)

```bashn# Required
POLYGON_RPC_URL=https://polygon.drpc.org
DB_PATH=data/polymarket.db

# Optional
RUST_LOG=info
POLL_INTERVAL_SECONDS=5
SIGNAL_REFRESH_MINUTES=5
MIN_CONFIDENCE=MEDIUM
TOP_N_WALLETS=20

# Notifications (optional)
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=
WEBHOOK_URL=
```

---

## 7. Phase 2: Semi-Automatic Execution (future)

After validating Phase 1 signals:

1. Add Polymarket CLOB API integration.
2. Maintain a hot wallet with USDC deposited on Polymarket.
3. Implement order signing and nonce management.
4. Add confirmation step: bot posts proposed order, operator approves via Telegram command or web UI.
5. Track bot PnL in a new `bot_trades` table.

---

## 8. Phase 3: Full Automation (future)

Only after profitable semi-automatic operation:

1. Remove manual approval.
2. Add dynamic position sizing based on Kelly criterion or volatility.
3. Add market-impact checks (spread, slippage).
4. Add kill switches and daily loss limits.

---

## 9. Success Metrics

- False-positive rate of signals over a 1-week live run.
- Median time from target trade to bot notification.
- Correlation between target wallet direction and market outcome.
- Estimated PnL if signals had been manually followed with fixed sizing.

---

## 10. Risks & Mitigations

| Risk | Mitigation |
|------|------------|
| RPC rate limits | Add exponential backoff, support multiple RPC fallbacks. |
| Re-orgs | Wait for 5 confirmations before acting. |
| Wallet is market maker | Analyze only taker side or filter by size thresholds. |
| Latency | Use WebSocket or paid RPC; run bot close to Polygon nodes. |
| Polymarket API changes | Keep execution layer separate from signal layer. |
| Key leak | Store private keys in environment or hardware wallet for execution phase. |

---

## 11. File Layout

```textnrust_bot/
├── Cargo.toml
├── .env.example
├── src/
│   ├── main.rs        # entry point & control loop
│   ├── config.rs      # env loading
│   ├── db.rs          # SQLite access
│   ├── decoder.rs     # OrderFilled decoding
│   ├── listener.rs    # RPC log polling
│   ├── signal.rs      # signal builder
│   ├── notifier.rs    # Telegram / console output
│   └── state.rs       # last block, cache
```

---

## 12. Implementation Notes

- Use `alloy` for Ethereum types and event decoding.
- Use `tokio` for async runtime.
- Use `rusqlite` for SQLite access.
- Use `reqwest` for Telegram / webhook notifications.
- Keep the bot stateless except for the SQLite DB and an optional JSON state file.
