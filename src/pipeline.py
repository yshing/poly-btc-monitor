"""Long-running incremental pipeline for BTC Up/Down 5m analysis."""

import os
import time
from datetime import datetime, timezone
from typing import Any

from tqdm import tqdm

from src import db
from src.analyzer import aggregate_pnl_by_wallet, analyze_wallet_pnl
from src.config import DATA_DIR, Market, hypersync_token
from src.fetchers.chain_rpc import build_web3, fetch_chain_trades_for_markets_batch
from src.fetchers.gamma_api import fetch_btc_updown_markets_concurrent
from src.models import Trade


def _now_epoch() -> int:
    return int(datetime.now(timezone.utc).timestamp())


def _market_to_dict(m: Market) -> dict[str, Any]:
    return {
        "slug": m.slug,
        "condition_id": m.condition_id,
        "question": m.question,
        "outcome_one": m.outcome_one,
        "outcome_two": m.outcome_two,
        "token_one": m.token_one,
        "token_two": m.token_two,
        "winning_outcome": m.winning_outcome,
        "resolved": m.resolved,
        "resolution_time": m.resolution_time,
        "start_ts": m.start_ts,
        "end_ts": m.end_ts,
    }


def _trade_to_dict(t: Trade) -> dict[str, Any]:
    return {
        "market_slug": t.market_slug,
        "condition_id": t.condition_id,
        "proxy_wallet": t.proxy_wallet,
        "side": t.side,
        "asset": t.asset,
        "size": t.size,
        "price": t.price,
        "usd_amount": t.usd_amount,
        "timestamp": int(t.timestamp) if t.timestamp else None,
        "transaction_hash": t.transaction_hash,
        "source": t.source,
    }


def init() -> None:
    """Initialize the SQLite database."""
    os.makedirs(DATA_DIR, exist_ok=True)
    db.init_db()
    print(f"[pipeline] database ready at {db.DB_PATH}")


def fetch_markets(
    start_ts: int,
    end_ts: int,
    max_workers: int = 50,
    only_resolved: bool = True,
) -> int:
    """Fetch market metadata and store in DB. Skip already-known slugs."""
    existing = db.get_market_slugs()
    print(f"[pipeline] {len(existing)} markets already in DB")
    markets = fetch_btc_updown_markets_concurrent(
        start_ts,
        end_ts,
        only_resolved=only_resolved,
        max_workers=max_workers,
        existing_slugs=existing,
    )
    if markets:
        rows = [_market_to_dict(m) for m in markets]
        inserted = db.upsert_markets(rows)
        print(f"[pipeline] stored {inserted} new/updated markets")
    return len(markets)


def fetch_trades(
    batch_hours: int = 1,
    buffer_blocks: int = 100,
    max_markets: int | None = None,
    use_hypersync: bool = True,
) -> int:
    """Fetch on-chain trades for all unresolved-fetched markets and store in DB."""
    markets = db.get_unfetched_markets(only_resolved=True)
    if max_markets:
        markets = markets[:max_markets]
    if not markets:
        print("[pipeline] no markets needing trade fetch")
        return 0

    print(f"[pipeline] fetching trades for {len(markets)} markets")

    token = hypersync_token() if use_hypersync else None
    if token:
        from src.fetchers.hypersync import fetch_hypersync_trades_for_markets_batch
        trades = fetch_hypersync_trades_for_markets_batch(markets, batch_hours=max(batch_hours, 6))
    else:
        if use_hypersync:
            print("[pipeline] HYPERSYNC_API_TOKEN not set, falling back to RPC")
        w3 = build_web3()
        trades = fetch_chain_trades_for_markets_batch(
            w3,
            markets,
            batch_hours=batch_hours,
            buffer_blocks=buffer_blocks,
            fetch_ts=False,
        )

    if trades:
        rows = [_trade_to_dict(t) for t in trades]
        inserted = db.upsert_trades(rows)
        print(f"[pipeline] stored {inserted} new trades")
    return len(trades)


def analyze() -> int:
    """Compute wallet PnL per market and aggregate stats using SQL."""
    with db.get_conn() as conn:
        # Compute per-wallet-market PnL directly in SQLite.
        conn.execute("""
            DELETE FROM wallet_pnl
            WHERE market_slug IN (
                SELECT m.slug FROM markets m WHERE m.resolved = 1
            )
        """)
        conn.execute("""
            INSERT INTO wallet_pnl (
                wallet, market_slug, condition_id, winning_token, losing_token,
                winner_bought_shares, winner_bought_cost, winner_sold_shares, winner_sold_revenue,
                loser_bought_shares, loser_bought_cost, loser_sold_shares, loser_sold_revenue,
                pnl, roi, computed_at
            )
            WITH marked AS (
                SELECT
                    t.proxy_wallet,
                    t.market_slug,
                    t.condition_id,
                    t.side,
                    t.size,
                    t.usd_amount,
                    CASE
                        WHEN m.winning_outcome = m.outcome_one AND t.asset = m.token_one THEN 1
                        WHEN m.winning_outcome = m.outcome_two AND t.asset = m.token_two THEN 1
                        ELSE 0
                    END AS is_winner
                FROM trades t
                JOIN markets m ON t.condition_id = m.condition_id
                WHERE m.resolved = 1
                  AND m.winning_outcome IS NOT NULL
                  AND t.proxy_wallet NOT IN (
                      '0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e',
                      '0xe111180000d2663c0091e4f400237545b87b996b',
                      '0xc5d563a36ae78145c45a50134d48a1215220f80a',
                      '0xd91e80cf2e7be2e162c6513ced06f1dd0da35296',
                      '0x4d97dcd97ec945f40cf65f87097ace5ea0476045'
                  )
            )
            SELECT
                proxy_wallet AS wallet,
                market_slug,
                condition_id,
                NULL AS winning_token,
                NULL AS losing_token,
                COALESCE(SUM(CASE WHEN is_winner=1 AND side='BUY' THEN size ELSE 0 END), 0) AS winner_bought_shares,
                COALESCE(SUM(CASE WHEN is_winner=1 AND side='BUY' THEN usd_amount ELSE 0 END), 0) AS winner_bought_cost,
                COALESCE(SUM(CASE WHEN is_winner=1 AND side='SELL' THEN size ELSE 0 END), 0) AS winner_sold_shares,
                COALESCE(SUM(CASE WHEN is_winner=1 AND side='SELL' THEN usd_amount ELSE 0 END), 0) AS winner_sold_revenue,
                COALESCE(SUM(CASE WHEN is_winner=0 AND side='BUY' THEN size ELSE 0 END), 0) AS loser_bought_shares,
                COALESCE(SUM(CASE WHEN is_winner=0 AND side='BUY' THEN usd_amount ELSE 0 END), 0) AS loser_bought_cost,
                COALESCE(SUM(CASE WHEN is_winner=0 AND side='SELL' THEN size ELSE 0 END), 0) AS loser_sold_shares,
                COALESCE(SUM(CASE WHEN is_winner=0 AND side='SELL' THEN usd_amount ELSE 0 END), 0) AS loser_sold_revenue,
                0 AS pnl,
                NULL AS roi,
                ? AS computed_at
            FROM marked
            GROUP BY proxy_wallet, market_slug, condition_id
        """, (db._now_ts(),))

        # Update pnl and roi.
        conn.execute("""
            UPDATE wallet_pnl
            SET pnl = (
                (winner_bought_shares - winner_sold_shares)
                + (winner_sold_revenue + loser_sold_revenue)
                - (winner_bought_cost + loser_bought_cost)
            ),
            roi = CASE
                WHEN (winner_bought_cost + loser_bought_cost) > 0 THEN
                    ((winner_bought_shares - winner_sold_shares)
                     + (winner_sold_revenue + loser_sold_revenue)
                     - (winner_bought_cost + loser_bought_cost))
                    / (winner_bought_cost + loser_bought_cost)
                ELSE NULL
            END
        """)

        # Aggregate wallet stats.
        conn.execute("DELETE FROM wallet_stats")
        conn.execute("""
            INSERT INTO wallet_stats (
                wallet, markets_traded, markets_won, markets_lost, total_invested,
                total_settlement, total_revenue, total_pnl, avg_roi, win_rate,
                avg_pnl_per_market, computed_at
            )
            SELECT
                wallet,
                COUNT(*) AS markets_traded,
                SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) AS markets_won,
                SUM(CASE WHEN pnl < 0 THEN 1 ELSE 0 END) AS markets_lost,
                SUM(winner_bought_cost + loser_bought_cost) AS total_invested,
                SUM(winner_bought_shares - winner_sold_shares) AS total_settlement,
                SUM(winner_sold_revenue + loser_sold_revenue) AS total_revenue,
                SUM(pnl) AS total_pnl,
                AVG(roi) AS avg_roi,
                CAST(SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) AS REAL) / COUNT(*) AS win_rate,
                SUM(pnl) / COUNT(*) AS avg_pnl_per_market,
                ? AS computed_at
            FROM wallet_pnl
            GROUP BY wallet
        """, (db._now_ts(),))

        row = conn.execute("SELECT COUNT(*) AS c FROM wallet_stats").fetchone()
        wallet_count = row["c"]
        row = conn.execute("SELECT COUNT(*) AS c FROM wallet_pnl").fetchone()
        pnl_count = row["c"]

    print(f"[pipeline] computed PnL for {pnl_count} wallet-market pairs, {wallet_count} wallets")
    return wallet_count


def run_loop(
    start_ts: int,
    end_ts: int | None = None,
    interval_seconds: int = 300,
    max_workers: int = 50,
) -> None:
    """Run the pipeline continuously: fetch markets, fetch trades, analyze."""
    init()
    while True:
        try:
            end = end_ts or _now_epoch()
            fetch_markets(start_ts, end, max_workers=max_workers)
            fetch_trades()
            analyze()
            db.set_state("last_run", str(_now_epoch()))
            print(f"[pipeline] run complete at {_now_epoch()}. sleeping {interval_seconds}s...")
            time.sleep(interval_seconds)
        except KeyboardInterrupt:
            print("[pipeline] stopped")
            break
        except Exception as exc:
            print(f"[pipeline] error: {exc}")
            time.sleep(30)


def run_once(
    start_ts: int,
    end_ts: int | None = None,
    max_workers: int = 50,
) -> None:
    """Run one full pass."""
    init()
    end = end_ts or _now_epoch()
    fetch_markets(start_ts, end, max_workers=max_workers)
    fetch_trades()
    analyze()
    db.set_state("last_run", str(_now_epoch()))
    print("[pipeline] run_once complete")
