"""SQLite persistence layer for the Polymarket analyzer."""

import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any

from src.config import DATA_DIR

DB_PATH = os.path.join(DATA_DIR, "polymarket.db")


def _now_ts() -> int:
    return int(datetime.now(timezone.utc).timestamp())


SCHEMA = """
CREATE TABLE IF NOT EXISTS markets (
    slug TEXT PRIMARY KEY,
    condition_id TEXT NOT NULL,
    question TEXT,
    outcome_one TEXT,
    outcome_two TEXT,
    token_one TEXT,
    token_two TEXT,
    winning_outcome TEXT,
    resolved INTEGER DEFAULT 0,
    resolution_time TEXT,
    start_ts INTEGER,
    end_ts INTEGER,
    fetched_at INTEGER,
    trades_fetched_at INTEGER
);

CREATE INDEX IF NOT EXISTS idx_markets_time ON markets(start_ts, end_ts);
CREATE INDEX IF NOT EXISTS idx_markets_condition ON markets(condition_id);

CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    market_slug TEXT NOT NULL,
    condition_id TEXT NOT NULL,
    proxy_wallet TEXT NOT NULL,
    side TEXT NOT NULL,
    asset TEXT NOT NULL,
    size REAL NOT NULL,
    price REAL NOT NULL,
    usd_amount REAL NOT NULL,
    timestamp INTEGER,
    transaction_hash TEXT,
    log_index INTEGER,
    source TEXT,
    fetched_at INTEGER,
    UNIQUE(market_slug, transaction_hash, log_index, proxy_wallet, side, asset)
);

CREATE INDEX IF NOT EXISTS idx_trades_wallet ON trades(proxy_wallet);
CREATE INDEX IF NOT EXISTS idx_trades_market ON trades(market_slug);
CREATE INDEX IF NOT EXISTS idx_trades_condition ON trades(condition_id);
CREATE INDEX IF NOT EXISTS idx_trades_asset ON trades(asset);

CREATE TABLE IF NOT EXISTS wallet_pnl (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    wallet TEXT NOT NULL,
    market_slug TEXT NOT NULL,
    condition_id TEXT NOT NULL,
    winning_token TEXT,
    losing_token TEXT,
    winner_bought_shares REAL DEFAULT 0,
    winner_bought_cost REAL DEFAULT 0,
    winner_sold_shares REAL DEFAULT 0,
    winner_sold_revenue REAL DEFAULT 0,
    loser_bought_shares REAL DEFAULT 0,
    loser_bought_cost REAL DEFAULT 0,
    loser_sold_shares REAL DEFAULT 0,
    loser_sold_revenue REAL DEFAULT 0,
    pnl REAL DEFAULT 0,
    roi REAL,
    computed_at INTEGER,
    UNIQUE(wallet, market_slug)
);

CREATE INDEX IF NOT EXISTS idx_wallet_pnl_wallet ON wallet_pnl(wallet);
CREATE INDEX IF NOT EXISTS idx_wallet_pnl_pnl ON wallet_pnl(pnl);

CREATE TABLE IF NOT EXISTS wallet_stats (
    wallet TEXT PRIMARY KEY,
    markets_traded INTEGER DEFAULT 0,
    markets_won INTEGER DEFAULT 0,
    markets_lost INTEGER DEFAULT 0,
    total_invested REAL DEFAULT 0,
    total_settlement REAL DEFAULT 0,
    total_revenue REAL DEFAULT 0,
    total_pnl REAL DEFAULT 0,
    avg_roi REAL DEFAULT 0,
    win_rate REAL DEFAULT 0,
    avg_pnl_per_market REAL DEFAULT 0,
    computed_at INTEGER
);

CREATE INDEX IF NOT EXISTS idx_wallet_stats_pnl ON wallet_stats(total_pnl);

CREATE TABLE IF NOT EXISTS copy_trade_signals (
    wallet TEXT PRIMARY KEY,
    rank INTEGER,
    strategy TEXT,
    rationale TEXT,
    confidence TEXT,
    expected_win_rate REAL,
    avg_roi REAL,
    total_pnl REAL,
    recommended_follow_size REAL,
    generated_at INTEGER
);

CREATE TABLE IF NOT EXISTS wallet_pnl_staging (
    wallet TEXT NOT NULL,
    market_slug TEXT NOT NULL,
    condition_id TEXT NOT NULL,
    winner_bought_shares REAL DEFAULT 0,
    winner_bought_cost REAL DEFAULT 0,
    winner_sold_shares REAL DEFAULT 0,
    winner_sold_revenue REAL DEFAULT 0,
    loser_bought_shares REAL DEFAULT 0,
    loser_bought_cost REAL DEFAULT 0,
    loser_sold_shares REAL DEFAULT 0,
    loser_sold_revenue REAL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_wallet_pnl_staging_key ON wallet_pnl_staging(wallet, market_slug);

CREATE TABLE IF NOT EXISTS pipeline_state (
    key TEXT PRIMARY KEY,
    value TEXT
);
"""


def init_db() -> None:
    os.makedirs(DATA_DIR, exist_ok=True)
    with get_conn() as conn:
        conn.executescript(SCHEMA)
        # Migration: trades_fetched_at was added after initial schema.
        try:
            conn.execute("ALTER TABLE markets ADD COLUMN trades_fetched_at INTEGER")
        except sqlite3.OperationalError:
            pass  # column already exists


@contextmanager
def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def upsert_markets(markets: list[dict[str, Any]]) -> int:
    if not markets:
        return 0
    with get_conn() as conn:
        conn.executemany(
            """
            INSERT INTO markets (
                slug, condition_id, question, outcome_one, outcome_two,
                token_one, token_two, winning_outcome, resolved, resolution_time,
                start_ts, end_ts, fetched_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(slug) DO UPDATE SET
                condition_id=excluded.condition_id,
                question=excluded.question,
                outcome_one=excluded.outcome_one,
                outcome_two=excluded.outcome_two,
                token_one=excluded.token_one,
                token_two=excluded.token_two,
                winning_outcome=excluded.winning_outcome,
                resolved=excluded.resolved,
                resolution_time=excluded.resolution_time,
                start_ts=excluded.start_ts,
                end_ts=excluded.end_ts,
                fetched_at=excluded.fetched_at
            """,
            [
                (
                    m["slug"],
                    m["condition_id"],
                    m.get("question"),
                    m.get("outcome_one"),
                    m.get("outcome_two"),
                    m.get("token_one"),
                    m.get("token_two"),
                    m.get("winning_outcome"),
                    1 if m.get("resolved") else 0,
                    m.get("resolution_time"),
                    m.get("start_ts"),
                    m.get("end_ts"),
                    _now_ts(),
                )
                for m in markets
            ],
        )
        return conn.total_changes


def get_markets(
    only_resolved: bool = True,
    start_ts: int | None = None,
    end_ts: int | None = None,
) -> list[dict[str, Any]]:
    query = "SELECT * FROM markets WHERE 1=1"
    params: list[Any] = []
    if only_resolved:
        query += " AND resolved = 1"
    if start_ts is not None:
        query += " AND end_ts >= ?"
        params.append(start_ts)
    if end_ts is not None:
        query += " AND start_ts <= ?"
        params.append(end_ts)
    query += " ORDER BY start_ts"
    with get_conn() as conn:
        rows = conn.execute(query, params).fetchall()
        return [dict(row) for row in rows]


def get_market_slugs() -> set[str]:
    with get_conn() as conn:
        rows = conn.execute("SELECT slug FROM markets").fetchall()
        return {row["slug"] for row in rows}


def upsert_trades(trades: list[dict[str, Any]]) -> int:
    if not trades:
        return 0
    with get_conn() as conn:
        conn.executemany(
            """
            INSERT INTO trades (
                market_slug, condition_id, proxy_wallet, side, asset, size, price,
                usd_amount, timestamp, transaction_hash, log_index, source, fetched_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(market_slug, transaction_hash, log_index, proxy_wallet, side, asset) DO NOTHING
            """,
            [
                (
                    t["market_slug"],
                    t["condition_id"],
                    t["proxy_wallet"],
                    t["side"],
                    t["asset"],
                    t["size"],
                    t["price"],
                    t["usd_amount"],
                    t.get("timestamp"),
                    t.get("transaction_hash"),
                    t.get("log_index"),
                    t.get("source", "chain"),
                    _now_ts(),
                )
                for t in trades
            ],
        )
        return conn.total_changes


def trades_exist_for_market(slug: str) -> bool:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT 1 FROM trades WHERE market_slug = ? LIMIT 1", (slug,)
        ).fetchone()
        return row is not None


def get_unfetched_markets(only_resolved: bool = True) -> list[dict[str, Any]]:
    query = """
        SELECT m.* FROM markets m
        LEFT JOIN trades t ON m.slug = t.market_slug
        WHERE t.id IS NULL
    """
    if only_resolved:
        query += " AND m.resolved = 1"
    query += " ORDER BY m.start_ts"
    with get_conn() as conn:
        rows = conn.execute(query).fetchall()
        return [dict(row) for row in rows]


def get_unfetched_markets(only_resolved: bool = True) -> list[dict[str, Any]]:
    query = """
        SELECT m.* FROM markets m
        WHERE m.trades_fetched_at IS NULL
    """
    if only_resolved:
        query += " AND m.resolved = 1"
    query += " ORDER BY m.start_ts"
    with get_conn() as conn:
        rows = conn.execute(query).fetchall()
        return [dict(row) for row in rows]


def mark_markets_trades_fetched(slugs: list[str]) -> None:
    if not slugs:
        return
    with get_conn() as conn:
        conn.executemany(
            "UPDATE markets SET trades_fetched_at = ? WHERE slug = ?",
            [(_now_ts(), s) for s in slugs],
        )


def clear_wallet_pnl_staging() -> None:
    with get_conn() as conn:
        conn.execute("DELETE FROM wallet_pnl_staging")


def insert_wallet_pnl_staging(rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    with get_conn() as conn:
        conn.executemany(
            """
            INSERT INTO wallet_pnl_staging (
                wallet, market_slug, condition_id,
                winner_bought_shares, winner_bought_cost,
                winner_sold_shares, winner_sold_revenue,
                loser_bought_shares, loser_bought_cost,
                loser_sold_shares, loser_sold_revenue
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    r["wallet"],
                    r["market_slug"],
                    r["condition_id"],
                    r.get("winner_bought_shares", 0),
                    r.get("winner_bought_cost", 0),
                    r.get("winner_sold_shares", 0),
                    r.get("winner_sold_revenue", 0),
                    r.get("loser_bought_shares", 0),
                    r.get("loser_bought_cost", 0),
                    r.get("loser_sold_shares", 0),
                    r.get("loser_sold_revenue", 0),
                )
                for r in rows
            ],
        )
        return conn.total_changes


def compute_wallet_stats_from_staging() -> int:
    """Aggregate staging rows into wallet_stats and clear the staging table."""
    with get_conn() as conn:
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
                SUM(total_invested) AS total_invested,
                SUM(net_winner_shares) AS total_settlement,
                SUM(total_revenue) AS total_revenue,
                SUM(pnl) AS total_pnl,
                NULL AS avg_roi,
                CAST(SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) AS REAL) / COUNT(*) AS win_rate,
                SUM(pnl) / COUNT(*) AS avg_pnl_per_market,
                ? AS computed_at
            FROM (
                SELECT
                    wallet,
                    market_slug,
                    SUM(winner_bought_cost + loser_bought_cost) AS total_invested,
                    SUM(winner_sold_revenue + loser_sold_revenue) AS total_revenue,
                    SUM(winner_bought_shares - winner_sold_shares) AS net_winner_shares,
                    (
                        SUM(winner_bought_shares - winner_sold_shares)
                        + SUM(winner_sold_revenue + loser_sold_revenue)
                        - SUM(winner_bought_cost + loser_bought_cost)
                    ) AS pnl
                FROM wallet_pnl_staging
                GROUP BY wallet, market_slug
            )
            GROUP BY wallet
        """, (_now_ts(),))

        # Capital-weighted ROI: total PnL / total invested. Averaging per-market
        # ROIs gives misleading results when position sizes differ.
        conn.execute("""
            UPDATE wallet_stats
            SET avg_roi = CASE
                WHEN total_invested > 0 THEN total_pnl / total_invested
                ELSE NULL
            END
        """)

        row = conn.execute("SELECT COUNT(*) AS c FROM wallet_stats").fetchone()
        conn.execute("DELETE FROM wallet_pnl_staging")
        return row["c"]


def upsert_wallet_pnl(rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    with get_conn() as conn:
        conn.executemany(
            """
            INSERT INTO wallet_pnl (
                wallet, market_slug, condition_id, winning_token, losing_token,
                winner_bought_shares, winner_bought_cost, winner_sold_shares, winner_sold_revenue,
                loser_bought_shares, loser_bought_cost, loser_sold_shares, loser_sold_revenue,
                pnl, roi, computed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(wallet, market_slug) DO UPDATE SET
                condition_id=excluded.condition_id,
                winning_token=excluded.winning_token,
                losing_token=excluded.losing_token,
                winner_bought_shares=excluded.winner_bought_shares,
                winner_bought_cost=excluded.winner_bought_cost,
                winner_sold_shares=excluded.winner_sold_shares,
                winner_sold_revenue=excluded.winner_sold_revenue,
                loser_bought_shares=excluded.loser_bought_shares,
                loser_bought_cost=excluded.loser_bought_cost,
                loser_sold_shares=excluded.loser_sold_shares,
                loser_sold_revenue=excluded.loser_sold_revenue,
                pnl=excluded.pnl,
                roi=excluded.roi,
                computed_at=excluded.computed_at
            """,
            [
                (
                    r["wallet"],
                    r["market_slug"],
                    r["condition_id"],
                    r.get("winning_token"),
                    r.get("losing_token"),
                    r["winner_bought_shares"],
                    r["winner_bought_cost"],
                    r["winner_sold_shares"],
                    r["winner_sold_revenue"],
                    r["loser_bought_shares"],
                    r["loser_bought_cost"],
                    r["loser_sold_shares"],
                    r["loser_sold_revenue"],
                    r["pnl"],
                    r["roi"],
                    _now_ts(),
                )
                for r in rows
            ],
        )
        return conn.total_changes


def upsert_wallet_stats(rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    with get_conn() as conn:
        conn.executemany(
            """
            INSERT INTO wallet_stats (
                wallet, markets_traded, markets_won, markets_lost, total_invested,
                total_settlement, total_revenue, total_pnl, avg_roi, win_rate,
                avg_pnl_per_market, computed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(wallet) DO UPDATE SET
                markets_traded=excluded.markets_traded,
                markets_won=excluded.markets_won,
                markets_lost=excluded.markets_lost,
                total_invested=excluded.total_invested,
                total_settlement=excluded.total_settlement,
                total_revenue=excluded.total_revenue,
                total_pnl=excluded.total_pnl,
                avg_roi=excluded.avg_roi,
                win_rate=excluded.win_rate,
                avg_pnl_per_market=excluded.avg_pnl_per_market,
                computed_at=excluded.computed_at
            """,
            [
                (
                    r["wallet"],
                    r["markets_traded"],
                    r["markets_won"],
                    r["markets_lost"],
                    r["total_invested"],
                    r["total_settlement"],
                    r["total_revenue"],
                    r["total_pnl"],
                    r["avg_roi"],
                    r["win_rate"],
                    r["avg_pnl_per_market"],
                    _now_ts(),
                )
                for r in rows
            ],
        )
        return conn.total_changes


def get_top_wallets(
    min_markets: int = 5,
    min_win_rate: float = 0.0,
    limit: int = 200,
) -> list[dict[str, Any]]:
    query = """
        SELECT * FROM wallet_stats
        WHERE markets_traded >= ? AND win_rate >= ?
        ORDER BY total_pnl DESC
        LIMIT ?
    """
    with get_conn() as conn:
        rows = conn.execute(query, (min_markets, min_win_rate, limit)).fetchall()
        return [dict(row) for row in rows]


def upsert_copy_trade_signals(rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    with get_conn() as conn:
        conn.executemany(
            """
            INSERT INTO copy_trade_signals (
                wallet, rank, strategy, rationale, confidence, expected_win_rate,
                avg_roi, total_pnl, recommended_follow_size, generated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(wallet) DO UPDATE SET
                rank=excluded.rank,
                strategy=excluded.strategy,
                rationale=excluded.rationale,
                confidence=excluded.confidence,
                expected_win_rate=excluded.expected_win_rate,
                avg_roi=excluded.avg_roi,
                total_pnl=excluded.total_pnl,
                recommended_follow_size=excluded.recommended_follow_size,
                generated_at=excluded.generated_at
            """,
            [
                (
                    r["wallet"],
                    r["rank"],
                    r["strategy"],
                    r["rationale"],
                    r["confidence"],
                    r["expected_win_rate"],
                    r["avg_roi"],
                    r["total_pnl"],
                    r["recommended_follow_size"],
                    _now_ts(),
                )
                for r in rows
            ],
        )
        return conn.total_changes


def get_state(key: str) -> str | None:
    with get_conn() as conn:
        row = conn.execute("SELECT value FROM pipeline_state WHERE key = ?", (key,)).fetchone()
        return row["value"] if row else None


def set_state(key: str, value: str) -> None:
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO pipeline_state (key, value) VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value
            """,
            (key, value),
        )
