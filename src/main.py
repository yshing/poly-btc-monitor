"""CLI entry point for Polymarket BTC Up/Down 5m PnL analysis."""

import argparse
import os
import sys
from datetime import datetime, timezone

from src import db
from src.analyzer import aggregate_pnl_by_wallet, analyze_wallet_pnl
from src.config import DATA_DIR, Market
from src.copy_trade_planner import generate_copy_trade_plan, print_plan
from src.fetchers.chain_rpc import backfill_wallet_stats, build_web3, fetch_chain_trades_for_market
from src.fetchers.data_api import fetch_trades_for_condition
from src.fetchers.gamma_api import fetch_btc_updown_markets
from src.fetchers.hypersync import backfill_wallet_stats_hypersync
from src.pipeline import fetch_markets, fetch_trades, init, run_loop, run_once


def _parse_epoch(value: str) -> int:
    try:
        return int(value)
    except ValueError:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return int(dt.timestamp())


def _now_epoch() -> int:
    return int(datetime.now(timezone.utc).timestamp())


def cmd_init(_: argparse.Namespace) -> int:
    init()
    return 0


def cmd_fetch(args: argparse.Namespace) -> int:
    start = _parse_epoch(args.start) if args.start else _now_epoch() - 86400
    end = _parse_epoch(args.end) if args.end else _now_epoch()
    n = fetch_markets(start, end, max_workers=args.workers)
    print(f"[fetch] stored {n} markets")
    return 0


def cmd_pipeline(args: argparse.Namespace) -> int:
    start = _parse_epoch(args.start) if args.start else _now_epoch() - 86400
    end = _parse_epoch(args.end) if args.end else _now_epoch()
    if args.loop:
        run_loop(start, end, interval_seconds=args.interval, max_workers=args.workers)
    else:
        run_once(start, end, max_workers=args.workers)
    return 0


def cmd_trades(args: argparse.Namespace) -> int:
    n = fetch_trades(batch_hours=args.batch_hours, buffer_blocks=args.buffer)
    print(f"[trades] fetched {n} trades")
    return 0


def cmd_backfill(args: argparse.Namespace) -> int:
    """Backfill wallet stats by scanning chain logs for all unfetched markets."""
    markets = db.get_unfetched_markets(only_resolved=True)
    if args.max_markets:
        markets = markets[:args.max_markets]
    if not markets:
        print("[backfill] no unfetched markets")
        return 0

    if args.start:
        start_ts = _parse_epoch(args.start)
        markets = [m for m in markets if m.get("end_ts", 0) >= start_ts]
    if args.end:
        end_ts = _parse_epoch(args.end)
        markets = [m for m in markets if m.get("start_ts", 0) <= end_ts]

    if not markets:
        print("[backfill] no markets in selected time window")
        return 0

    print(f"[backfill] {len(markets)} markets to backfill")
    if args.use_hypersync:
        wallet_count = backfill_wallet_stats_hypersync(
            markets,
            bucket_hours=args.bucket_hours,
            buffer_blocks=args.buffer,
            flush_every=args.flush_every,
            max_workers=args.workers,
        )
    else:
        w3 = build_web3()
        wallet_count = backfill_wallet_stats(
            w3,
            markets,
            bucket_hours=args.bucket_hours,
            chunk_size=args.chunk_size,
            buffer_blocks=args.buffer,
            flush_every=args.flush_every,
            max_workers=args.workers,
        )
    print(f"[backfill] done: {wallet_count} wallets")
    return 0


def cmd_analyze(args: argparse.Namespace) -> int:
    if args.source == "chain":
        # Legacy single-run chain analysis from a markets JSON file.
        markets_path = args.markets or os.path.join(DATA_DIR, "markets.json")
        import json
        with open(markets_path) as f:
            raw_markets = json.load(f)
        markets = [Market(**m) for m in raw_markets]
        w3 = build_web3()
        all_trades = []
        for market in markets:
            trades = fetch_chain_trades_for_market(
                w3,
                market.slug,
                market.condition_id,
                market.token_one,
                market.token_two,
                market.start_ts or _now_epoch() - 3600,
                market.end_ts or _now_epoch(),
                buffer_blocks=args.buffer,
                fetch_ts=args.timestamps,
            )
            all_trades.extend(trades)
        pnl_by_key = analyze_wallet_pnl(markets, all_trades)
        rankings = aggregate_pnl_by_wallet(pnl_by_key)
        for i, row in enumerate(rankings[: args.top], 1):
            print(
                f"{i:2}. {row['wallet']} | PnL: ${row['total_pnl']:,.2f} | "
                f"ROI: {row['avg_roi']*100:+.2f}% | Markets: {row['markets_traded']} "
                f"(W{row['markets_won']}/L{row['markets_lost']})"
            )
    else:
        # Use existing wallet_stats from backfill.
        with db.get_conn() as conn:
            rows = conn.execute(
                """
                SELECT wallet, markets_traded, markets_won, markets_lost, total_invested,
                       total_settlement, total_revenue, total_pnl, avg_roi, win_rate
                FROM wallet_stats
                ORDER BY total_pnl DESC
                LIMIT ?
                """,
                (args.top,),
            ).fetchall()
        for i, row in enumerate(rows, 1):
            avg_roi = row["avg_roi"] or 0.0
            print(
                f"{i:2}. {row['wallet']} | PnL: ${row['total_pnl']:,.2f} | "
                f"ROI: {avg_roi * 100:+.2f}% | WinRate: {row['win_rate'] * 100:.2f}% | "
                f"Markets: {row['markets_traded']} (W{row['markets_won']}/L{row['markets_lost']})"
            )
        print(f"[analyze] displayed top {len(rows)} wallets from existing stats")
    return 0


def cmd_plan(args: argparse.Namespace) -> int:
    signals = generate_copy_trade_plan(
        min_markets=args.min_markets,
        min_win_rate=args.min_win_rate,
        min_avg_roi=args.min_avg_roi,
        min_total_invested=args.min_total_invested,
        top_n=args.top,
        capital_per_wallet=args.capital,
    )
    print_plan(signals)
    return 0


def cmd_report(args: argparse.Namespace) -> int:
    """Quick DB summary."""
    with db.get_conn() as conn:
        market_count = conn.execute("SELECT COUNT(*) AS c FROM markets").fetchone()["c"]
        resolved_count = conn.execute("SELECT COUNT(*) AS c FROM markets WHERE resolved=1").fetchone()["c"]
        trade_count = conn.execute("SELECT COUNT(*) AS c FROM trades").fetchone()["c"]
        wallet_count = conn.execute("SELECT COUNT(*) AS c FROM wallet_stats").fetchone()["c"]
    print("Database summary:")
    print(f"  markets: {market_count} ({resolved_count} resolved)")
    print(f"  trades:  {trade_count}")
    print(f"  wallets: {wallet_count}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="poly-btc-monitor",
        description="Analyze Polymarket BTC Up/Down 5m on-chain PnL.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    init_p = sub.add_parser("init", help="initialize the SQLite database")
    init_p.set_defaults(func=cmd_init)

    fetch_p = sub.add_parser("fetch", help="fetch BTC Up/Down 5m market metadata into DB")
    fetch_p.add_argument("--start", help="start time as ISO-8601 or epoch seconds (default: 24h ago)")
    fetch_p.add_argument("--end", help="end time as ISO-8601 or epoch seconds (default: now)")
    fetch_p.add_argument("--workers", type=int, default=50, help="concurrent Gamma API workers (default: 50)")
    fetch_p.set_defaults(func=cmd_fetch)

    pipeline_p = sub.add_parser("pipeline", help="run the full pipeline (fetch markets + trades + analyze)")
    pipeline_p.add_argument("--start", help="start time as ISO-8601 or epoch seconds (default: 24h ago)")
    pipeline_p.add_argument("--end", help="end time as ISO-8601 or epoch seconds (default: now)")
    pipeline_p.add_argument("--loop", action="store_true", help="run continuously every --interval seconds")
    pipeline_p.add_argument("--interval", type=int, default=300, help="loop interval in seconds (default: 300)")
    pipeline_p.add_argument("--workers", type=int, default=50, help="concurrent Gamma API workers (default: 50)")
    pipeline_p.set_defaults(func=cmd_pipeline)

    trades_p = sub.add_parser("trades", help="fetch on-chain trades for unresolved-fetched markets")
    trades_p.add_argument("--batch-hours", type=int, default=1, help="hours per chain batch (default: 1)")
    trades_p.add_argument("--buffer", type=int, default=100, help="extra blocks around each batch (default: 100)")
    trades_p.set_defaults(func=cmd_trades)

    backfill_p = sub.add_parser("backfill", help="scan chain logs for all unfetched markets and build wallet_stats")
    backfill_p.add_argument("--start", help="only backfill markets ending on or after this time (ISO-8601 or epoch)")
    backfill_p.add_argument("--end", help="only backfill markets starting on or before this time (ISO-8601 or epoch)")
    backfill_p.add_argument("--use-hypersync", action="store_true", help="use Envio HyperSync instead of RPC")
    backfill_p.add_argument("--bucket-hours", type=int, default=1, help="hours per chain scan bucket (default: 1)")
    backfill_p.add_argument("--chunk-size", type=int, default=80, help="blocks per getLogs call (default: 80)")
    backfill_p.add_argument("--buffer", type=int, default=200, help="extra blocks around market window (default: 200)")
    backfill_p.add_argument("--flush-every", type=int, default=50000, help="flush aggregates after N trades (default: 50000)")
    backfill_p.add_argument("--workers", type=int, default=4, help="concurrent RPC/HyperSync workers (default: 4)")
    backfill_p.add_argument("--max-markets", type=int, default=None, help="limit backfill to first N markets for testing")
    backfill_p.set_defaults(func=cmd_backfill)

    analyze_p = sub.add_parser("analyze", help="compute wallet PnL from DB or a markets JSON file")
    analyze_p.add_argument(
        "--markets",
        help="path to markets JSON (legacy chain mode; default: data/markets.json)",
    )
    analyze_p.add_argument(
        "--source",
        choices=["db", "api", "chain"],
        default="db",
        help="data source: db (stored trades), chain (legacy JSON file), api (not recommended)",
    )
    analyze_p.add_argument("--buffer", type=int, default=100, help="extra blocks for legacy chain mode")
    analyze_p.add_argument("--timestamps", action="store_true", help="fetch block timestamps (legacy chain mode)")
    analyze_p.add_argument("--top", type=int, default=10, help="number of top wallets to display")
    analyze_p.set_defaults(func=cmd_analyze)

    plan_p = sub.add_parser("plan", help="generate a copy-trade plan from DB stats")
    plan_p.add_argument("--min-markets", type=int, default=20, help="minimum markets traded (default: 20)")
    plan_p.add_argument("--min-win-rate", type=float, default=0.55, help="minimum win rate (default: 0.55)")
    plan_p.add_argument("--min-avg-roi", type=float, default=0.10, help="minimum average ROI (default: 0.10)")
    plan_p.add_argument("--min-total-invested", type=float, default=1000.0, help="minimum total invested USD (default: 1000)")
    plan_p.add_argument("--top", type=int, default=20, help="top N wallets in plan (default: 20)")
    plan_p.add_argument("--capital", type=float, default=100.0, help="base capital per wallet (default: 100)")
    plan_p.set_defaults(func=cmd_plan)

    report_p = sub.add_parser("report", help="show DB summary")
    report_p.set_defaults(func=cmd_report)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
