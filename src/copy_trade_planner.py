"""Generate copy-trade plans from wallet PnL statistics."""

from typing import Any

from src import db


def _format_usd(n: float) -> str:
    return f"${n:,.2f}"


def _format_pct(n: float) -> str:
    return f"{n * 100:.2f}%"


def generate_copy_trade_plan(
    min_markets: int = 20,
    min_win_rate: float = 0.55,
    min_avg_roi: float = 0.10,
    min_total_invested: float = 1000.0,
    top_n: int = 30,
    capital_per_wallet: float = 100.0,
) -> list[dict[str, Any]]:
    """Select wallets worth copying and build a concrete plan."""
    wallets = db.get_top_wallets(
        min_markets=min_markets,
        min_win_rate=min_win_rate,
        limit=top_n * 10,
    )
    if not wallets:
        print("[copy] no qualified wallets found")
        return []

    # Filter in Python for metrics not in the DB index.
    qualified = []
    for w in wallets:
        win_rate = w["win_rate"] or 0.0
        avg_roi = w["avg_roi"] or 0.0
        total_pnl = w["total_pnl"] or 0.0
        total_invested = w["total_invested"] or 0.0
        markets = w["markets_traded"] or 0
        if (
            markets >= min_markets
            and win_rate >= min_win_rate
            and avg_roi >= min_avg_roi
            and total_invested >= min_total_invested
            and total_pnl > 0
        ):
            qualified.append(w)

    if not qualified:
        print("[copy] no wallets passed all filters")
        return []

    # Sort by a blended score: realized edge * consistency * scale.
    def _score(w: dict[str, Any]) -> float:
        return (
            (w["total_pnl"] or 0.0)
            * (1 + (w["avg_roi"] or 0.0))
            * (w["win_rate"] or 0.0)
            * min((w["markets_traded"] or 0), 500)
        )

    qualified.sort(key=_score, reverse=True)

    signals: list[dict[str, Any]] = []
    for rank, w in enumerate(qualified[:top_n], 1):
        wallet = w["wallet"]
        win_rate = w["win_rate"] or 0.0
        avg_roi = w["avg_roi"] or 0.0
        total_pnl = w["total_pnl"] or 0.0
        total_invested = w["total_invested"] or 0.0
        markets = w["markets_traded"] or 0
        avg_pnl = w["avg_pnl_per_market"] or 0.0

        # Confidence band.
        if win_rate >= 0.65 and avg_roi > 0.3 and markets >= 100:
            confidence = "HIGH"
        elif win_rate >= 0.60 and avg_roi > 0.15 and markets >= 50:
            confidence = "MEDIUM"
        else:
            confidence = "LOW"

        # Strategy label.
        if avg_roi > 1.0 and total_pnl > 10000:
            strategy = "Aggressive high-alpha"
        elif win_rate >= 0.65 and avg_roi > 0.15:
            strategy = "Consistent winner"
        elif markets >= 100 and total_pnl > 2000:
            strategy = "High-frequency grinder"
        else:
            strategy = "Speculative follow"

        # Suggested follow size: scale with expected edge, capped by avg market exposure.
        suggested_size = min(
            capital_per_wallet,
            max(20.0, avg_pnl * 2, total_invested / markets * 0.5),
        )

        rationale = (
            f"Traded {markets} resolved BTC Up/Down 5m markets with {_format_pct(win_rate)} win rate "
            f"and {_format_pct(avg_roi)} average ROI. Total realized PnL {_format_usd(total_pnl)}, "
            f"avg PnL per market {_format_usd(avg_pnl)}."
        )

        signals.append({
            "wallet": wallet,
            "rank": rank,
            "strategy": strategy,
            "rationale": rationale,
            "confidence": confidence,
            "expected_win_rate": win_rate,
            "avg_roi": avg_roi,
            "total_pnl": total_pnl,
            "recommended_follow_size": suggested_size,
        })

    db.upsert_copy_trade_signals(signals)
    return signals


def print_plan(signals: list[dict[str, Any]]) -> None:
    if not signals:
        print("No copy-trade signals generated.")
        return

    print("\n=== Copy-Trade Plan ===\n")
    print(f"{'Rank':<5} {'Wallet':<44} {'Conf':<8} {'Strategy':<25} {'Follow':<10} {'WinRate':<10} {'AvgROI':<10} {'TotalPnL':<12}")
    print("-" * 140)
    for s in signals:
        print(
            f"{s['rank']:<5} {s['wallet']:<44} {s['confidence']:<8} "
            f"{s['strategy']:<25} ${s['recommended_follow_size']:<9.2f} "
            f"{s['expected_win_rate'] * 100:>7.2f}%   {s['avg_roi'] * 100:>7.2f}%   ${s['total_pnl']:>10.2f}"
        )

    total_alloc = sum(s["recommended_follow_size"] for s in signals)
    print("-" * 140)
    print(f"Total suggested allocation: {_format_usd(total_alloc)}")
    print("\nExecution rules:")
    print("- Monitor these wallets' new positions on Polymarket BTC Up/Down 5m markets.")
    print("- Copy their net directional bias (Up/Down) within 1-2 blocks of their trade.")
    print("- Size each position at ~10-20% of the wallet's recommended follow size.")
    print("- Stop copying a wallet if its trailing 20-market win rate drops below 50%.")
    print("- Split capital across HIGH/MEDIUM confidence wallets; keep LOW as a small basket.")
