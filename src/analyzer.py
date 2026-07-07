"""Core PnL analyzer for Polymarket binary markets."""

from collections import defaultdict
from typing import Iterable

from src.config import (
    CTF_CONTRACT,
    CTF_EXCHANGE_V1,
    CTF_EXCHANGE_V2,
    Market,
    NEG_RISK_ADAPTER,
    NEG_RISK_CTF_EXCHANGE,
)
from src.models import Trade, WalletMarketPnl

# Addresses that are Polymarket infrastructure, not user wallets.
BLACKLISTED_WALLETS = {
    CTF_EXCHANGE_V1.lower(),
    CTF_EXCHANGE_V2.lower(),
    NEG_RISK_CTF_EXCHANGE.lower(),
    NEG_RISK_ADAPTER.lower(),
    CTF_CONTRACT.lower(),
}


def analyze_wallet_pnl(
    markets: Iterable[Market],
    trades: Iterable[Trade],
) -> dict[str, WalletMarketPnl]:
    """Aggregate PnL per (wallet, market)."""
    by_market: dict[str, Market] = {m.condition_id: m for m in markets}
    pnl_by_key: dict[str, WalletMarketPnl] = {}

    for trade in trades:
        market = by_market.get(trade.condition_id)
        if market is None:
            continue
        if not market.resolved:
            continue
        if trade.proxy_wallet.lower() in BLACKLISTED_WALLETS:
            continue

        key = f"{trade.proxy_wallet}:{trade.condition_id}"
        if key not in pnl_by_key:
            pnl_by_key[key] = WalletMarketPnl(
                wallet=trade.proxy_wallet,
                market_slug=trade.market_slug,
                condition_id=trade.condition_id,
                winning_token=market.winning_token(),
                losing_token=market.losing_token(),
            )

        pnl = pnl_by_key[key]
        is_winner = trade.asset == market.winning_token()
        is_loser = trade.asset == market.losing_token()
        if not (is_winner or is_loser):
            continue

        if trade.side == "BUY":
            if is_winner:
                pnl.winner_bought_shares += trade.size
                pnl.winner_bought_cost += trade.usd_amount
            else:
                pnl.loser_bought_shares += trade.size
                pnl.loser_bought_cost += trade.usd_amount
        else:  # SELL
            if is_winner:
                pnl.winner_sold_shares += trade.size
                pnl.winner_sold_revenue += trade.usd_amount
            else:
                pnl.loser_sold_shares += trade.size
                pnl.loser_sold_revenue += trade.usd_amount

    return pnl_by_key


def aggregate_pnl_by_wallet(
    pnl_by_key: dict[str, WalletMarketPnl],
) -> list[dict]:
    """Roll up per-wallet totals across all markets."""
    by_wallet: dict[str, dict] = defaultdict(
        lambda: {
            "wallet": "",
            "markets_traded": 0,
            "markets_won": 0,
            "markets_lost": 0,
            "total_invested": 0.0,
            "total_settlement": 0.0,
            "total_revenue": 0.0,
            "total_pnl": 0.0,
            "avg_roi": 0.0,
        }
    )

    for pnl in pnl_by_key.values():
        entry = by_wallet[pnl.wallet]
        entry["wallet"] = pnl.wallet
        entry["markets_traded"] += 1
        if pnl.pnl() > 0:
            entry["markets_won"] += 1
        elif pnl.pnl() < 0:
            entry["markets_lost"] += 1
        entry["total_invested"] += pnl.invested()
        entry["total_settlement"] += pnl.settlement_value()
        entry["total_revenue"] += (
            pnl.winner_sold_revenue + pnl.loser_sold_revenue
        )
        entry["total_pnl"] += pnl.pnl()

    results = []
    for entry in by_wallet.values():
        inv = entry["total_invested"]
        entry["avg_roi"] = entry["total_pnl"] / inv if inv > 0 else 0.0
        results.append(entry)

    return sorted(results, key=lambda x: x["total_pnl"], reverse=True)


def top_winners(
    pnl_by_key: dict[str, WalletMarketPnl],
    n: int = 20,
) -> list[dict]:
    return aggregate_pnl_by_wallet(pnl_by_key)[:n]


def top_losers(
    pnl_by_key: dict[str, WalletMarketPnl],
    n: int = 20,
) -> list[dict]:
    return aggregate_pnl_by_wallet(pnl_by_key)[-n:][::-1]
