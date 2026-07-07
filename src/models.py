"""Lightweight data models used by the analyzer."""

from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class Trade:
    """A single trade (either from Data API or decoded from chain)."""

    market_slug: str
    condition_id: str
    proxy_wallet: str
    side: Literal["BUY", "SELL"]
    asset: str          # token id / asset id
    size: float         # share count (USDC decimals)
    price: float        # USDC per share
    usd_amount: float   # size * price
    timestamp: str | None = None
    transaction_hash: str | None = None
    source: str = "api"  # api | chain


@dataclass
class WalletMarketPnl:
    """PnL for one wallet in one market."""

    wallet: str
    market_slug: str
    condition_id: str
    winning_token: str | None
    losing_token: str | None
    winner_bought_shares: float = 0.0
    winner_bought_cost: float = 0.0
    winner_sold_shares: float = 0.0
    winner_sold_revenue: float = 0.0
    loser_bought_shares: float = 0.0
    loser_bought_cost: float = 0.0
    loser_sold_shares: float = 0.0
    loser_sold_revenue: float = 0.0

    def net_winner_shares(self) -> float:
        return self.winner_bought_shares - self.winner_sold_shares

    def net_loser_shares(self) -> float:
        return self.loser_bought_shares - self.loser_sold_shares

    def settlement_value(self) -> float:
        """Winning shares settle to 1 USDC, losing shares to 0."""
        return self.net_winner_shares()

    def pnl(self) -> float:
        cost = self.winner_bought_cost + self.loser_bought_cost
        revenue = self.winner_sold_revenue + self.loser_sold_revenue
        return self.settlement_value() + revenue - cost

    def invested(self) -> float:
        return self.winner_bought_cost + self.loser_bought_cost

    def roi(self) -> float | None:
        inv = self.invested()
        if inv == 0:
            return None
        return self.pnl() / inv
