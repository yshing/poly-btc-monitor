"""Shared configuration for Polymarket BTC Up/Down 5m analyzer."""

import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

# Polymarket contract addresses on Polygon
CTF_EXCHANGE_V2 = "0xe111180000d2663c0091e4f400237545b87b996b"  # post 2026-04-28
CTF_EXCHANGE_V1 = "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e"
NEG_RISK_CTF_EXCHANGE = "0xC5d563A36AE78145C45a50134d48A1215220f80a"
NEG_RISK_CTF_EXCHANGE_V2 = "0xe2222d279d744050d28e00520010520000310f59"
NEG_RISK_ADAPTER = "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296"
CTF_CONTRACT = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
USDC_CONTRACT = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"

# Event signatures (topic0)
# V2 OrderFilled: OrderFilled(bytes32 orderHash, address maker, address taker,
#   uint8 side, uint256 tokenId, uint256 makerAmountFilled, uint256 takerAmountFilled,
#   uint256 fee, bytes32 builder, bytes32 metadata)
ORDER_FILLED_V2_TOPIC = "0xd543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee"
# V1 OrderFilled: OrderFilled(bytes32 orderHash, address maker, address taker,
#   uint256 makerAssetId, uint256 takerAssetId, uint256 makerAmountFilled, uint256 takerAmountFilled, uint256 fee)
ORDER_FILLED_V1_TOPIC = "0xd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6"
ORDERS_MATCHED_TOPIC = "0x63bf4d16b7fa898ef4c4b2b6d90fd201e9c56313b65638af6088d149d2ce956c"

# API endpoints
GAMMA_API_BASE = "https://gamma-api.polymarket.com"
DATA_API_BASE = "https://data-api.polymarket.com"
CLOB_API_BASE = "https://clob.polymarket.com"

# Market constants
BTC_UPDOWN_CADENCE_SECONDS = 300  # 5 minutes
BTC_UPDOWN_SLUG_PREFIX = "btc-updown-5m"

# Defaults
DEFAULT_PAGE_LIMIT = 100
DEFAULT_REQUEST_DELAY = 0.05  # seconds between Gamma API calls (be polite)
DATA_API_LIMIT = 1000
USDC_DECIMALS = 6

# Files
DATA_DIR = "data"


def polygon_rpc_url() -> str:
    """Hard-coded reliable Polygon RPC.

    The env var POLYGON_RPC_URL is still honored if you explicitly want to
    override, but we default to a known-working public endpoint so a stale
    .env cannot block the tool.
    """
    env = os.getenv("POLYGON_RPC_URL", "").strip()
    # Keep the override mechanism, but ignore common broken defaults.
    if env and env not in {"https://polygon-rpc.com", "http://polygon-rpc.com"}:
        return env
    return "https://polygon.drpc.org"


def rpc_fallbacks() -> list[str]:
    """Public RPC endpoints tried in order if the configured RPC fails."""
    return [
        "https://polygon.drpc.org",
        "https://polygon-bor-rpc.publicnode.com",
        "https://polygon.llamarpc.com",
    ]


def hypersync_token() -> str | None:
    return os.getenv("HYPERSYNC_API_TOKEN") or os.getenv("HYPERSYNC_API")


@dataclass(frozen=True)
class Market:
    """Minimal market metadata needed for PnL analysis."""

    slug: str
    condition_id: str
    question: str
    outcome_one: str  # e.g. "Up"
    outcome_two: str  # e.g. "Down"
    token_one: str    # clob token id / asset id for outcome_one
    token_two: str    # clob token id / asset id for outcome_two
    winning_outcome: str | None = None  # None if not resolved yet
    resolved: bool = False
    resolution_time: str | None = None
    start_ts: int | None = None  # epoch seconds when market starts accepting bets
    end_ts: int | None = None    # epoch seconds when market closes

    def winning_token(self) -> str | None:
        if not self.resolved or not self.winning_outcome:
            return None
        if self.winning_outcome.lower() == self.outcome_one.lower():
            return self.token_one
        if self.winning_outcome.lower() == self.outcome_two.lower():
            return self.token_two
        return None

    def losing_token(self) -> str | None:
        if not self.resolved or not self.winning_outcome:
            return None
        if self.winning_outcome.lower() == self.outcome_one.lower():
            return self.token_two
        if self.winning_outcome.lower() == self.outcome_two.lower():
            return self.token_one
        return None

    def chain_start_ts(self) -> int | None:
        return self.start_ts

    def chain_end_ts(self) -> int | None:
        return self.end_ts or self.resolution_ts()

    def resolution_ts(self) -> int | None:
        if not self.resolution_time:
            return None
        try:
            from datetime import datetime, timezone
            dt = datetime.fromisoformat(str(self.resolution_time).replace("Z", "+00:00"))
            return int(dt.timestamp())
        except Exception:
            return None
