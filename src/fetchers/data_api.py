"""Fetch public trade history from Polymarket Data API."""

import time
from typing import Any

import requests

from src.config import DATA_API_BASE, DATA_API_LIMIT, DEFAULT_REQUEST_DELAY
from src.models import Trade


def _get(url: str, params: dict | None = None, retries: int = 3) -> dict | list | None:
    for attempt in range(retries):
        try:
            resp = requests.get(url, params=params, timeout=30)
            if resp.status_code == 429:
                wait = 2 ** attempt
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as exc:
            if attempt == retries - 1:
                print(f"[data-api] failed {url}: {exc}")
                return None
            time.sleep(1)
    return None


def fetch_trades_for_condition(
    condition_id: str,
    market_slug: str,
    limit: int = DATA_API_LIMIT,
) -> list[Trade]:
    """Page through data-api /trades filtered by conditionId."""
    trades: list[Trade] = []
    offset = 0
    while True:
        url = f"{DATA_API_BASE}/trades"
        params = {
            "conditionId": condition_id,
            "limit": limit,
            "offset": offset,
        }
        data = _get(url, params=params)
        time.sleep(DEFAULT_REQUEST_DELAY)
        if not isinstance(data, list):
            break
        if not data:
            break

        for row in data:
            trade = _row_to_trade(row, market_slug, condition_id)
            if trade:
                trades.append(trade)

        if len(data) < limit:
            break
        offset += limit

    return trades


def _row_to_trade(row: dict[str, Any], market_slug: str, condition_id: str) -> Trade | None:
    side = str(row.get("side", "")).upper()
    if side not in {"BUY", "SELL"}:
        return None
    size = float(row.get("size", 0))
    price = float(row.get("price", 0))
    if size <= 0 or price < 0:
        return None
    return Trade(
        market_slug=market_slug,
        condition_id=condition_id,
        proxy_wallet=str(row.get("proxyWallet", "")).lower(),
        side=side,  # type: ignore[arg-type]
        asset=str(row.get("asset", "")),
        size=size,
        price=price,
        usd_amount=size * price,
        timestamp=row.get("timestamp"),
        transaction_hash=row.get("transactionHash") or row.get("tx_hash"),
        source="api",
    )
