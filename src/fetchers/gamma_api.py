"""Fetch market metadata from Polymarket Gamma API."""

import json
import time
from typing import Any

import requests

from tqdm import tqdm

from src.config import (
    BTC_UPDOWN_CADENCE_SECONDS,
    BTC_UPDOWN_SLUG_PREFIX,
    DEFAULT_REQUEST_DELAY,
    GAMMA_API_BASE,
    Market,
)


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
                print(f"[gamma] failed {url}: {exc}")
                return None
            time.sleep(1)
    return None


def make_btc_updown_slugs(
    start_epoch: int,
    end_epoch: int,
    cadence: int = BTC_UPDOWN_CADENCE_SECONDS,
) -> list[str]:
    """Generate slugs like btc-updown-5m-1709571300 for each 5-min window."""
    slugs = []
    cursor = (start_epoch // cadence) * cadence
    end_aligned = (end_epoch // cadence) * cadence
    while cursor <= end_aligned:
        slugs.append(f"{BTC_UPDOWN_SLUG_PREFIX}-{cursor}")
        cursor += cadence
    return slugs


def fetch_event_by_slug(slug: str) -> dict | None:
    """Fetch a single event by slug. Returns None if missing."""
    url = f"{GAMMA_API_BASE}/events"
    data = _get(url, params={"slug": slug})
    time.sleep(DEFAULT_REQUEST_DELAY)
    if isinstance(data, list) and data:
        return data[0]
    return None


def _parse_json_list(value: str | list | None) -> list:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    try:
        return json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return []


def parse_market_from_event(event: dict[str, Any]) -> Market | None:
    """Extract the binary Up/Down market from a Gamma event object."""
    markets = event.get("markets") or []
    if not markets:
        return None

    market = markets[0]

    # outcomes/clobTokenIds come back as JSON strings from Gamma API.
    outcomes = _parse_json_list(market.get("outcomes"))
    token_ids = _parse_json_list(market.get("clobTokenIds"))
    outcome_prices = _parse_json_list(market.get("outcomePrices"))

    if len(outcomes) < 2 or len(token_ids) < 2:
        return None

    outcome_one, outcome_two = outcomes[0], outcomes[1]
    token_one, token_two = str(token_ids[0]), str(token_ids[1])

    # Derive the winner from outcomePrices: the outcome priced at "1" won.
    winning_outcome = None
    resolved = market.get("umaResolutionStatus") == "resolved" or market.get("resolved") is True
    if resolved and len(outcome_prices) >= 2:
        for outcome, price in zip(outcomes, outcome_prices):
            if str(price) == "1" or str(price).lower() == "true":
                winning_outcome = outcome
                break

    if not resolved:
        resolved = bool(market.get("resolution") or market.get("winningOutcome"))
    if resolved and not winning_outcome:
        winning_outcome = market.get("resolution") or market.get("winningOutcome")

    def _ts(value: str | None) -> int | None:
        if not value:
            return None
        try:
            from datetime import datetime, timezone
            dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            return int(dt.timestamp())
        except Exception:
            return None

    # Use the event-level trading window if available (more accurate than market creation time).
    start_ts = _ts(event.get("startTime") or event.get("eventStartTime") or market.get("startDate"))
    end_ts = _ts(event.get("endDate") or market.get("endDate"))

    return Market(
        slug=event.get("slug", ""),
        condition_id=market.get("conditionId", ""),
        question=market.get("question", ""),
        outcome_one=outcome_one,
        outcome_two=outcome_two,
        token_one=token_one,
        token_two=token_two,
        winning_outcome=winning_outcome,
        resolved=resolved,
        resolution_time=market.get("resolutionTime") or market.get("closedTime"),
        start_ts=start_ts,
        end_ts=end_ts,
    )


def fetch_btc_updown_markets(
    start_epoch: int,
    end_epoch: int,
    only_resolved: bool = True,
) -> list[Market]:
    """Fetch all BTC Up/Down 5m markets in the epoch range (sequential)."""
    slugs = make_btc_updown_slugs(start_epoch, end_epoch)
    markets: list[Market] = []
    print(f"[gamma] fetching {len(slugs)} potential market slugs...")
    for slug in tqdm(slugs, desc="gamma"):
        event = fetch_event_by_slug(slug)
        if event is None:
            continue
        market = parse_market_from_event(event)
        if market is None:
            continue
        if only_resolved and not market.resolved:
            continue
        markets.append(market)
    print(f"[gamma] found {len(markets)} markets")
    return markets


def _fetch_event_batch(slugs: list[str]) -> list[dict]:
    """Fetch up to 100 events by exact slug in a single Gamma API call."""
    url = f"{GAMMA_API_BASE}/events"
    for attempt in range(5):
        data = _get(url, params={"slug": slugs, "limit": len(slugs)})
        if isinstance(data, list):
            return data
        if attempt < 4:
            time.sleep(0.5 * (attempt + 1))
    return []


def fetch_btc_updown_markets_concurrent(
    start_epoch: int,
    end_epoch: int,
    only_resolved: bool = True,
    max_workers: int = 20,
    existing_slugs: set[str] | None = None,
) -> list[Market]:
    """Fetch markets concurrently in batches of 100 slugs.

    Gamma accepts repeated `slug` query parameters, so we can resolve up to
    100 slugs per request. This is ~100x faster than one request per slug.
    """
    slugs = make_btc_updown_slugs(start_epoch, end_epoch)
    if existing_slugs:
        slugs = [s for s in slugs if s not in existing_slugs]
    if not slugs:
        return []

    from concurrent.futures import ThreadPoolExecutor, as_completed

    BATCH_SIZE = 100
    batches = [slugs[i : i + BATCH_SIZE] for i in range(0, len(slugs), BATCH_SIZE)]
    markets: list[Market] = []
    skipped_unresolved = 0

    print(
        f"[gamma] fetching {len(slugs)} slugs in {len(batches)} batches "
        f"({max_workers} workers, batch size {BATCH_SIZE})..."
    )

    def _fetch_batch(batch: list[str]) -> list[Market]:
        events = _fetch_event_batch(batch)
        # Gamma only returns events that exist; missing slugs are silently dropped.
        return [parse_market_from_event(e) for e in events if parse_market_from_event(e) is not None]

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_fetch_batch, batch): batch for batch in batches}
        for future in tqdm(as_completed(futures), total=len(batches), desc="gamma"):
            try:
                batch_markets = future.result()
            except Exception as exc:
                print(f"[gamma] batch error: {exc}")
                continue
            for market in batch_markets:
                if only_resolved and not market.resolved:
                    skipped_unresolved += 1
                    continue
                markets.append(market)

    missing = len(slugs) - len(markets) - skipped_unresolved
    print(
        f"[gamma] found {len(markets)} markets "
        f"(skipped {skipped_unresolved} unresolved, {missing} missing)"
    )
    return markets
