"""HyperSync fetcher for high-volume Polymarket OrderFilled events.

Requires a free/paid Envio HyperSync token. Much faster than RPC for
backfilling large date ranges.
"""

from typing import Any

import time

import requests

from src.config import (
    CTF_EXCHANGE_V1,
    CTF_EXCHANGE_V2,
    NEG_RISK_CTF_EXCHANGE_V2,
    ORDER_FILLED_V1_TOPIC,
    ORDER_FILLED_V2_TOPIC,
    USDC_DECIMALS,
    hypersync_token,
)
from src.fetchers.chain_rpc import (
    BLACKLISTED_WALLETS,
    V1_ORDER_FILLED_ABI,
    V2_ORDER_FILLED_ABI,
    _estimate_block_fast,
    build_web3,
    estimate_block_by_timestamp,
)
from src.models import Trade
from src import db
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
import requests

HYPERSYNC_URL = "https://polygon.hypersync.xyz/query"

V2_GENESIS_TIMESTAMP = 1776432000  # 2026-04-28 00:00:00 UTC


def _decode_v2_log(log: dict[str, Any], market_lookup: dict[str, tuple]) -> list[Trade]:
    data = log["data"]
    side = int(data[:64], 16)  # 0 = maker BUY, 1 = maker SELL
    token_id = str(int(data[64:128], 16)) if data.startswith("0x") else str(int(data, 16))
    if token_id not in market_lookup:
        return []
    slug, condition_id, t1, t2 = market_lookup[token_id]
    maker_amount = int(data[128:192], 16) / 10 ** USDC_DECIMALS
    taker_amount = int(data[192:256], 16) / 10 ** USDC_DECIMALS
    # side: 0 = maker BUY (maker pays USDC, receives shares), 1 = maker SELL (maker pays shares, receives USDC)
    if side == 0:
        shares = taker_amount
        usdc = maker_amount
    else:
        shares = maker_amount
        usdc = taker_amount
    size = shares
    price = usdc / shares if shares > 0 else 0.0
    maker = "0x" + log["topic1"][-40:]
    taker = "0x" + log["topic2"][-40:]
    tx_hash = log["transaction_hash"]
    ts = log.get("block_timestamp")

    maker_side = "BUY" if side == 0 else "SELL"
    taker_side = "SELL" if side == 0 else "BUY"
    return [
        Trade(market_slug=slug, condition_id=condition_id, proxy_wallet=maker.lower(), side=maker_side,
              asset=token_id, size=size, price=price, usd_amount=size * price,
              timestamp=str(ts) if ts else None, transaction_hash=tx_hash, source="hypersync"),
        Trade(market_slug=slug, condition_id=condition_id, proxy_wallet=taker.lower(), side=taker_side,
              asset=token_id, size=size, price=price, usd_amount=size * price,
              timestamp=str(ts) if ts else None, transaction_hash=tx_hash, source="hypersync"),
    ]


def _decode_v1_log(log: dict[str, Any], market_lookup: dict[str, tuple]) -> list[Trade]:
    data = log["data"]
    maker_asset = str(int(data[:64], 16))
    taker_asset = str(int(data[64:128], 16))
    maker_amount = int(data[128:192], 16) / 10 ** USDC_DECIMALS
    taker_amount = int(data[192:256], 16) / 10 ** USDC_DECIMALS
    maker = "0x" + log["topic1"][-40:]
    taker = "0x" + log["topic2"][-40:]
    tx_hash = log["transaction_hash"]
    ts = log.get("block_timestamp")

    target_tokens = set(market_lookup.keys())
    matched = ({maker_asset, taker_asset} & target_tokens)
    if not matched:
        return []

    trades: list[Trade] = []
    for token_id in matched:
        slug, condition_id, t1, t2 = market_lookup[token_id]
        is_maker_asset = token_id == maker_asset
        size = maker_amount if is_maker_asset else taker_amount
        usdc = taker_amount if is_maker_asset else maker_amount
        price = usdc / size if size > 0 else 0.0
        if is_maker_asset:
            trades.append(Trade(market_slug=slug, condition_id=condition_id, proxy_wallet=maker.lower(), side="SELL",
                                asset=token_id, size=size, price=price, usd_amount=size * price,
                                timestamp=str(ts) if ts else None, transaction_hash=tx_hash, source="hypersync"))
            trades.append(Trade(market_slug=slug, condition_id=condition_id, proxy_wallet=taker.lower(), side="BUY",
                                asset=token_id, size=size, price=price, usd_amount=size * price,
                                timestamp=str(ts) if ts else None, transaction_hash=tx_hash, source="hypersync"))
        else:
            trades.append(Trade(market_slug=slug, condition_id=condition_id, proxy_wallet=maker.lower(), side="BUY",
                                asset=token_id, size=size, price=price, usd_amount=size * price,
                                timestamp=str(ts) if ts else None, transaction_hash=tx_hash, source="hypersync"))
            trades.append(Trade(market_slug=slug, condition_id=condition_id, proxy_wallet=taker.lower(), side="SELL",
                                asset=token_id, size=size, price=price, usd_amount=size * price,
                                timestamp=str(ts) if ts else None, transaction_hash=tx_hash, source="hypersync"))
    return trades


def fetch_hypersync_trades(
    from_block: int,
    to_block: int,
    contract_address: str,
    topic: str,
    market_lookup: dict[str, tuple],
    version: str,
) -> list[Trade]:
    """Fetch OrderFilled logs from HyperSync for a block range."""
    token = hypersync_token()
    if not token:
        raise RuntimeError("HYPERSYNC_API_TOKEN not set")

    query = {
        "from_block": from_block,
        "to_block": to_block,
        "logs": [{"address": [contract_address], "topics": [[topic]]}],
        "field_selection": {
            "log": ["block_number", "log_index", "transaction_hash", "address", "data", "topic0", "topic1", "topic2", "topic3"],
            "block": ["timestamp"],
        },
    }

    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    resp = requests.post(HYPERSYNC_URL, json=query, headers=headers, timeout=120)
    resp.raise_for_status()
    data = resp.json()

    block_ts: dict[int, int] = {}
    pages = data.get("data", [])
    if isinstance(pages, dict):
        pages = [pages]
    for page in pages:
        if not isinstance(page, dict):
            continue
        for b in page.get("blocks", []):
            block_ts[b["number"]] = int(b["timestamp"], 16) if isinstance(b["timestamp"], str) else b["timestamp"]

    trades: list[Trade] = []
    for page in pages:
        if not isinstance(page, dict):
            continue
        for log in page.get("logs", []):
            log["block_timestamp"] = block_ts.get(log["block_number"])
            if version == "v2":
                trades.extend(_decode_v2_log(log, market_lookup))
            else:
                trades.extend(_decode_v1_log(log, market_lookup))

    return trades


def fetch_hypersync_trades_for_markets_batch(
    markets: list[dict[str, Any]],
    batch_hours: int = 6,
) -> list[Trade]:
    """Batch-fetch trades via HyperSync."""
    if not markets:
        return []

    from collections import defaultdict

    v1_markets = [m for m in markets if m.get("start_ts", 0) < 1776432000]
    v2_markets = [m for m in markets if m.get("start_ts", 0) >= 1776432000]

    all_trades: list[Trade] = []
    for version, version_markets in [("v1", v1_markets), ("v2", v2_markets)]:
        if not version_markets:
            continue
        contract = CTF_EXCHANGE_V1 if version == "v1" else [CTF_EXCHANGE_V2, NEG_RISK_CTF_EXCHANGE_V2]
        topic = ORDER_FILLED_V1_TOPIC if version == "v1" else ORDER_FILLED_V2_TOPIC

        version_markets.sort(key=lambda m: m.get("start_ts", 0))
        bucket_seconds = batch_hours * 3600
        buckets: dict[int, list[dict]] = defaultdict(list)
        for m in version_markets:
            buckets[m.get("start_ts", 0) // bucket_seconds].append(m)

        print(f"[hypersync] batching {len(version_markets)} {version} markets into {len(buckets)} buckets")

        # Need block numbers - use RPC quickly to estimate boundaries.
        from src.fetchers.chain_rpc import build_web3, estimate_block_by_timestamp
        w3 = build_web3()

        for bucket in sorted(buckets.keys()):
            bucket_markets = buckets[bucket]
            start_ts = min(m.get("start_ts", 0) for m in bucket_markets)
            end_ts = max(m.get("end_ts", 0) for m in bucket_markets)
            from_block = max(1, estimate_block_by_timestamp(w3, start_ts) - 50)
            to_block = min(w3.eth.block_number, estimate_block_by_timestamp(w3, end_ts) + 50)

            lookup: dict[str, tuple] = {}
            for m in bucket_markets:
                lookup[m["token_one"]] = (m["slug"], m["condition_id"], m["token_one"], m["token_two"])
                lookup[m["token_two"]] = (m["slug"], m["condition_id"], m["token_one"], m["token_two"])

            print(f"[hypersync] fetching blocks {from_block}-{to_block} ({len(bucket_markets)} markets)")
            trades = fetch_hypersync_trades(from_block, to_block, contract, topic, lookup, version)
            print(f"[hypersync] got {len(trades)} trades")
            all_trades.extend(trades)

    return all_trades


def _fetch_hypersync_logs_for_range(
    from_block: int,
    to_block: int,
    contract_address: str | list[str],
    topic: str,
) -> list[dict[str, Any]]:
    """Fetch all OrderFilled logs from HyperSync for a block range, handling pagination and rate limits."""
    token = hypersync_token()
    if not token:
        raise RuntimeError("HYPERSYNC_API_TOKEN not set")

    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    field_selection = {
        "log": ["block_number", "log_index", "transaction_hash", "address", "data", "topic0", "topic1", "topic2", "topic3"],
        "block": ["number", "timestamp"],
    }

    addresses = [contract_address] if isinstance(contract_address, str) else contract_address
    all_logs: list[dict[str, Any]] = []
    block_ts: dict[int, int] = {}

    for addr in addresses:
        cursor = from_block
        addr_log_count = 0
        while cursor <= to_block:
            query = {
                "from_block": cursor,
                "to_block": to_block,
                "logs": [{"address": [addr], "topics": [[topic]]}],
                "field_selection": field_selection,
            }
            data = None
            for attempt in range(5):
                try:
                    resp = requests.post(HYPERSYNC_URL, json=query, headers=headers, timeout=120)
                    if resp.status_code == 429:
                        sleep_s = min(2 ** attempt * 5, 60)
                        print(f"[hypersync] 429 for {addr} {cursor}-{to_block}, sleeping {sleep_s}s")
                        time.sleep(sleep_s)
                        continue
                    resp.raise_for_status()
                    data = resp.json()
                    break
                except Exception as exc:
                    if attempt == 4:
                        raise
                    print(f"[hypersync] query {addr} {cursor}-{to_block} failed ({exc}), retry {attempt + 1}/5")
                    time.sleep(min(2 ** attempt, 30))
                    continue

            if data is None:
                print(f"[hypersync] no usable response for {addr} {cursor}-{to_block}, stopping this address")
                break

            if not isinstance(data, dict):
                print(f"[hypersync] unexpected response type {type(data)} for {addr} {cursor}-{to_block}: {str(data)[:200]}")
                break

            pages = data.get("data", [])
            if isinstance(pages, dict):
                pages = [pages]
            elif not isinstance(pages, list):
                print(f"[hypersync] unexpected data type {type(pages)} for {addr} {cursor}-{to_block}")
                break

            page_log_count = 0
            for page in pages:
                if not isinstance(page, dict):
                    continue
                for b in page.get("blocks", []):
                    try:
                        block_ts[b["number"]] = int(b["timestamp"], 16) if isinstance(b["timestamp"], str) else b["timestamp"]
                    except Exception as exc:
                        print(f"[hypersync] block parse error {b}: {exc}")
                        continue

                logs = page.get("logs", [])
                for log in logs:
                    log["block_timestamp"] = block_ts.get(log["block_number"])
                all_logs.extend(logs)
                page_log_count += len(logs)

            addr_log_count += page_log_count

            next_block = data.get("next_block")
            if next_block is None or next_block <= cursor:
                break
            cursor = next_block
        print(f"[hypersync] fetched {addr_log_count} logs for {addr} blocks {from_block}-{to_block}")

    return all_logs


def backfill_wallet_stats_hypersync(
    markets: list[dict[str, Any]],
    bucket_hours: int = 1,
    buffer_blocks: int = 200,
    flush_every: int = 50000,
    max_workers: int = 4,
) -> int:
    """Backfill wallet stats using Envio HyperSync instead of RPC."""
    if not markets:
        return 0

    def _set_tokens(m: dict[str, Any]) -> None:
        win = m.get("winning_outcome")
        one = m.get("outcome_one", "")
        two = m.get("outcome_two", "")
        if win and one and two:
            if str(win).lower() == str(one).lower():
                m["winning_token"] = m.get("token_one")
                m["losing_token"] = m.get("token_two")
            elif str(win).lower() == str(two).lower():
                m["winning_token"] = m.get("token_two")
                m["losing_token"] = m.get("token_one")
            else:
                m["winning_token"] = None
                m["losing_token"] = None
        else:
            m["winning_token"] = None
            m["losing_token"] = None

    for m in markets:
        _set_tokens(m)

    v1_markets = [m for m in markets if m.get("start_ts", 0) < V2_GENESIS_TIMESTAMP]
    v2_markets = [m for m in markets if m.get("start_ts", 0) >= V2_GENESIS_TIMESTAMP]

    market_by_condition: dict[str, dict[str, Any]] = {}
    for m in markets:
        market_by_condition[m["condition_id"].lower()] = m

    db.clear_wallet_pnl_staging()
    processed_log_count = 0
    processed_trade_count = 0

    def _new_agg() -> dict[str, float]:
        return {
            "winner_bought_shares": 0.0,
            "winner_bought_cost": 0.0,
            "winner_sold_shares": 0.0,
            "winner_sold_revenue": 0.0,
            "loser_bought_shares": 0.0,
            "loser_bought_cost": 0.0,
            "loser_sold_shares": 0.0,
            "loser_sold_revenue": 0.0,
        }

    def _bucket_worker(
        bucket: int,
        buckets: dict[int, list[dict[str, Any]]],
        version: str,
        contract: str,
        topic: str,
        decode_fast: Any,
        version_start_ts: int,
        version_start_block: int,
        version_end_ts: int,
        version_end_block: int,
    ) -> tuple[dict, int, int]:
        bucket_markets = buckets[bucket]
        start_ts = min(m.get("start_ts", 0) for m in bucket_markets)
        end_ts = max(m.get("end_ts", 0) for m in bucket_markets)

        from_block = _estimate_block_fast(
            start_ts, version_start_ts, version_start_block, version_end_ts, version_end_block
        ) - buffer_blocks
        to_block = _estimate_block_fast(
            end_ts, version_start_ts, version_start_block, version_end_ts, version_end_block
        ) + buffer_blocks
        from_block = max(1, from_block)
        to_block = min(w3.eth.block_number, to_block)

        token_to_market: dict[str, tuple[str, str, str, str]] = {}
        for m in bucket_markets:
            t1, t2 = m["token_one"], m["token_two"]
            slug, cond = m["slug"], m["condition_id"]
            token_to_market[t1] = (slug, cond, t1, t2)
            token_to_market[t2] = (slug, cond, t1, t2)

        logs = _fetch_hypersync_logs_for_range(from_block, to_block, contract, topic)

        local_agg: dict[tuple[str, str, str], dict[str, float]] = defaultdict(_new_agg)
        local_log_count = 0
        local_trade_count = 0

        for log in logs:
            trades = decode_fast(log, token_to_market)
            if not trades:
                continue
            local_log_count += 1
            for trade in trades:
                if trade.proxy_wallet.lower() in BLACKLISTED_WALLETS:
                    continue
                market = market_by_condition.get(trade.condition_id.lower())
                if market is None:
                    continue
                win_tok = market.get("winning_token")
                lose_tok = market.get("losing_token")
                is_winner = trade.asset == win_tok
                is_loser = trade.asset == lose_tok
                if not (is_winner or is_loser):
                    continue
                key = (trade.proxy_wallet, trade.market_slug, trade.condition_id)
                agg = local_agg[key]
                if trade.side == "BUY":
                    if is_winner:
                        agg["winner_bought_shares"] += trade.size
                        agg["winner_bought_cost"] += trade.usd_amount
                    else:
                        agg["loser_bought_shares"] += trade.size
                        agg["loser_bought_cost"] += trade.usd_amount
                else:
                    if is_winner:
                        agg["winner_sold_shares"] += trade.size
                        agg["winner_sold_revenue"] += trade.usd_amount
                    else:
                        agg["loser_sold_shares"] += trade.size
                        agg["loser_sold_revenue"] += trade.usd_amount
                local_trade_count += 1

        return local_agg, local_log_count, local_trade_count

    w3 = build_web3()

    for version_markets in [v1_markets, v2_markets]:
        if not version_markets:
            continue
        version = "v1" if version_markets[0].get("start_ts", 0) < V2_GENESIS_TIMESTAMP else "v2"
        # Polymarket BTC Up/Down 5m markets live on the main CTF V2 exchange.
        # Skip neg-risk exchange for now to halve HyperSync requests on the free tier.
        contract = CTF_EXCHANGE_V1 if version == "v1" else CTF_EXCHANGE_V2
        topic = ORDER_FILLED_V1_TOPIC if version == "v1" else ORDER_FILLED_V2_TOPIC
        decode_fast = _decode_v1_log if version == "v1" else _decode_v2_log

        version_start_ts = min(m.get("start_ts", 0) for m in version_markets)
        version_end_ts = max(m.get("end_ts", 0) for m in version_markets)
        version_start_block = estimate_block_by_timestamp(w3, version_start_ts)
        version_end_block = estimate_block_by_timestamp(w3, version_end_ts)

        bucket_seconds = bucket_hours * 3600
        buckets: dict[int, list[dict[str, Any]]] = defaultdict(list)
        for m in version_markets:
            buckets[m.get("start_ts", 0) // bucket_seconds].append(m)

        print(
            f"[hypersync] backfill {version}: {len(version_markets)} markets, "
            f"{len(buckets)} buckets (~{bucket_hours}h), blocks "
            f"{version_start_block}-{version_end_block}"
        )

        chunk_agg: dict[tuple[str, str, str], dict[str, float]] = defaultdict(_new_agg)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    _bucket_worker,
                    bucket,
                    buckets,
                    version,
                    contract,
                    topic,
                    decode_fast,
                    version_start_ts,
                    version_start_block,
                    version_end_ts,
                    version_end_block,
                ): bucket
                for bucket in buckets.keys()
            }
            for future in tqdm(
                as_completed(futures), total=len(futures), desc=f"hypersync-{version}"
            ):
                try:
                    local_agg, logs_i, trades_i = future.result()
                except Exception as exc:
                    print(f"[hypersync] bucket worker error: {exc}")
                    continue
                processed_log_count += logs_i
                processed_trade_count += trades_i
                for key, vals in local_agg.items():
                    agg = chunk_agg[key]
                    for kk, vv in vals.items():
                        agg[kk] += vv
                if processed_trade_count >= flush_every:
                    db.insert_wallet_pnl_staging(
                        [
                            {
                                "wallet": wallet,
                                "market_slug": slug,
                                "condition_id": cond,
                                **vals,
                            }
                            for (wallet, slug, cond), vals in chunk_agg.items()
                        ]
                    )
                    chunk_agg.clear()
                    processed_trade_count = 0

        if chunk_agg:
            db.insert_wallet_pnl_staging(
                [
                    {
                        "wallet": wallet,
                        "market_slug": slug,
                        "condition_id": cond,
                        **vals,
                    }
                    for (wallet, slug, cond), vals in chunk_agg.items()
                ]
            )
            chunk_agg.clear()

    wallet_count = db.compute_wallet_stats_from_staging()
    db.mark_markets_trades_fetched([m["slug"] for m in markets])
    print(
        f"[hypersync] backfilled wallet stats for {wallet_count} wallets "
        f"from {processed_log_count} logs"
    )
    return wallet_count
