"""On-chain fetcher for Polymarket CTF Exchange OrderFilled events.

Requires a Polygon RPC URL (set POLYGON_RPC_URL in .env).
This is slower than the Data API but gives raw on-chain events.
"""

import time
from typing import Any

from tqdm import tqdm
from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware

from src import db
from src.config import (
    CTF_CONTRACT,
    CTF_EXCHANGE_V1,
    CTF_EXCHANGE_V2,
    NEG_RISK_ADAPTER,
    NEG_RISK_CTF_EXCHANGE,
    NEG_RISK_CTF_EXCHANGE_V2,
    ORDER_FILLED_V1_TOPIC,
    ORDER_FILLED_V2_TOPIC,
    USDC_DECIMALS,
    polygon_rpc_url,
    rpc_fallbacks,
)
from src.models import Trade

# Addresses that are Polymarket infrastructure, not user wallets.
BLACKLISTED_WALLETS = {
    CTF_EXCHANGE_V1.lower(),
    CTF_EXCHANGE_V2.lower(),
    NEG_RISK_CTF_EXCHANGE.lower(),
    NEG_RISK_CTF_EXCHANGE_V2.lower(),
    NEG_RISK_ADAPTER.lower(),
    CTF_CONTRACT.lower(),
}

V2_ORDER_FILLED_ABI = {
    "anonymous": False,
    "inputs": [
        {"indexed": True, "internalType": "bytes32", "name": "orderHash", "type": "bytes32"},
        {"indexed": True, "internalType": "address", "name": "maker", "type": "address"},
        {"indexed": True, "internalType": "address", "name": "taker", "type": "address"},
        {"indexed": False, "internalType": "uint8", "name": "side", "type": "uint8"},
        {"indexed": False, "internalType": "uint256", "name": "tokenId", "type": "uint256"},
        {"indexed": False, "internalType": "uint256", "name": "makerAmountFilled", "type": "uint256"},
        {"indexed": False, "internalType": "uint256", "name": "takerAmountFilled", "type": "uint256"},
        {"indexed": False, "internalType": "uint256", "name": "fee", "type": "uint256"},
        {"indexed": False, "internalType": "bytes32", "name": "builder", "type": "bytes32"},
        {"indexed": False, "internalType": "bytes32", "name": "metadata", "type": "bytes32"},
    ],
    "name": "OrderFilled",
    "type": "event",
}

V1_ORDER_FILLED_ABI = {
    "anonymous": False,
    "inputs": [
        {"indexed": True, "internalType": "bytes32", "name": "orderHash", "type": "bytes32"},
        {"indexed": True, "internalType": "address", "name": "maker", "type": "address"},
        {"indexed": True, "internalType": "address", "name": "taker", "type": "address"},
        {"indexed": False, "internalType": "uint256", "name": "makerAssetId", "type": "uint256"},
        {"indexed": False, "internalType": "uint256", "name": "takerAssetId", "type": "uint256"},
        {"indexed": False, "internalType": "uint256", "name": "makerAmountFilled", "type": "uint256"},
        {"indexed": False, "internalType": "uint256", "name": "takerAmountFilled", "type": "uint256"},
        {"indexed": False, "internalType": "uint256", "name": "fee", "type": "uint256"},
    ],
    "name": "OrderFilled",
    "type": "event",
}

# Polymarket migrated CTF Exchange to V2 on 2026-04-28.
V2_GENESIS_TIMESTAMP = 1776432000  # 2026-04-28 00:00:00 UTC


def build_web3() -> Web3:
    """Connect to Polygon, trying configured RPC then fallbacks."""
    candidates = [polygon_rpc_url()] + rpc_fallbacks()
    last_error = None
    for rpc in candidates:
        if not rpc:
            continue
        try:
            w3 = Web3(
                Web3.HTTPProvider(rpc, request_kwargs={"timeout": 10}),
                middleware=[ExtraDataToPOAMiddleware],
            )
            if w3.is_connected():
                print(f"[chain] connected to {rpc}")
                return w3
        except Exception as exc:
            last_error = exc
            print(f"[chain] {rpc} failed: {exc}")
    raise ConnectionError(f"cannot connect to any Polygon RPC. Last error: {last_error}")


def estimate_block_by_timestamp(w3: Web3, target_ts: int) -> int:
    """Binary-search for the first block with timestamp >= target_ts."""
    lo, hi = 1, w3.eth.block_number
    while lo <= hi:
        mid = (lo + hi) // 2
        try:
            ts = w3.eth.get_block(mid)["timestamp"]
        except Exception:
            # Fall back to linear interpolation if a block is unavailable.
            return int(lo + (hi - lo) * (target_ts - w3.eth.get_block(lo)["timestamp"]) / max(1, w3.eth.get_block(hi)["timestamp"] - w3.eth.get_block(lo)["timestamp"]))
        if ts < target_ts:
            lo = mid + 1
        else:
            hi = mid - 1
    return lo


def fetch_block_timestamp(w3: Web3, block_number: int, _cache: dict[int, int] = {}) -> int | None:
    if block_number in _cache:
        return _cache[block_number]
    try:
        block = w3.eth.get_block(block_number)
        ts = int(block["timestamp"])
        _cache[block_number] = ts
        return ts
    except Exception as exc:
        print(f"[chain] failed to fetch block {block_number}: {exc}")
        return None


def chunked_block_ranges(
    from_block: int,
    to_block: int,
    chunk_size: int = 80,
) -> list[tuple[int, int]]:
    ranges = []
    start = from_block
    while start <= to_block:
        end = min(start + chunk_size - 1, to_block)
        ranges.append((start, end))
        start = end + 1
    return ranges


def fetch_order_filled_logs(
    w3: Web3,
    from_block: int,
    to_block: int,
    contract_address: str | list[str],
    topic: str,
    chunk_size: int = 80,
) -> list[dict[str, Any]]:
    """Fetch raw OrderFilled logs from a block range in small chunks."""
    all_logs: list[dict[str, Any]] = []
    ranges = chunked_block_ranges(from_block, to_block, chunk_size)
    for start, end in ranges:
        all_logs.extend(_get_logs_with_retry(w3, start, end, contract_address, topic))
    return all_logs


_RETRYABLE_MESSAGES = (
    "too many",
    "too large",
    "limit",
    "10000",
    "response size",
    "exceed",
    "timeout",
    "connection",
    "503",
    "502",
    "429",
)


def _get_logs_with_retry(
    w3: Web3,
    from_block: int,
    to_block: int,
    contract_address: str | list[str],
    topic: str,
    max_retries: int = 5,
) -> list[dict[str, Any]]:
    """Fetch logs for a single block range, splitting+retrying on RPC errors."""
    if from_block > to_block:
        return []

    if isinstance(contract_address, list):
        addresses = [Web3.to_checksum_address(a) for a in contract_address]
    else:
        addresses = Web3.to_checksum_address(contract_address)

    for attempt in range(max_retries):
        try:
            return w3.eth.get_logs({
                "fromBlock": from_block,
                "toBlock": to_block,
                "address": addresses,
                "topics": [topic],
            })
        except Exception as exc:
            msg = str(exc).lower()
            should_split = (
                to_block > from_block
                and any(k in msg for k in _RETRYABLE_MESSAGES)
            )
            if should_split:
                mid = (from_block + to_block) // 2
                left = _get_logs_with_retry(w3, from_block, mid, contract_address, topic, max_retries)
                right = _get_logs_with_retry(w3, mid + 1, to_block, contract_address, topic, max_retries)
                return left + right

            backoff = min(2 ** attempt, 30)
            print(f"[chain] get_logs {from_block}-{to_block} failed ({exc}), retry {attempt + 1}/{max_retries} in {backoff}s")
            time.sleep(backoff)

    print(f"[chain] get_logs {from_block}-{to_block} exhausted retries, returning empty")
    return []


def _to_trade(
    market_slug: str,
    condition_id: str,
    wallet: str,
    side: str,
    asset: str,
    size: float,
    price: float,
    block_ts: int | None,
    tx_hash: str,
) -> Trade:
    return Trade(
        market_slug=market_slug,
        condition_id=condition_id,
        proxy_wallet=wallet.lower(),
        side=side,  # type: ignore[arg-type]
        asset=asset,
        size=size,
        price=price,
        usd_amount=size * price,
        timestamp=str(block_ts) if block_ts else None,
        transaction_hash=tx_hash,
        source="chain",
    )


def decode_v2_order_filled(
    w3: Web3,
    log: dict[str, Any],
    market_slug: str,
    condition_id: str,
    token_one: str,
    token_two: str,
    fetch_ts: bool = False,
) -> list[Trade]:
    """Decode a V2 OrderFilled log into maker/taker Trade objects."""
    try:
        event = w3.eth.contract(abi=[V2_ORDER_FILLED_ABI]).events.OrderFilled()
        decoded = event.process_log(log)
        args = decoded["args"]
        token_id = str(args["tokenId"])
        if token_id not in (token_one, token_two):
            return []
        side = int(args["side"])  # 0 = maker BUY, 1 = maker SELL
        maker_amount = int(args["makerAmountFilled"]) / 10 ** USDC_DECIMALS
        taker_amount = int(args["takerAmountFilled"]) / 10 ** USDC_DECIMALS
        if side == 0:
            shares = taker_amount
            usdc = maker_amount
        else:
            shares = maker_amount
            usdc = taker_amount
        size = shares
        price = usdc / shares if shares > 0 else 0.0
        maker = args["maker"].lower()
        taker = args["taker"].lower()
        tx_hash = log["transactionHash"].hex()
        ts = fetch_block_timestamp(w3, log["blockNumber"]) if fetch_ts else None

        maker_side = "BUY" if side == 0 else "SELL"
        taker_side = "SELL" if side == 0 else "BUY"
        return [
            _to_trade(market_slug, condition_id, maker, maker_side, token_id, size, price, ts, tx_hash),
            _to_trade(market_slug, condition_id, taker, taker_side, token_id, size, price, ts, tx_hash),
        ]
    except Exception as exc:
        print(f"[chain] decode v2 error: {exc}")
        return []


def decode_v1_order_filled(
    w3: Web3,
    log: dict[str, Any],
    market_slug: str,
    condition_id: str,
    token_one: str,
    token_two: str,
    fetch_ts: bool = False,
) -> list[Trade]:
    """Decode a V1 OrderFilled log into maker/taker Trade objects."""
    try:
        event = w3.eth.contract(abi=[V1_ORDER_FILLED_ABI]).events.OrderFilled()
        decoded = event.process_log(log)
        args = decoded["args"]
        maker_asset = str(args["makerAssetId"])
        taker_asset = str(args["takerAssetId"])
        maker_amount = int(args["makerAmountFilled"]) / 10 ** USDC_DECIMALS
        taker_amount = int(args["takerAmountFilled"]) / 10 ** USDC_DECIMALS
        maker = args["maker"].lower()
        taker = args["taker"].lower()
        tx_hash = log["transactionHash"].hex()
        ts = fetch_block_timestamp(w3, log["blockNumber"]) if fetch_ts else None

        trades: list[Trade] = []
        for asset, is_maker_asset in [(maker_asset, True), (taker_asset, False)]:
            if asset not in (token_one, token_two):
                continue
            size = maker_amount if is_maker_asset else taker_amount
            usdc = taker_amount if is_maker_asset else maker_amount
            price = usdc / size if size > 0 else 0.0
            if is_maker_asset:
                trades.append(_to_trade(market_slug, condition_id, maker, "SELL", asset, size, price, ts, tx_hash))
                trades.append(_to_trade(market_slug, condition_id, taker, "BUY", asset, size, price, ts, tx_hash))
            else:
                trades.append(_to_trade(market_slug, condition_id, maker, "BUY", asset, size, price, ts, tx_hash))
                trades.append(_to_trade(market_slug, condition_id, taker, "SELL", asset, size, price, ts, tx_hash))
        return trades
    except Exception as exc:
        print(f"[chain] decode v1 error: {exc}")
        return []


def _decode_v2_log_fast(log: dict[str, Any], market_lookup: dict[str, tuple]) -> list[Trade]:
    """Decode a V2 OrderFilled log without web3 event processing."""
    data = log["data"].hex() if not isinstance(log["data"], str) else log["data"]
    side = int(data[:64], 16)  # 0 = maker BUY, 1 = maker SELL
    token_id = str(int(data[64:128], 16))
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
    maker = "0x" + log["topics"][1].hex()[-40:] if hasattr(log["topics"][1], "hex") else str(log["topics"][1])[-42:]
    taker = "0x" + log["topics"][2].hex()[-40:] if hasattr(log["topics"][2], "hex") else str(log["topics"][2])[-42:]
    tx_hash = log["transactionHash"].hex() if hasattr(log["transactionHash"], "hex") else str(log["transactionHash"])
    ts = None

    maker_side = "BUY" if side == 0 else "SELL"
    taker_side = "SELL" if side == 0 else "BUY"
    return [
        _to_trade(slug, condition_id, maker, maker_side, token_id, size, price, ts, tx_hash),
        _to_trade(slug, condition_id, taker, taker_side, token_id, size, price, ts, tx_hash),
    ]


def _decode_v1_log_fast(log: dict[str, Any], market_lookup: dict[str, tuple]) -> list[Trade]:
    """Decode a V1 OrderFilled log without web3 event processing."""
    data = log["data"].hex() if not isinstance(log["data"], str) else log["data"]
    maker_asset = str(int(data[:64], 16))
    taker_asset = str(int(data[64:128], 16))
    maker_amount = int(data[128:192], 16) / 10 ** USDC_DECIMALS
    taker_amount = int(data[192:256], 16) / 10 ** USDC_DECIMALS
    maker = "0x" + log["topics"][1].hex()[-40:] if hasattr(log["topics"][1], "hex") else str(log["topics"][1])[-42:]
    taker = "0x" + log["topics"][2].hex()[-40:] if hasattr(log["topics"][2], "hex") else str(log["topics"][2])[-42:]
    tx_hash = log["transactionHash"].hex() if hasattr(log["transactionHash"], "hex") else str(log["transactionHash"])
    ts = None

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
            trades.append(_to_trade(slug, condition_id, maker, "SELL", token_id, size, price, ts, tx_hash))
            trades.append(_to_trade(slug, condition_id, taker, "BUY", token_id, size, price, ts, tx_hash))
        else:
            trades.append(_to_trade(slug, condition_id, maker, "BUY", token_id, size, price, ts, tx_hash))
            trades.append(_to_trade(slug, condition_id, taker, "SELL", token_id, size, price, ts, tx_hash))
    return trades


def fetch_chain_trades_for_markets_batch(
    w3: Web3,
    markets: list[dict[str, Any]],
    batch_hours: int = 1,
    buffer_blocks: int = 100,
    fetch_ts: bool = False,
) -> list[Trade]:
    """Batch-fetch on-chain trades for many markets by grouping adjacent time windows."""
    if not markets:
        return []

    from collections import defaultdict

    v1_markets = [m for m in markets if m.get("start_ts", 0) < V2_GENESIS_TIMESTAMP]
    v2_markets = [m for m in markets if m.get("start_ts", 0) >= V2_GENESIS_TIMESTAMP]

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
            bucket = m.get("start_ts", 0) // bucket_seconds
            buckets[bucket].append(m)

        print(f"[chain] batching {len(version_markets)} {version} markets into {len(buckets)} buckets (~{batch_hours}h each)")

        for bucket in tqdm(sorted(buckets.keys()), desc=f"chain-{version}"):
            bucket_markets = buckets[bucket]
            start_ts = min(m.get("start_ts", 0) for m in bucket_markets)
            end_ts = max(m.get("end_ts", 0) for m in bucket_markets)

            from_block = estimate_block_by_timestamp(w3, start_ts) - buffer_blocks
            to_block = estimate_block_by_timestamp(w3, end_ts) + buffer_blocks
            from_block = max(1, from_block)
            to_block = min(w3.eth.block_number, to_block)

            logs = fetch_order_filled_logs(w3, from_block, to_block, contract, topic)

            token_to_market: dict[str, tuple[str, str, str, str]] = {}
            for m in bucket_markets:
                token_to_market[m["token_one"]] = (m["slug"], m["condition_id"], m["token_one"], m["token_two"])
                token_to_market[m["token_two"]] = (m["slug"], m["condition_id"], m["token_one"], m["token_two"])

            decode_fast = _decode_v1_log_fast if version == "v1" else _decode_v2_log_fast
            for log in logs:
                all_trades.extend(decode_fast(log, token_to_market))

    print(f"[chain] batch fetch decoded {len(all_trades)} total trades")
    return all_trades


def _estimate_block_fast(
    target_ts: int,
    ts_min: int,
    block_min: int,
    ts_max: int,
    block_max: int,
) -> int:
    """Linearly interpolate a block number from a known (timestamp, block) window."""
    if ts_max <= ts_min:
        return block_min
    return int(block_min + (target_ts - ts_min) * (block_max - block_min) / (ts_max - ts_min))


def backfill_wallet_stats(
    w3: Web3,
    markets: list[dict[str, Any]],
    bucket_hours: int = 1,
    chunk_size: int = 80,
    buffer_blocks: int = 200,
    flush_every: int = 50000,
    max_workers: int = 4,
) -> int:
    """Backfill wallet PnL stats by scanning OrderFilled logs in hourly buckets.

    Markets are grouped into small time buckets so we never fetch logs for the
    entire exchange history at once. Each bucket's logs are decoded and
    aggregated per (wallet, market) on the fly, then flushed to SQLite.
    """
    from collections import defaultdict
    from concurrent.futures import ThreadPoolExecutor, as_completed

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
    ) -> tuple[dict, int, int, list[dict]]:
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

        logs = fetch_order_filled_logs(w3, from_block, to_block, contract, topic, chunk_size=chunk_size)

        local_agg: dict[tuple[str, str, str], dict[str, float]] = defaultdict(
            lambda: {
                "winner_bought_shares": 0.0,
                "winner_bought_cost": 0.0,
                "winner_sold_shares": 0.0,
                "winner_sold_revenue": 0.0,
                "loser_bought_shares": 0.0,
                "loser_bought_cost": 0.0,
                "loser_sold_shares": 0.0,
                "loser_sold_revenue": 0.0,
            }
        )
        local_log_count = 0
        local_trade_count = 0
        local_trades: list[dict] = []

        for log in logs:
            trades = decode_fast(log, token_to_market)
            if not trades:
                continue
            local_log_count += 1
            log_index = log.get("logIndex")
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
                local_trades.append({
                    "market_slug": trade.market_slug,
                    "condition_id": trade.condition_id,
                    "proxy_wallet": trade.proxy_wallet,
                    "side": trade.side,
                    "asset": trade.asset,
                    "size": trade.size,
                    "price": trade.price,
                    "usd_amount": trade.usd_amount,
                    "timestamp": trade.timestamp,
                    "transaction_hash": trade.transaction_hash,
                    "log_index": log_index,
                    "source": "chain",
                })

        return local_agg, local_log_count, local_trade_count, local_trades

    for version_markets in [v1_markets, v2_markets]:
        if not version_markets:
            continue
        version = "v1" if version_markets[0].get("start_ts", 0) < V2_GENESIS_TIMESTAMP else "v2"
        # BTC Up/Down 5m markets live on the main CTF V2 exchange; skip neg-risk to halve RPC calls.
        contract = CTF_EXCHANGE_V1 if version == "v1" else CTF_EXCHANGE_V2
        topic = ORDER_FILLED_V1_TOPIC if version == "v1" else ORDER_FILLED_V2_TOPIC
        decode_fast = _decode_v1_log_fast if version == "v1" else _decode_v2_log_fast

        version_start_ts = min(m.get("start_ts", 0) for m in version_markets)
        version_end_ts = max(m.get("end_ts", 0) for m in version_markets)
        version_start_block = estimate_block_by_timestamp(w3, version_start_ts)
        version_end_block = estimate_block_by_timestamp(w3, version_end_ts)

        bucket_seconds = bucket_hours * 3600
        buckets: dict[int, list[dict[str, Any]]] = defaultdict(list)
        for m in version_markets:
            buckets[m.get("start_ts", 0) // bucket_seconds].append(m)

        print(
            f"[chain] backfill {version}: {len(version_markets)} markets, "
            f"{len(buckets)} buckets (~{bucket_hours}h), blocks "
            f"{version_start_block}-{version_end_block}"
        )

        chunk_agg: dict[tuple[str, str, str], dict[str, float]] = defaultdict(
            lambda: {
                "winner_bought_shares": 0.0,
                "winner_bought_cost": 0.0,
                "winner_sold_shares": 0.0,
                "winner_sold_revenue": 0.0,
                "loser_bought_shares": 0.0,
                "loser_bought_cost": 0.0,
                "loser_sold_shares": 0.0,
                "loser_sold_revenue": 0.0,
            }
        )

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
            trade_buffer: list[dict] = []
            for future in tqdm(
                as_completed(futures), total=len(futures), desc=f"backfill-{version}"
            ):
                try:
                    local_agg, logs_i, trades_i, local_trades = future.result()
                except Exception as exc:
                    print(f"[chain] bucket worker error: {exc}")
                    continue
                processed_log_count += logs_i
                processed_trade_count += trades_i
                trade_buffer.extend(local_trades)
                for key, vals in local_agg.items():
                    agg = chunk_agg[key]
                    for kk, vv in vals.items():
                        agg[kk] += vv
                if processed_trade_count >= flush_every:
                    _flush_chunk_agg(chunk_agg)
                    chunk_agg.clear()
                    processed_trade_count = 0
                if len(trade_buffer) >= flush_every:
                    db.upsert_trades(trade_buffer)
                    trade_buffer.clear()

        if chunk_agg:
            _flush_chunk_agg(chunk_agg)
            chunk_agg.clear()
        if trade_buffer:
            db.upsert_trades(trade_buffer)
            trade_buffer.clear()

    wallet_count = db.compute_wallet_stats_from_staging()
    db.mark_markets_trades_fetched([m["slug"] for m in markets])
    with db.get_conn() as conn:
        trade_count = conn.execute("SELECT COUNT(*) AS c FROM trades").fetchone()["c"]
    print(
        f"[chain] backfilled wallet stats for {wallet_count} wallets "
        f"from {processed_log_count} logs ({trade_count} trades persisted)"
    )
    return wallet_count


def _flush_chunk_agg(chunk_agg: dict[tuple[str, str, str], dict[str, float]]) -> None:
    rows = []
    for (wallet, slug, condition_id), agg in chunk_agg.items():
        rows.append({
            "wallet": wallet,
            "market_slug": slug,
            "condition_id": condition_id,
            **agg,
        })
    db.insert_wallet_pnl_staging(rows)


def fetch_chain_trades_for_market(
    w3: Web3,
    market_slug: str,
    condition_id: str,
    token_one: str,
    token_two: str,
    start_ts: int,
    end_ts: int,
    buffer_blocks: int = 300,
    fetch_ts: bool = False,
) -> list[Trade]:
    """Fetch all on-chain OrderFilled trades for a market's two tokens."""
    from_block = estimate_block_by_timestamp(w3, start_ts) - buffer_blocks
    to_block = estimate_block_by_timestamp(w3, end_ts) + buffer_blocks
    from_block = max(1, from_block)
    to_block = min(w3.eth.block_number, to_block)

    # Decide which exchange version based on market start time.
    if start_ts >= V2_GENESIS_TIMESTAMP:
        contract = CTF_EXCHANGE_V2
        topic = ORDER_FILLED_V2_TOPIC
        decode_fast = _decode_v2_log_fast
        print(f"[chain] {market_slug}: using V2 exchange {contract} blocks {from_block}-{to_block}")
    else:
        contract = CTF_EXCHANGE_V1
        topic = ORDER_FILLED_V1_TOPIC
        decode_fast = _decode_v1_log_fast
        print(f"[chain] {market_slug}: using V1 exchange {contract} blocks {from_block}-{to_block}")

    logs = fetch_order_filled_logs(w3, from_block, to_block, contract, topic)
    print(f"[chain] {market_slug}: fetched {len(logs)} raw OrderFilled logs")

    lookup = {
        token_one: (market_slug, condition_id, token_one, token_two),
        token_two: (market_slug, condition_id, token_one, token_two),
    }
    trades: list[Trade] = []
    for log in logs:
        trades.extend(decode_fast(log, lookup))
    print(f"[chain] {market_slug}: decoded {len(trades)} trades for target tokens")
    return trades
