#!/usr/bin/env python3
"""
Polymarket Conditional Token Redeemer
=====================================
Scans your wallet for unredeemed winning positions on Polymarket's
Conditional Tokens contract and redeems them for USDC.e.

Requirements:
    pip install -r requirements.txt

Usage:
    # Dry run (default) - just shows what can be redeemed
    python redeem_positions.py

    # Actually redeem
    python redeem_positions.py --execute

Environment variables (or edit the CONFIG section below):
    POLYGON_PRIVATE_KEY  - Your bot wallet's private key
    POLYGON_RPC_URL      - Polygon RPC endpoint (default: https://polygon-rpc.com)
"""

import argparse
import json
import os
import queue
import sys
import threading
import time
from typing import Optional


class _Tee:
    """
    Write to stdout, a log file, and a Telegram chat simultaneously.
    Telegram messages are sent in a background thread to avoid blocking.
    Lines are batched (up to 4000 chars) before sending to stay within
    Telegram's message-size limit and reduce API calls.
    """

    _TELEGRAM_MAX = 4000   # chars per message (Telegram limit is 4096)
    _COLLECT_INTERVAL = 5.0  # seconds to accumulate output before sending
    _SEND_DELAY = 1.1        # seconds between sends (Telegram: max 1 msg/sec)

    def __init__(self, log_path: str, tg_token: str, tg_chat_id: str):
        self._stdout = sys.stdout
        self._file = open(log_path, "a", buffering=1, encoding="utf-8")
        self._tg_token = tg_token
        self._tg_chat_id = tg_chat_id
        self._tg_enabled = bool(tg_token and tg_chat_id)

        self._tg_queue: queue.Queue = queue.Queue()

        if self._tg_enabled:
            t = threading.Thread(target=self._tg_worker, daemon=True)
            t.start()

    # ------------------------------------------------------------------
    # Background worker — collects output for 5s then sends in chunks,
    # respecting Telegram's 1 message/second rate limit.
    # ------------------------------------------------------------------
    def _tg_worker(self):
        import requests as _req
        url = f"https://api.telegram.org/bot{self._tg_token}/sendMessage"
        pending = ""

        while True:
            # Drain the queue for up to _COLLECT_INTERVAL seconds
            deadline = time.monotonic() + self._COLLECT_INTERVAL
            while time.monotonic() < deadline:
                try:
                    chunk = self._tg_queue.get(timeout=max(0.1, deadline - time.monotonic()))
                    if chunk is None:   # sentinel → shut down
                        return
                    pending += chunk
                except queue.Empty:
                    break

            if not pending.strip():
                continue

            # Send in ≤4000-char slices with a delay between each
            while pending:
                slice_ = pending[:self._TELEGRAM_MAX]
                pending = pending[self._TELEGRAM_MAX:]
                self._post(url, _req, slice_)
                if pending.strip():
                    time.sleep(self._SEND_DELAY)

    def _post(self, url: str, _req, text: str):
        try:
            _req.post(url, json={
                "chat_id": self._tg_chat_id,
                "text": text,
                "parse_mode": "HTML",
            }, timeout=10)
        except Exception:
            pass  # never crash the bot because Telegram is unreachable

    # ------------------------------------------------------------------
    # sys.stdout interface
    # ------------------------------------------------------------------
    def write(self, data: str):
        self._stdout.write(data)
        self._file.write(data)
        if self._tg_enabled and data:
            self._tg_queue.put(data)

    def flush(self):
        self._stdout.flush()
        self._file.flush()

    def __getattr__(self, name):
        return getattr(self._stdout, name)


LOG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "redeem.log")

from dotenv import load_dotenv

load_dotenv()

from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware

# =============================================================================
# CONFIG - Edit these or set environment variables
# =============================================================================
PRIVATE_KEY = os.environ.get("POLYGON_PRIVATE_KEY", "")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
# Polymarket proxy wallet — holds CTF tokens and USDC.e.
# The EOA (PRIVATE_KEY) only pays gas; all positions live here.
# Find yours at: https://polygonscan.com/address/<your_eoa>#tokentxns
POLYMARKET_PROXY_ADDRESS = os.environ.get("POLYMARKET_PROXY_ADDRESS", "")

# Slug prefix shared by every BTC Up/Down 5-minute event on Polymarket.
MARKET_SLUG_PREFIX = "btc-updown-5m"

# How many days back to scan for claimable positions via slug enumeration.
SLUG_LOOKBACK_SECONDS = 3 * 3600  # 3 hours

sys.stdout = _Tee(LOG_PATH, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID)

_alchemy_key = os.environ.get("ALCHEMY_API_KEY", "")
_default_rpc = f"https://polygon-mainnet.g.alchemy.com/v2/{_alchemy_key}" if _alchemy_key else ""
RPC_URL = os.environ.get("POLYGON_RPC_URL", _default_rpc)

# Contract addresses on Polygon
CTF_ADDRESS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"  # Conditional Tokens
USDC_E_ADDRESS = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"  # USDC.e (bridged)
NATIVE_USDC_ADDRESS = "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359"  # Native USDC
COLLATERAL_CANDIDATES = [USDC_E_ADDRESS, NATIVE_USDC_ADDRESS]

# Polymarket uses parentCollectionId = bytes32(0) for all markets
PARENT_COLLECTION_ID = bytes(32)

# Polymarket binary markets use index sets [1, 2] (outcome 0 and outcome 1)
INDEX_SETS = [1, 2]


# =============================================================================
# ABIs
# =============================================================================
CTF_ABI = json.loads("""[
    {
        "constant": false,
        "inputs": [
            {"name": "collateralToken", "type": "address"},
            {"name": "parentCollectionId", "type": "bytes32"},
            {"name": "conditionId", "type": "bytes32"},
            {"name": "indexSets", "type": "uint256[]"}
        ],
        "name": "redeemPositions",
        "outputs": [],
        "payable": false,
        "stateMutability": "nonpayable",
        "type": "function"
    },
    {
        "constant": true,
        "inputs": [
            {"name": "owner", "type": "address"},
            {"name": "id", "type": "uint256"}
        ],
        "name": "balanceOf",
        "outputs": [{"name": "", "type": "uint256"}],
        "payable": false,
        "stateMutability": "view",
        "type": "function"
    },
    {
        "constant": true,
        "inputs": [
            {"name": "", "type": "bytes32"},
            {"name": "", "type": "uint256"}
        ],
        "name": "payoutNumerators",
        "outputs": [{"name": "", "type": "uint256"}],
        "payable": false,
        "stateMutability": "view",
        "type": "function"
    },
    {
        "constant": true,
        "inputs": [
            {"name": "", "type": "bytes32"}
        ],
        "name": "payoutDenominator",
        "outputs": [{"name": "", "type": "uint256"}],
        "payable": false,
        "stateMutability": "view",
        "type": "function"
    },
    {
        "constant": false,
        "inputs": [
            {"name": "operator", "type": "address"},
            {"name": "approved", "type": "bool"}
        ],
        "name": "setApprovalForAll",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function"
    },
    {
        "constant": false,
        "inputs": [
            {"name": "from",  "type": "address"},
            {"name": "to",    "type": "address"},
            {"name": "id",    "type": "uint256"},
            {"name": "value", "type": "uint256"},
            {"name": "data",  "type": "bytes"}
        ],
        "name": "safeTransferFrom",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function"
    },
    {
        "anonymous": false,
        "inputs": [
            {"indexed": true, "name": "stakeholder", "type": "address"},
            {"indexed": false, "name": "collateralToken", "type": "address"},
            {"indexed": true, "name": "parentCollectionId", "type": "bytes32"},
            {"indexed": true, "name": "conditionId", "type": "bytes32"},
            {"indexed": false, "name": "partition", "type": "uint256[]"},
            {"indexed": false, "name": "amount", "type": "uint256"}
        ],
        "name": "PositionSplit",
        "type": "event"
    },
    {
        "anonymous": false,
        "inputs": [
            {"indexed": true, "name": "operator", "type": "address"},
            {"indexed": true, "name": "from", "type": "address"},
            {"indexed": true, "name": "to", "type": "address"},
            {"indexed": false, "name": "id", "type": "uint256"},
            {"indexed": false, "name": "value", "type": "uint256"}
        ],
        "name": "TransferSingle",
        "type": "event"
    },
    {
        "anonymous": false,
        "inputs": [
            {"indexed": true, "name": "operator", "type": "address"},
            {"indexed": true, "name": "from", "type": "address"},
            {"indexed": true, "name": "to", "type": "address"},
            {"indexed": false, "name": "ids", "type": "uint256[]"},
            {"indexed": false, "name": "values", "type": "uint256[]"}
        ],
        "name": "TransferBatch",
        "type": "event"
    }
]""")

USDC_ABI = json.loads("""[
    {
        "constant": true,
        "inputs": [{"name": "account", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function"
    }
]""")

# Polymarket proxy wallet — forwards arbitrary calls on behalf of the EOA owner.
PROXY_ABI = json.loads("""[
    {
        "inputs": [
            {"name": "to",    "type": "address"},
            {"name": "value", "type": "uint256"},
            {"name": "data",  "type": "bytes"}
        ],
        "name": "execute",
        "outputs": [
            {"name": "success",    "type": "bool"},
            {"name": "returnData", "type": "bytes"}
        ],
        "stateMutability": "nonpayable",
        "type": "function"
    }
]""")


def get_position_id(collateral_token: str, collection_id: bytes) -> int:
    """Compute ERC-1155 position/token ID from collateral token and collection ID."""
    # positionId = uint256(keccak256(abi.encodePacked(collateralToken, collectionId)))
    packed = Web3.to_bytes(hexstr=collateral_token) + collection_id
    return int.from_bytes(Web3.keccak(packed), "big")


def get_collection_id(parent_collection_id: bytes, condition_id: bytes, index_set: int) -> bytes:
    """Compute collection ID for a given condition and index set."""
    # collectionId = keccak256(abi.encodePacked(conditionId, indexSet))
    # If parentCollectionId != 0, it's added on an elliptic curve (but for Polymarket it's always 0)
    encoded = condition_id + index_set.to_bytes(32, "big")
    return Web3.keccak(encoded)


def _get_proxy_token_ids(address: str, label: str = "proxy") -> set:
    """
    Return the full set of ERC-1155 token IDs ever received by *address*
    using Alchemy's getAssetTransfers.  Returns an empty set if no Alchemy
    key is configured.
    """
    import requests as _req

    alchemy_key = os.environ.get("ALCHEMY_API_KEY", "")
    if not alchemy_key or alchemy_key not in RPC_URL:
        return set()

    token_ids: set = set()
    page_key = None
    try:
        while True:
            params = {
                "toAddress": address,
                "contractAddresses": [CTF_ADDRESS],
                "category": ["erc1155"],
                "withMetadata": False,
                "excludeZeroValue": True,
                "maxCount": "0x3e8",
            }
            if page_key:
                params["pageKey"] = page_key
            payload = {"jsonrpc": "2.0", "id": 1,
                       "method": "alchemy_getAssetTransfers", "params": [params]}
            resp = _req.post(RPC_URL, json=payload, timeout=15)
            resp.raise_for_status()
            result = resp.json().get("result", {})
            for transfer in result.get("transfers", []):
                for meta in (transfer.get("erc1155Metadata") or []):
                    tid_hex = meta.get("tokenId", "")
                    if tid_hex:
                        token_ids.add(int(tid_hex, 16))
            page_key = result.get("pageKey")
            if not page_key:
                break
        print(f"  Alchemy: {len(token_ids)} token IDs in {label} wallet")
    except Exception as e:
        print(f"  Alchemy error ({label}): {e}")
    return token_ids


def _position_ids_for_condition(condition_id_hex: str) -> tuple:
    """Return (position_id_0, position_id_1) for a given condition ID hex string."""
    cid_bytes = bytes.fromhex(condition_id_hex.replace("0x", ""))
    col0 = get_collection_id(PARENT_COLLECTION_ID, cid_bytes, 1)
    col1 = get_collection_id(PARENT_COLLECTION_ID, cid_bytes, 2)
    return get_position_id(USDC_E_ADDRESS, col0), get_position_id(USDC_E_ADDRESS, col1)


def fetch_all_btc5m_conditions(proxy_address: str, eoa_address: str) -> dict:
    """
    Discover ALL BTC Up/Down 5-minute conditions where the proxy holds tokens.

    Method 1 — Alchemy: get every ERC-1155 token ID ever received by the proxy.
                        Used as a ground-truth filter for methods below.
    Method 2 — Data API (proxy + EOA): fast scan for recently-active positions.
    Method 3 — Gamma API title search: 'Bitcoin Up or Down' keyword search.
    Method 4 — Gamma API slug scan (backwards from now): generate
                btc-updown-5m-<ts> slugs and walk backwards through time,
                cross-referencing each condition's position IDs with the known
                token IDs from Method 1.  Stops early once all tokens are matched.

    Returns {conditionId_hex: {question, outcome, resolved}}.
    """
    import requests

    conditions: dict = {}

    # ── Method 1: Alchemy — ground-truth token IDs in proxy AND EOA ─────────
    # unmatched_ids shrinks as we match condition → position IDs.
    # When empty we stop the slug scan early.
    # We scan both the proxy and the EOA because CTF tokens may be held by
    # either address depending on how positions were originally purchased.
    unmatched_ids: set = set()
    if proxy_address:
        unmatched_ids |= _get_proxy_token_ids(proxy_address, "proxy")
    unmatched_ids |= _get_proxy_token_ids(eoa_address, "EOA")

    def _try_add_market(market: dict) -> bool:
        """Add market if its position IDs are in unmatched_ids (or no Alchemy data).
        Returns True if added, False otherwise.  Removes matched IDs from unmatched_ids."""
        cid = market.get("conditionId") or market.get("condition_id", "")
        if not cid:
            return False
        cid_clean = cid.replace("0x", "")
        if cid_clean in conditions:
            return False
        if unmatched_ids:
            # Strategy 1: direct match via 'asset' field (Data API provides the exact
            # ERC-1155 token ID we already fetched from Alchemy — most reliable).
            direct_matched: set = set()
            for asset_field in ("asset", "oppositeAsset"):
                raw = market.get(asset_field)
                if raw is not None:
                    try:
                        tid = int(raw)
                        if tid in unmatched_ids:
                            direct_matched.add(tid)
                    except (ValueError, TypeError):
                        pass
            if direct_matched:
                unmatched_ids.difference_update(direct_matched)
            else:
                # Strategy 2: compute position IDs from condition ID and check
                pid0, pid1 = _position_ids_for_condition(cid_clean)
                match = unmatched_ids & {pid0, pid1}
                if not match:
                    return False
                unmatched_ids.difference_update(match)
        # Store raw asset token IDs so check_and_redeem can use them directly
        # instead of recomputing (which requires knowing the collateral token).
        def _to_int(v):
            try:
                return int(v) if v is not None else None
            except (ValueError, TypeError):
                return None

        conditions[cid_clean] = {
            "question": market.get("question") or market.get("title") or "BTC Up/Down 5m",
            "outcome": market.get("outcome", ""),
            "resolved": market.get("closed", False),
            "asset": _to_int(market.get("asset")),
            "oppositeAsset": _to_int(market.get("oppositeAsset")),
            "outcomeIndex": market.get("outcomeIndex"),
            "endDate": market.get("endDate") or market.get("end_date") or market.get("resolvedAt"),
        }
        return True

    # ── Method 2: Data API (proxy + EOA) ────────────────────────────────────
    for label, addr in [("proxy", proxy_address), ("EOA", eoa_address)]:
        if not addr:
            continue
        try:
            resp = requests.get(
                "https://data-api.polymarket.com/positions",
                params={"user": addr},
                timeout=15,
            )
            print(f"  Data API [{label}]: HTTP {resp.status_code}")
            if resp.status_code != 200:
                continue
            positions = resp.json()
            if not isinstance(positions, list):
                positions = positions.get("data", [])
            print(f"  Data API [{label}]: {len(positions)} raw positions")
            before = len(conditions)
            for pos in positions:
                slug = (pos.get("marketSlug") or pos.get("slug") or
                        pos.get("eventSlug") or pos.get("market_slug") or "").lower()
                title = (pos.get("title") or pos.get("question") or "").lower()
                # When we have Alchemy token IDs, skip slug/title pre-filter;
                # _try_add_market will reject non-matching conditions via position ID check.
                if not unmatched_ids:
                    if MARKET_SLUG_PREFIX not in slug and not (
                        ("btc" in title or "bitcoin" in title) and "5" in title
                    ):
                        continue
                _try_add_market(pos)
            print(f"  Data API [{label}]: {len(conditions) - before} BTC 5m conditions matched")
        except Exception as e:
            print(f"  Data API [{label}] error: {e}")

    # ── Method 2b: Gamma API lookup by Alchemy token IDs ────────────────────
    # The Gamma /markets endpoint accepts clob_token_ids (= ERC-1155 position IDs).
    # This directly maps the token IDs we know we hold to their condition IDs —
    # the most reliable approach when slug/title filters fail.
    if unmatched_ids:
        print(f"  Gamma token-ID lookup ({len(unmatched_ids)} IDs)...")
        token_added = 0
        # Query in batches of 20 to stay within URL length limits
        id_list = list(unmatched_ids)
        batch_size = 20
        for i in range(0, len(id_list), batch_size):
            batch = id_list[i:i + batch_size]
            try:
                resp = requests.get(
                    "https://gamma-api.polymarket.com/markets",
                    params={"clob_token_ids": ",".join(str(tid) for tid in batch)},
                    timeout=15,
                )
                if resp.status_code == 200:
                    items = resp.json()
                    if not isinstance(items, list):
                        items = items.get("data", [])
                    for market in items:
                        if _try_add_market(market):
                            token_added += 1
            except Exception as e:
                print(f"  Gamma token-ID lookup error: {e}")
        if token_added:
            print(f"  Gamma token-ID lookup: {token_added} conditions added")

    # ── Method 3: Gamma API title search ────────────────────────────────────
    print("  Gamma title search for 'Bitcoin Up or Down'...")
    gamma_title_added = 0
    # Search both events and markets endpoints; omit 'closed' filter so recently
    # resolved markets (not yet fully settled in the API) are also returned.
    for gamma_url, result_key in [
        ("https://gamma-api.polymarket.com/events", None),
        ("https://gamma-api.polymarket.com/markets", None),
    ]:
        try:
            resp = requests.get(
                gamma_url,
                params={"q": "Bitcoin Up or Down", "limit": 500},
                timeout=15,
            )
            if resp.status_code != 200:
                continue
            items = resp.json()
            if not isinstance(items, list):
                items = items.get("data", [])
            for item in items:
                # Events have nested markets; markets are top-level
                markets = item.get("markets") if "markets" in item else [item]
                for market in markets:
                    if _try_add_market(market):
                        gamma_title_added += 1
        except Exception as e:
            print(f"  Gamma title search error ({gamma_url}): {e}")
    if gamma_title_added:
        print(f"  Gamma title search: {gamma_title_added} conditions added")

    # ── Method 4: Gamma API slug scan (backwards from now) ──────────────────
    # Only run if Alchemy found token IDs and some are still unmatched.
    if unmatched_ids:
        now_ts = int(time.time())
        cutoff_ts = now_ts - SLUG_LOOKBACK_SECONDS
        # Round to 5-minute boundary and scan backwards
        start_ts = (now_ts // 300) * 300
        print(f"  Gamma slug scan backwards ({SLUG_LOOKBACK_SECONDS // 3600}h, "
              f"{len(unmatched_ids)} token IDs still unmatched)...")
        slug_added = 0
        for ts in range(start_ts, cutoff_ts, -300):
            if not unmatched_ids:
                break  # all proxy token IDs matched
            slug = f"{MARKET_SLUG_PREFIX}-{ts}"
            try:
                # Try both the events and markets Gamma endpoints for each slug
                for gamma_url in [
                    "https://gamma-api.polymarket.com/events",
                    "https://gamma-api.polymarket.com/markets",
                ]:
                    resp = requests.get(gamma_url, params={"slug": slug}, timeout=5)
                    if resp.status_code == 200:
                        items = resp.json()
                        if not isinstance(items, list):
                            items = items.get("data", [])
                        for item in items:
                            markets = item.get("markets") if "markets" in item else [item]
                            for market in markets:
                                if _try_add_market(market):
                                    slug_added += 1
                time.sleep(0.05)
            except Exception:
                pass
        if slug_added:
            print(f"  Gamma slug scan: {slug_added} conditions added")

    return conditions


def check_and_redeem(
    w3: Web3,
    ctf_contract,
    eoa_address: str,
    condition_id_hex: str,
    market_info: dict,
    execute: bool = False,
    proxy_address: str = "",
    proxy_contract=None,
) -> Optional[str]:
    """
    Check if a condition is resolved, if we have winning tokens, and redeem.

    Checks the proxy wallet first; if it holds no tokens for this condition,
    falls back to checking the EOA directly.  Redemption is routed through
    the proxy.execute() call when tokens are in the proxy, or sent directly
    from the EOA when tokens are held there.

    Returns tx hash if redeemed, None otherwise.
    """
    condition_id_bytes = bytes.fromhex(condition_id_hex.replace("0x", ""))

    # Check if condition is resolved (payoutDenominator > 0 means resolved)
    try:
        payout_denominator = ctf_contract.functions.payoutDenominator(condition_id_bytes).call()
    except Exception:
        return None

    if payout_denominator == 0:
        print(f"  ⏳ Not yet resolved: {market_info.get('question', condition_id_hex[:20])}")
        return None

    # Get payout numerators for both outcomes
    payout_0 = ctf_contract.functions.payoutNumerators(condition_id_bytes, 0).call()
    payout_1 = ctf_contract.functions.payoutNumerators(condition_id_bytes, 1).call()

    winning_index = 0 if payout_0 > 0 else 1
    winning_label = "Yes/Up" if winning_index == 0 else "No/Down"

    # Resolve position IDs and collateral token.
    # Prefer stored asset/oppositeAsset from the Data API (exact values) over
    # recomputing, because newer markets may use native USDC instead of USDC.e.
    stored_asset = market_info.get("asset")
    stored_opposite = market_info.get("oppositeAsset")
    stored_outcome_idx = market_info.get("outcomeIndex")

    if stored_asset is not None and stored_opposite is not None and stored_outcome_idx is not None:
        if int(stored_outcome_idx) == 0:
            position_id_0, position_id_1 = int(stored_asset), int(stored_opposite)
        else:
            position_id_0, position_id_1 = int(stored_opposite), int(stored_asset)
    else:
        # Fall back to computing — try every collateral candidate until non-zero balance found
        position_id_0 = position_id_1 = None

    # Auto-detect collateral token: the one whose computed position IDs match
    # the known position IDs (works for both USDC.e and native USDC markets).
    collateral_token = USDC_E_ADDRESS  # default
    col0 = get_collection_id(PARENT_COLLECTION_ID, condition_id_bytes, 1)
    col1 = get_collection_id(PARENT_COLLECTION_ID, condition_id_bytes, 2)
    for candidate in COLLATERAL_CANDIDATES:
        if position_id_0 is not None:
            # Verify the candidate matches the known position IDs
            if get_position_id(candidate, col0) == position_id_0:
                collateral_token = candidate
                break
        else:
            # No stored IDs — use computed ones for this candidate
            position_id_0 = get_position_id(candidate, col0)
            position_id_1 = get_position_id(candidate, col1)
            collateral_token = candidate
            break  # will re-check via balance below

    # Determine which address actually holds the tokens: try proxy first, then EOA.
    use_proxy = False
    if proxy_address and proxy_contract:
        proxy_cs = Web3.to_checksum_address(proxy_address)
        balance_0 = ctf_contract.functions.balanceOf(proxy_cs, position_id_0).call()
        balance_1 = ctf_contract.functions.balanceOf(proxy_cs, position_id_1).call()
        if balance_0 > 0 or balance_1 > 0:
            holder = proxy_cs
            use_proxy = True

    if not use_proxy:
        eoa_cs = Web3.to_checksum_address(eoa_address)
        balance_0 = ctf_contract.functions.balanceOf(eoa_cs, position_id_0).call()
        balance_1 = ctf_contract.functions.balanceOf(eoa_cs, position_id_1).call()
        # If still zero and we guessed the collateral token, try the other candidate
        if balance_0 == 0 and balance_1 == 0 and stored_asset is None:
            for candidate in COLLATERAL_CANDIDATES:
                if candidate == collateral_token:
                    continue
                alt_pid0 = get_position_id(candidate, col0)
                alt_pid1 = get_position_id(candidate, col1)
                b0 = ctf_contract.functions.balanceOf(eoa_cs, alt_pid0).call()
                b1 = ctf_contract.functions.balanceOf(eoa_cs, alt_pid1).call()
                if b0 > 0 or b1 > 0:
                    position_id_0, position_id_1 = alt_pid0, alt_pid1
                    balance_0, balance_1 = b0, b1
                    collateral_token = candidate
                    break
        holder = eoa_cs

    winning_balance = balance_0 if winning_index == 0 else balance_1
    losing_balance = balance_1 if winning_index == 0 else balance_0

    if winning_balance == 0 and losing_balance == 0:
        return None  # No tokens to redeem

    winning_payout_usdc = winning_balance / 1_000_000

    question = market_info.get("question", f"Condition {condition_id_hex[:20]}...")
    print(f"\n  ✅ RESOLVED: {question}")
    print(f"     Winner: {winning_label} (payout: [{payout_0}/{payout_1}], denom: {payout_denominator})")
    print(f"     Winning tokens ({holder[:10]}...): {winning_balance:,} (≈ ${winning_payout_usdc:.6f} USDC.e)")
    if losing_balance > 0:
        print(f"     Losing tokens: {losing_balance:,} (worthless)")

    if winning_balance == 0:
        print(f"     ❌ You hold only losing tokens — nothing to redeem.")
        return None

    if not execute:
        print(f"     🔍 DRY RUN — would redeem {winning_balance:,} winning tokens")
        return "DRY_RUN"

    # Start with the auto-detected candidate, then fall back to the other
    candidates_ordered = [collateral_token] + [
        c for c in COLLATERAL_CANDIDATES if c != collateral_token
    ]

    # ── Helper: build and simulate a call cheaply (no gas/nonce overhead) ────
    def _eth_call(from_addr: str, to_addr: str, data: bytes) -> bool:
        try:
            w3.eth.call({"from": from_addr, "to": to_addr, "data": data})
            return True
        except Exception:
            return False

    def _redeem_calldata(ctoken: str) -> bytes:
        return ctf_contract.encode_abi(
            "redeemPositions",
            args=[Web3.to_checksum_address(ctoken), PARENT_COLLECTION_ID, condition_id_bytes, INDEX_SETS],
        )

    def _proxy_execute_data(inner_data: bytes) -> bytes:
        return proxy_contract.encode_abi(
            "execute",
            args=[Web3.to_checksum_address(CTF_ADDRESS), 0, inner_data],
        )

    # Execute redemption
    print(f"     🔄 Redeeming...")
    try:
        nonce = w3.eth.get_transaction_count(eoa_address)
        base = {
            "from": eoa_address,
            "nonce": nonce,
            "gas": 400_000,
            "maxFeePerGas": w3.eth.gas_price * 2,
            "maxPriorityFeePerGas": w3.to_wei(30, "gwei"),
            "chainId": 137,
        }

        # ── Strategy 1: proxy.execute() → CTF.redeemPositions ───────────────
        working_collateral = None
        if use_proxy:
            for candidate in candidates_ordered:
                data = _proxy_execute_data(_redeem_calldata(candidate))
                if _eth_call(eoa_address, proxy_address, data):
                    working_collateral = candidate
                    break
                print(f"     ⚠️  proxy.execute() sim failed for collateral {candidate[:10]}...")

        else:
            for candidate in candidates_ordered:
                data = _redeem_calldata(candidate)
                if _eth_call(eoa_address, CTF_ADDRESS, data):
                    working_collateral = candidate
                    break
                print(f"     ⚠️  Direct redeem sim failed for collateral {candidate[:10]}...")

        # ── Strategy 2 (proxy only): approve EOA → transfer tokens → redeem ─
        # Used when proxy.execute(redeemPositions) fails but direct CTF works.
        use_transfer_strategy = False
        _safe_exec_ctx = None
        if working_collateral is None and use_proxy:
            proxy_cs_str = Web3.to_checksum_address(proxy_address)
            eoa_cs_str   = Web3.to_checksum_address(eoa_address)
            # Check: would CTF.redeemPositions succeed if called directly from proxy?
            direct_candidate = None
            for candidate in candidates_ordered:
                if _eth_call(proxy_cs_str, CTF_ADDRESS, _redeem_calldata(candidate)):
                    direct_candidate = candidate
                    break
            if direct_candidate:
                print(f"     ℹ️  CTF direct sim OK (collateral {direct_candidate[:10]}...) — proxy.execute() ABI mismatch")
                # ── Strategy 2a: Gnosis Safe execTransaction ─────────────────
                # Polymarket proxy wallets are Gnosis Safes.  Use execTransaction
                # (with EOA owner signature) instead of the simple execute().
                print(f"     🔄 Trying Gnosis Safe execTransaction...")
                _ZERO = "0x0000000000000000000000000000000000000000"
                _SAFE_ABI = [
                    {"name": "nonce", "inputs": [], "outputs": [{"type": "uint256"}],
                     "stateMutability": "view", "type": "function"},
                    {"name": "getTransactionHash", "inputs": [
                        {"name": "to",             "type": "address"},
                        {"name": "value",          "type": "uint256"},
                        {"name": "data",           "type": "bytes"},
                        {"name": "operation",      "type": "uint8"},
                        {"name": "safeTxGas",      "type": "uint256"},
                        {"name": "baseGas",        "type": "uint256"},
                        {"name": "gasPrice",       "type": "uint256"},
                        {"name": "gasToken",       "type": "address"},
                        {"name": "refundReceiver", "type": "address"},
                        {"name": "_nonce",         "type": "uint256"},
                    ], "outputs": [{"type": "bytes32"}],
                    "stateMutability": "view", "type": "function"},
                    {"name": "execTransaction", "inputs": [
                        {"name": "to",             "type": "address"},
                        {"name": "value",          "type": "uint256"},
                        {"name": "data",           "type": "bytes"},
                        {"name": "operation",      "type": "uint8"},
                        {"name": "safeTxGas",      "type": "uint256"},
                        {"name": "baseGas",        "type": "uint256"},
                        {"name": "gasPrice",       "type": "uint256"},
                        {"name": "gasToken",       "type": "address"},
                        {"name": "refundReceiver", "type": "address"},
                        {"name": "signatures",     "type": "bytes"},
                    ], "outputs": [{"type": "bool"}],
                    "stateMutability": "payable", "type": "function"},
                ]
                try:
                    proxy_safe = w3.eth.contract(
                        address=Web3.to_checksum_address(proxy_address), abi=_SAFE_ABI
                    )
                    inner = _redeem_calldata(direct_candidate)
                    safe_nonce = proxy_safe.functions.nonce().call()
                    safe_tx_hash = proxy_safe.functions.getTransactionHash(
                        Web3.to_checksum_address(CTF_ADDRESS), 0, inner,
                        0, 0, 0, 0, _ZERO, _ZERO, safe_nonce,
                    ).call()
                    # Sign the raw Safe tx hash (EIP-712, no extra prefix needed)
                    signed = w3.eth.account.unsafe_sign_hash(safe_tx_hash, private_key=PRIVATE_KEY)
                    sig = signed.signature
                    ctf_cs_for_safe = Web3.to_checksum_address(CTF_ADDRESS)
                    exec_data = proxy_safe.encode_abi(
                        "execTransaction",
                        args=[ctf_cs_for_safe, 0, inner, 0, 0, 0, 0, _ZERO, _ZERO, sig],
                    )
                    if _eth_call(eoa_address, proxy_cs_str, exec_data):
                        working_collateral = direct_candidate
                        use_transfer_strategy = False  # handled differently below
                        # Store Safe execution context for the send phase
                        _safe_exec_ctx = (proxy_safe, ctf_cs_for_safe, inner, _ZERO, sig)
                    else:
                        print(f"     ⚠️  Safe execTransaction sim failed")
                        _safe_exec_ctx = None
                except Exception as _se:
                    print(f"     ⚠️  Safe execTransaction error: {str(_se)[:100]}")
                    _safe_exec_ctx = None

                # ── Strategy 2b: setApprovalForAll + transfer fallback ────────
                if working_collateral is None:
                    print(f"     🔄 Trying: approve EOA → transfer tokens → redeem from EOA...")
                    approval_data = _proxy_execute_data(
                        ctf_contract.encode_abi("setApprovalForAll", args=[eoa_cs_str, True])
                    )
                    if _eth_call(eoa_address, proxy_address, approval_data):
                        working_collateral = direct_candidate
                        use_transfer_strategy = True
                    else:
                        print(f"     ⚠️  setApprovalForAll via proxy.execute() also failed")
            else:
                print(f"     ❌ CTF direct sim also failed — conditionId: 0x{condition_id_hex}")
                print(f"        position_id (asset): {market_info.get('asset')}")
                print(f"        Market may require NegRisk/custom redemption path")
                _safe_exec_ctx = None

        if working_collateral is None:
            print(f"     ❌ All redemption strategies failed simulation.")
            return None

        collateral_token = working_collateral

        # ── Build and send the transaction(s) ────────────────────────────────
        # Check if Safe execTransaction context is available and use it
        _use_safe_exec = (
            use_proxy and not use_transfer_strategy
            and working_collateral is not None
            and _safe_exec_ctx is not None
        )

        if _use_safe_exec:
            # Send via Gnosis Safe execTransaction (EOA signs the Safe tx hash)
            proxy_safe_c, ctf_cs_s, inner_s, zero_s, sig_s = _safe_exec_ctx
            # Re-sign with fresh nonce (nonce was fetched during simulation)
            safe_nonce2 = proxy_safe_c.functions.nonce().call()
            safe_tx_hash2 = proxy_safe_c.functions.getTransactionHash(
                ctf_cs_s, 0, inner_s, 0, 0, 0, 0, zero_s, zero_s, safe_nonce2,
            ).call()
            signed2 = w3.eth.account.unsafe_sign_hash(safe_tx_hash2, private_key=PRIVATE_KEY)
            sig2 = signed2.signature
            tx = proxy_safe_c.functions.execTransaction(
                ctf_cs_s, 0, inner_s, 0, 0, 0, 0, zero_s, zero_s, sig2
            ).build_transaction(base)
            print(f"     📡 Gnosis Safe execTransaction (collateral: {collateral_token[:10]}...)")

        elif use_transfer_strategy:
            # Step A: proxy.execute(CTF.setApprovalForAll(EOA, true))
            approval_calldata = ctf_contract.encode_abi("setApprovalForAll", args=[eoa_address, True])
            tx_approve = proxy_contract.functions.execute(
                Web3.to_checksum_address(CTF_ADDRESS), 0, approval_calldata
            ).build_transaction(base)
            signed = w3.eth.account.sign_transaction(tx_approve, PRIVATE_KEY)
            tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
            print(f"     📤 Approval tx: {w3.to_hex(tx_hash)}")
            w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)

            # Step B: EOA → CTF.safeTransferFrom(proxy, EOA, position_id, balance, "")
            base["nonce"] += 1
            winning_pid = position_id_1 if winning_index == 1 else position_id_0
            tx_transfer = ctf_contract.functions.safeTransferFrom(
                Web3.to_checksum_address(proxy_address),
                Web3.to_checksum_address(eoa_address),
                winning_pid,
                winning_balance,
                b"",
            ).build_transaction(base)
            signed = w3.eth.account.sign_transaction(tx_transfer, PRIVATE_KEY)
            tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
            print(f"     📤 Transfer tx: {w3.to_hex(tx_hash)}")
            w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)

            # Step C: EOA redeems from its own balance
            base["nonce"] += 1
            use_proxy = False  # redeem directly from EOA now
            print(f"     📡 Tokens transferred to EOA — redeeming directly...")

        collateral_cs = Web3.to_checksum_address(collateral_token)
        if not _use_safe_exec:
            if use_proxy:
                redeem_calldata = ctf_contract.encode_abi(
                    "redeemPositions",
                    args=[collateral_cs, PARENT_COLLECTION_ID, condition_id_bytes, INDEX_SETS],
                )
                tx = proxy_contract.functions.execute(
                    Web3.to_checksum_address(CTF_ADDRESS), 0, redeem_calldata
                ).build_transaction(base)
                print(f"     📡 Routing through proxy: {proxy_address[:10]}... (collateral: {collateral_token[:10]}...)")
            else:
                tx = ctf_contract.functions.redeemPositions(
                    collateral_cs, PARENT_COLLECTION_ID, condition_id_bytes, INDEX_SETS,
                ).build_transaction(base)
                print(f"     📡 Redeeming from EOA... (collateral: {collateral_token[:10]}...)")

        signed_tx = w3.eth.account.sign_transaction(tx, PRIVATE_KEY)
        tx_hash = w3.eth.send_raw_transaction(signed_tx.raw_transaction)
        tx_hash_hex = w3.to_hex(tx_hash)

        print(f"     📤 Tx sent: {tx_hash_hex}")
        print(f"     ⏳ Waiting for confirmation...")

        receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)

        if receipt["status"] == 1:
            print(f"     ✅ Redeemed! Gas used: {receipt['gasUsed']:,}")
            return tx_hash_hex
        else:
            print(f"     ❌ Transaction reverted!")
            return None

    except Exception as e:
        print(f"     ❌ Error: {e}")
        return None


LOOP_INTERVAL_SECONDS = 600  # 10 minutes


def run_once(w3, ctf_contract, usdc_contract, eoa_address, args,
             proxy_address="", proxy_contract=None):
    """Run one full scan-and-redeem cycle."""
    # Tokens and USDC live in the proxy; gas comes from the EOA
    holder = Web3.to_checksum_address(proxy_address) if proxy_address else eoa_address

    pol_balance = w3.eth.get_balance(eoa_address)
    print(f"⛽ POL balance (EOA): {w3.from_wei(pol_balance, 'ether'):.4f} POL")
    if pol_balance < w3.to_wei(0.01, "ether"):
        print("⚠️  Low POL balance — you may not have enough gas to redeem.")

    usdc_before = usdc_contract.functions.balanceOf(holder).call()
    print(f"💰 USDC.e balance (proxy): {usdc_before / 1_000_000:.6f}" if proxy_address
          else f"💰 USDC.e balance: {usdc_before / 1_000_000:.6f}")

    # If specific condition ID provided, just redeem that one
    if args.condition_id:
        cid = args.condition_id.replace("0x", "")
        info = {"question": f"Manual condition: 0x{cid[:20]}..."}

        try:
            import requests
            resp = requests.get(
                "https://gamma-api.polymarket.com/markets",
                params={"condition_id": f"0x{cid}"},
                timeout=10,
            )
            if resp.status_code == 200:
                markets = resp.json()
                if markets:
                    info["question"] = markets[0].get("question", info["question"])
        except Exception:
            pass

        check_and_redeem(w3, ctf_contract, eoa_address, cid, info, args.execute,
                         proxy_address, proxy_contract)
        return

    # Discover all BTC Up/Down 5m conditions across every time frame
    print("\n" + "=" * 60)
    print("PHASE 1: Discovering BTC Up/Down 5m positions")
    print("=" * 60)

    all_condition_ids = fetch_all_btc5m_conditions(proxy_address, eoa_address)

    if not all_condition_ids:
        print("\n⚠️  No BTC Up/Down 5m conditions found.")
        print("   Ensure POLYMARKET_PROXY_ADDRESS is set and the APIs are reachable.")
        return

    print(f"\n{'=' * 60}")
    print(f"PHASE 2: Checking {len(all_condition_ids)} conditions for redemption")
    print(f"{'=' * 60}")

    redeemed = 0
    pending = 0
    no_balance = 0
    skipped_old = 0

    cutoff_24h = time.time() - 86400  # 24 hours ago

    for cid_hex, info in all_condition_ids.items():
        end_date_str = info.get("endDate")
        if end_date_str:
            try:
                from datetime import datetime, timezone
                end_dt = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
                if end_dt.timestamp() < cutoff_24h:
                    skipped_old += 1
                    continue
            except Exception:
                pass  # If we can't parse the date, proceed anyway

        result = check_and_redeem(
            w3, ctf_contract, eoa_address, cid_hex, info, args.execute,
            proxy_address, proxy_contract,
        )
        if result == "DRY_RUN":
            redeemed += 1
        elif result is not None:
            redeemed += 1
            time.sleep(2)  # Wait between txs
        elif result is None:
            condition_id_bytes = bytes.fromhex(cid_hex)
            try:
                pd = ctf_contract.functions.payoutDenominator(condition_id_bytes).call()
                if pd == 0:
                    pending += 1
                else:
                    no_balance += 1
            except Exception:
                no_balance += 1

    print(f"\n{'=' * 60}")
    print("SUMMARY")
    print(f"{'=' * 60}")
    print(f"  Conditions scanned: {len(all_condition_ids)}")
    print(f"  Skipped (>24h old): {skipped_old}")
    print(f"  Redeemable:         {redeemed}")
    print(f"  Pending resolution: {pending}")
    print(f"  No balance/already redeemed: {no_balance}")

    if args.execute and redeemed > 0:
        usdc_after = usdc_contract.functions.balanceOf(holder).call()
        gained = (usdc_after - usdc_before) / 1_000_000
        print(f"\n  💰 USDC.e before: {usdc_before / 1_000_000:.6f}")
        print(f"  💰 USDC.e after:  {usdc_after / 1_000_000:.6f}")
        print(f"  💰 Gained:        {gained:.6f} USDC.e")
    elif not args.execute and redeemed > 0:
        print(f"\n  ℹ️  Run with --execute to actually redeem")


def main():
    parser = argparse.ArgumentParser(description="Redeem Polymarket winning positions")
    parser.add_argument("--execute", action="store_true", help="Actually send redemption transactions")
    parser.add_argument("--condition-id", type=str, help="Redeem a specific condition ID (hex)")
    parser.add_argument("--once", action="store_true", help="Run a single cycle and exit (default: loop every 10 min)")
    args = parser.parse_args()

    if not PRIVATE_KEY:
        print("❌ Set POLYGON_PRIVATE_KEY environment variable or edit the CONFIG section.")
        sys.exit(1)

    # Connect to Polygon
    w3 = Web3(Web3.HTTPProvider(RPC_URL))
    w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)

    if not w3.is_connected():
        print(f"❌ Cannot connect to {RPC_URL}")
        sys.exit(1)

    account = w3.eth.account.from_key(PRIVATE_KEY)
    eoa_address = account.address
    print(f"🔑 EOA (signer/gas): {eoa_address}")

    proxy_address = ""
    proxy_contract = None
    if POLYMARKET_PROXY_ADDRESS:
        proxy_address = Web3.to_checksum_address(POLYMARKET_PROXY_ADDRESS)
        proxy_contract = w3.eth.contract(address=proxy_address, abi=PROXY_ABI)
        print(f"🏦 Proxy (holds tokens): {proxy_address}")
    else:
        print("ℹ️  No POLYMARKET_PROXY_ADDRESS set — scanning EOA directly.")

    usdc_contract = w3.eth.contract(
        address=Web3.to_checksum_address(USDC_E_ADDRESS), abi=USDC_ABI
    )
    ctf_contract = w3.eth.contract(
        address=Web3.to_checksum_address(CTF_ADDRESS), abi=CTF_ABI
    )

    if args.once:
        run_once(w3, ctf_contract, usdc_contract, eoa_address, args,
                 proxy_address, proxy_contract)
        return

    print(f"🔁 Running continuously every {LOOP_INTERVAL_SECONDS // 60} minutes. Press Ctrl+C to stop.\n")
    cycle = 0
    try:
        while True:
            cycle += 1
            print(f"\n{'#' * 60}")
            print(f"# CYCLE {cycle}  —  {time.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"{'#' * 60}")
            # Retry up to 3 times on network/connection errors before giving up
            for attempt in range(1, 4):
                try:
                    run_once(w3, ctf_contract, usdc_contract, eoa_address, args,
                             proxy_address, proxy_contract)
                    break  # success
                except Exception as e:
                    err = str(e)
                    is_network = any(k in err for k in (
                        "NameResolutionError", "ConnectionError", "Max retries",
                        "RemoteDisconnected", "TimeoutError", "ConnectTimeout",
                    ))
                    if is_network and attempt < 3:
                        wait = 60 * attempt  # 1 min, then 2 min
                        print(f"⚠️  Network error (attempt {attempt}/3): {err[:120]}")
                        print(f"   Retrying in {wait}s...")
                        time.sleep(wait)
                    else:
                        print(f"❌ Cycle {cycle} error (attempt {attempt}/3): {e}")
                        break
            print(f"\n⏳ Next run in {LOOP_INTERVAL_SECONDS // 60} minutes...")
            time.sleep(LOOP_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        print("\n🛑 Stopped by user.")


if __name__ == "__main__":
    main()