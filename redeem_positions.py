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

sys.stdout = _Tee(LOG_PATH, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID)

_alchemy_key = os.environ.get("ALCHEMY_API_KEY", "")
_default_rpc = f"https://polygon-mainnet.g.alchemy.com/v2/{_alchemy_key}" if _alchemy_key else ""
RPC_URL = os.environ.get("POLYGON_RPC_URL", _default_rpc)

# Contract addresses on Polygon
CTF_ADDRESS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"  # Conditional Tokens
USDC_E_ADDRESS = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"  # USDC.e (bridged)

# Polymarket uses parentCollectionId = bytes32(0) for all markets
PARENT_COLLECTION_ID = bytes(32)

# Polymarket binary markets use index sets [1, 2] (outcome 0 and outcome 1)
INDEX_SETS = [1, 2]

# PolygonScan API (free tier, no key needed for basic queries)
POLYGONSCAN_API_URL = "https://api.polygonscan.com/api"
POLYGONSCAN_API_KEY = os.environ.get("POLYGONSCAN_API_KEY", "")  # Optional, higher rate limits

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


def discover_condition_ids_from_logs(w3: Web3, wallet_address: str, ctf_contract) -> set:
    """
    Discover condition IDs by scanning PositionSplit events and PolygonScan ERC-1155 transfers.
    Uses PolygonScan as the primary (full-history) source, with on-chain log scan as fallback.
    """
    import requests as _req

    condition_ids = set()
    wallet_lower = wallet_address.lower()

    print("Scanning for your positions...")

    # Method 1: Alchemy getAssetTransfers — full history, no block range limit
    alchemy_key = os.environ.get("ALCHEMY_API_KEY", "")
    if alchemy_key and alchemy_key in RPC_URL:
        try:
            token_ids_received = set()
            page_key = None
            while True:
                params_inner = {
                    "toAddress": wallet_address,
                    "contractAddresses": [CTF_ADDRESS],
                    "category": ["erc1155"],
                    "withMetadata": False,
                    "excludeZeroValue": True,
                    "maxCount": "0x3e8",
                }
                if page_key:
                    params_inner["pageKey"] = page_key
                payload = {"jsonrpc": "2.0", "id": 1,
                           "method": "alchemy_getAssetTransfers", "params": [params_inner]}
                resp = _req.post(RPC_URL, json=payload, timeout=15)
                resp.raise_for_status()
                result = resp.json().get("result", {})
                for transfer in result.get("transfers", []):
                    for meta in transfer.get("erc1155Metadata") or []:
                        tid_hex = meta.get("tokenId", "")
                        if tid_hex:
                            token_ids_received.add(int(tid_hex, 16))
                page_key = result.get("pageKey")
                if not page_key:
                    break

            print(f"  Found {len(token_ids_received)} unique token IDs via Alchemy API")
            return condition_ids, token_ids_received
        except Exception as e:
            print(f"  Alchemy getAssetTransfers error: {e}")

    # Method 1b: PolygonScan API — full history fallback
    if POLYGONSCAN_API_KEY:
        try:
            params = {
                "module": "account",
                "action": "token1155tx",
                "address": wallet_address,
                "contractaddress": CTF_ADDRESS,
                "sort": "desc",
                "apikey": POLYGONSCAN_API_KEY,
            }
            resp = _req.get(POLYGONSCAN_API_URL, params=params, timeout=30)
            data = resp.json()

            if data.get("status") == "1" and data.get("result"):
                token_ids_received = set()
                for tx in data["result"]:
                    if tx.get("to", "").lower() == wallet_lower:
                        token_id = int(tx.get("tokenID", "0"))
                        if token_id > 0:
                            token_ids_received.add(token_id)

                print(f"  Found {len(token_ids_received)} unique token IDs via PolygonScan API")
                return condition_ids, token_ids_received
            else:
                print(f"  PolygonScan returned no results: {data.get('message')}")
        except Exception as e:
            print(f"  PolygonScan API error: {e}")

    # Method 2: On-chain eth_getLogs via a public RPC (supports larger block ranges)
    FALLBACK_RPC_URLS = [
        "https://polygon-bor-rpc.publicnode.com",
        "https://rpc.ankr.com/polygon",
        "https://polygon-rpc.com",
    ]
    print("  Falling back to on-chain log scan via public RPC...")
    latest_block = w3.eth.block_number
    from_block = max(0, latest_block - 500_000)

    split_event_sig = "0x" + Web3.keccak(text="PositionSplit(address,address,bytes32,bytes32,uint256[],uint256)").hex()
    wallet_topic = "0x" + wallet_address[2:].lower().zfill(64)

    # Pick the first public RPC that responds and supports 2000-block ranges
    rpc_url = None
    chunk_size = 2_000
    for candidate in FALLBACK_RPC_URLS:
        try:
            probe_payload = {
                "jsonrpc": "2.0", "id": 1, "method": "eth_getLogs",
                "params": [{"address": CTF_ADDRESS, "topics": [split_event_sig],
                            "fromBlock": hex(latest_block - chunk_size), "toBlock": hex(latest_block)}]
            }
            probe = _req.post(candidate, json=probe_payload, timeout=10)
            body = probe.json()
            if "error" not in body:
                rpc_url = candidate
                print(f"  Using public RPC: {candidate}")
                break
            else:
                msg = body["error"].get("message", "")
                import re
                match = re.search(r'\[0x[0-9a-f]+, (0x[0-9a-f]+)\]', msg)
                if match:
                    suggested_end = int(match.group(1), 16)
                    detected_chunk = suggested_end - (latest_block - chunk_size)
                    if detected_chunk >= 100:
                        chunk_size = detected_chunk
                        rpc_url = candidate
                        print(f"  Using public RPC: {candidate} (max {chunk_size} blocks/request)")
                        break
        except Exception:
            continue

    if not rpc_url:
        print("  No suitable public RPC found — add POLYGONSCAN_API_KEY for full history")
        return condition_ids, set()

    found_splits = 0
    total_blocks = latest_block - from_block
    total_chunks = total_blocks // chunk_size + 1

    if total_chunks > 5_000:
        print(f"  Log scan would require {total_chunks:,} calls — skipping (add POLYGONSCAN_API_KEY for full history)")
        return condition_ids, set()

    try:
        for i, chunk_start in enumerate(range(from_block, latest_block + 1, chunk_size)):
            chunk_end = min(chunk_start + chunk_size - 1, latest_block)
            payload = {
                "jsonrpc": "2.0", "id": 1, "method": "eth_getLogs",
                "params": [{"address": CTF_ADDRESS,
                            "topics": [split_event_sig, wallet_topic],
                            "fromBlock": hex(chunk_start), "toBlock": hex(chunk_end)}]
            }
            resp = _req.post(rpc_url, json=payload, timeout=10)
            result = resp.json().get("result", [])
            for log in result:
                topics = log.get("topics", [])
                if len(topics) >= 4:
                    cid = bytes.fromhex(topics[3][2:])
                    condition_ids.add(cid)
                    found_splits += 1
                    print(f"  Found condition: 0x{cid.hex()[:16]}...")
            if (i + 1) % 100 == 0:
                print(f"  Scanned {i + 1}/{total_chunks} chunks...")
        print(f"  Log scan complete ({found_splits} events found)")
    except Exception as e:
        print(f"  Log scan error: {e}")

    return condition_ids, set()


def discover_condition_ids_from_gamma_api(token_ids: set, eoa_address: str = "") -> dict:
    """
    Resolve token IDs → condition IDs + market info.

    Strategy (in order):
    1. Polymarket Data API — single call using EOA address, returns all current positions.
    2. Gamma API batch     — query up to 20 token IDs per request as fallback.
    """
    import requests

    conditions = {}

    # ----------------------------------------------------------------
    # Method 1: Polymarket Data API (one call for everything)
    # ----------------------------------------------------------------
    if eoa_address:
        try:
            resp = requests.get(
                "https://data-api.polymarket.com/positions",
                params={"user": eoa_address},
                timeout=15,
            )
            print(f"  Data API status: {resp.status_code}")
            if resp.status_code == 200:
                positions = resp.json()
                if not isinstance(positions, list):
                    positions = positions.get("data", [])
                print(f"  Data API raw count: {len(positions)}")
                for pos in positions:
                    cid = pos.get("conditionId") or pos.get("condition_id", "")
                    if not cid:
                        continue
                    conditions[cid] = {
                        "question": pos.get("title") or pos.get("question", f"Condition {cid[:16]}..."),
                        "outcome": pos.get("outcome", ""),
                        "resolved": pos.get("redeemable", False),
                    }
                if conditions:
                    print(f"  Found {len(conditions)} conditions via Data API")
                    return conditions
                else:
                    print(f"  Data API returned no usable conditions (raw: {str(positions)[:200]})")
                    print("  Falling back to Gamma API...")
            else:
                print(f"  Data API error response: {resp.text[:200]}")
        except Exception as e:
            print(f"  Data API error: {e}")

    # ----------------------------------------------------------------
    # Method 2: Gamma API — batch up to 20 token IDs per request
    # ----------------------------------------------------------------
    token_list = list(token_ids)
    batch_size = 20
    for i in range(0, len(token_list), batch_size):
        batch = token_list[i:i + batch_size]
        try:
            resp = requests.get(
                "https://gamma-api.polymarket.com/markets",
                params={"clobTokenIds": ",".join(str(t) for t in batch)},
                timeout=15,
            )
            if resp.status_code == 200:
                markets = resp.json()
                batch_set = set(str(t) for t in batch)
                for market in markets:
                    # Only include markets whose clobTokenIds intersect our batch
                    raw_ids = market.get("clobTokenIds", "[]")
                    try:
                        market_token_ids = set(json.loads(raw_ids)) if isinstance(raw_ids, str) else set(str(t) for t in raw_ids)
                    except Exception:
                        market_token_ids = set()
                    if not market_token_ids.intersection(batch_set):
                        continue
                    cid = market.get("conditionId") or market.get("condition_id", "")
                    if not cid:
                        continue
                    conditions[cid] = {
                        "question": market.get("question", "Unknown"),
                        "outcome": market.get("outcome", ""),
                        "resolved": market.get("closed", False),
                    }
            time.sleep(1.0)  # Respect Gamma API rate limit between batches
        except Exception as e:
            print(f"  Gamma API batch error: {e}")
            continue

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

    If proxy_address is set:
      - balances are checked on the proxy (it holds the CTF tokens)
      - redemption is executed via proxy.execute(CTF, redeemPositions_calldata)
        so USDC.e lands back in the proxy
    Otherwise falls back to direct call from the EOA.

    Returns tx hash if redeemed, None otherwise.
    """
    # The address that actually holds the tokens
    holder = Web3.to_checksum_address(proxy_address) if proxy_address else eoa_address

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

    # Compute position IDs for both outcomes
    collection_id_0 = get_collection_id(PARENT_COLLECTION_ID, condition_id_bytes, 1)  # indexSet=1 -> outcome 0
    collection_id_1 = get_collection_id(PARENT_COLLECTION_ID, condition_id_bytes, 2)  # indexSet=2 -> outcome 1

    position_id_0 = get_position_id(USDC_E_ADDRESS, collection_id_0)
    position_id_1 = get_position_id(USDC_E_ADDRESS, collection_id_1)

    # Check balance on the token holder (proxy or EOA)
    balance_0 = ctf_contract.functions.balanceOf(holder, position_id_0).call()
    balance_1 = ctf_contract.functions.balanceOf(holder, position_id_1).call()

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

        if proxy_contract and proxy_address:
            # Encode the redeemPositions call and forward it through the proxy
            redeem_calldata = ctf_contract.encode_abi(
                "redeemPositions",
                args=[
                    Web3.to_checksum_address(USDC_E_ADDRESS),
                    PARENT_COLLECTION_ID,
                    condition_id_bytes,
                    INDEX_SETS,
                ],
            )
            tx = proxy_contract.functions.execute(
                Web3.to_checksum_address(CTF_ADDRESS),
                0,
                redeem_calldata,
            ).build_transaction(base)
            print(f"     📡 Routing through proxy: {proxy_address[:10]}...")
        else:
            tx = ctf_contract.functions.redeemPositions(
                Web3.to_checksum_address(USDC_E_ADDRESS),
                PARENT_COLLECTION_ID,
                condition_id_bytes,
                INDEX_SETS,
            ).build_transaction(base)

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

    # Discover positions — scan the holder address (proxy or EOA)
    print("\n" + "=" * 60)
    print("PHASE 1: Discovering your positions")
    print("=" * 60)

    condition_ids_from_events, token_ids = discover_condition_ids_from_logs(
        w3, holder, ctf_contract
    )

    print("\nLooking up market info from Data API...")
    conditions_from_gamma = {}
    if token_ids:
        conditions_from_gamma = discover_condition_ids_from_gamma_api(token_ids, eoa_address)
        print(f"  Found {len(conditions_from_gamma)} unique conditions from Data API")

    all_condition_ids = {}

    for cid_bytes in condition_ids_from_events:
        cid_hex = cid_bytes.hex()
        all_condition_ids[cid_hex] = {"question": f"Condition 0x{cid_hex[:16]}..."}

    for cid_hex, info in conditions_from_gamma.items():
        cid_hex_clean = cid_hex.replace("0x", "")
        all_condition_ids[cid_hex_clean] = info

    if not all_condition_ids:
        print("\n⚠️  No positions found. This could mean:")
        print("   - No trades in the scanned block range")
        print("   - PolygonScan API rate limited (try adding POLYGONSCAN_API_KEY)")
        print("   - Try passing --condition-id directly if you know the condition")
        print(f"\n   You can also manually check your tokens at:")
        print(f"   https://polygonscan.com/token/{CTF_ADDRESS}?a={holder}")
        return

    print(f"\n{'=' * 60}")
    print(f"PHASE 2: Checking {len(all_condition_ids)} conditions for redemption")
    print(f"{'=' * 60}")

    redeemed = 0
    pending = 0
    no_balance = 0

    for cid_hex, info in all_condition_ids.items():
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
    parser.add_argument("--blocks", type=int, default=500_000, help="Number of blocks to scan back (default: 500000)")
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