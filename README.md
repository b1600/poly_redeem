# poly_redeem

Automatically redeems winning Polymarket BTC Up/Down 5-minute positions and converts any legacy USDC.e proceeds to pUSD. Runs on Polygon via your EOA (gas wallet) and Polymarket proxy wallet (Gnosis Safe).

---

## Overview

1. **Discover** — scans your proxy and EOA wallets for BTC Up/Down 5m conditional tokens using Alchemy transfer history, the Polymarket Data API, and the Gamma API
2. **Redeem** — calls `redeemPositions` on the Polymarket CTF contract for every resolved winning position, routing through your proxy wallet (or directly from EOA if needed)
3. **Convert** — swaps any leftover USDC.e balance to pUSD via Uniswap V3

By default the script runs in **dry-run mode** (read-only). Pass `--execute` to send real transactions.

---

## Setup

### 1. Clone and install dependencies

```bash
git clone <repo>
cd poly_redeem
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Create a `.env` file

```env
# Required
POLYGON_PRIVATE_KEY=0xYOUR_PRIVATE_KEY

# Recommended — your Polymarket proxy wallet (Gnosis Safe)
# Find it at https://polygonscan.com/address/<your_eoa>#tokentxns
POLYMARKET_PROXY_ADDRESS=0xYOUR_PROXY_ADDRESS

# RPC — use one of:
ALCHEMY_API_KEY=your_alchemy_key          # enables full token-ID discovery
# OR
POLYGON_RPC_URL=https://polygon-rpc.com   # public fallback (slower discovery)

# Optional — Telegram notifications
TELEGRAM_BOT_TOKEN=your_bot_token
TELEGRAM_CHAT_ID=your_chat_id

# Optional — loop frequency in seconds (default: 900 = 15 min)
LOOP_INTERVAL_SECONDS=900
```

### 3. Dry run to verify

```bash
python redeem_positions.py
```

This prints what would be redeemed without sending any transactions.

### 4. Execute redemptions

```bash
python redeem_positions.py --execute
```

### 5. Run continuously (loop mode)

```bash
python redeem_positions.py --execute
```

The script loops every `LOOP_INTERVAL_SECONDS` (default 15 min) until you press `Ctrl+C`. Logs are written to `redeem.log` and optionally forwarded to Telegram.

---

## Usage Reference

```
python redeem_positions.py [OPTIONS]

Options:
  --execute         Send real transactions (default: dry run)
  --once            Run a single cycle then exit (default: loop forever)
  --condition-id    Redeem a specific condition ID (hex) instead of auto-scanning
```

**Examples:**

```bash
# Dry run — show what can be redeemed
python redeem_positions.py

# Single cycle, live transactions
python redeem_positions.py --execute --once

# Redeem a specific market condition
python redeem_positions.py --execute --condition-id 0xabc123...

# Loop forever with live transactions
python redeem_positions.py --execute
```

---

## FAQ

**Do I need the proxy address?**
Yes, in most cases. Polymarket routes positions through a Gnosis Safe proxy wallet. Without `POLYMARKET_PROXY_ADDRESS`, the script will only scan your EOA directly and may miss positions. Find your proxy address by looking at token transactions on your EOA at Polygonscan.

**Do I need an Alchemy key?**
It's strongly recommended. Without one, position discovery falls back to the Polymarket APIs alone, which may miss older positions. With an Alchemy key the script uses `alchemy_getAssetTransfers` to get a complete history of ERC-1155 tokens ever received by your wallet.

**What tokens does the script redeem?**
Only BTC Up/Down 5-minute markets (`btc-updown-5m-*` slugs). It supports both old markets (USDC.e collateral) and new markets (pUSD collateral, introduced April 28, 2026).

**What happens to USDC.e after redemption?**
Any USDC.e balance in your proxy (or EOA) is automatically swapped to pUSD via Uniswap V3 with 1% max slippage. This runs at the start and end of every cycle.

**Will it redeem losing positions?**
No. The script checks on-chain payout numerators and only redeems positions where you hold the winning outcome token.

**How much gas does it need?**
Each redemption uses up to ~400,000 gas. Keep at least 0.05 POL in your EOA. The script warns you if the balance drops below 0.01 POL.

**How do I run it as a background service?**
Use `nohup` or a process manager:
```bash
nohup python redeem_positions.py --execute > /dev/null 2>&1 &
```
Or configure a `systemd` service or `cron` job pointing to the script.

**Where are the logs?**
`redeem.log` in the project directory. If Telegram credentials are set, logs are also sent to your Telegram chat in batched messages.
