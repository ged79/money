# Whale Transaction Tracking API Comparison
# FREE & Cheap Alternatives to Arkham Intelligence ($999/mo)
# Last Updated: 2026-02-23

---

## EXECUTIVE SUMMARY

| Rank | Service | Monthly Cost | Best For |
|------|---------|-------------|----------|
| 1 | **Whale Alert (Free)** | $0 | Real-time whale alerts, exchange labels, 11 chains |
| 2 | **ClankApp** | $0 | Free whale tx API, 24 blockchains |
| 3 | **Blockchain.com API** | $0 | Free BTC transaction data, MVRV |
| 4 | **Etherscan Free** | $0 | ETH large tx monitoring (no labels) |
| 5 | **Mempool.space** | $0 | BTC mempool & transaction monitoring |
| 6 | **Dune Analytics** | $0 (2,500 credits) | Custom SQL queries, community dashboards |
| 7 | **Flipside Crypto** | $0 | Free SQL queries, 20+ chains |
| 8 | **BGeometrics** | $0 | Free BTC on-chain metrics (MVRV, SOPR) |
| 9 | **Nansen (Free)** | $0 | Solana analytics, wallet profiler |
| 10 | **Whale Alert (Alerts)** | $29.95/mo | Custom real-time alerts |
| 11 | **Bitquery** | $0 (trial) | GraphQL blockchain data, 40+ chains |
| 12 | **CoinGlass** | $29/mo min | Derivatives data, whale alerts |
| 13 | **CryptoQuant** | $39/mo min | Exchange netflow, on-chain metrics |
| 14 | **Nansen Pro** | $49/mo | Smart money tracking, 500M+ labels |
| 15 | **Santiment** | $44/mo min | Social + on-chain combined |
| 16 | **Glassnode** | $29-79/mo | Institutional-grade on-chain metrics |

---

## DETAILED COMPARISON

---

### 1. WHALE ALERT API
**Website:** https://whale-alert.io
**Docs:** https://developer.whale-alert.io/documentation/

#### Pricing Tiers
| Plan | Cost | Rate Limit | History | Key Features |
|------|------|-----------|---------|--------------|
| Free (Developer) | $0 | 10 req/min | ~1 month | REST API, basic attribution |
| Custom Alerts | $29.95/mo | 100 alerts/hr | N/A | WebSocket, custom filters, 11 chains |
| Enterprise REST | $699/mo | 1,000 CPM | 30 days | Full attribution, compliance features |
| Priority Alerts | Contact | 10,000/hr | N/A | 1-min faster delivery, unlimited alerts |

#### Supported Blockchains (11)
Bitcoin, Ethereum, Solana, Algorand, Bitcoin Cash, Dogecoin, Litecoin, Polygon, Ripple, Cardano, Tron

#### Exchange/Address Labels: YES
- Identifies exchanges by default
- Labels: deposit_wallet, hot_wallet, cold_wallet, exchange_wallet, fraud/hack, burn_address, mixer_wallet, merchant_wallet, treasury_address
- Depth of attribution depends on subscription plan

#### API Endpoint Examples
```
# Get transactions (min $1M)
GET https://api.whale-alert.io/v1/transactions?api_key=YOUR_KEY&min_value=1000000&start=UNIX_TIMESTAMP

# Get single transaction
GET https://api.whale-alert.io/v1/transactions/{blockchain}/{hash}?api_key=YOUR_KEY

# Filter by blockchain
GET https://api.whale-alert.io/v1/transactions?api_key=YOUR_KEY&min_value=1000000&blockchain=bitcoin

# WebSocket custom alerts
wss://leviathan.whale-alert.io/ws?api_key=YOUR_API_KEY
```

#### Response Format (includes exchange labels)
```json
{
  "result": "success",
  "cursor": "2bc7e46-...",
  "count": 1,
  "transactions": [{
    "blockchain": "bitcoin",
    "symbol": "BTC",
    "transaction_type": "transfer",
    "hash": "abc123...",
    "from": {
      "address": "1A1z...",
      "owner": "binance",
      "owner_type": "exchange"
    },
    "to": {
      "address": "3J98...",
      "owner": "unknown",
      "owner_type": "unknown"
    },
    "timestamp": 1550237797,
    "amount": 100,
    "amount_usd": 4500000
  }]
}
```

#### Verdict
**BEST FREE OPTION for whale tracking.** Free tier gives 10 req/min with exchange labels and direction data. Covers BTC, ETH, SOL. 1-month history for backtesting. The $29.95/mo alerts plan is excellent value for real-time custom alerts.

---

### 2. CLANKAPP
**Website:** https://clankapp.com
**API:** https://clankapp.com/api/

#### Pricing: COMPLETELY FREE
- Free API key (email required)
- 24 blockchains supported
- Real-time whale transaction tracking
- Push notification system

#### Supported Blockchains (24)
Bitcoin, Ethereum, Binance Chain, Stellar, and 20+ more

#### Features
- Tracks millions of transactions daily
- Shows: value, sender, recipient, date
- Real-time push notifications
- Free API access with email registration

#### Exchange Labels: LIMITED
- Shows sender/recipient addresses
- Some exchange identification but less comprehensive than Whale Alert

#### Verdict
**Good free supplement** to Whale Alert. More chains but less entity labeling. Best for raw large transaction data.

---

### 3. ETHERSCAN API (+ BscScan, etc.)
**Website:** https://etherscan.io/apis
**Docs:** https://docs.etherscan.io

#### Pricing
| Plan | Cost | Rate Limit | Daily Limit |
|------|------|-----------|-------------|
| Free | $0 | 5 calls/sec | 100,000 calls/day |
| Standard | Paid | 10 calls/sec | 200,000 calls/day |
| Advanced | Paid | 20 calls/sec | 500,000 calls/day |

#### Important: Recent Free Tier Changes
- Free tier suspended for: Avalanche, Base, BNB, OP (and testnets)
- Ethereum mainnet still available on free tier

#### Exchange Labels: PAID ONLY (Pro Plus)
- `getaddresstag` endpoint returns nametag + labels (e.g., "Coinbase", "Exchange")
- This is a PRO endpoint - NOT available on free tier
- **FREE WORKAROUND:** Use `eth-labels` open-source dataset (170k+ labeled addresses)

#### API Endpoint Examples
```
# Get normal transactions for address
GET https://api.etherscan.io/api?module=account&action=txlist&address=0x...&apikey=YOUR_KEY

# Get internal transactions
GET https://api.etherscan.io/api?module=account&action=txlistinternal&address=0x...&apikey=YOUR_KEY

# Get ERC-20 token transfers
GET https://api.etherscan.io/api?module=account&action=tokentx&address=0x...&apikey=YOUR_KEY
```

#### Strategy for Large TX Monitoring
Poll known exchange hot wallets for transactions > threshold. Combine with eth-labels dataset for exchange identification.

#### Verdict
**Free and reliable for ETH**, but requires you to build your own large-tx detection logic. No built-in whale alerts. Must combine with external label dataset for exchange identification.

---

### 4. BLOCKCHAIN.COM API
**Website:** https://www.blockchain.com/api

#### Pricing: COMPLETELY FREE
- No API key required for basic endpoints
- WebSocket support for real-time data
- MVRV data available

#### API Types
1. **Blockchain Data API** - Blocks, transactions, addresses (JSON)
2. **WebSocket API** - Real-time streaming of blocks/transactions
3. **Simple Query API** - Hashrate, difficulty, block height
4. **Charts API** - MVRV, market data

#### API Endpoint Examples
```
# Get single transaction
GET https://blockchain.info/rawtx/{tx_hash}

# Get address transactions
GET https://blockchain.info/rawaddr/{address}

# Get latest block
GET https://blockchain.info/latestblock

# Get unconfirmed transactions (real-time)
GET https://blockchain.info/unconfirmed-transactions?format=json

# MVRV chart data
GET https://api.blockchain.info/charts/mvrv?format=json

# WebSocket (subscribe to all transactions)
wss://ws.blockchain.info/inv
{"op":"unconfirmed_sub"}
```

#### Exchange Labels: NO
- Raw transaction data only
- No entity labeling
- Must combine with external label datasets

#### Verdict
**Best free BTC data source.** Unlimited free access to all BTC transactions. WebSocket for real-time. MVRV available. But NO exchange labeling - combine with Whale Alert for labels.

---

### 5. BITQUERY
**Website:** https://bitquery.io
**Docs:** https://docs.bitquery.io

#### Pricing
| Plan | Cost | API Points | Features |
|------|------|-----------|----------|
| Free Trial | $0 | 100,000 points (1 month) | GraphQL, 40+ chains |
| Free Tier | $0 | 1,000 calls/day | Basic access |
| Growth | Paid | More points | Historical data |
| Startup | Paid | More points | Priority support |
| Enterprise | Custom | Unlimited | Full access |

#### Supported Blockchains: 40+
Bitcoin, Ethereum, Solana, BSC, Polygon, Arbitrum, and many more

#### Key Features
- GraphQL API (query exactly what you need)
- WebSocket for real-time streaming
- Historical + real-time indexed data
- SQL access available
- Cloud provider integrations (AWS, Snowflake, Google, Azure)

#### API Endpoint Example (GraphQL)
```graphql
# Large ETH transfers > $1M
{
  ethereum {
    transfers(
      options: {limit: 10, desc: "amount"}
      amount: {gt: 1000000}
      date: {after: "2025-01-01"}
    ) {
      sender {
        address
        annotation
      }
      receiver {
        address
        annotation
      }
      amount
      currency {
        symbol
      }
      transaction {
        hash
      }
    }
  }
}
```

#### Exchange Labels: PARTIAL
- Has address annotations
- Can identify some known entities
- Less comprehensive than Whale Alert/Arkham

#### Verdict
**Excellent for custom queries across 40+ chains.** Free trial is generous (100K points). GraphQL lets you build exactly what you need. Good for backtesting with historical data. Limited free tier after trial expires.

---

### 6. NANSEN
**Website:** https://www.nansen.ai
**Plans:** https://www.nansen.ai/plans

#### Pricing (Updated Sep 2025)
| Plan | Cost | Key Features |
|------|------|-------------|
| Free | $0 | Solana analytics, Token God Mode, Wallet Profiler, holder distributions, exchange flows |
| Pro | $49/mo (annual) or $69/mo (monthly) | All premium features, API access, 500M+ wallet labels |

#### Previous pricing was Pioneer ($129/mo) + Professional ($999/mo) - now simplified.

#### Exchange Labels: YES (EXCELLENT)
- 500M+ labeled wallets
- Identifies: whales, exchanges, smart money, funds, DAOs
- Smart Money tracking (wallets with proven profitability)

#### Free Tier Includes
- Solana: Token analysis, wallet tracking, token screener
- Holder distributions, transaction histories
- PnL leaderboards
- Exchange flows
- Smart money activity

#### API Access: Pro plan only ($49-69/mo)

#### Verdict
**Incredible value at $49/mo** (was $999+). 500M+ labeled wallets is second only to Arkham. Free tier covers Solana basics. Pro gives full multi-chain access with API.

---

### 7. GLASSNODE
**Website:** https://glassnode.com
**Review:** https://captainaltcoin.com/glassnode-review/

#### Pricing
| Plan | Cost | Resolution | Metrics |
|------|------|-----------|---------|
| Free (Standard) | $0 | Daily (delayed) | Tier 1 basic metrics |
| Advanced | ~$29-49/mo | Hourly | Tier 2 essential metrics |
| Professional | ~$79/mo | 10-min updates | All metrics (Tier 3) |
| Enterprise | Custom | Real-time | API access, custom |

#### API Access: Professional/Enterprise ONLY
- Free tier = web charts only, no API
- API requires $79+/mo minimum

#### Key Metrics Available
- MVRV, NUPL, SOPR, STH/LTH SOPR
- Exchange inflow/outflow/netflow
- Active addresses, supply metrics
- Realized price, NVT ratio

#### Exchange Flow Detection: YES (paid tiers)
- Exchange inflow/outflow for BTC, ETH
- Net exchange position change
- Per-exchange breakdown

#### Verdict
**Gold standard for on-chain metrics** but expensive for API access. Free tier is charts-only with daily delayed data. Not practical for automated whale tracking due to cost. Better alternatives exist for API access.

---

### 8. DUNE ANALYTICS
**Website:** https://dune.com
**Pricing:** https://dune.com/pricing

#### Pricing
| Plan | Cost | Credits/mo | Key Features |
|------|------|-----------|-------------|
| Free | $0 | 2,500 | Unlimited dashboards, API access |
| Pay-as-you-go | $5/100 credits | Flexible | Beyond free tier |
| Plus | $399/mo | 25,000 | CSV exports, 10x credits |
| Premium | $999/mo | Custom | Private queries |

#### API Access: YES (Free tier included!)
- 2,500 credits/month free
- Unlimited dashboards
- Unlimited free teammates

#### Exchange Labels: YES (via community queries)
- Massive community of analysts
- Pre-built dashboards for whale tracking
- Exchange flow dashboards available
- Custom SQL to identify exchange addresses

#### Strategy for Whale Tracking
```sql
-- Example: Large ETH transfers > $1M in last 24h
SELECT
  block_time,
  "from",
  "to",
  value / 1e18 as eth_amount,
  value / 1e18 * p.price as usd_value
FROM ethereum.transactions t
JOIN prices.usd p ON p.symbol = 'ETH' AND p.minute = date_trunc('minute', t.block_time)
WHERE value / 1e18 * p.price > 1000000
  AND block_time > now() - interval '24 hours'
ORDER BY usd_value DESC
```

#### Verdict
**Best free option for custom analytics.** 2,500 credits/month is enough for moderate use. Huge community means pre-built whale dashboards exist. SQL-based = maximum flexibility. Great for backtesting. Rate: queries consume credits, so heavy use needs paid plan.

---

### 9. COINGLASS
**Website:** https://www.coinglass.com
**API Docs:** https://docs.coinglass.com

#### Pricing (NO free API tier)
| Plan | Cost | Endpoints | Rate Limit |
|------|------|----------|-----------|
| Hobbyist | $29/mo | 70+ | 30 req/min |
| Startup | $79/mo | 80+ | 80 req/min |
| Standard | $299/mo | 90+ | 300 req/min |
| Professional | $699/mo | 100+ | 1,200 req/min |
| Enterprise | Custom | 100+ | 6,000 req/min |

#### Free Website Features (no API)
- Liquidation heatmaps
- Whale alert page (web only)
- MVRV ratio charts
- Exchange spot inflow/outflow
- Open interest, funding rates

#### Whale Tracking Features
- Hyperliquid whale monitoring
- Large order/trade tracking
- Exchange spot flow statistics

#### API Endpoints
```
GET /api/public/v2/indicator/whale-index
GET /api/public/v2/indicator/whale-alert (Hyperliquid)
```

#### Verdict
**No free API tier.** $29/mo minimum. Web interface has good free whale data but not programmable. Better to use Whale Alert free API + CoinGlass website for manual checking.

---

### 10. COINALYZE
**Website:** https://coinalyze.net
**API Docs:** https://api.coinalyze.net/v1/doc/

#### Pricing: FREE (no sign-up required!)
- All dashboards and tools free
- API access free
- Optional paid "ad-free" version for support

#### API Features
- Funding rates
- Open interest
- Liquidation history
- Cumulative Volume Delta (CVD)
- Supported exchanges and markets

#### Exchange Labels: NO
- Derivatives/futures data focus
- No on-chain transaction tracking
- No whale address identification

#### Verdict
**Free derivatives data** but NOT useful for on-chain whale tracking. Good supplement for futures/derivatives analysis only.

---

### 11. MEMPOOL.SPACE (Bitcoin)
**Website:** https://mempool.space
**API Docs:** https://mempool.space/docs/api/rest
**GitHub:** https://github.com/mempool/mempool

#### Pricing: COMPLETELY FREE + OPEN SOURCE
- Self-hostable
- No API key required
- REST API + WebSocket

#### API Endpoint Examples
```
# Get transaction details
GET https://mempool.space/api/tx/{txid}

# Get address transactions
GET https://mempool.space/api/address/{address}/txs

# Get mempool (unconfirmed) transactions
GET https://mempool.space/api/mempool/recent

# WebSocket for real-time
wss://mempool.space/api/v1/ws
```

#### Exchange Labels: NO
- Raw BTC transaction data only
- No entity labeling

#### Verdict
**Best free open-source BTC explorer API.** Self-hostable for zero rate limits. Good for BTC transaction monitoring. No exchange labels.

---

### 12. CRYPTOQUANT
**Website:** https://cryptoquant.com
**Pricing:** https://cryptoquant.com/pricing

#### Pricing
| Plan | Cost | Key Features |
|------|------|-------------|
| Basic (Free) | $0 | Very limited data, delayed |
| Advanced | $39/mo ($29 annual) | More metrics, alerts |
| Professional | $109/mo ($99 annual) | Minute-level data, more alerts |
| Premium | $799/mo ($699 annual) | API access, full data, AI assistant |

#### API Access: Premium only ($699-799/mo) - TOO EXPENSIVE

#### Key Metrics
- Exchange netflow (BTC, ETH, alts)
- MVRV, SOPR, NUPL
- Miner flows
- Whale alerts
- Stablecoin flows

#### Exchange Flow Detection: YES (EXCELLENT)
- Per-exchange breakdown
- Inflow/outflow/netflow
- Reserve tracking

#### Verdict
**Excellent data but API is $699+/mo.** Free tier is too limited. Web charts are useful for manual analysis. For API access, look elsewhere.

---

### 13. SANTIMENT
**Website:** https://app.santiment.net
**API:** https://api.santiment.net

#### Pricing
| Plan | Cost | Key Features |
|------|------|-------------|
| Free | $0 | Basic charts, limited history |
| Pro (Sanbase) | $44+/mo | On-chain + social data, alerts |

#### API: SanAPI (GraphQL)
- Requires paid plan for real-time/historical data
- Free tier = very limited

#### Key Features
- Whale transaction counts
- Exchange inflow/outflow
- Social volume + sentiment
- Holder distribution
- Development activity
- SOPR-style profit/loss metrics

#### Exchange Labels: YES
- Tracks exchange inflows/outflows
- Whale detection built-in

#### Verdict
**Unique combo of on-chain + social data.** $44/mo is reasonable but no free API. Good for combining whale tracking with sentiment analysis.

---

### 14. FLIPSIDE CRYPTO
**Website:** https://flipsidecrypto.xyz
**Pricing:** https://flipsidecrypto.xyz/pricing

#### Pricing
| Plan | Cost | Query Seconds | Key Features |
|------|------|--------------|-------------|
| Free | $0 | 500 API query seconds | Unlimited queries, 20+ chains, dashboards |
| Builder | Paid | 10,000 premium | For protocol builders |
| Pro | Paid | 60,000 premium | Snowflake access, full API |

#### Features
- SQL queries on blockchain data
- 20+ chains supported
- 60+ 3rd-party API integrations
- Unlimited dashboards and downloads

#### Verdict
**Great free SQL analytics platform.** Similar to Dune. Good for custom whale queries and backtesting. 500 free query seconds is decent for moderate use.

---

### 15. BGEOMETRICS
**Website:** https://bgeometrics.com
**API Docs:** https://charts.bgeometrics.com/bitcoin_api.html
**Full API:** https://bitcoin-data.com/api/scalar.html

#### Pricing: FREE

#### Available Metrics (Bitcoin focused)
- **Price**: OHLC, Realized Price, Delta Price, Mayer Multiple
- **Valuation**: MVRV Z-Score, MVRV Ratio, NUPL, NRPL
- **Spending**: SOPR, SOPR STH, SOPR LTH
- **Exchange**: Inflow, Outflow, Netflow, Reserve
- **Mining**: Hashrate, Puell Multiple
- **Supply**: Active Addresses, HODL Waves
- **Derivatives**: Open Interest, Funding Rate, Basis
- **Technical**: RSI, MACD, SMA, EMA
- **Macro**: ETF balances, M2, Stablecoin supply
- **30+ Cryptocurrencies** supported

#### Verdict
**HIDDEN GEM - Best free on-chain metrics API.** Covers MVRV, SOPR, exchange flows, and more. Free. Excellent CryptoQuant/Glassnode alternative for BTC metrics.

---

### 16. BLOCKCHAIR
**Website:** https://blockchair.com
**API Docs:** https://blockchair.com/api/docs

#### Pricing
| Plan | Cost | Calls | Rate Limit |
|------|------|-------|-----------|
| Free | $0 | 1,000/day | 30 req/min |
| Pay-as-you-go | $1/1K calls | Flexible | Higher |
| Specialist | $25-100/mo | 1,250-5,000/day | Higher |
| Enterprise | Custom | High volume | Custom |

#### Features
- 41 blockchains supported
- Database dumps available (TSV)
- Full transaction data

#### Exchange Labels: LIMITED
- Some known address identification
- Not as comprehensive as Whale Alert

#### Verdict
**Good multi-chain explorer API.** Free tier is small (1,000/day) but cheap to scale. Database dumps are excellent for backtesting.

---

## FREE ADDRESS LABELING RESOURCES

### eth-labels (Open Source)
- **GitHub:** https://github.com/dawsbot/eth-labels
- 170,000+ labeled addresses across EVM chains
- Free public API
- Labels include: exchanges, protocols, funds
- MCP server integration available

### etherscan-labels (Data Dump)
- **GitHub:** https://github.com/brianleect/etherscan-labels
- Full label data from top EVM chains
- JSON/CSV format
- Automated scraper

---

## RECOMMENDED STACK (FREE / <$50/mo)

### Option A: 100% FREE Stack
| Need | Service | Cost |
|------|---------|------|
| Whale Alerts (BTC/ETH/SOL) | Whale Alert Free API | $0 |
| BTC Transactions | Blockchain.com API + Mempool.space | $0 |
| ETH Transactions | Etherscan Free API | $0 |
| Address Labels | eth-labels + Whale Alert labels | $0 |
| On-chain Metrics (MVRV/SOPR) | BGeometrics API | $0 |
| Custom Analytics/Backtesting | Dune Analytics (2,500 credits) | $0 |
| Supplementary Whale Data | ClankApp API | $0 |
| **TOTAL** | | **$0/mo** |

### Option B: Best Value Stack (<$80/mo)
| Need | Service | Cost |
|------|---------|------|
| Whale Alerts + Exchange Labels | Whale Alert (Custom Alerts) | $29.95/mo |
| Smart Money + 500M labels | Nansen Pro | $49/mo |
| BTC On-chain Metrics | BGeometrics API | $0 |
| Custom Analytics | Dune Analytics Free | $0 |
| BTC Raw Data | Blockchain.com + Mempool.space | $0 |
| **TOTAL** | | **~$79/mo** |

### Option C: Maximum Coverage (~$120/mo)
| Need | Service | Cost |
|------|---------|------|
| Whale Alerts + Exchange Labels | Whale Alert (Custom Alerts) | $29.95/mo |
| Smart Money + Labels | Nansen Pro | $49/mo |
| Exchange Netflow + MVRV | CryptoQuant Advanced | $39/mo |
| Custom Analytics | Dune Analytics Free | $0 |
| Free supplements | BGeometrics + Blockchain.com | $0 |
| **TOTAL** | | **~$118/mo** |

---

## FEATURE MATRIX

| Feature | Whale Alert | ClankApp | Etherscan | Blockchain.com | Bitquery | Nansen | Glassnode | Dune | CoinGlass | BGeometrics |
|---------|:-----------:|:--------:|:---------:|:--------------:|:--------:|:------:|:---------:|:----:|:---------:|:-----------:|
| **Free API** | YES (limited) | YES | YES | YES | Trial | Partial | NO | YES | NO | YES |
| **BTC Support** | YES | YES | NO | YES | YES | YES | YES | YES | YES | YES |
| **ETH Support** | YES | YES | YES | NO | YES | YES | YES | YES | YES | Partial |
| **SOL Support** | YES | YES | NO | NO | YES | YES | NO | YES | YES | NO |
| **Exchange Labels** | YES | Limited | Paid only | NO | Partial | YES | YES | Community | Web only | YES |
| **Whale Alerts** | YES | YES | NO | NO | NO | YES | NO | Community | Web only | NO |
| **MVRV** | NO | NO | NO | YES | NO | NO | YES | Community | YES | YES |
| **SOPR** | NO | NO | NO | NO | NO | NO | YES | Community | NO | YES |
| **Exchange Netflow** | NO | NO | NO | NO | YES | YES | YES | Community | YES | YES |
| **Historical Data** | 1 month | Limited | Full | Full | YES | YES | YES | Full | YES | YES |
| **Real-time** | YES | YES | Near | YES | YES | YES | Paid | Query | YES | NO |
| **Rate Limit (Free)** | 10/min | Unknown | 5/sec | None | 1K/day | N/A | N/A | 2,500 cred | N/A | Unknown |

---

## API ENDPOINT QUICK REFERENCE

### Whale Alert - Large Transactions
```
GET https://api.whale-alert.io/v1/transactions?api_key=KEY&min_value=1000000&start=TIMESTAMP
```

### Blockchain.com - BTC Transaction
```
GET https://blockchain.info/rawtx/{tx_hash}
GET https://api.blockchain.info/charts/mvrv?format=json
```

### Etherscan - ETH Transactions
```
GET https://api.etherscan.io/api?module=account&action=txlist&address=ADDR&apikey=KEY
```

### Mempool.space - BTC Real-time
```
GET https://mempool.space/api/tx/{txid}
wss://mempool.space/api/v1/ws
```

### BGeometrics - On-chain Metrics
```
API Base: https://bitcoin-data.com/api/
Docs: https://bitcoin-data.com/api/scalar.html
Metrics: MVRV, SOPR, Exchange Netflow, Hashrate, NUPL, etc.
```

### Dune Analytics - Custom SQL
```
POST https://api.dune.com/api/v1/query/{query_id}/execute
GET https://api.dune.com/api/v1/query/{query_id}/results
```

### Bitquery - GraphQL
```
POST https://graphql.bitquery.io
Header: X-API-KEY: YOUR_KEY
Body: { "query": "{ bitcoin { transactions(options: {limit: 10}) { ... } } }" }
```

### ClankApp - Whale Transactions
```
API Base: https://clankapp.com/api/
Email registration required for API key
```

---

## NOTES

1. **Arkham Intelligence** ($999/mo) remains the gold standard for entity labeling (800M+ labels) but the free platform (web-only) allows manual research. Their API pricing is "flexible" / contact sales.

2. **Nansen's price drop** (from $999+ to $49/mo) makes it the single best value for whale tracking with labels.

3. **BGeometrics** is an underrated free API that covers most CryptoQuant metrics for Bitcoin.

4. **Dune Analytics** free tier (2,500 credits) is powerful because community dashboards for whale tracking already exist.

5. For **backtesting**, combine:
   - Whale Alert historical (1 month free)
   - Dune Analytics (full historical via SQL)
   - Blockchain.com (full BTC history)
   - Blockchair database dumps (full history, TSV format)
