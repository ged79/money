"""③ Whale Alert 고래 거래 수집기 — 대형 트랜잭션 방향 감지
(Arkham $999/월 대체 → Whale Alert 무료 10 req/min)

API: https://api.whale-alert.io/v1
무료: 10 req/min, ~1개월 히스토리, 거래소 라벨 내장
"""
import time
import requests
from db import get_connection
from config import WHALE_ALERT_API_KEY

WHALE_ALERT_BASE = "https://api.whale-alert.io/v1"

# 심볼 → Whale Alert 블록체인 매핑
SYMBOL_TO_BLOCKCHAIN = {
    "BTCUSDT": "bitcoin",
    "ETHUSDT": "ethereum",
    "SOLUSDT": "solana",
}

# Whale Alert용 토큰 ID (하위 호환)
SYMBOL_TO_TOKEN = SYMBOL_TO_BLOCKCHAIN

# 최소 $1M 이상 거래만 추적
MIN_USD_VALUE = 1_000_000


def _wa_get(endpoint: str, params: dict = None) -> dict | None:
    """Whale Alert API 호출"""
    url = f"{WHALE_ALERT_BASE}{endpoint}"
    params = params or {}
    params["api_key"] = WHALE_ALERT_API_KEY
    try:
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 429:
            print("[WhaleAlert] 레이트 리밋 — 1분 후 재시도")
        else:
            print(f"[WhaleAlert] HTTP 에러: {e.response.status_code} - {e.response.text[:200]}")
        return None
    except Exception as e:
        print(f"[WhaleAlert] 요청 실패: {e}")
        return None


def collect_whale_transactions():
    """고래 대형 거래 수집 — $1M+ 트랜잭션"""
    if not WHALE_ALERT_API_KEY:
        print("[WhaleAlert] API 키 미설정 — 스킵 (추후 .env에 WHALE_ALERT_API_KEY 설정)")
        return

    conn = get_connection()
    total_inserted = 0

    # 최근 6시간 거래 조회
    start_ts = int(time.time()) - 6 * 3600

    data = _wa_get("/transactions", {
        "min_value": MIN_USD_VALUE,
        "start": start_ts,
        "limit": 100,
    })

    if not data or data.get("result") != "success":
        error = data.get("message", "unknown") if data else "no response"
        print(f"[WhaleAlert] 조회 실패: {error}")
        conn.close()
        return

    transactions = data.get("transactions", [])

    for tx in transactions:
        tx_hash = tx.get("hash", "")
        blockchain = tx.get("blockchain", "")
        symbol_match = None
        for sym, chain in SYMBOL_TO_BLOCKCHAIN.items():
            if chain == blockchain:
                symbol_match = sym
                break
        if not symbol_match:
            continue  # 우리가 추적하는 체인이 아님

        from_owner = tx.get("from", {}).get("owner", "unknown")
        to_owner = tx.get("to", {}).get("owner", "unknown")
        from_type = tx.get("from", {}).get("owner_type", "unknown")
        to_type = tx.get("to", {}).get("owner_type", "unknown")
        amount = float(tx.get("amount", 0))
        usd_value = float(tx.get("amount_usd", 0))
        block_time = int(tx.get("timestamp", 0)) * 1000  # ms 변환

        # 중복 체크
        exists = conn.execute(
            "SELECT 1 FROM whale_transactions WHERE tx_hash = ?",
            (tx_hash,),
        ).fetchone()
        if exists:
            continue

        # from_label/to_label에 거래소명 또는 타입 저장
        from_label = f"{from_owner}({from_type})" if from_owner != "unknown" else from_type
        to_label = f"{to_owner}({to_type})" if to_owner != "unknown" else to_type

        conn.execute(
            "INSERT INTO whale_transactions "
            "(tx_hash, from_address, to_address, from_label, to_label, "
            "asset, amount, usd_value, block_time) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (tx_hash,
             tx.get("from", {}).get("address", ""),
             tx.get("to", {}).get("address", ""),
             from_label, to_label,
             blockchain, amount, usd_value, block_time),
        )
        total_inserted += 1

    conn.commit()
    conn.close()

    # 블록체인별 카운트
    chain_counts = {}
    for tx in transactions:
        bc = tx.get("blockchain", "other")
        chain_counts[bc] = chain_counts.get(bc, 0) + 1

    counts_str = " | ".join(f"{k}={v}" for k, v in chain_counts.items())
    print(f"[WhaleAlert] {len(transactions)}건 조회 / {total_inserted}건 신규 | {counts_str}")


def get_whale_direction(asset: str = "bitcoin", hours: int = 6) -> dict:
    """최근 N시간 고래 거래 방향 분석 (M.whale 스코어링용)

    Whale Alert의 owner_type 필드로 거래소 유입/유출 판단:
    - owner_type = "exchange" → 거래소
    - owner_type = "unknown" → 개인 지갑
    """
    conn = get_connection()
    cutoff_ms = int((time.time() - hours * 3600) * 1000)

    rows = conn.execute(
        "SELECT from_label, to_label, usd_value FROM whale_transactions "
        "WHERE asset = ? AND block_time > ?",
        (asset, cutoff_ms),
    ).fetchall()
    conn.close()

    if not rows:
        return {
            "direction": "neutral", "inflow_usd": 0, "outflow_usd": 0,
            "net_flow_usd": 0, "tx_count": 0, "score": 0.0,
        }

    inflow_usd = 0.0   # → 거래소로 이동 = 매도 압력
    outflow_usd = 0.0   # ← 거래소에서 이동 = 축적

    for from_label, to_label, usd_value in rows:
        from_is_ex = "exchange" in (from_label or "").lower()
        to_is_ex = "exchange" in (to_label or "").lower()

        if to_is_ex and not from_is_ex:
            inflow_usd += usd_value
        elif from_is_ex and not to_is_ex:
            outflow_usd += usd_value

    net_flow = inflow_usd - outflow_usd
    tx_count = len(rows)

    # 방향 + 점수
    if tx_count == 0 or abs(net_flow) < MIN_USD_VALUE:
        direction = "neutral"
        score = 0.0
    elif net_flow > 0:
        direction = "exchange_inflow"  # bearish
        score = min(1.0, net_flow / (MIN_USD_VALUE * 10))
    else:
        direction = "exchange_outflow"  # bullish
        score = min(1.0, abs(net_flow) / (MIN_USD_VALUE * 10))

    return {
        "direction": direction,
        "inflow_usd": round(inflow_usd, 2),
        "outflow_usd": round(outflow_usd, 2),
        "net_flow_usd": round(net_flow, 2),
        "tx_count": tx_count,
        "score": round(score, 2),
    }


if __name__ == "__main__":
    import sys, os
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    if sys.platform == "win32":
        sys.stdout.reconfigure(encoding="utf-8")
    from db import init_db
    init_db()
    collect_whale_transactions()
    for asset in ["bitcoin", "ethereum", "solana"]:
        result = get_whale_direction(asset)
        print(f"[Whale] {asset}: {result['direction']} | "
              f"in=${result['inflow_usd']:,.0f} out=${result['outflow_usd']:,.0f} | "
              f"net=${result['net_flow_usd']:,.0f} | score={result['score']}")
