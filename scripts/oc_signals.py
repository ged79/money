"""OpenClaw용 시그널 이력 조회 - JSON 출력 (멀티 심볼)"""
import sys, os, json, argparse
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

from db import get_connection


def get_signals(limit=20, symbol=None):
    conn = get_connection()

    if symbol:
        rows = conn.execute(
            "SELECT symbol, signal_type, direction, details, ssm_score, created_at "
            "FROM signal_log WHERE symbol=? ORDER BY id DESC LIMIT ?",
            (symbol, limit),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT symbol, signal_type, direction, details, ssm_score, created_at "
            "FROM signal_log ORDER BY id DESC LIMIT ?",
            (limit,),
        ).fetchall()
    conn.close()

    signals = []
    for r in rows:
        detail = None
        try:
            detail = json.loads(r[3]) if r[3] else None
        except:
            detail = r[3]
        signals.append({
            "symbol": r[0], "type": r[1], "direction": r[2],
            "details": detail, "score": r[4], "time": r[5],
        })
    return signals


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--symbol", type=str, default=None)
    args = parser.parse_args()
    data = get_signals(args.limit, args.symbol)
    print(json.dumps(data, ensure_ascii=False, indent=2, default=str))
