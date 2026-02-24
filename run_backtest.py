"""백테스트 CLI 엔트리포인트

Usage:
    python run_backtest.py                    # 90일 전체 백테스트
    python run_backtest.py --days 7           # 7일 빠른 테스트
    python run_backtest.py --download-only    # 데이터 다운로드만
    python run_backtest.py --symbol ETHUSDT   # 특정 심볼
    python run_backtest.py --csv              # CSV 리포트 내보내기
"""
import sys
import os
import argparse
import time

# 프로젝트 루트를 sys.path에 추가
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description="Trading System Backtester")
    parser.add_argument("--days", type=int, default=90,
                        help="백테스트 기간 (일, 기본: 90)")
    parser.add_argument("--symbol", type=str, default=None,
                        help="심볼 (기본: BTCUSDT)")
    parser.add_argument("--download-only", action="store_true",
                        help="데이터 다운로드만 실행")
    parser.add_argument("--skip-download", action="store_true",
                        help="다운로드 건너뛰기 (기존 데이터 사용)")
    parser.add_argument("--csv", action="store_true",
                        help="CSV 리포트 내보내기")
    args = parser.parse_args()

    from backtest.config_bt import BT_SYMBOLS, BT_DB_PATH
    symbols = [args.symbol] if args.symbol else BT_SYMBOLS

    print(f"{'='*60}")
    print(f"  BACKTEST ENGINE v1.0")
    print(f"  Period: {args.days} days | Symbols: {', '.join(symbols)}")
    print(f"  DB: {BT_DB_PATH}")
    print(f"{'='*60}")

    # Step 1: DB 초기화 + 데이터 다운로드
    if not args.skip_download:
        print("\n[1/4] DB 초기화...")
        from backtest.db_bt import init_backtest_db
        init_backtest_db()

        print("\n[2/4] 히스토리 데이터 다운로드...")
        from backtest.downloader import download_all, generate_synthetic_liquidations

        # 다운로더에서 사용할 심볼 임시 설정
        import backtest.config_bt as bt_config
        original_symbols = bt_config.BT_SYMBOLS
        bt_config.BT_SYMBOLS = symbols

        download_start = time.time()
        download_all(days=args.days)

        print("\n[2.5/4] 합성 청산 데이터 생성...")
        generate_synthetic_liquidations(symbols)

        bt_config.BT_SYMBOLS = original_symbols

        download_elapsed = time.time() - download_start
        print(f"\n[Download] 완료 ({download_elapsed:.1f}초)")

        if args.download_only:
            print("\n--download-only 모드: 다운로드 완료. 백테스트 스킵.")
            _print_db_stats()
            return
    else:
        print("\n[1-2/4] 다운로드 스킵 (기존 데이터 사용)")
        if not BT_DB_PATH.exists():
            print("[ERROR] backtest.db가 없습니다. --skip-download 없이 다시 실행하세요.")
            return

    # Step 2: 백테스트 실행
    print("\n[3/4] 백테스트 실행...")
    from backtest.runner import run_backtest
    results = run_backtest(days=args.days, symbols=symbols)

    # Step 3: 리포트 생성
    print("\n[4/4] 리포트 생성...")
    from backtest.report import generate_report

    end_ts = time.time()
    start_ts = end_ts - (args.days * 86400)
    report = generate_report(
        symbols=symbols,
        start_ts=start_ts,
        end_ts=end_ts,
        equity_data=results,
        export_csv=args.csv,
    )

    _print_db_stats()


def _print_db_stats():
    """DB 테이블별 레코드 수 출력"""
    import sqlite3
    from backtest.config_bt import BT_DB_PATH

    if not BT_DB_PATH.exists():
        return

    conn = sqlite3.connect(str(BT_DB_PATH))
    tables = [
        "klines", "oi_snapshots", "funding_rates", "long_short_ratios",
        "taker_ratio", "fear_greed", "liquidations",
        "atr_values", "threshold_signals", "grid_configs", "ssm_scores",
        "strategy_state", "signal_log",
        "paper_trades", "paper_l1_funding", "paper_l4_grid", "paper_summary",
    ]

    print(f"\n{'='*40}")
    print(f"  DB Stats: backtest.db")
    print(f"{'='*40}")

    for table in tables:
        try:
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            category = "DATA" if table in ("klines", "oi_snapshots", "funding_rates",
                                            "long_short_ratios", "taker_ratio",
                                            "fear_greed", "liquidations") else "ENGINE"
            print(f"  [{category}] {table:.<30} {count:>8,}")
        except Exception:
            pass

    conn.close()
    print(f"{'='*40}")


if __name__ == "__main__":
    main()
