@echo off
chcp 65001 >nul
echo ============================================
echo   LIVE TRADING AUTO-START
echo   SOL L4 Grid - Binance Futures Mainnet
echo ============================================
echo.

:: 기존 python 프로세스 종료
echo [1/4] 기존 프로세스 정리...
taskkill /F /IM python.exe >nul 2>&1
timeout /t 3 /nobreak >nul

:: 환경변수 설정
echo [2/4] 환경변수 설정...
set LIVE_TRADING_ENABLED=true
set LIVE_USE_TESTNET=false
set PYTHONUNBUFFERED=1

:: 작업 디렉토리 이동
echo [3/4] 시스템 시작 중...
cd /d C:\Users\lungg\.openclaw\workspace\money

:: 실행
echo [4/4] LIVE TRADING START!
echo.
python -u main.py
