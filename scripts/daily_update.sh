#!/bin/bash
# 매일 아침 ERP → Supabase 자동 업데이트 스크립트입니다.
# launchd에서 호출됩니다.

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_FILE="$SCRIPT_DIR/output/daily_update.log"
PYTHON="/Library/Developer/CommandLineTools/Library/Frameworks/Python3.framework/Versions/3.9/bin/python3"

echo "===== $(date '+%Y-%m-%d %H:%M:%S') 시작 =====" >> "$LOG_FILE"

# 1. ERP DB 조회 → pricing_audit.json 생성 (영업자 대시보드용)
echo "[1/3] ERP 분석 실행 (영업자 대시보드)..." >> "$LOG_FILE"
$PYTHON "$SCRIPT_DIR/reverse_engineer_pricing.py" >> "$LOG_FILE" 2>&1
STEP1=$?

if [ $STEP1 -ne 0 ]; then
    echo "❌ ERP 분석 실패 (exit $STEP1) — 사내망 연결을 확인하세요." >> "$LOG_FILE"
    echo "===== $(date '+%Y-%m-%d %H:%M:%S') 실패 =====" >> "$LOG_FILE"
    exit 1
fi

# 2. pricing_audit JSON 파일들 Supabase Storage 업로드 (영업자 대시보드용)
echo "[2/3] Supabase Storage 업로드 (영업자 대시보드)..." >> "$LOG_FILE"
$PYTHON "$SCRIPT_DIR/upload_to_supabase.py" >> "$LOG_FILE" 2>&1
STEP2=$?

if [ $STEP2 -ne 0 ]; then
    echo "❌ Supabase Storage 업로드 실패 (exit $STEP2)" >> "$LOG_FILE"
    echo "===== $(date '+%Y-%m-%d %H:%M:%S') 실패 =====" >> "$LOG_FILE"
    exit 1
fi

# 3. ERP → Supabase DB 동기화 (경영 대시보드용 erp_quotes/erp_quote_lines + erp_sales)
echo "[3/4] Supabase DB 동기화 (경영 대시보드 — 견적 + 매출)..." >> "$LOG_FILE"
$PYTHON "$SCRIPT_DIR/sync_erp_to_supabase.py" >> "$LOG_FILE" 2>&1
STEP3=$?

if [ $STEP3 -ne 0 ]; then
    echo "❌ Supabase DB 동기화 실패 (exit $STEP3)" >> "$LOG_FILE"
    echo "===== $(date '+%Y-%m-%d %H:%M:%S') 실패 =====" >> "$LOG_FILE"
    exit 1
fi

# 4. AI YoY 인사이트 생성 (매출 동기화 이후 실행)
echo "[4/4] AI 인사이트 생성 (YoY 분석)..." >> "$LOG_FILE"
$PYTHON "$SCRIPT_DIR/generate_insights.py" >> "$LOG_FILE" 2>&1
STEP4=$?

if [ $STEP4 -ne 0 ]; then
    echo "⚠️ 인사이트 생성 실패 (exit $STEP4) — 대시보드는 계속 동작합니다." >> "$LOG_FILE"
fi

echo "✅ 완료" >> "$LOG_FILE"
echo "===== $(date '+%Y-%m-%d %H:%M:%S') 종료 =====" >> "$LOG_FILE"
echo "" >> "$LOG_FILE"
