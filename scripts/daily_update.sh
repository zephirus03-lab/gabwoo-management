#!/bin/bash
# 매일 아침 ERP → Supabase 자동 업데이트 스크립트입니다.
# launchd에서 호출됩니다.
#
# v1 개선 (2026-04-15, CTO 리뷰 TOP-1):
# - macOS 알림: 실패 시 화면 우상단에 즉시 팝업 (로그 안 열어봐도 인지)
# - step [2/6] 추가: 매출 집중도/키맨 리스크 집계 (영업자 대시보드 상단 경보)
# - step [5/6] 추가: 용지 매입 동기화 (KPI-6)
# - 각 step 종료 결과는 Supabase sync_log 테이블에도 기록되어
#   대시보드 상단 "데이터 기준 시각" 배너에 표시됩니다.

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_FILE="$SCRIPT_DIR/output/daily_update.log"
PYTHON="/Library/Developer/CommandLineTools/Library/Frameworks/Python3.framework/Versions/3.9/bin/python3"

# 실패 시 macOS 우상단에 알림 띄우는 헬퍼.
notify_fail() {
    local step_name="$1"
    local exit_code="$2"
    osascript -e "display notification \"$step_name 실패 (exit $exit_code). 로그 확인 필요.\" with title \"GMD 배치 실패 ⚠️\" sound name \"Basso\"" 2>/dev/null || true
}

notify_success() {
    osascript -e "display notification \"ERP → Supabase 동기화 완료\" with title \"GMD 배치 성공 ✅\"" 2>/dev/null || true
}

echo "===== $(date '+%Y-%m-%d %H:%M:%S') 시작 =====" >> "$LOG_FILE"

# 1. ERP DB 조회 → pricing_audit.json 생성 (영업자 대시보드용)
echo "[1/6] ERP 분석 실행 (영업자 대시보드)..." >> "$LOG_FILE"
$PYTHON "$SCRIPT_DIR/reverse_engineer_pricing.py" >> "$LOG_FILE" 2>&1
STEP1=$?

if [ $STEP1 -ne 0 ]; then
    echo "❌ ERP 분석 실패 (exit $STEP1) — 사내망 연결을 확인하세요." >> "$LOG_FILE"
    echo "===== $(date '+%Y-%m-%d %H:%M:%S') 실패 =====" >> "$LOG_FILE"
    notify_fail "[1/6] ERP 분석" "$STEP1"
    exit 1
fi

# 1.5. 매출 집중도 + 키맨 리스크 집계 (영업자 대시보드 상단 경보용)
echo "[2/6] 매출 집중도/키맨 리스크 집계..." >> "$LOG_FILE"
$PYTHON "$SCRIPT_DIR/compute_sales_concentration.py" >> "$LOG_FILE" 2>&1
STEP_CONC=$?
if [ $STEP_CONC -ne 0 ]; then
    echo "⚠️ 집중도 집계 실패 (exit $STEP_CONC) — 이 단계 실패해도 파이프라인은 계속됨" >> "$LOG_FILE"
    # 비치명: 기존 JSON 유지한 채로 업로드 진행
fi

# 2. pricing_audit JSON + sales_concentration JSON 파일들 Supabase Storage 업로드
echo "[3/6] Supabase Storage 업로드 (영업자 대시보드)..." >> "$LOG_FILE"
$PYTHON "$SCRIPT_DIR/upload_to_supabase.py" >> "$LOG_FILE" 2>&1
STEP2=$?

if [ $STEP2 -ne 0 ]; then
    echo "❌ Supabase Storage 업로드 실패 (exit $STEP2)" >> "$LOG_FILE"
    echo "===== $(date '+%Y-%m-%d %H:%M:%S') 실패 =====" >> "$LOG_FILE"
    notify_fail "[3/6] Supabase Storage 업로드" "$STEP2"
    exit 1
fi

# 3. ERP → Supabase DB 동기화 (견적 + 매출)
echo "[4/6] Supabase DB 동기화 (경영 대시보드 — 견적 + 매출)..." >> "$LOG_FILE"
$PYTHON "$SCRIPT_DIR/sync_erp_to_supabase.py" >> "$LOG_FILE" 2>&1
STEP3=$?

if [ $STEP3 -ne 0 ]; then
    echo "❌ Supabase DB 동기화 실패 (exit $STEP3)" >> "$LOG_FILE"
    echo "===== $(date '+%Y-%m-%d %H:%M:%S') 실패 =====" >> "$LOG_FILE"
    notify_fail "[4/6] Supabase DB 동기화 (견적+매출)" "$STEP3"
    exit 1
fi

# 4. 용지 매입 동기화 (KPI-6, erp_paper_purchases 테이블)
echo "[5/6] Supabase DB 동기화 (용지 매입)..." >> "$LOG_FILE"
$PYTHON "$SCRIPT_DIR/sync_paper_purchases.py" >> "$LOG_FILE" 2>&1
STEP4=$?

if [ $STEP4 -ne 0 ]; then
    echo "❌ 용지 매입 동기화 실패 (exit $STEP4)" >> "$LOG_FILE"
    echo "===== $(date '+%Y-%m-%d %H:%M:%S') 실패 =====" >> "$LOG_FILE"
    notify_fail "[5/6] 용지 매입 동기화" "$STEP4"
    exit 1
fi

# 5. 용지원가 vs 판매단가 월별 추세 동기화 (paper_vs_sales_monthly)
echo "[6/7] 용지원가 vs 판매단가 월별 추세 동기화..." >> "$LOG_FILE"
$PYTHON "$SCRIPT_DIR/sync_paper_vs_sales.py" >> "$LOG_FILE" 2>&1
STEP_PVS=$?
if [ $STEP_PVS -ne 0 ]; then
    echo "⚠️ 용지원가-판매단가 동기화 실패 (exit $STEP_PVS) — 대시보드 Feature 1만 비어 보일 수 있음" >> "$LOG_FILE"
    # 비치명: 대시보드는 계속 동작
fi

# 6. AI YoY 인사이트 생성 (매출 동기화 이후 실행, 실패해도 대시보드는 동작)
echo "[7/7] AI 인사이트 생성 (YoY 분석)..." >> "$LOG_FILE"
$PYTHON "$SCRIPT_DIR/generate_insights.py" >> "$LOG_FILE" 2>&1
STEP5=$?

if [ $STEP5 -ne 0 ]; then
    echo "⚠️ 인사이트 생성 실패 (exit $STEP5) — 대시보드는 계속 동작합니다." >> "$LOG_FILE"
    # 인사이트는 선택 기능이라 notify_fail 호출 안 함
fi

echo "✅ 완료" >> "$LOG_FILE"
echo "===== $(date '+%Y-%m-%d %H:%M:%S') 종료 =====" >> "$LOG_FILE"
echo "" >> "$LOG_FILE"
notify_success
