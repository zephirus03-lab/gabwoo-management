"""
갑우그룹 통합 진단 v1 — Evidence v0(손익·원인분류) × 건전도 프로파일(신뢰도·운영신호등)

두 분석의 보완 관계:
  - Evidence v0: 손익 추정(용지원가 실측 + 간접비 배분), 3년 단가추세, 원인 3분류 → 경영 의사결정
  - 건전도 프로파일: ERP/세금계산서 신뢰도, 취소·미처리·할인·월변동·공백 신호등 → 운영 품질

v1 통합 로직:
  1. 건전도 프로파일을 base (1억+ 46개 + 부실 + 소규모 + 지입)
  2. Evidence 시트 3/5/6/7의 마진·추세·원인 정보를 조인
  3. '조치 카테고리' 재분류: 🔴🔴 즉시 / 🔴 소명 / 🟡 강화 / 🟢 유지 / ⚫ 조사 / ⚪ 해석주의
  4. 통합 위험 점수(0~10, 높을수록 악화) 산출

라벨: 초안(ERP 역산 + 원가 추정) — 영업자 확인 전
"""

from __future__ import annotations

import sys
import re
from datetime import datetime
from pathlib import Path

try:
    import numpy as np
    import pandas as pd
except ImportError as e:
    print(f"❌ pip3 install pandas numpy openpyxl xlrd: {e}")
    sys.exit(1)


# ───────── 경로 ─────────
_HEALTH_CANDIDATES = sorted(
    Path("/Users/jack/dev/gabwoo/관리 대시보드/scripts/output").glob("거래처_건전도_2025_*.xlsx")
)
if not _HEALTH_CANDIDATES:
    raise FileNotFoundError("거래처_건전도_2025_*.xlsx 파일이 없습니다. 먼저 build_customer_health_2025.py를 실행해주세요.")
MY_XLSX = _HEALTH_CANDIDATES[-1]  # 가장 최신 날짜 자동 선택
EVID_XLSX = Path("/Users/jack/Downloads/갑우그룹_경영증거패키지_v0_통합.xlsx")
OUT_DIR = Path(__file__).parent / "output"
OUT_XLSX = OUT_DIR / f"갑우그룹_통합진단_v1_{datetime.now():%Y%m%d}.xlsx"


# ───────── 거래처명 정규화 (매칭용) ─────────
_SUFFIX_RE = re.compile(r"(\(주\)|\(재\)|\(사\)|주식회사|유한회사|\s+)")


def norm_name(s) -> str:
    if pd.isna(s):
        return ""
    return _SUFFIX_RE.sub("", str(s).strip()).upper()


# ───────── 1. 건전도 프로파일 로드 (내 분석) ─────────
def load_my_profile() -> pd.DataFrame:
    """내 건전도 프로파일 — 시트 2(부실) + 3(평가가능) + 4(지입) + 5(소규모) 통합."""
    frames = []
    for sh, cat in [("2_신뢰도부실_판단보류", "부실"), ("3_전체_평가가능", "평가가능"),
                    ("4_지입거래처_별도", "지입"), ("5_소규모_1억미만", "소규모")]:
        df = pd.read_excel(MY_XLSX, sheet_name=sh)
        df["원천시트"] = cat
        frames.append(df)
    all_df = pd.concat(frames, ignore_index=True)
    # NO_BIZ 기준 중복 제거 (지입은 3_전체와 겹치므로 우선순위: 평가가능 > 부실 > 지입 > 소규모)
    pri = {"평가가능": 0, "부실": 1, "지입": 2, "소규모": 3}
    all_df["_pri"] = all_df["원천시트"].map(pri)
    all_df = all_df.sort_values("_pri").drop_duplicates(subset=["NO_BIZ", "소속사"], keep="first")
    all_df = all_df.drop(columns=["_pri"])
    return all_df.reset_index(drop=True)


# ───────── 2. Evidence 시트 파싱 ─────────
def parse_evidence_margin() -> pd.DataFrame:
    """시트 3: 손익 랭킹 5억+ 15개사 → 거래처명 기반 매핑."""
    df = pd.read_excel(EVID_XLSX, sheet_name="3_손익랭킹_5억이상15개사", header=3)
    df = df[df["순위"].apply(lambda x: str(x).isdigit() if pd.notna(x) else False)]
    # 필요한 컬럼
    keep = ["순위", "회사", "거래처", "매출", "용지원가", "용지원가율", "간접비", "간접비율",
            "총원가(추정)", "추정마진", "추정마진율", "용지원가 신뢰도", "분류", "특이사항"]
    df = df[[c for c in keep if c in df.columns]]
    df["매칭키"] = df["거래처"].apply(norm_name)
    df = df.rename(columns={
        "매출": "EvidM_매출", "용지원가": "EvidM_용지원가", "용지원가율": "EvidM_용지원가율",
        "간접비": "EvidM_간접비", "총원가(추정)": "EvidM_총원가추정",
        "추정마진": "EvidM_추정마진", "추정마진율": "EvidM_추정마진율",
        "용지원가 신뢰도": "EvidM_용지신뢰도", "분류": "EvidM_손익분류", "특이사항": "EvidM_특이사항",
    })
    return df


def parse_evidence_trend() -> pd.DataFrame:
    """시트 5/6/7: 단가 추세 분석 — 거래처코드(V1389 등) 기반."""
    frames = []
    # 5_계속하락
    df5 = pd.read_excel(EVID_XLSX, sheet_name="5_계속하락_9개사", header=3)
    df5["원가반영"] = "🔴 역행(계속하락)"
    frames.append(df5)
    # 6_역행 단가인하
    df6 = pd.read_excel(EVID_XLSX, sheet_name="6_역행_단가인하_23개사", header=3)
    df6["원가반영"] = "🔴 역행(단가인하)"
    frames.append(df6)
    # 7_일부반영(상단) + 반영충분(하단)
    df7a = pd.read_excel(EVID_XLSX, sheet_name="7_일부+반영충분_28개사", header=4, nrows=8)
    df7a["원가반영"] = "🟡 일부반영"
    frames.append(df7a)
    df7b = pd.read_excel(EVID_XLSX, sheet_name="7_일부+반영충분_28개사", header=15, nrows=20)
    df7b["원가반영"] = "✅ 반영충분"
    frames.append(df7b)

    combined = pd.concat(frames, ignore_index=True)
    # 거래처코드 결측 행 제거
    combined = combined[combined["거래처코드"].notna() & (combined["거래처코드"].astype(str).str.strip() != "거래처코드")]
    combined = combined[combined["거래처코드"].astype(str).str.strip() != ""]
    # 역행과 계속하락 중복 → 계속하락 우선
    trend_priority = {"🔴 역행(계속하락)": 0, "🔴 역행(단가인하)": 1, "🟡 일부반영": 2, "✅ 반영충분": 3}
    combined["_tp"] = combined["원가반영"].map(trend_priority)
    combined = combined.sort_values("_tp").drop_duplicates(subset=["거래처코드"], keep="first")
    combined = combined.drop(columns=["_tp"])
    keep = ["거래처코드", "거래처명", "매출23(억)", "매출24(억)", "매출25(억)",
            "단가Δ 23→25(%)", "단가추세", "원가반영", "원인주석"]
    combined = combined[[c for c in keep if c in combined.columns]]
    combined.columns = ["EvidT_CD_CUST", "EvidT_거래처명", "매출23_억", "매출24_억", "매출25_억",
                         "단가변화_23_25", "EvidT_단가추세", "EvidT_원가반영", "EvidT_원인주석"]
    # 매칭키(거래처명 정규화)
    combined["매칭키"] = combined["EvidT_거래처명"].apply(norm_name)
    return combined.reset_index(drop=True)


# ───────── 3. 통합 + 조치 카테고리 ─────────
# Evidence 원인 3분류 매핑 (본부장 확인 대상에서 수작업 추출)
MARKET_COMPETITION = {"V1389": "한국조폐공사 — 시장·경쟁 (담당자 소명 대상 아님)"}
EXCLUDED_BY_TRANSFER = {"V00749": "지에스리테일 — 프린트뱅크 이관 (분석 제외)"}
RELATED_PARTY_NAMES = ["갑우문화사", "비피앤피", "더원프린팅", "프린트뱅크"]
JIIP_FLAGS = ["교원", "이투스", "에듀윌", "동행복권"]   # 용지사급(=아방)


def classify_action(row) -> str:
    """통합 조치 카테고리. 우선순위: 적자 > 해석주의(시장·관계사·지입) > 조사필요 > 소명 > 강화 > 유지."""
    nm = str(row.get("거래처명", "")) if pd.notna(row.get("거래처명")) else ""
    m = row.get("EvidM_추정마진율")

    # 🔴🔴 적자는 최우선 (지입·관계사라도 잡아야 함)
    if pd.notna(m) and m < 0:
        return "🔴🔴 즉시조치(적자)"

    # ⚪ 해석주의 — 한국조폐공사·지에스리테일·관계사·지입 (판단 보류)
    if "한국조폐공사" in nm:
        return "⚪ 해석주의(시장·경쟁)"
    if "지에스리테일" in nm:
        return "⚪ 해석주의(작업이관)"
    if any(k in nm for k in RELATED_PARTY_NAMES):
        return "⚪ 해석주의(관계사)"
    # 지입 플래그 — 거래처명에 키워드가 있고 & Evidence 반영충분으로 분류된 경우만
    jiip_val = row.get("지입여부")
    has_jiip = (pd.notna(jiip_val) and str(jiip_val).strip() not in ("", "nan"))
    # 이름 매칭은 단어 경계를 살짝 고려 (교원만 있는 관련 거래처 모두 포함)
    name_jiip = any(k in nm for k in JIIP_FLAGS)
    if has_jiip or (name_jiip and str(row.get("EvidT_원가반영", "")) == "✅ 반영충분"):
        return "⚪ 해석주의(지입·용지사급)"

    # ⚫ 조사필요
    rel = str(row.get("데이터신뢰도", ""))
    if "부실" in rel or "세금無" in rel:
        return "⚫ 조사필요(데이터 부실)"

    # 🔴🔴 적자
    if pd.notna(row.get("EvidM_추정마진율")) and row["EvidM_추정마진율"] < 0:
        return "🔴🔴 즉시조치(적자)"

    # 🔴 계속하락 or 역행
    trend_label = str(row.get("EvidT_원가반영", ""))
    trend_arrow = str(row.get("EvidT_단가추세", ""))
    if trend_label.startswith("🔴") or trend_arrow.startswith("🔴🔴"):
        return "🔴 소명대상(단가하락·역행)"

    # 🟡 관리강화
    score = row.get("건전도평균")
    if pd.notna(score) and score <= 1.0:
        return "🟡 관리강화(운영지표 나쁨)"

    # 🟢 유지·확대
    if trend_label == "✅ 반영충분" and pd.notna(score) and score >= 1.5:
        return "🟢 유지·확대"

    return "· 기본(추가정보 부족)"


def integrated_risk_score(row) -> float:
    """0~10 (높을수록 위험). 마진·추세·운영·데이터 가중합."""
    # 마진 점수 (0~3)
    m = row.get("EvidM_추정마진율")
    if pd.isna(m):
        m_score = 1.0  # 정보없음 중립 (다만 불이익 없게 작게)
    elif m < 0:
        m_score = 3.0
    elif m < 0.1:
        m_score = 2.0
    elif m < 0.2:
        m_score = 1.0
    else:
        m_score = 0.0

    # 추세 점수 (0~3)
    trend = str(row.get("EvidT_원가반영", ""))
    arrow = str(row.get("EvidT_단가추세", ""))
    if arrow.startswith("🔴🔴"):
        t_score = 3.0
    elif trend == "🔴 역행(단가인하)":
        t_score = 2.0
    elif trend == "🟡 일부반영":
        t_score = 1.0
    elif trend == "✅ 반영충분":
        t_score = 0.0
    else:
        t_score = 1.0  # 정보없음

    # 운영 점수 (0~2) — 내 건전도 평균을 뒤집어서 점수화
    s = row.get("건전도평균")
    if pd.isna(s):
        o_score = 1.0
    else:
        # 평균 0(최악) ~ 2(최고) → 역 스케일
        o_score = max(0.0, min(2.0, 2.0 - s))

    # 데이터 품질 (0~2)
    rel = str(row.get("데이터신뢰도", ""))
    if "부실" in rel:
        d_score = 2.0
    elif "세금無" in rel:
        d_score = 1.5
    elif "주의" in rel:
        d_score = 0.5
    elif "신뢰" in rel:
        d_score = 0.0
    else:
        d_score = 0.5

    return round(m_score + t_score + o_score + d_score, 2)


def build_integrated():
    print("📥 건전도 프로파일 로드...")
    my = load_my_profile()
    print(f"   → {len(my):,}건")

    print("📥 Evidence 시트 3 (손익) 파싱...")
    em = parse_evidence_margin()
    print(f"   → {len(em):,}건 (5억+ 15개사)")

    print("📥 Evidence 시트 5/6/7 (단가추세 + 원가반영) 파싱...")
    et = parse_evidence_trend()
    print(f"   → {len(et):,}건")

    # 매칭키 준비
    my["매칭키"] = my["거래처명"].apply(norm_name)

    # 시트 3 (손익) 매칭: 거래처명 기반
    my2 = my.merge(em.drop(columns=["순위", "회사", "거래처"]), on="매칭키", how="left")
    # 시트 5/6/7 (추세) 매칭: 거래처명 기반 (CD_CUST는 내 시트에 노출 안 되어 있음)
    my2 = my2.merge(et.drop(columns=["EvidT_거래처명"]), on="매칭키", how="left")

    # 조치·점수
    my2["조치카테고리"] = my2.apply(classify_action, axis=1)
    my2["통합위험점수"] = my2.apply(integrated_risk_score, axis=1)

    return my2


# ───────── 4. 시트 출력 ─────────
VIEW_COLS = [
    "소속사", "NO_BIZ", "거래처명", "EvidT_CD_CUST",
    "조치카테고리", "통합위험점수",
    "데이터신뢰도", "ERP매출(억)", "세금매출(억)", "갭(억)",
    "EvidM_추정마진율", "EvidM_용지원가율", "EvidM_손익분류", "EvidM_특이사항",
    "EvidT_원가반영", "EvidT_단가추세", "EvidT_원인주석",
    "매출23_억", "매출24_억", "매출25_억", "단가변화_23_25",
    "건전도평균",
    "신호_YoY", "신호_취소율", "신호_미처리율", "신호_할인율",
    "신호_월변동", "신호_공백", "신호_승인누락", "신호_단가추세",
    "취소율_pct", "미처리율_pct", "평균할인율", "거래공백일",
    "지입여부",
]


def format_master(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    for col in VIEW_COLS:
        if col not in d.columns:
            d[col] = pd.NA
    # 백분율 포맷
    if "EvidM_추정마진율" in d.columns:
        d["EvidM_추정마진율"] = pd.to_numeric(d["EvidM_추정마진율"], errors="coerce").round(3)
    if "EvidM_용지원가율" in d.columns:
        d["EvidM_용지원가율"] = pd.to_numeric(d["EvidM_용지원가율"], errors="coerce").round(3)
    return d[VIEW_COLS]


def main():
    data = build_integrated()

    # 카테고리 분류
    master = format_master(data.sort_values("통합위험점수", ascending=False))

    immediate = master[master["조치카테고리"].str.startswith("🔴🔴", na=False)]
    somyung = master[master["조치카테고리"].str.startswith("🔴 소명", na=False)]
    enforce = master[master["조치카테고리"].str.startswith("🟡", na=False)]
    investigate = master[master["조치카테고리"].str.startswith("⚫", na=False)]
    caution = master[master["조치카테고리"].str.startswith("⚪", na=False)]
    maintain = master[master["조치카테고리"].str.startswith("🟢", na=False)]
    others = master[master["조치카테고리"].str.startswith("·", na=False)]

    # 경영진 1페이지 (요약 블록)
    exec_summary = pd.DataFrame({
        "카테고리": ["🔴🔴 즉시조치(적자)", "🔴 소명대상(단가하락·역행)", "🟡 관리강화(운영나쁨)",
                      "⚫ 조사필요(데이터 부실)", "⚪ 해석주의(관계사·지입·시장)",
                      "🟢 유지·확대", "· 기본"],
        "거래처수": [len(immediate), len(somyung), len(enforce), len(investigate),
                    len(caution), len(maintain), len(others)],
        "합계 매출(억)": [
            round(immediate["세금매출(억)"].fillna(immediate["ERP매출(억)"]).sum(), 1),
            round(somyung["세금매출(억)"].fillna(somyung["ERP매출(억)"]).sum(), 1),
            round(enforce["세금매출(억)"].fillna(enforce["ERP매출(억)"]).sum(), 1),
            round(investigate["세금매출(억)"].fillna(investigate["ERP매출(억)"]).sum(), 1),
            round(caution["세금매출(억)"].fillna(caution["ERP매출(억)"]).sum(), 1),
            round(maintain["세금매출(억)"].fillna(maintain["ERP매출(억)"]).sum(), 1),
            round(others["세금매출(억)"].fillna(others["ERP매출(억)"]).sum(), 1),
        ],
        "조치": [
            "단가 재협상 or 거래 축소 (데드라인 2026-05-31)",
            "담당 영업자 1:1 소명 → 단가 재협상",
            "본부장 월 1회 점검 + 영업자 면담",
            "ERP 이중등록·관계사 미등록 조사",
            "판단 보류 · 원인별 별도 트랙",
            "본부장 월 1회 관리, 확대 전략",
            "추가 데이터 수집 필요",
        ],
    })

    # 지표 출처 매트릭스
    sources = pd.DataFrame({
        "지표": ["EvidM_추정마진율", "EvidM_용지원가율", "EvidT_원가반영", "EvidT_단가추세",
                "매출23/24/25_억", "데이터신뢰도", "신호_YoY~신호_단가추세", "건전도평균",
                "조치카테고리", "통합위험점수"],
        "출처": [
            "Evidence v0 시트 3 (용지비 실측 + 간접비 매출비율 배분)",
            "Evidence v0 시트 3 (viewGabwoo_마감 용지 매입)",
            "Evidence v0 시트 6·7 (용지원가 +22.6% 벤치마크 대비)",
            "Evidence v0 시트 5·6·7 (23→24→25 평균단가 변화 패턴)",
            "Evidence v0 시트 5·6·7 (SAL_SALESH 합)",
            "건전도 프로파일 (ERP/세금계산서 비율)",
            "건전도 프로파일 (SAL_SALESH/L 운영 지표)",
            "건전도 프로파일 (7개 신호등 평균)",
            "v1 통합 로직 (본 스크립트)",
            "v1 통합 로직 (마진·추세·운영·데이터 가중합)",
        ],
        "한계": [
            "5억+ 15개사만 산출. 나머지는 Evidence 시트 5·6·7의 '역행/반영충분' 분류로만",
            "동일",
            "벤치마크 +22.6%는 2년 누적 실측, 연도별은 가정치",
            "23→24→25 3년 추세만. 월별 패턴은 미반영",
            "ERP 기준. 세금계산서·감사와 갭 있음 (소속사별)",
            "세금계산서는 25년 한 해만. 24·23년 추가 시 정확도 상승",
            "할인율·취소·미처리는 세금계산서 검증 불가 (간접 판단)",
            "7개 신호등 단순 평균 — 가중치 없음",
            "수작업 매핑 (시장·경쟁 / 작업이관 / 관계사). 사업자번호 기반 플래그 미완성",
            "가중치는 초안 (마진 3 + 추세 3 + 운영 2 + 데이터 2 = 최대 10)",
        ],
    })

    # 한계·주의 시트
    notes = pd.DataFrame({
        "항목": ["범위", "기간", "매출 기준", "원가 기준", "매칭 방법",
                "24·23년 세금계산서", "관계사 처리", "지입 처리", "승인 전 사용 금지"],
        "내용": [
            "갑우문화사·비피앤피·더원프린팅 3사 전체 (ERP SAL_SALESH + 세금계산서 xls 거래처 합집합)",
            "2025-01-01 ~ 2025-12-31 (추세 컬럼만 2023~2025 3년)",
            "ERP: ST_SALES='Y' OR NULL / 세금: 제품매출현황 xls '대변' 합",
            "용지: viewGabwoo_마감 실측 / 간접비: 감사보고서 매출비율 배분 (5억+ 15개사만)",
            "거래처코드(V...) = CD_CUST ↔ 사업자번호 정규화 ↔ 거래처명 정규화(공백·(주) 제거)",
            "아직 미수령 (2026-04-16 기준). 도착 시 YoY·단가추세 교차검증 예정",
            "갑우·비피·더원·프린트뱅크 간 내부거래는 '⚪ 해석주의(관계사)'로 분류",
            "교원·이투스·에듀윌 등 용지사급 → '⚪ 해석주의(지입)'",
            "영업자 확인 전이므로 단독 의사결정 금지. 해당 거래처 담당자 소명 후 확정",
        ],
    })

    with pd.ExcelWriter(OUT_XLSX, engine="openpyxl") as w:
        exec_summary.to_excel(w, sheet_name="0_경영진_1페이지", index=False)
        master.to_excel(w, sheet_name="1_통합마스터", index=False)
        immediate.to_excel(w, sheet_name="2_🔴🔴_즉시조치", index=False)
        somyung.to_excel(w, sheet_name="3_🔴_소명대상", index=False)
        enforce.to_excel(w, sheet_name="4_🟡_관리강화", index=False)
        investigate.to_excel(w, sheet_name="5_⚫_조사필요", index=False)
        caution.to_excel(w, sheet_name="6_⚪_해석주의", index=False)
        maintain.to_excel(w, sheet_name="7_🟢_유지확대", index=False)
        others.to_excel(w, sheet_name="8_기본_정보부족", index=False)
        sources.to_excel(w, sheet_name="9_지표_출처_매트릭스", index=False)
        notes.to_excel(w, sheet_name="10_한계·주의", index=False)

    print(f"\n📤 {OUT_XLSX}")
    print("=" * 100)
    print("▼ 경영진 1페이지 요약")
    print("=" * 100)
    print(exec_summary.to_string(index=False))

    if len(immediate):
        print("\n" + "=" * 100)
        print("▼ 🔴🔴 즉시조치 (적자)")
        print("=" * 100)
        cols = ["소속사", "거래처명", "통합위험점수", "EvidM_추정마진율",
                "세금매출(억)", "EvidM_용지원가율", "데이터신뢰도"]
        print(immediate[cols].to_string(index=False))

    if len(somyung):
        print("\n" + "=" * 100)
        print("▼ 🔴 소명대상 Top 10")
        print("=" * 100)
        cols = ["소속사", "거래처명", "통합위험점수", "EvidT_원가반영", "EvidT_단가추세",
                "매출25_억", "건전도평균"]
        print(somyung[cols].head(10).to_string(index=False))


if __name__ == "__main__":
    main()
