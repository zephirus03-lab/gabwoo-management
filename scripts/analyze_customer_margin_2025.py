"""
2025년 거래처별 마진 Top 30 (낮은 순) — B1 방식 (갑우 출판사업부 한정).

⚠️ 범위 제한 (2026-04-16 조정):
  PRT_ESTL/ESTH는 갑우(10000)·비피(20000)·패키지 데이터가 섞여 있고
  표준 매입단가표(standard_pricing.json)는 출판사업부 기준이므로
  3사 전체에 그대로 적용하면 서로 다른 공정을 비교하게 됩니다.
  따라서 본 분석은 **CD_CUST_OWN='10000'(갑우) AND CD_ITEM이 '0' prefix인
  출판 항목코드**로 범위를 좁힙니다.

로직:
  1. PRT_ESTL + PRT_ESTH 2025년 갑우(10000) 견적 라인 로드
  2. 출판 항목(CTP/인쇄판/인쇄대 계열)만 표준 매입단가로 라인원가 산출
  3. 라인매출(AM) - 라인원가 = 라인마진
  4. 거래처로 집계
  5. SAL_SALESH 2025 갑우 실매출을 거래처별로 교차 참조
  6. 매출 1억+ AND 원가매칭 커버리지 ≥ 30% 필터
  7. 마진율 낮은 순 Top 30 + 지입/제외 거래처 플래그

라벨: 초안(ERP 역산) — 실제 회계 마진과 다릅니다. 영업자 확인 전 데이터.
"""

import sys
from pathlib import Path
from datetime import datetime

try:
    import pymssql
    import pandas as pd
except ImportError as e:
    print(f"❌ pip3 install pymssql pandas openpyxl: {e}")
    sys.exit(1)

# --- 경로 ---
ENV_FILE = Path("/Users/jack/dev/gabwoo/견적계산기/.env.local")
OUT_DIR = Path(__file__).parent / "output"
OUT_DIR.mkdir(exist_ok=True)
OUT_XLSX = OUT_DIR / f"갑우_거래처_마진_Top30_2025_{datetime.now():%Y%m%d}.xlsx"

FIRM = "7000"
CUST_OWN = "10000"   # 갑우
YEAR_START = "20250101"
YEAR_END = "20251231"
MIN_SALES = 100_000_000
MIN_COVERAGE = 0.30
TOP_N = 30

# --- 지입거래처 (용지 직접 구매 — 원가 구조 다름) ---
JIIP_CUST = {
    "V00661": "교원구몬",
    "V00712": "교원",
    "V01222": "이투스",
    "V1526":  "에듀윌",
    "V01119": "교원프라퍼티",
}

# --- 특수 케이스 제외 ---
EXCLUDE_CUST = {
    "V00749": "지에스리테일",   # 프린트뱅크 이관 특수케이스
}

# --- 출판 항목코드 → 표준 매입원가 매핑 ---
# standard_pricing.json 내부거래.판비.CTP=5000 / 인쇄.본문(부수 구간별) 기준
# '0' prefix 가 있는 출판 코드만 대상 (303001, 703002 등 패키지 코드와 혼동 방지)
PUB_ITEM_COST_TYPE = {
    # 판비 계열 — 고정 5,000원 (외주 CTP 매입원가)
    "0703001": "CTP",   # 인쇄판
    "0703002": "CTP",   # CTP
    "0703003": "CTP",   # 표지인쇄판대
    # 인쇄대 계열 — 부수 구간별 매트릭스
    "0303001": "PRINT", # 표진인쇄대
    "0303002": "PRINT", # 표지인쇄대
    "0303003": "PRINT", # 인쇄대
}

CTP_COST = 5000  # 원/판

# 인쇄 표준 매입단가 (국전 4도, 외주 매입 기준)
# 출처: 매입(외주) 단가표(출판).numbers > 인쇄 시트
PRINT_COST_BY_VOLUME = [
    (1000,  3000),
    (2000,  2000),
    (3000,  1500),
    (4000,  1400),
    (5000,  1300),
    (6000,  1200),
    (7000,  1100),
    (8000,  1000),
    (10000, 1000),
    (30000, 1100),
    (100000, 1000),
]


def get_print_cost(volume):
    if pd.isna(volume) or volume <= 0:
        return 1500
    v = float(volume)
    for th, price in PRINT_COST_BY_VOLUME:
        if v < th:
            return price
    return 900  # 100K 이상


def load_env(env_path: Path) -> dict:
    env = {}
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" in line:
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip()
    return env


def main():
    env = load_env(ENV_FILE)
    conn = pymssql.connect(
        server=env["ERP_HOST"], port=int(env.get("ERP_PORT", 1433)),
        user=env["ERP_USER"], password=env["ERP_PASSWORD"],
        database=env["ERP_DATABASE"], login_timeout=15,
    )
    print(f"✅ ERP 연결. 기간 {YEAR_START}~{YEAR_END}, 회사 {FIRM} / 갑우({CUST_OWN})만")

    # --- 1. 갑우(10000) 견적 라인 ---
    print("\n📥 PRT_ESTL + PRT_ESTH 2025 갑우 견적 라인 조회...")
    lines = pd.read_sql(f"""
        SELECT
            h.NO_EST,
            h.DT_EST,
            h.CD_CUST_OWN,
            h.CD_PARTNER AS CD_CUST,
            h.NM_PARTNER AS NM_CUST,
            h.YN_APP,
            l.CD_ITEM,
            l.NM_ITEM,
            l.DC_ITEM_SPEC,
            l.QT AS QT_R,
            l.UM,
            l.AM AS AM_LINE,
            l.RT_DISCOUNT
        FROM PRT_ESTL l
        JOIN PRT_ESTH h
            ON l.NO_EST = h.NO_EST
            AND l.CD_FIRM = h.CD_FIRM
            AND l.NO_HST = h.NO_HST
        WHERE h.CD_FIRM = '{FIRM}'
          AND h.CD_CUST_OWN = '{CUST_OWN}'
          AND h.DT_EST >= '{YEAR_START}' AND h.DT_EST <= '{YEAR_END}'
          AND l.UM > 0 AND l.QT > 0 AND l.AM > 0
    """, conn)
    print(f"   → {len(lines):,}행 로드")

    # --- 2. SAL_SALESH 2025 갑우 실매출 ---
    print("📥 SAL_SALESH 2025 갑우 거래처별 실매출 조회...")
    sales = pd.read_sql(f"""
        SELECT
            h.CD_CUST,
            c.NM_CUST,
            SUM(h.AM) AS 실매출_공급가
        FROM SAL_SALESH h
        LEFT JOIN MAS_CUST c ON h.CD_CUST = c.CD_CUST AND h.CD_FIRM = c.CD_FIRM
        WHERE h.CD_FIRM = '{FIRM}'
          AND h.CD_CUST_OWN = '{CUST_OWN}'
          AND h.DT_SALES >= '{YEAR_START}' AND h.DT_SALES <= '{YEAR_END}'
          AND (h.ST_SALES='Y' OR h.ST_SALES IS NULL)
          AND h.AM > 0
        GROUP BY h.CD_CUST, c.NM_CUST
    """, conn)
    print(f"   → {len(sales):,} 거래처 실매출 로드")
    conn.close()

    # --- 3. 항목별 라인원가 계산 (출판 0-prefix 항목만 매칭) ---
    lines["CD_ITEM"] = lines["CD_ITEM"].astype(str).str.strip()

    def line_cost(row):
        item = row["CD_ITEM"]
        if item not in PUB_ITEM_COST_TYPE:
            return None
        kind = PUB_ITEM_COST_TYPE[item]
        qt = float(row["QT_R"])
        if kind == "CTP":
            return CTP_COST * qt
        if kind == "PRINT":
            return get_print_cost(qt) * qt
        return None

    lines["라인원가"] = lines.apply(line_cost, axis=1)
    lines["매칭여부"] = lines["라인원가"].notna()
    lines["라인마진"] = lines["AM_LINE"] - lines["라인원가"]

    matched = int(lines["매칭여부"].sum())
    total = len(lines)
    print(f"   원가매칭(출판항목): {matched:,}/{total:,} ({matched/total*100:.1f}%)" if total else "   (라인 없음)")

    if matched == 0:
        print("❌ 매칭된 라인이 없습니다 — 분석 중단")
        sys.exit(1)

    # --- 4. 거래처별 집계 ---
    print("\n🧮 거래처 집계...")
    agg = lines.groupby(["CD_CUST", "NM_CUST"], dropna=False).agg(
        견적매출_전체=("AM_LINE", "sum"),
        견적매출_매칭=("AM_LINE", lambda s: s[lines.loc[s.index, "매칭여부"]].sum()),
        견적원가_매칭=("라인원가", "sum"),
        견적마진_매칭=("라인마진", "sum"),
        라인수_전체=("AM_LINE", "count"),
        라인수_매칭=("매칭여부", "sum"),
    ).reset_index()
    agg["견적원가_매칭"] = pd.to_numeric(agg["견적원가_매칭"], errors="coerce")
    agg["견적마진_매칭"] = pd.to_numeric(agg["견적마진_매칭"], errors="coerce")

    agg["커버리지"] = (agg["견적매출_매칭"] / agg["견적매출_전체"]).round(3)
    agg["마진율_매칭영역"] = (
        agg["견적마진_매칭"] / agg["견적매출_매칭"].replace(0, pd.NA)
    ).astype(float).round(4)

    # --- 5. 실매출 조인 ---
    agg = agg.merge(sales[["CD_CUST", "실매출_공급가"]], on="CD_CUST", how="left")

    # --- 6. 필터 ---
    base = agg.copy()
    base["매출기준"] = base["실매출_공급가"].fillna(base["견적매출_전체"])
    filt = base[
        (base["매출기준"] >= MIN_SALES) &
        (base["커버리지"] >= MIN_COVERAGE) &
        (base["라인수_매칭"] >= 5)   # 라인 너무 적으면 우연성 큼
    ].copy()
    print(f"   매출{MIN_SALES//1_0000_0000}억+·커버리지{MIN_COVERAGE*100:.0f}%+·매칭라인≥5 → {len(filt):,}건")

    # --- 7. 플래그 ---
    filt["지입여부"] = filt["CD_CUST"].map(JIIP_CUST).fillna("")
    filt["제외플래그"] = filt["CD_CUST"].map(EXCLUDE_CUST).fillna("")

    main = filt[filt["제외플래그"] == ""].copy()
    main = main.sort_values("마진율_매칭영역", ascending=True, na_position="last").reset_index(drop=True)
    top = main.head(TOP_N).copy()

    # --- 8. 출력 포맷 ---
    def fmt(df):
        d = df.copy()
        d["실매출(억)"] = (d["실매출_공급가"].fillna(d["견적매출_전체"]) / 1e8).round(2)
        d["견적매출(억)"] = (d["견적매출_전체"] / 1e8).round(2)
        d["매칭매출(억)"] = (d["견적매출_매칭"] / 1e8).round(2)
        d["매칭원가(억)"] = (d["견적원가_매칭"] / 1e8).round(2)
        d["매칭마진(억)"] = (d["견적마진_매칭"] / 1e8).round(2)
        d["마진율(%)"] = (d["마진율_매칭영역"] * 100).round(1)
        d["커버리지(%)"] = (d["커버리지"] * 100).round(1)
        return d[[
            "CD_CUST", "NM_CUST",
            "실매출(억)", "견적매출(억)",
            "매칭매출(억)", "매칭원가(억)", "매칭마진(억)",
            "마진율(%)", "커버리지(%)",
            "라인수_전체", "라인수_매칭",
            "지입여부",
        ]]

    top_out = fmt(top)
    main_out = fmt(main)
    jiip_out = fmt(main[main["지입여부"] != ""])
    excluded_out = fmt(filt[filt["제외플래그"] != ""])

    # --- 9. 요약 ---
    avg_top = top["마진율_매칭영역"].mean()
    avg_all = main["마진율_매칭영역"].mean()
    summary = pd.DataFrame({
        "지표": [
            "분석 범위", "기간", "회사(CD_FIRM)", "소속사(CD_CUST_OWN)",
            "매출 하한", "원가매칭 커버리지 하한", "매칭 라인 하한",
            "필터 통과 거래처", "Top N", f"Top{TOP_N} 평균 마진율",
            "필터 통과 전체 평균 마진율", "Top 중 지입거래처", "분석 제외(특수)",
            "주의 (라벨)",
        ],
        "값": [
            "갑우 출판사업부 (CD_ITEM 0-prefix만)",
            f"{YEAR_START}~{YEAR_END}",
            FIRM, CUST_OWN,
            f"{MIN_SALES:,}",
            f"{MIN_COVERAGE*100:.0f}%",
            "5라인 이상",
            f"{len(main):,}",
            TOP_N,
            f"{avg_top*100:.1f}%" if pd.notna(avg_top) else "—",
            f"{avg_all*100:.1f}%" if pd.notna(avg_all) else "—",
            f"{(top['지입여부']!='').sum()}",
            f"{len(excluded_out)}",
            "초안(ERP 역산) — 영업자 확인 전",
        ],
    })

    with pd.ExcelWriter(OUT_XLSX, engine="openpyxl") as w:
        summary.to_excel(w, sheet_name="0_요약", index=False)
        top_out.to_excel(w, sheet_name=f"1_Top{TOP_N}_마진낮은순", index=False)
        main_out.to_excel(w, sheet_name="2_전체필터통과", index=False)
        if len(jiip_out):
            jiip_out.to_excel(w, sheet_name="3_지입거래처_별도", index=False)
        if len(excluded_out):
            excluded_out.to_excel(w, sheet_name="4_분석제외_특수", index=False)

    print(f"\n📤 {OUT_XLSX}\n")
    print("=" * 100)
    print(f"▼ Top {TOP_N} 마진율 낮은 순 (2025, 갑우, 출판항목 매칭)")
    print("=" * 100)
    pd.set_option("display.max_rows", TOP_N)
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 220)
    print(top_out.to_string(index=False))


if __name__ == "__main__":
    main()
