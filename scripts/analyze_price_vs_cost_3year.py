"""
2023 → 2024 → 2025 3년치 단가·매출 추이로 패턴 식별.

기존 analyze_price_vs_cost.py 를 확장:
- 2024 컬럼 추가
- 연도별 추세 분류: 계속하락 / 반등 / 계속상승 / 혼조

벤치마크: 용지 매입 가중평균 단가
  - 2023 → 2024: +9.5% (가정치, 실제는 ERP로 재추출 필요)
  - 2024 → 2025: +12.0% (가정치)
  - 2023 → 2025: +22.6% (실측)
"""
import sys
from pathlib import Path
from datetime import datetime

try:
    import pymssql
    import pandas as pd
except ImportError as e:
    print(f"pip3 install pymssql pandas openpyxl: {e}"); sys.exit(1)

ENV_FILE = Path("/Users/jack/dev/gabwoo/견적계산기/.env.local")
OUT_DIR = Path(__file__).parent / "output"
OUT_XLSX = OUT_DIR / f"원가반영_점검_3년_{datetime.now():%Y%m%d}.xlsx"

FIRM = "7000"
COST_INFLATION_23_25 = 22.6  # %

# 분석 제외 거래처 (특수 케이스 — 일반 단가·원가 기준 적용 부적절)
EXCLUDE_CUST_CODES = {
    # 작업 공정 문제로 전체 매출이 비피앤피 자회사 프린트뱅크로 이관됨.
    # 2026-04-16 본부장 확인: 프린트뱅크 이관 케이스는 V00749 1건뿐.
    "V00749",  # (주)지에스리테일
}

# 거래처별 원인 주석 (분석 결과 해석 시 영업자 소명 전에 확인해야 할 맥락)
CUST_CAUSE_NOTES = {
    # 2026-04-16 본부장 확인: 경쟁사 저가 공세 대응 중 — 영업자 단가 양보 아님.
    # 전략적 의사결정 대상 (저가 수용 vs 철수). 소명 요구 대상 아님.
    "V1389": "경쟁사 저가공세 대응 (전략 의사결정, 소명대상 아님)",
}


def load_env():
    env = {}
    for line in ENV_FILE.read_text().splitlines():
        if "=" in line and not line.strip().startswith("#"):
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip()
    return env


def classify_trend(p23, p24, p25):
    """3년 단가 방향 분류"""
    if p23 == 0 or p24 == 0 or p25 == 0:
        return "데이터부족"
    d1 = (p24 - p23) / p23 * 100
    d2 = (p25 - p24) / p24 * 100
    if d1 < -3 and d2 < -3:
        return "🔴🔴 계속하락"
    if d1 < -3 and d2 > 3:
        return "🟡 하락→반등"
    if d1 > 3 and d2 < -3:
        return "🟠 상승→하락"
    if d1 > 3 and d2 > 3:
        return "✅ 계속상승"
    return "⚪ 거의평탄"


def main():
    env = load_env()
    conn = pymssql.connect(
        server=env["ERP_HOST"], port=int(env.get("ERP_PORT", 1433)),
        user=env["ERP_USER"], password=env["ERP_PASSWORD"],
        database=env["ERP_DATABASE"], login_timeout=15,
    )
    print(f"✅ ERP 연결. 용지원가 벤치마크: 2023→2025 +{COST_INFLATION_23_25}%")

    line = pd.read_sql(f"""
        SELECT
            LEFT(h.DT_SALES, 4) AS 연도,
            h.CD_CUST AS 거래처코드,
            c.NM_CUST AS 거래처명,
            SUM(l.QT) AS 수량,
            SUM(l.AM) AS 라인매출
        FROM SAL_SALESH h
        JOIN SAL_SALESL l ON h.CD_FIRM=l.CD_FIRM AND h.NO_SALES=l.NO_SALES
        LEFT JOIN MAS_CUST c ON h.CD_CUST=c.CD_CUST AND h.CD_FIRM=c.CD_FIRM
        WHERE h.CD_FIRM='{FIRM}'
          AND h.DT_SALES >= '20230101' AND h.DT_SALES <= '20251231'
          AND (h.ST_SALES='Y' OR h.ST_SALES IS NULL)
          AND l.QT > 0 AND l.AM > 0
        GROUP BY LEFT(h.DT_SALES, 4), h.CD_CUST, c.NM_CUST
    """, conn)
    hdr = pd.read_sql(f"""
        SELECT
            LEFT(DT_SALES, 4) AS 연도,
            CD_CUST AS 거래처코드,
            SUM(AM) AS 매출
        FROM SAL_SALESH
        WHERE CD_FIRM='{FIRM}'
          AND DT_SALES >= '20230101' AND DT_SALES <= '20251231'
          AND AM > 0 AND (ST_SALES='Y' OR ST_SALES IS NULL)
        GROUP BY LEFT(DT_SALES, 4), CD_CUST
    """, conn)
    conn.close()

    line["단가"] = line["라인매출"] / line["수량"]

    um = line.pivot_table(index=["거래처코드", "거래처명"], columns="연도",
                          values="단가", aggfunc="mean").fillna(0)
    qty = line.pivot_table(index=["거래처코드", "거래처명"], columns="연도",
                           values="수량", aggfunc="sum").fillna(0)
    amt = hdr.pivot_table(index=["거래처코드"], columns="연도",
                          values="매출", aggfunc="sum").fillna(0)

    r = pd.DataFrame(index=um.index).reset_index()
    for y in ["2023", "2024", "2025"]:
        r[f"단가_{y}"] = um.get(y, 0).values
        r[f"수량_{y}"] = qty.get(y, 0).values
        r[f"매출_{y}"] = r["거래처코드"].map(amt.get(y, pd.Series(dtype=float))).fillna(0)

    # 2023 매출 1억+ AND 2025 매출 있음 AND 2023 단가 있음
    r = r[(r["매출_2023"] >= 100_000_000) & (r["매출_2025"] > 0) & (r["단가_2023"] > 0)].copy()

    # 특수 케이스 제외
    before = len(r)
    r = r[~r["거래처코드"].isin(EXCLUDE_CUST_CODES)].copy()
    if before != len(r):
        print(f"⚠️  특수 케이스 제외: {before - len(r)}개사 ({EXCLUDE_CUST_CODES})")

    # 단가 변화율
    r["단가Δ_23→24(%)"] = ((r["단가_2024"] - r["단가_2023"]) / r["단가_2023"] * 100).round(1)
    r["단가Δ_24→25(%)"] = ((r["단가_2025"] - r["단가_2024"]).div(r["단가_2024"].replace(0, pd.NA)) * 100).round(1)
    r["단가Δ_23→25(%)"] = ((r["단가_2025"] - r["단가_2023"]) / r["단가_2023"] * 100).round(1)
    r["원가전가격차(%p)"] = (r["단가Δ_23→25(%)"] - COST_INFLATION_23_25).round(1)

    # 매출 변화
    r["매출Δ_23→24(%)"] = ((r["매출_2024"] - r["매출_2023"]) / r["매출_2023"].replace(0, pd.NA) * 100).round(1)
    r["매출Δ_24→25(%)"] = ((r["매출_2025"] - r["매출_2024"]).div(r["매출_2024"].replace(0, pd.NA)) * 100).round(1)

    # 추세 패턴
    r["단가추세"] = r.apply(lambda x: classify_trend(x["단가_2023"], x["단가_2024"], x["단가_2025"]), axis=1)

    def classify_overall(d):
        if d >= COST_INFLATION_23_25: return "✅ 반영충분"
        if d >= 0: return "🟡 일부반영"
        return "🔴 역행(단가인하)"
    r["원가반영(23→25)"] = r["단가Δ_23→25(%)"].apply(classify_overall)

    # 거래처별 원인 주석 적용
    r["원인주석"] = r["거래처코드"].map(CUST_CAUSE_NOTES).fillna("")

    def fmt(df):
        d = df.copy()
        d["매출23(억)"] = (d["매출_2023"] / 1e8).round(2)
        d["매출24(억)"] = (d["매출_2024"] / 1e8).round(2)
        d["매출25(억)"] = (d["매출_2025"] / 1e8).round(2)
        d["단가23"] = d["단가_2023"].round(0).astype("Int64")
        d["단가24"] = d["단가_2024"].round(0).astype("Int64")
        d["단가25"] = d["단가_2025"].round(0).astype("Int64")
        return d[[
            "거래처코드", "거래처명",
            "매출23(억)", "매출24(억)", "매출25(억)",
            "매출Δ_23→24(%)", "매출Δ_24→25(%)",
            "단가23", "단가24", "단가25",
            "단가Δ_23→24(%)", "단가Δ_24→25(%)", "단가Δ_23→25(%)",
            "원가전가격차(%p)", "단가추세", "원인주석",
        ]]

    # 시트 분할
    summary = r.groupby("원가반영(23→25)").agg(
        거래처수=("거래처코드", "count"),
        매출23_억=("매출_2023", lambda x: round(x.sum() / 1e8, 1)),
        매출24_억=("매출_2024", lambda x: round(x.sum() / 1e8, 1)),
        매출25_억=("매출_2025", lambda x: round(x.sum() / 1e8, 1)),
        평균단가Δ_23_25=("단가Δ_23→25(%)", "mean"),
    ).reset_index()
    summary["평균단가Δ_23_25"] = summary["평균단가Δ_23_25"].round(1)
    summary["매출_2년손실(억)"] = summary["매출23_억"] - summary["매출25_억"]

    # 추세 × 원가반영 교차표
    trend_x = pd.crosstab(r["단가추세"], r["원가반영(23→25)"], margins=True, margins_name="합계")

    rev = fmt(r[r["원가반영(23→25)"] == "🔴 역행(단가인하)"].sort_values("매출_2023", ascending=False))
    part = fmt(r[r["원가반영(23→25)"] == "🟡 일부반영"].sort_values("매출_2023", ascending=False))
    hit = fmt(r[r["원가반영(23→25)"] == "✅ 반영충분"].sort_values("매출_2023", ascending=False))

    # 계속하락 특별 시트 (가장 위험)
    danger = fmt(r[r["단가추세"] == "🔴🔴 계속하락"].sort_values("매출_2023", ascending=False))

    # 해석 가이드 시트 (맨 앞)
    guide = pd.DataFrame([
        ["원가역전 거래처 분석 — 해석 가이드", ""],
        ["", ""],
        ["■ 분석 방법", ""],
        ["ERP SAL_SALESH/SAL_SALESL 에서 거래처별 2023·2024·2025년 단가·수량·매출을 추출.",
         "단가 = 라인매출 / 수량 (연도별 평균). 매출은 헤더(DT_SALES/AM) 기준."],
        ["벤치마크: 용지 매입 가중평균 단가 2023→2025 +22.6% (viewGabwoo_마감).",
         "거래처별 단가Δ_23→25(%) 가 +22.6% 이상이면 반영충분, 0~22.6%면 일부반영, 음수면 역행(단가인하)."],
        ["단가추세: 2023→24, 24→25 두 방향을 조합해 5개 패턴으로 분류.", ""],
        ["  🔴🔴 계속하락: 둘 다 하락 (둘 다 -3% 이하)", ""],
        ["  🟠 상승→하락: 2024에 오르고 2025에 꺾임", ""],
        ["  🟡 하락→반등: 2024 저점 찍고 2025 회복", ""],
        ["  ✅ 계속상승: 둘 다 상승 (둘 다 +3% 이상)", ""],
        ["  ⚪ 거의평탄: 둘 다 ±3% 이내", ""],
        ["", ""],
        ["■ 제외·맥락", ""],
        ["① 분석 제외: (주)지에스리테일(V00749)",
         "— 작업 공정 문제로 전체 매출이 비피앤피 자회사 프린트뱅크로 이관된 특수 케이스. 2026-04-16 본부장 확인. 단가 해석 대상 아님."],
        ["② 경쟁 요인 주석: 한국조폐공사(V1389)",
         "— 계속하락 1위이지만 원인은 '경쟁사 저가공세 대응'. 영업자 단가 양보가 아니라 시장 경쟁. 담당자 소명 대상 아니라 전략 의사결정 대상 (저가 수용 vs 철수)."],
        ["③ 나머지 거래처는 원인 주석 없음 = 시장 이슈 1차 확인 후 담당자 소명 가능",
         "— 특히 '2_계속하락_위험', '3_역행_단가인하' 시트의 원인주석 칸이 비어있는 곳."],
        ["", ""],
        ["■ 원인 3분류 원칙 (영업자 억울함 방지)", ""],
        ["① 시장·경쟁 요인 (소명대상 아님, 전략 결정)", "한국조폐공사 등"],
        ["② 작업 이관 (분석 제외)", "(주)지에스리테일 — 프린트뱅크 이관"],
        ["③ 영업·단가 요인 (소명대상)", "①②에 해당하지 않는 계속하락·역행 거래처"],
        ["", ""],
        ["■ 시트 구성", ""],
        ["0_요약", "원가반영 3분류별 거래처수·매출합계"],
        ["1_추세×반영_교차표", "단가추세 × 원가반영 크로스탭"],
        ["2_계속하락_위험", "2023→24→25 연속하락 (가장 위험, 원인주석 확인 후 조치)"],
        ["3_역행_단가인하", "23→25 누적 단가 하락 전체"],
        ["4_일부반영", "단가 인상했으나 원가상승률(+22.6%) 미달"],
        ["5_반영충분", "원가 상승을 온전히 반영한 거래처"],
    ], columns=["항목", "설명"])

    with pd.ExcelWriter(OUT_XLSX, engine="openpyxl") as w:
        guide.to_excel(w, sheet_name="0_해석가이드", index=False)
        summary.to_excel(w, sheet_name="1_요약", index=False)
        trend_x.to_excel(w, sheet_name="2_추세×반영_교차표")
        danger.to_excel(w, sheet_name="3_계속하락_위험", index=False)
        rev.to_excel(w, sheet_name="4_역행_단가인하", index=False)
        part.to_excel(w, sheet_name="5_일부반영", index=False)
        hit.to_excel(w, sheet_name="6_반영충분", index=False)

    print(f"\n📤 {OUT_XLSX}\n")
    print("=" * 70)
    print(summary.to_string(index=False))
    print("\n▼ 단가 추세 × 원가반영 교차표")
    print(trend_x.to_string())
    print(f"\n▼ 🔴🔴 계속하락 (2023→24→25 둘 다 하락) — {len(danger)}개사")
    print(danger.to_string(index=False))


if __name__ == "__main__":
    main()
