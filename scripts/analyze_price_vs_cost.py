"""
원가 상승률 대비 판매단가 인상 여부 검증

벤치마크: 용지 매입 가중평균 단가 (viewGabwoo_마감)
  - 2021→2025: +70.6%
  - 2023→2025: +22.6%

거래처별로 2023→2025 단가Δ vs 원가Δ 비교:
  [반영충분]  단가Δ >= +22%   (원가 인상 온전히 전가)
  [일부반영]  0 ≤ 단가Δ < +22%  (인상했으나 부족)
  [역행]      단가Δ < 0        (원가 올랐는데 오히려 인하)
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
OUT_XLSX = OUT_DIR / f"원가반영_점검_{datetime.now():%Y%m%d}.xlsx"

FIRM = "7000"
COST_INFLATION_23_25 = 22.6  # %, 용지 단가 가중평균 기준

def load_env():
    env = {}
    for line in ENV_FILE.read_text().splitlines():
        if "=" in line and not line.strip().startswith("#"):
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip()
    return env


def main():
    env = load_env()
    conn = pymssql.connect(
        server=env["ERP_HOST"], port=int(env.get("ERP_PORT", 1433)),
        user=env["ERP_USER"], password=env["ERP_PASSWORD"],
        database=env["ERP_DATABASE"], login_timeout=15,
    )
    print(f"✅ ERP 연결. 용지원가 벤치마크: 2023→2025 +{COST_INFLATION_23_25}%")

    # 거래처 × 연도별 라인 기준 단가·매출
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
    # 헤더 기준 매출(더 안전한 매출 지표)
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

    um = line.pivot_table(index=["거래처코드","거래처명"], columns="연도", values="단가", aggfunc="mean").fillna(0)
    qty = line.pivot_table(index=["거래처코드","거래처명"], columns="연도", values="수량", aggfunc="sum").fillna(0)
    amt = hdr.pivot_table(index=["거래처코드"], columns="연도", values="매출", aggfunc="sum").fillna(0)

    r = pd.DataFrame(index=um.index).reset_index()
    r["단가_2023"] = um.get("2023", 0).values
    r["단가_2025"] = um.get("2025", 0).values
    r["수량_2023"] = qty.get("2023", 0).values
    r["수량_2025"] = qty.get("2025", 0).values
    r["매출_2023"] = r["거래처코드"].map(amt.get("2023", pd.Series(dtype=float))).fillna(0)
    r["매출_2025"] = r["거래처코드"].map(amt.get("2025", pd.Series(dtype=float))).fillna(0)

    # 2023·2025 모두 매출 있고 1억 이상
    r = r[(r["매출_2023"] >= 100_000_000) & (r["매출_2025"] > 0) & (r["단가_2023"] > 0)].copy()
    r["단가Δ(%)"] = (r["단가_2025"] - r["단가_2023"]) / r["단가_2023"] * 100
    r["원가전가격차(%p)"] = r["단가Δ(%)"] - COST_INFLATION_23_25

    def classify(d):
        if d >= COST_INFLATION_23_25: return "✅ 반영충분"
        if d >= 0: return "🟡 일부반영"
        return "🔴 역행(단가인하)"
    r["원가반영"] = r["단가Δ(%)"].apply(classify)

    # 요약
    summary = r.groupby("원가반영").agg(
        거래처수=("거래처코드","count"),
        매출_2023_억=("매출_2023", lambda x: round(x.sum()/1e8, 1)),
        매출_2025_억=("매출_2025", lambda x: round(x.sum()/1e8, 1)),
        평균단가Δ=("단가Δ(%)","mean"),
    ).reset_index()
    summary["평균단가Δ"] = summary["평균단가Δ"].round(1)
    summary["손실(억)"] = summary["매출_2023_억"] - summary["매출_2025_억"]

    def fmt(df):
        d = df.copy()
        d["매출23(억)"] = (d["매출_2023"]/1e8).round(2)
        d["매출25(억)"] = (d["매출_2025"]/1e8).round(2)
        d["단가Δ(%)"] = d["단가Δ(%)"].round(1)
        d["원가전가격차(%p)"] = d["원가전가격차(%p)"].round(1)
        d["수량Δ(%)"] = ((d["수량_2025"]-d["수량_2023"])/d["수량_2023"].replace(0, pd.NA)*100).round(1)
        return d[["거래처코드","거래처명","매출23(억)","매출25(억)","단가Δ(%)","원가전가격차(%p)","수량Δ(%)"]]

    hit = fmt(r[r["원가반영"]=="✅ 반영충분"].sort_values("매출_2023", ascending=False))
    part = fmt(r[r["원가반영"]=="🟡 일부반영"].sort_values("매출_2023", ascending=False))
    rev = fmt(r[r["원가반영"]=="🔴 역행(단가인하)"].sort_values("매출_2023", ascending=False))

    with pd.ExcelWriter(OUT_XLSX, engine="openpyxl") as w:
        summary.to_excel(w, sheet_name="0_요약", index=False)
        hit.to_excel(w, sheet_name="반영충분", index=False)
        part.to_excel(w, sheet_name="일부반영", index=False)
        rev.to_excel(w, sheet_name="역행_단가인하", index=False)

    print(f"\n📤 {OUT_XLSX}\n")
    print("="*70)
    print(f"벤치마크: 용지 가중평균 단가 2023→2025 = +{COST_INFLATION_23_25}%")
    print("="*70)
    print(summary.to_string(index=False))
    print(f"\n▼ 🔴 역행 (원가 올랐는데 단가 내림) — {len(rev)}개사")
    print(rev.to_string(index=False))
    print(f"\n▼ 🟡 일부반영 (인상했으나 원가상승률 미달) — {len(part)}개사")
    print(part.to_string(index=False))
    print(f"\n▼ ✅ 반영충분 — {len(hit)}개사")
    print(hit.to_string(index=False))


if __name__ == "__main__":
    main()
