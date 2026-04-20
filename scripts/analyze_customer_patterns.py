"""
거래처별 YoY 패턴 분석 (중복집계 버그 수정판)

매출: SAL_SALESH 헤더 기준 (라인 JOIN으로 인한 중복 없음)
수량·단가: SAL_SALESL 라인 기준 (별도 집계 후 연도×거래처로 merge)

3대 패턴 분류 (2023 vs 2025):
  A. 단가 내렸는데 물량도 잃음 — 매출Δ<-20% AND 단가Δ<-10%
  B. 단가 올렸더니 이탈     — 매출Δ<-20% AND 단가Δ>+5%
  C. 완전 이탈              — 2025 매출 = 0
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
OUT_DIR.mkdir(exist_ok=True)
OUT_XLSX = OUT_DIR / f"거래처패턴분석_{datetime.now():%Y%m%d}.xlsx"

FIRM = "7000"
YEAR_FROM = "20210101"
YEAR_TO = "20261231"


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
    print(f"✅ ERP 연결 ({env['ERP_HOST']})")

    # ── (1) 헤더만: 매출·건수 (중복 없음) ──
    print("🔍 헤더 집계 (매출·건수)...")
    header = pd.read_sql(f"""
        SELECT
            LEFT(h.DT_SALES, 4) AS 연도,
            h.CD_CUST AS 거래처코드,
            c.NM_CUST AS 거래처명,
            COUNT(DISTINCT h.NO_SALES) AS 매출건수,
            SUM(h.AM) AS 매출공급가
        FROM SAL_SALESH h
        LEFT JOIN MAS_CUST c ON h.CD_CUST=c.CD_CUST AND h.CD_FIRM=c.CD_FIRM
        WHERE h.CD_FIRM='{FIRM}'
          AND h.DT_SALES >= '{YEAR_FROM}' AND h.DT_SALES <= '{YEAR_TO}'
          AND h.AM > 0
          AND (h.ST_SALES='Y' OR h.ST_SALES IS NULL)
        GROUP BY LEFT(h.DT_SALES, 4), h.CD_CUST, c.NM_CUST
    """, conn)

    # ── (2) 라인만: 수량·단가·할인율 ──
    print("🔍 라인 집계 (수량·단가)...")
    line = pd.read_sql(f"""
        SELECT
            LEFT(h.DT_SALES, 4) AS 연도,
            h.CD_CUST AS 거래처코드,
            SUM(l.QT) AS 총수량,
            SUM(l.AM) AS 라인금액합계,
            AVG(CASE WHEN l.RT_DISCOUNT>0 THEN l.RT_DISCOUNT END) AS 평균할인율
        FROM SAL_SALESH h
        JOIN SAL_SALESL l ON h.CD_FIRM=l.CD_FIRM AND h.NO_SALES=l.NO_SALES
        WHERE h.CD_FIRM='{FIRM}'
          AND h.DT_SALES >= '{YEAR_FROM}' AND h.DT_SALES <= '{YEAR_TO}'
          AND (h.ST_SALES='Y' OR h.ST_SALES IS NULL)
          AND l.QT > 0 AND l.AM > 0
        GROUP BY LEFT(h.DT_SALES, 4), h.CD_CUST
    """, conn)
    conn.close()

    merged = header.merge(line, on=["연도", "거래처코드"], how="left").fillna(0)
    merged["단가가중평균"] = merged.apply(
        lambda r: r["라인금액합계"]/r["총수량"] if r["총수량"]>0 else 0, axis=1
    )

    # ── (3) 피벗: 거래처 × 연도 ──
    amt = merged.pivot_table(index=["거래처코드","거래처명"], columns="연도", values="매출공급가", aggfunc="sum").fillna(0)
    qty = merged.pivot_table(index=["거래처코드","거래처명"], columns="연도", values="총수량", aggfunc="sum").fillna(0)
    um  = merged.pivot_table(index=["거래처코드","거래처명"], columns="연도", values="단가가중평균", aggfunc="mean").fillna(0)
    cnt = merged.pivot_table(index=["거래처코드","거래처명"], columns="연도", values="매출건수", aggfunc="sum").fillna(0)

    # ── (4) YoY 계산 (2023 → 2025) ──
    r = pd.DataFrame(index=amt.index)
    r["매출_2023"] = amt.get("2023", 0)
    r["매출_2025"] = amt.get("2025", 0)
    r["매출Δ(%)"] = (r["매출_2025"] - r["매출_2023"]) / r["매출_2023"].replace(0, pd.NA) * 100
    r["수량_2023"] = qty.get("2023", 0)
    r["수량_2025"] = qty.get("2025", 0)
    r["수량Δ(%)"] = (r["수량_2025"] - r["수량_2023"]) / r["수량_2023"].replace(0, pd.NA) * 100
    r["단가_2023"] = um.get("2023", 0)
    r["단가_2025"] = um.get("2025", 0)
    r["단가Δ(%)"] = (r["단가_2025"] - r["단가_2023"]) / r["단가_2023"].replace(0, pd.NA) * 100
    r["건수_2023"] = cnt.get("2023", 0)
    r["건수_2025"] = cnt.get("2025", 0)
    r = r.reset_index()

    # 거래처 정제: 2023 매출 1억 이상만 (의미 있는 규모)
    big = r[r["매출_2023"] >= 100_000_000].copy()

    # ── (5) 패턴 분류 ──
    def classify(row):
        if row["매출_2023"] == 0:
            return "N/A"
        if row["매출_2025"] == 0:
            return "C. 완전이탈"
        d_amt = row["매출Δ(%)"]
        d_um = row["단가Δ(%)"]
        if d_amt <= -20 and d_um <= -10:
            return "A. 단가↓+물량↓ (양보실패)"
        if d_amt <= -20 and d_um >= 5:
            return "B. 단가↑→고객이탈"
        if d_amt <= -20 and -10 < d_um < 5:
            return "D. 물량감소·단가유지 (원래 가설)"
        if d_amt > -20 and d_amt < 20:
            return "E. 안정적"
        if d_amt >= 20:
            return "F. 성장"
        return "기타"

    big["패턴"] = big.apply(classify, axis=1)

    # ── (6) 패턴별 요약 ──
    summary = big.groupby("패턴").agg(
        거래처수=("거래처코드","count"),
        매출_2023합계=("매출_2023","sum"),
        매출_2025합계=("매출_2025","sum"),
    ).reset_index()
    summary["매출손실(억)"] = (summary["매출_2023합계"] - summary["매출_2025합계"]) / 1e8
    summary["매출_2023(억)"] = summary["매출_2023합계"] / 1e8
    summary["매출_2025(억)"] = summary["매출_2025합계"] / 1e8
    summary = summary[["패턴","거래처수","매출_2023(억)","매출_2025(억)","매출손실(억)"]]

    # 각 패턴별 테이블 (매출 2023 큰 순)
    def fmt(df):
        d = df.copy()
        for c in ["매출_2023","매출_2025"]:
            d[c] = (d[c]/1e8).round(2)
        d = d.rename(columns={"매출_2023":"매출23(억)","매출_2025":"매출25(억)"})
        for c in ["매출Δ(%)","수량Δ(%)","단가Δ(%)"]:
            d[c] = pd.to_numeric(d[c], errors="coerce").round(1)
        return d[["거래처코드","거래처명","매출23(억)","매출25(억)","매출Δ(%)","수량Δ(%)","단가Δ(%)","건수_2023","건수_2025"]]

    A = fmt(big[big["패턴"]=="A. 단가↓+물량↓ (양보실패)"].sort_values("매출_2023", ascending=False))
    B = fmt(big[big["패턴"]=="B. 단가↑→고객이탈"].sort_values("매출_2023", ascending=False))
    C = fmt(big[big["패턴"]=="C. 완전이탈"].sort_values("매출_2023", ascending=False))
    D = fmt(big[big["패턴"]=="D. 물량감소·단가유지 (원래 가설)"].sort_values("매출_2023", ascending=False))

    print(f"\n📤 {OUT_XLSX}")
    with pd.ExcelWriter(OUT_XLSX, engine="openpyxl") as w:
        summary.to_excel(w, sheet_name="0_패턴요약", index=False)
        A.to_excel(w, sheet_name="A_단가↓물량↓", index=False)
        B.to_excel(w, sheet_name="B_단가↑이탈", index=False)
        C.to_excel(w, sheet_name="C_완전이탈", index=False)
        D.to_excel(w, sheet_name="D_물량↓단가유지", index=False)
        fmt(big.sort_values("매출_2023", ascending=False)).to_excel(w, sheet_name="전체_1억이상", index=False)

    # 콘솔 출력
    print("\n" + "="*60)
    print("패턴별 요약 (2023 매출 1억 이상 거래처, 2023→2025 비교)")
    print("="*60)
    print(summary.to_string(index=False))
    print("\n▼ A. 단가↓ + 물량↓ (양보실패)")
    print(A.to_string(index=False))
    print("\n▼ B. 단가↑ → 고객이탈")
    print(B.to_string(index=False))
    print("\n▼ C. 완전이탈")
    print(C.to_string(index=False))


if __name__ == "__main__":
    main()
