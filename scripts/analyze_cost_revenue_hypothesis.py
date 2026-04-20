"""
경영진단 가설 검증 — "비싸게 사서 싸게 팔고 있다"

4트랙 병렬 분석:
  1) 매입 원가 추이 (용지 + 외주/부자재)
  2) 판매 단가 YoY (거래처 × 품목군)
  3) 매출원가율 재구성 (월별 매출 vs 매입)
  4) 물량↓ 단가↔ (핵심 가설): 매출 급감 거래처의 단가·할인율 변화

출력: scripts/output/경영진단_가설검증_YYYYMMDD.xlsx (다중 시트)
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

TODAY = datetime.now().strftime("%Y%m%d")
OUT_XLSX = OUT_DIR / f"경영진단_가설검증_{TODAY}.xlsx"

# 분석 범위: 2021~2026 (5개년 + 올해)
YEAR_FROM = "20210101"
YEAR_TO = "20261231"

FIRM = "7000"  # 갑우문화사만. 비피앤피(8000)은 SNOTES에 0건


def load_env():
    env = {}
    for line in ENV_FILE.read_text().splitlines():
        if "=" in line and not line.strip().startswith("#"):
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip()
    return env


def get_conn(env):
    return pymssql.connect(
        server=env["ERP_HOST"], port=int(env.get("ERP_PORT", 1433)),
        user=env["ERP_USER"], password=env["ERP_PASSWORD"],
        database=env["ERP_DATABASE"], login_timeout=15,
    )


def q(conn, sql, **params):
    """쿼리 결과를 pandas DataFrame으로."""
    return pd.read_sql(sql, conn, params=params)


# ─────────────────────────────────────────────────────────────────────
# Track 1 — 매입 원가 추이
# ─────────────────────────────────────────────────────────────────────

def track1_purchase_cost(conn):
    """용지 매입 단가 추이 + 외주/부자재 매입 추이."""
    print("🔍 Track 1: 매입 원가 추이 분석...")

    # 1-a. 용지: viewGabwoo_마감 연도별
    paper_yearly = pd.read_sql(f"""
        SELECT
            YEAR(일자) AS 연도,
            SUM(수량) AS 총수량,
            SUM(공급가액) AS 총매입액,
            CASE WHEN SUM(수량)>0 THEN SUM(공급가액)*1.0/SUM(수량) ELSE 0 END AS 단가가중평균,
            COUNT(*) AS 건수,
            COUNT(DISTINCT 제조사명) AS 제조사수
        FROM dbo.viewGabwoo_마감
        WHERE 일자 >= '2021-01-01' AND 일자 <= '2026-12-31'
          AND 수량 > 0
        GROUP BY YEAR(일자)
        ORDER BY YEAR(일자)
    """, conn)

    # 1-b. 용지 지종별 단가 YoY (지종 code × 연도)
    paper_by_type = pd.read_sql(f"""
        SELECT
            YEAR(일자) AS 연도,
            지종 AS 지종코드,
            SUM(수량) AS 수량,
            SUM(공급가액) AS 매입액,
            CASE WHEN SUM(수량)>0 THEN SUM(공급가액)*1.0/SUM(수량) ELSE 0 END AS 단가
        FROM dbo.viewGabwoo_마감
        WHERE 일자 >= '2021-01-01' AND 일자 <= '2026-12-31'
          AND 수량 > 0 AND 지종 IS NOT NULL
        GROUP BY YEAR(일자), 지종
        ORDER BY 지종, YEAR(일자)
    """, conn)

    # 지종별 피벗 (2021 vs 2025 비교)
    paper_pivot = paper_by_type.pivot_table(
        index="지종코드", columns="연도", values="단가", aggfunc="mean"
    ).fillna(0)
    if 2021 in paper_pivot.columns and 2025 in paper_pivot.columns:
        paper_pivot["단가변화율_21→25"] = (
            (paper_pivot[2025] - paper_pivot[2021]) / paper_pivot[2021].replace(0, pd.NA) * 100
        )
    # 매입액 기준 상위 30개 지종만
    volume_by_type = paper_by_type.groupby("지종코드")["매입액"].sum().sort_values(ascending=False)
    top_types = volume_by_type.head(30).index.tolist()
    paper_pivot_top = paper_pivot.loc[paper_pivot.index.isin(top_types)].copy()
    paper_pivot_top["총매입액5년"] = volume_by_type.loc[paper_pivot_top.index]
    paper_pivot_top = paper_pivot_top.sort_values("총매입액5년", ascending=False)

    # 1-c. 외주/부자재 (PUR_ETCCLSH = 기타마감, 실제 금액 확정) 연도별
    pur_yearly = pd.read_sql(f"""
        SELECT
            LEFT(DT_PUR, 4) AS 연도,
            COUNT(*) AS 마감건수,
            SUM(AM_SUPPLY) AS 총매입액,
            COUNT(DISTINCT CD_CUST) AS 거래처수
        FROM PUR_ETCCLSH
        WHERE CD_FIRM='{FIRM}'
          AND DT_PUR >= '{YEAR_FROM}' AND DT_PUR <= '{YEAR_TO}'
          AND AM_SUPPLY > 0
        GROUP BY LEFT(DT_PUR, 4)
        ORDER BY LEFT(DT_PUR, 4)
    """, conn)

    # 1-d. 주요 외주 거래처(매입처) TOP 15 — 연도별 지급액
    pur_topvendor = pd.read_sql(f"""
        SELECT
            LEFT(DT_PUR, 4) AS 연도,
            CD_CUST AS 거래처코드,
            SUM(AM_SUPPLY) AS 매입액,
            COUNT(*) AS 마감건수
        FROM PUR_ETCCLSH
        WHERE CD_FIRM='{FIRM}'
          AND DT_PUR >= '{YEAR_FROM}' AND DT_PUR <= '{YEAR_TO}'
          AND AM_SUPPLY > 0
        GROUP BY LEFT(DT_PUR, 4), CD_CUST
    """, conn)
    pur_topvendor["거래처명"] = pur_topvendor["거래처코드"]  # MAS_CUST 매칭 실패 시 코드로 대체
    total_by_vendor = pur_topvendor.groupby(["거래처코드", "거래처명"])["매입액"].sum().sort_values(ascending=False)
    top_vendors = total_by_vendor.head(15).index.tolist()
    pur_vendor_pivot = pur_topvendor[
        pur_topvendor.set_index(["거래처코드", "거래처명"]).index.isin(top_vendors)
    ].pivot_table(index=["거래처코드", "거래처명"], columns="연도", values="매입액", aggfunc="sum").fillna(0)
    pur_vendor_pivot["총매입액5년"] = total_by_vendor.loc[pur_vendor_pivot.index]
    pur_vendor_pivot = pur_vendor_pivot.sort_values("총매입액5년", ascending=False)

    return {
        "1a_용지매입_연도별": paper_yearly,
        "1b_용지단가_지종×연도": paper_pivot_top.reset_index(),
        "1c_외주매입_연도별": pur_yearly,
        "1d_외주매입처_TOP15": pur_vendor_pivot.reset_index(),
    }


# ─────────────────────────────────────────────────────────────────────
# Track 2 — 판매 단가 YoY
# ─────────────────────────────────────────────────────────────────────

def track2_selling_price(conn):
    """거래처 × 품목군 × 연도별 평균 단가."""
    print("🔍 Track 2: 판매 단가 YoY 분석...")

    # 2-a. 전체 연도별: 매출(헤더 기준)과 단가·할인율(라인 기준) 분리 집계
    header_yearly = pd.read_sql(f"""
        SELECT
            LEFT(DT_SALES, 4) AS 연도,
            COUNT(DISTINCT NO_SALES) AS 매출건수,
            SUM(AM) AS 매출공급가
        FROM SAL_SALESH
        WHERE CD_FIRM='{FIRM}'
          AND DT_SALES >= '{YEAR_FROM}' AND DT_SALES <= '{YEAR_TO}'
          AND AM > 0 AND (ST_SALES='Y' OR ST_SALES IS NULL)
        GROUP BY LEFT(DT_SALES, 4)
    """, conn)
    line_yearly = pd.read_sql(f"""
        SELECT
            LEFT(h.DT_SALES, 4) AS 연도,
            SUM(l.QT) AS 총수량,
            SUM(l.AM) AS 라인합계,
            CASE WHEN SUM(l.QT)>0 THEN SUM(l.AM)*1.0/SUM(l.QT) ELSE 0 END AS 단가가중평균,
            AVG(CASE WHEN l.RT_DISCOUNT>0 THEN l.RT_DISCOUNT END) AS 평균할인율_명시적
        FROM SAL_SALESH h
        JOIN SAL_SALESL l ON h.CD_FIRM=l.CD_FIRM AND h.NO_SALES=l.NO_SALES
        WHERE h.CD_FIRM='{FIRM}'
          AND h.DT_SALES >= '{YEAR_FROM}' AND h.DT_SALES <= '{YEAR_TO}'
          AND (h.ST_SALES='Y' OR h.ST_SALES IS NULL)
          AND l.AM > 0 AND l.QT > 0
        GROUP BY LEFT(h.DT_SALES, 4)
    """, conn)
    sales_yearly = header_yearly.merge(line_yearly, on="연도", how="outer").sort_values("연도")
    sales_yearly["건당매출(백만원)"] = sales_yearly.apply(
        lambda r: (r["매출공급가"]/r["매출건수"]/1e6) if r["매출건수"] else 0, axis=1
    )

    # 2-b. 품목군별(CD_ITEM 앞 3자리) 연도별 가중평균 단가
    item_yearly = pd.read_sql(f"""
        SELECT
            LEFT(h.DT_SALES, 4) AS 연도,
            LEFT(l.CD_ITEM, 3) AS 품목군,
            SUM(l.QT) AS 수량,
            SUM(l.AM) AS 금액,
            CASE WHEN SUM(l.QT)>0 THEN SUM(l.AM)*1.0/SUM(l.QT) ELSE 0 END AS 단가,
            AVG(CASE WHEN l.RT_DISCOUNT>0 THEN l.RT_DISCOUNT END) AS 평균할인율,
            COUNT(*) AS 라인수
        FROM SAL_SALESH h
        JOIN SAL_SALESL l ON h.CD_FIRM=l.CD_FIRM AND h.NO_SALES=l.NO_SALES
        WHERE h.CD_FIRM='{FIRM}'
          AND h.DT_SALES >= '{YEAR_FROM}' AND h.DT_SALES <= '{YEAR_TO}'
          AND l.CD_ITEM IS NOT NULL AND l.QT > 0 AND l.AM > 0
          AND (h.ST_SALES = 'Y' OR h.ST_SALES IS NULL)
        GROUP BY LEFT(h.DT_SALES, 4), LEFT(l.CD_ITEM, 3)
    """, conn)
    item_volume = item_yearly.groupby("품목군")["금액"].sum().sort_values(ascending=False)
    top_items = item_volume.head(20).index.tolist()
    item_pivot = item_yearly[item_yearly["품목군"].isin(top_items)].pivot_table(
        index="품목군", columns="연도", values="단가", aggfunc="mean"
    ).fillna(0)
    if "2021" in item_pivot.columns and "2025" in item_pivot.columns:
        item_pivot["단가변화율_21→25(%)"] = (
            (item_pivot["2025"] - item_pivot["2021"]) / item_pivot["2021"].replace(0, pd.NA) * 100
        )
    item_pivot["총매출5년"] = item_volume.loc[item_pivot.index]
    item_pivot = item_pivot.sort_values("총매출5년", ascending=False)

    # 2-c. 할인율 분포 YoY
    discount_dist = pd.read_sql(f"""
        SELECT
            LEFT(h.DT_SALES, 4) AS 연도,
            CASE
                WHEN l.RT_DISCOUNT IS NULL OR l.RT_DISCOUNT = 0 THEN '0% (할인없음)'
                WHEN l.RT_DISCOUNT < 5 THEN '1-5%'
                WHEN l.RT_DISCOUNT < 10 THEN '5-10%'
                WHEN l.RT_DISCOUNT < 20 THEN '10-20%'
                WHEN l.RT_DISCOUNT < 30 THEN '20-30%'
                ELSE '30%+'
            END AS 할인율구간,
            COUNT(*) AS 라인수,
            SUM(l.AM) AS 금액
        FROM SAL_SALESH h
        JOIN SAL_SALESL l ON h.CD_FIRM=l.CD_FIRM AND h.NO_SALES=l.NO_SALES
        WHERE h.CD_FIRM='{FIRM}'
          AND h.DT_SALES >= '{YEAR_FROM}' AND h.DT_SALES <= '{YEAR_TO}'
          AND l.AM > 0
          AND (h.ST_SALES = 'Y' OR h.ST_SALES IS NULL)
        GROUP BY LEFT(h.DT_SALES, 4),
            CASE
                WHEN l.RT_DISCOUNT IS NULL OR l.RT_DISCOUNT = 0 THEN '0% (할인없음)'
                WHEN l.RT_DISCOUNT < 5 THEN '1-5%'
                WHEN l.RT_DISCOUNT < 10 THEN '5-10%'
                WHEN l.RT_DISCOUNT < 20 THEN '10-20%'
                WHEN l.RT_DISCOUNT < 30 THEN '20-30%'
                ELSE '30%+'
            END
        ORDER BY LEFT(h.DT_SALES, 4)
    """, conn)
    disc_pivot = discount_dist.pivot_table(
        index="할인율구간", columns="연도", values="금액", aggfunc="sum"
    ).fillna(0)

    return {
        "2a_매출_연도별지표": sales_yearly,
        "2b_품목군_단가YoY": item_pivot.reset_index(),
        "2c_할인율구간_YoY": disc_pivot.reset_index(),
    }


# ─────────────────────────────────────────────────────────────────────
# Track 3 — 매출원가율 재구성 (월별)
# ─────────────────────────────────────────────────────────────────────

def track3_cost_ratio(conn):
    """월별 매출 vs 매입(용지+외주)으로 매출원가율 추이."""
    print("🔍 Track 3: 매출원가율 재구성...")

    sales_monthly = pd.read_sql(f"""
        SELECT
            LEFT(DT_SALES, 6) AS 연월,
            SUM(AM) AS 매출공급가
        FROM SAL_SALESH
        WHERE CD_FIRM='{FIRM}'
          AND DT_SALES >= '{YEAR_FROM}' AND DT_SALES <= '{YEAR_TO}'
          AND (ST_SALES='Y' OR ST_SALES IS NULL)
          AND AM > 0
        GROUP BY LEFT(DT_SALES, 6)
    """, conn)

    paper_monthly = pd.read_sql(f"""
        SELECT
            FORMAT(일자, 'yyyyMM') AS 연월,
            SUM(공급가액) AS 용지매입
        FROM dbo.viewGabwoo_마감
        WHERE 일자 >= '2021-01-01' AND 일자 <= '2026-12-31'
        GROUP BY FORMAT(일자, 'yyyyMM')
    """, conn)

    pur_monthly = pd.read_sql(f"""
        SELECT
            LEFT(DT_PUR, 6) AS 연월,
            SUM(AM_SUPPLY) AS 외주매입
        FROM PUR_ETCCLSH
        WHERE CD_FIRM='{FIRM}'
          AND DT_PUR >= '{YEAR_FROM}' AND DT_PUR <= '{YEAR_TO}'
          AND AM_SUPPLY > 0
        GROUP BY LEFT(DT_PUR, 6)
    """, conn)

    merged = sales_monthly.merge(paper_monthly, on="연월", how="outer") \
                          .merge(pur_monthly, on="연월", how="outer").fillna(0)
    merged["총매입"] = merged["용지매입"] + merged["외주매입"]
    merged["매입매출비"] = merged.apply(
        lambda r: (r["총매입"] / r["매출공급가"] * 100) if r["매출공급가"] > 0 else 0, axis=1
    )
    merged = merged.sort_values("연월")

    yearly = merged.copy()
    yearly["연도"] = yearly["연월"].str[:4]
    yearly_agg = yearly.groupby("연도").agg(
        매출공급가=("매출공급가", "sum"),
        용지매입=("용지매입", "sum"),
        외주매입=("외주매입", "sum"),
        총매입=("총매입", "sum"),
    ).reset_index()
    yearly_agg["매입매출비(%)"] = yearly_agg.apply(
        lambda r: r["총매입"] / r["매출공급가"] * 100 if r["매출공급가"] > 0 else 0, axis=1
    )
    yearly_agg["용지매입비중(%)"] = yearly_agg.apply(
        lambda r: r["용지매입"] / r["매출공급가"] * 100 if r["매출공급가"] > 0 else 0, axis=1
    )

    return {
        "3a_원가율_월별": merged,
        "3b_원가율_연도별": yearly_agg,
    }


# ─────────────────────────────────────────────────────────────────────
# Track 4 — 핵심 가설: 물량↓ 단가/할인↔
# ─────────────────────────────────────────────────────────────────────

def track4_volume_vs_price(conn):
    """거래처별 매출 변동 vs 단가·할인율 변동 매트릭스 (핵심 가설)."""
    print("🔍 Track 4: 물량↓ 단가↔ 분석...")

    # 거래처 × 연도별 매출·건수·평균단가·할인율
    cust_yearly = pd.read_sql(f"""
        SELECT
            LEFT(h.DT_SALES, 4) AS 연도,
            h.CD_CUST AS 거래처코드,
            c.NM_CUST AS 거래처명,
            COUNT(DISTINCT h.NO_SALES) AS 매출건수,
            SUM(h.AM) AS 매출공급가,
            SUM(l.QT) AS 총수량,
            CASE WHEN SUM(l.QT)>0 THEN SUM(l.AM)*1.0/SUM(l.QT) ELSE 0 END AS 단가가중평균,
            AVG(CASE WHEN l.RT_DISCOUNT>0 THEN l.RT_DISCOUNT END) AS 평균할인율,
            SUM(CASE WHEN l.RT_DISCOUNT>0 THEN l.AM ELSE 0 END) AS 할인적용금액,
            SUM(l.AM) AS 라인합계
        FROM SAL_SALESH h
        JOIN SAL_SALESL l ON h.CD_FIRM=l.CD_FIRM AND h.NO_SALES=l.NO_SALES
        LEFT JOIN MAS_CUST c ON h.CD_CUST=c.CD_CUST AND h.CD_FIRM=c.CD_FIRM
        WHERE h.CD_FIRM='{FIRM}'
          AND h.DT_SALES >= '{YEAR_FROM}' AND h.DT_SALES <= '{YEAR_TO}'
          AND h.AM > 0
          AND (h.ST_SALES='Y' OR h.ST_SALES IS NULL)
        GROUP BY LEFT(h.DT_SALES, 4), h.CD_CUST, c.NM_CUST
    """, conn)

    # 2023 vs 2025 비교
    pivot_amt = cust_yearly.pivot_table(
        index=["거래처코드", "거래처명"], columns="연도", values="매출공급가", aggfunc="sum"
    ).fillna(0)
    pivot_qty = cust_yearly.pivot_table(
        index=["거래처코드", "거래처명"], columns="연도", values="총수량", aggfunc="sum"
    ).fillna(0)
    pivot_um = cust_yearly.pivot_table(
        index=["거래처코드", "거래처명"], columns="연도", values="단가가중평균", aggfunc="mean"
    ).fillna(0)
    pivot_disc = cust_yearly.pivot_table(
        index=["거래처코드", "거래처명"], columns="연도", values="평균할인율", aggfunc="mean"
    ).fillna(0)

    # 2023→2025 변화 (두 해 모두 매출 있는 거래처만)
    if "2023" in pivot_amt.columns and "2025" in pivot_amt.columns:
        result = pd.DataFrame(index=pivot_amt.index)
        result["매출_2023"] = pivot_amt["2023"]
        result["매출_2025"] = pivot_amt["2025"]
        result["매출변동률(%)"] = (result["매출_2025"] - result["매출_2023"]) / result["매출_2023"].replace(0, pd.NA) * 100
        result["수량_2023"] = pivot_qty.get("2023", 0)
        result["수량_2025"] = pivot_qty.get("2025", 0)
        result["수량변동률(%)"] = (result["수량_2025"] - result["수량_2023"]) / result["수량_2023"].replace(0, pd.NA) * 100
        result["단가_2023"] = pivot_um.get("2023", 0)
        result["단가_2025"] = pivot_um.get("2025", 0)
        result["단가변동률(%)"] = (result["단가_2025"] - result["단가_2023"]) / result["단가_2023"].replace(0, pd.NA) * 100
        result["할인율_2023(%)"] = pivot_disc.get("2023", 0)
        result["할인율_2025(%)"] = pivot_disc.get("2025", 0)
        result["할인율변동(%p)"] = result["할인율_2025(%)"] - result["할인율_2023(%)"]

        # 의미있는 거래처만: 2023에 매출 1천만원 이상
        result = result[result["매출_2023"] >= 10_000_000].copy()

        # 핵심 가설: 매출 -30% 이상 감소 & 단가 변화 ±5% 이내 (물량 반토막인데 단가 유지)
        hypothesis_hit = result[
            (result["매출변동률(%)"] <= -30)
            & (result["단가변동률(%)"].abs() <= 5)
        ].sort_values("매출_2023", ascending=False)

        # 더 넓은 범주: 매출 감소 거래처 (Top 50, 매출 2023 기준 큰 순)
        declining = result[result["매출변동률(%)"] < 0].sort_values("매출_2023", ascending=False).head(50)

        # 물량은 급감했는데 할인율은 그대로/더 큼 (할인 변동 +2%p 이내)
        same_discount = result[
            (result["수량변동률(%)"] <= -30)
            & (result["할인율변동(%p)"] >= -1)
        ].sort_values("매출_2023", ascending=False)

        return {
            "4a_거래처YoY_전체": result.sort_values("매출_2023", ascending=False).reset_index(),
            "4b_핵심가설HIT_매출-30%단가유지": hypothesis_hit.reset_index(),
            "4c_매출감소거래처TOP50": declining.reset_index(),
            "4d_물량급감_할인유지": same_discount.reset_index(),
        }
    return {}


# ─────────────────────────────────────────────────────────────────────

def main():
    env = load_env()
    conn = get_conn(env)
    print(f"✅ ERP 연결 완료 ({env['ERP_HOST']})")

    results = {}
    results.update(track1_purchase_cost(conn))
    results.update(track2_selling_price(conn))
    results.update(track3_cost_ratio(conn))
    results.update(track4_volume_vs_price(conn))

    conn.close()

    # Excel 출력
    print(f"\n📤 Excel 저장: {OUT_XLSX}")
    with pd.ExcelWriter(OUT_XLSX, engine="openpyxl") as w:
        # 표지
        summary = pd.DataFrame({
            "항목": [
                "생성일시", "분석범위", "대상회사",
                "가설1", "가설2", "가설3",
                "출처"
            ],
            "값": [
                datetime.now().strftime("%Y-%m-%d %H:%M"),
                "2021~2026 (감사보고서 FY2020~FY2025 대응)",
                "갑우문화사 (CD_FIRM=7000). 비피앤피 제외",
                "매입 원가가 상승했는가 — Track 1",
                "판매 단가가 하락/유지되었는가 — Track 2",
                "물량 급감 거래처에도 할인율은 유지되고 있는가 — Track 4 (핵심)",
                "SNOTES ERP 직접 조회 (SAL_SALESH/L, SCT_*, PUR_*, viewGabwoo_마감, MAS_CUST)",
            ],
        })
        summary.to_excel(w, sheet_name="0_표지", index=False)
        for sheet, df in results.items():
            sheet_name = sheet[:31]  # Excel 31자 제한
            df.to_excel(w, sheet_name=sheet_name, index=False)
            print(f"   ✅ {sheet_name}: {len(df):,}행")

    print(f"\n🎉 완료: {OUT_XLSX}")


if __name__ == "__main__":
    main()
