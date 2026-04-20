"""
거래처 건전도 프로파일 2025 — 세금계산서 준거 검증 포함

설계 (2026-04-16 Jack 합의):
  1. 세금계산서 원본 xls 파싱 → 사업자번호별 2025 매출 (3사 각각)
  2. ERP SAL_SALESH/L에서 거래처별 매출·할인·취소·미처리·승인·단가추세 집계
  3. NO_BIZ로 매칭, ERP/세금계산서 비율 산출 → 데이터 신뢰도 등급
  4. 건전도 지표(신호등) 조합 → 총점 → Top 30 건전도 나쁨
  5. 🔴 부실(검증 불가) 거래처는 별도 시트로 분리, 판단 보류 권고

⚠️ 용어 규칙: "세금계산서 = 제품매출현황 xls" / "ERP = SAL_SALESH 기록"
  24년·23년 세금계산서 파일 미수령 (2026-04-16) → YoY·단가추세는 ERP 내부 데이터로만 산출
"""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

try:
    import numpy as np
    import pandas as pd
    import pymssql
except ImportError as e:
    print(f"❌ pip3 install pymssql pandas numpy xlrd openpyxl: {e}")
    sys.exit(1)

# ───────── 경로 ─────────
ENV_FILE = Path("/Users/jack/dev/gabwoo/견적계산기/.env.local")
INVOICE_XLS = Path("/Users/jack/dev/gabwoo/25년 제품매출현황(갑우,비피,더원).xls")
OUT_DIR = Path(__file__).parent / "output"
OUT_DIR.mkdir(exist_ok=True)
OUT_XLSX = OUT_DIR / f"거래처_건전도_2025_{datetime.now():%Y%m%d}.xlsx"

FIRM = "7000"
YEAR = "2025"
YEAR_START = "20250101"
YEAR_END = "20251231"
PREV_START = "20230101"   # 3년 단가 추세 계산용 시작 (2023)

# 건전도 점수 대상 하한 (건전도 판정이 의미 있으려면 최소 규모 필요)
MIN_SALES_FOR_SCORING = 100_000_000   # 1억

FIRM_NAME = {"10000": "갑우", "20000": "비피", "30000": "더원"}

JIIP_CUST_BIZ = {
    "1078140772": "(주)갑우문화사(관계)",
    "1418115138": "(주)비피앤피(관계)",
    # 지입(=아방) — 용지 직접 구매·지급, 갑우는 제작만 (확인된 사업자번호)
    "4968602009": "(주)교원",
    "6678702103": "(주)교원구몬",
    "8668700833": "(주)동행복권",  # 2026-04-20 Jack 확인 — 아방(지입) 거래처
    # 추가 지입 사업자번호는 Jack 확인 후 보완
}
EXCLUDE_BIZ: set[str] = set()  # 향후 확장 (프린트뱅크 이관 등)


# ───────── 1. 세금계산서 xls 파싱 ─────────
def parse_invoice_xls(xls_path: Path) -> pd.DataFrame:
    """세금계산서 xls의 3시트를 읽어 (소속사, 사업자번호, 날짜, 대변, 거래처명, 프로젝트명) 통합.
    '월계/누계' 같은 요약 행(사업자번호 NaN)은 제외합니다."""
    out = []
    for sheet, firm_code in [("갑우", "10000"), ("비피", "20000"), ("더원", "30000")]:
        df = pd.read_excel(xls_path, sheet_name=sheet, header=0)
        df.columns = [str(c).strip() for c in df.columns]
        # 실제 컬럼명은 첫 행 기준: 날짜, 적요란, 거래처명, 사업자번호, 대변, (프로젝트명)
        col_map = {}
        for c in df.columns:
            if "날짜" in c: col_map[c] = "날짜"
            elif "적요" in c: col_map[c] = "적요"
            elif "거래처" in c: col_map[c] = "거래처명"
            elif "사업자" in c: col_map[c] = "사업자번호"
            elif "대변" in c: col_map[c] = "대변"
            elif "프로젝트" in c: col_map[c] = "프로젝트"
        df = df.rename(columns=col_map)
        if "프로젝트" not in df.columns:
            df["프로젝트"] = None
        df["소속사코드"] = firm_code
        df = df[["소속사코드", "날짜", "거래처명", "사업자번호", "대변", "프로젝트", "적요"]]
        # 요약행(사업자번호 비어있음) 제외
        df = df[df["사업자번호"].notna() & (df["사업자번호"].astype(str).str.strip() != "")]
        # 날짜 포맷 통일
        df["날짜"] = pd.to_datetime(df["날짜"], errors="coerce")
        df["대변"] = pd.to_numeric(df["대변"], errors="coerce").fillna(0)
        df = df[df["날짜"].notna()]
        # 사업자번호 포맷 정규화: 공백·하이픈 제거해 10자리 숫자만 (ERP MAS_CUST.NO_BIZ 형식)
        df["사업자번호"] = df["사업자번호"].astype(str).str.strip().str.replace("-", "", regex=False).str.replace(" ", "", regex=False)
        df = df[df["사업자번호"].str.match(r"^\d{10}$")]
        out.append(df)
    all_rows = pd.concat(out, ignore_index=True)
    return all_rows


def summarize_invoice(rows: pd.DataFrame) -> pd.DataFrame:
    """사업자번호 × 소속사 × 월로 세금계산서 매출 집계. 순매출(음수 포함)과 발행건수 분리."""
    rows = rows.copy()
    rows["연월"] = rows["날짜"].dt.strftime("%Y%m")
    agg = rows.groupby(["소속사코드", "사업자번호"], as_index=False).agg(
        세금_순매출=("대변", "sum"),
        세금_총발행=("대변", lambda s: s[s > 0].sum()),
        세금_총취소=("대변", lambda s: -s[s < 0].sum()),
        세금_건수_양=("대변", lambda s: (s > 0).sum()),
        세금_건수_음=("대변", lambda s: (s < 0).sum()),
        세금_거래처명=("거래처명", "last"),
        세금_첫거래=("날짜", "min"),
        세금_최근거래=("날짜", "max"),
    )
    # 월별 매출 → 변동성 계산용
    monthly = rows.groupby(["소속사코드", "사업자번호", "연월"], as_index=False)["대변"].sum()
    vol = monthly.groupby(["소속사코드", "사업자번호"]).agg(
        세금_월평균매출=("대변", "mean"),
        세금_월표준편차=("대변", "std"),
        세금_월활동개수=("대변", "count"),
    ).reset_index()
    vol["세금_월변동계수"] = (vol["세금_월표준편차"] / vol["세금_월평균매출"].replace(0, np.nan)).round(3)
    return agg.merge(vol, on=["소속사코드", "사업자번호"], how="left")


# ───────── 2. ERP 쿼리 ─────────
def load_env(p: Path) -> dict:
    env = {}
    for line in p.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        env[k.strip()] = v.strip()
    return env


def erp_connection():
    env = load_env(ENV_FILE)
    return pymssql.connect(
        server=env["ERP_HOST"], port=int(env.get("ERP_PORT", 1433)),
        user=env["ERP_USER"], password=env["ERP_PASSWORD"],
        database=env["ERP_DATABASE"], login_timeout=15,
    )


def fetch_erp_sales_by_cust(conn) -> pd.DataFrame:
    """거래처별 2025 매출 — ST_SALES·YN_APP 시나리오 분해."""
    q = f"""
    SELECT
        c.CD_CUST, c.NM_CUST, c.NO_BIZ,
        h.CD_CUST_OWN,
        -- 금액 시나리오
        SUM(CASE WHEN h.ST_SALES='Y' AND h.AM>0 THEN CAST(h.AM AS BIGINT) ELSE 0 END)                       AS 매출_확정Y,
        SUM(CASE WHEN h.ST_SALES IS NULL AND h.AM>0 THEN CAST(h.AM AS BIGINT) ELSE 0 END)                   AS 매출_미처리NULL,
        SUM(CASE WHEN h.ST_SALES='N' THEN CAST(h.AM AS BIGINT) ELSE 0 END)                                  AS 매출_취소N,
        SUM(CASE WHEN (h.ST_SALES='Y' OR h.ST_SALES IS NULL) AND h.AM>0 THEN CAST(h.AM AS BIGINT) ELSE 0 END) AS 매출_합산B,
        SUM(CASE WHEN h.YN_APP='N' AND h.ST_SALES='Y' AND h.AM>0 THEN CAST(h.AM AS BIGINT) ELSE 0 END)      AS 매출_승인없이확정,
        -- 건수
        COUNT(*)                                                                                            AS 건수_전체,
        SUM(CASE WHEN (h.ST_SALES='Y' OR h.ST_SALES IS NULL) AND h.AM>0 THEN 1 ELSE 0 END)                  AS 건수_확정,
        SUM(CASE WHEN h.ST_SALES='N' THEN 1 ELSE 0 END)                                                     AS 건수_취소,
        SUM(CASE WHEN h.ST_SALES IS NULL AND h.AM>0 THEN 1 ELSE 0 END)                                      AS 건수_미처리,
        -- 거래 시기
        MIN(h.DT_SALES) AS 첫거래_ERP,
        MAX(h.DT_SALES) AS 최근거래_ERP
    FROM SAL_SALESH h
    JOIN MAS_CUST c ON h.CD_CUST=c.CD_CUST AND h.CD_FIRM=c.CD_FIRM
    WHERE h.CD_FIRM='{FIRM}'
      AND h.DT_SALES BETWEEN '{YEAR_START}' AND '{YEAR_END}'
    GROUP BY c.CD_CUST, c.NM_CUST, c.NO_BIZ, h.CD_CUST_OWN
    """
    return pd.read_sql(q, conn)


def fetch_erp_discount(conn) -> pd.DataFrame:
    """거래처별 평균 할인율 (라인 단위, 확정/미처리 매출 기준)."""
    q = f"""
    SELECT
        h.CD_CUST, h.CD_CUST_OWN,
        AVG(CAST(l.RT_DISCOUNT AS FLOAT))         AS 평균할인율,
        SUM(CASE WHEN l.RT_DISCOUNT>0 THEN 1 ELSE 0 END) AS 할인적용건,
        COUNT(*) AS 라인수
    FROM SAL_SALESH h
    JOIN SAL_SALESL l ON h.CD_FIRM=l.CD_FIRM AND h.NO_SALES=l.NO_SALES
    WHERE h.CD_FIRM='{FIRM}'
      AND h.DT_SALES BETWEEN '{YEAR_START}' AND '{YEAR_END}'
      AND (h.ST_SALES='Y' OR h.ST_SALES IS NULL) AND h.AM>0
    GROUP BY h.CD_CUST, h.CD_CUST_OWN
    """
    return pd.read_sql(q, conn)


def fetch_erp_monthly(conn) -> pd.DataFrame:
    """거래처별 월매출 — ERP 기준 변동성 계산용."""
    q = f"""
    SELECT
        h.CD_CUST, h.CD_CUST_OWN,
        LEFT(h.DT_SALES, 6) AS 연월,
        SUM(CAST(h.AM AS BIGINT)) AS 월매출
    FROM SAL_SALESH h
    WHERE h.CD_FIRM='{FIRM}'
      AND h.DT_SALES BETWEEN '{YEAR_START}' AND '{YEAR_END}'
      AND (h.ST_SALES='Y' OR h.ST_SALES IS NULL) AND h.AM>0
    GROUP BY h.CD_CUST, h.CD_CUST_OWN, LEFT(h.DT_SALES, 6)
    """
    return pd.read_sql(q, conn)


def fetch_erp_yoy(conn) -> pd.DataFrame:
    """거래처 × 연도 매출 (2023~2025) → YoY·단가추세 계산용."""
    q = f"""
    SELECT
        h.CD_CUST, h.CD_CUST_OWN,
        LEFT(h.DT_SALES, 4) AS 연도,
        SUM(CAST(h.AM AS BIGINT)) AS 연매출,
        SUM(CAST(l.AM AS BIGINT)) AS 라인매출,
        SUM(CAST(l.QT AS FLOAT)) AS 라인수량,
        COUNT(DISTINCT h.NO_SALES) AS 건수
    FROM SAL_SALESH h
    JOIN SAL_SALESL l ON h.CD_FIRM=l.CD_FIRM AND h.NO_SALES=l.NO_SALES
    WHERE h.CD_FIRM='{FIRM}'
      AND h.DT_SALES BETWEEN '{PREV_START}' AND '{YEAR_END}'
      AND (h.ST_SALES='Y' OR h.ST_SALES IS NULL) AND h.AM>0
      AND l.QT>0 AND l.AM>0
    GROUP BY h.CD_CUST, h.CD_CUST_OWN, LEFT(h.DT_SALES, 4)
    """
    return pd.read_sql(q, conn)


# ───────── 3. 신호등 분류 함수 ─────────
def traffic(value, green_max, yellow_max, lower_is_better=True):
    """값을 🟢/🟡/🔴 분류. lower_is_better=True: 낮을수록 좋음."""
    if pd.isna(value):
        return "⚫ 불명"
    if lower_is_better:
        if value <= green_max:  return "🟢"
        if value <= yellow_max: return "🟡"
        return "🔴"
    else:
        if value >= green_max:  return "🟢"
        if value >= yellow_max: return "🟡"
        return "🔴"


def score(light: str) -> int:
    return {"🟢": 2, "🟡": 1, "🔴": 0, "⚫ 불명": None}.get(light, None)


def classify_reliability(row) -> str:
    """ERP/세금계산서 비율 + 절대 갭 기준으로 데이터 신뢰도 등급."""
    erp = row["매출_합산B"]
    inv = row["세금_순매출"]
    # 세금계산서에 없는 거래처(거의 소액이거나 신규)
    if pd.isna(inv) or inv == 0:
        if erp > 0:
            return "⚫ 세금無/ERP有"  # ERP엔 있는데 세금계산서엔 없음
        return "⚫ 양쪽無"
    # ERP에 없는데 세금계산서엔 있음
    if erp == 0:
        return "🔴 ERP누락"
    ratio = erp / inv
    gap_abs = abs(erp - inv)
    if 0.95 <= ratio <= 1.05 and gap_abs < 5_000_000:
        return "🟢 신뢰"
    if 0.80 <= ratio <= 1.20:
        return "🟡 주의"
    return "🔴 부실"


def classify_trend(r2023, r2024, r2025):
    """단가 3년 추세 — 평균단가 = 라인매출/수량."""
    if any(pd.isna(x) or x <= 0 for x in (r2023, r2024, r2025)):
        return "⚫ 데이터부족"
    d1 = (r2024 - r2023) / r2023 * 100
    d2 = (r2025 - r2024) / r2024 * 100
    if d1 < -3 and d2 < -3: return "🔴🔴 계속하락"
    if d1 < -3 and d2 > 3:  return "🟡 하락→반등"
    if d1 > 3 and d2 < -3:  return "🟠 상승→하락"
    if d1 > 3 and d2 > 3:   return "✅ 계속상승"
    return "⚪ 평탄"


# ───────── 4. 조립 ─────────
def build_profile():
    print("📥 세금계산서 xls 파싱...")
    rows = parse_invoice_xls(INVOICE_XLS)
    print(f"   → {len(rows):,}행 (3사 합산, 요약행 제외)")
    invoice = summarize_invoice(rows)
    print(f"   → 거래처×소속사 {len(invoice):,}건 집계")

    print("📥 ERP 쿼리 (4종) 실행...")
    conn = erp_connection()
    try:
        sales = fetch_erp_sales_by_cust(conn)
        disc  = fetch_erp_discount(conn)
        monthly = fetch_erp_monthly(conn)
        yoy   = fetch_erp_yoy(conn)
    finally:
        conn.close()
    print(f"   → sales {len(sales)}, discount {len(disc)}, monthly {len(monthly)}, yoy {len(yoy)}")

    # ERP 월매출 변동성
    mv = monthly.groupby(["CD_CUST", "CD_CUST_OWN"]).agg(
        ERP_월평균=("월매출", "mean"),
        ERP_월표준편차=("월매출", "std"),
        ERP_월활동개수=("월매출", "count"),
    ).reset_index()
    mv["ERP_월변동계수"] = (mv["ERP_월표준편차"] / mv["ERP_월평균"].replace(0, np.nan)).round(3)

    # 연도별 피벗 → YoY + 단가
    yoy["단가"] = yoy["라인매출"] / yoy["라인수량"].replace(0, np.nan)
    am_piv = yoy.pivot_table(index=["CD_CUST", "CD_CUST_OWN"], columns="연도", values="연매출", aggfunc="sum").reset_index()
    um_piv = yoy.pivot_table(index=["CD_CUST", "CD_CUST_OWN"], columns="연도", values="단가",  aggfunc="mean").reset_index()
    am_piv.columns = [f"매출_{c}" if c in ("2023","2024","2025") else c for c in am_piv.columns]
    um_piv.columns = [f"단가_{c}" if c in ("2023","2024","2025") else c for c in um_piv.columns]

    # ─ merge ─
    base = sales.copy()
    base["NO_BIZ"] = base["NO_BIZ"].astype(str).str.strip().str.replace("-", "", regex=False)
    invoice["사업자번호"] = invoice["사업자번호"].astype(str).str.strip().str.replace("-", "", regex=False)

    base = base.merge(disc, on=["CD_CUST", "CD_CUST_OWN"], how="left")
    base = base.merge(mv,   on=["CD_CUST", "CD_CUST_OWN"], how="left")
    base = base.merge(am_piv, on=["CD_CUST", "CD_CUST_OWN"], how="left")
    base = base.merge(um_piv, on=["CD_CUST", "CD_CUST_OWN"], how="left")

    # 세금계산서 join: NO_BIZ + 소속사코드 둘다로 매칭
    invoice_key = invoice.rename(columns={"소속사코드": "CD_CUST_OWN", "사업자번호": "NO_BIZ"})
    base = base.merge(invoice_key, on=["NO_BIZ", "CD_CUST_OWN"], how="outer")
    # outer join 뒤 세금계산서만 있는 행도 보존. CD_CUST 비어있는 행은 "ERP 누락"
    base["소속사"] = base["CD_CUST_OWN"].map(FIRM_NAME).fillna(base["CD_CUST_OWN"])

    # 이름 보정
    base["거래처명"] = base["NM_CUST"].fillna(base["세금_거래처명"])

    # ─ 비율·등급 ─
    base["매출_합산B"] = pd.to_numeric(base["매출_합산B"], errors="coerce").fillna(0)
    base["세금_순매출"] = pd.to_numeric(base["세금_순매출"], errors="coerce").fillna(0)
    base["ERP_세금_비율"] = (base["매출_합산B"] / base["세금_순매출"].replace(0, np.nan)).round(3)
    base["갭_절대_억"] = ((base["매출_합산B"] - base["세금_순매출"]) / 1e8).round(2)
    base["데이터신뢰도"] = base.apply(classify_reliability, axis=1)

    # ─ 건전도 지표(신호등) ─
    # YoY 24→25
    base["YoY_24_25_pct"] = ((base.get("매출_2025", 0) - base.get("매출_2024", 0)) / base.get("매출_2024", pd.NA).replace(0, pd.NA) * 100).round(1)
    # 취소율/미처리율
    total = base["매출_합산B"] + base["매출_취소N"].abs() + 0.01
    base["취소율_pct"] = (base["매출_취소N"].abs() / total * 100).round(1)
    base["미처리율_pct"] = (base["매출_미처리NULL"] / (base["매출_확정Y"] + base["매출_미처리NULL"]).replace(0, np.nan) * 100).round(1)
    base["승인없이확정_pct"] = (base["매출_승인없이확정"] / base["매출_확정Y"].replace(0, np.nan) * 100).round(1)
    # 단가추세
    base["단가추세"] = base.apply(lambda r: classify_trend(
        r.get("단가_2023"), r.get("단가_2024"), r.get("단가_2025")), axis=1)
    # 최근거래 공백일수
    today = pd.Timestamp(YEAR_END[:4]+"-"+YEAR_END[4:6]+"-"+YEAR_END[6:])
    base["최근거래_dt"] = pd.to_datetime(base["최근거래_ERP"], format="%Y%m%d", errors="coerce")
    base["거래공백일"] = (today - base["최근거래_dt"]).dt.days

    # 신호등
    base["신호_YoY"]          = base["YoY_24_25_pct"].apply(lambda v: traffic(v, 0, -20, lower_is_better=False))
    base["신호_취소율"]        = base["취소율_pct"].apply(lambda v: traffic(v, 1, 5))
    base["신호_미처리율"]      = base["미처리율_pct"].apply(lambda v: traffic(v, 5, 20))
    base["신호_할인율"]        = base["평균할인율"].apply(lambda v: traffic(v, 5, 15))
    base["신호_월변동"]        = base["ERP_월변동계수"].apply(lambda v: traffic(v, 0.4, 0.8))
    base["신호_공백"]          = base["거래공백일"].apply(lambda v: traffic(v, 30, 90))
    base["신호_승인누락"]      = base["승인없이확정_pct"].apply(lambda v: traffic(v, 0, 10))
    # 단가추세는 기존 방식 레이블 → 색 값
    def trend_light(label):
        if label.startswith("🔴🔴"): return "🔴"
        if label.startswith("🟠"):   return "🔴"
        if label.startswith("🟡"):   return "🟡"
        if label.startswith("✅"):   return "🟢"
        if label.startswith("⚪"):   return "🟡"
        return "⚫ 불명"
    base["신호_단가추세"] = base["단가추세"].apply(trend_light)

    # ─ 건전도 총점 ─
    signals = ["신호_YoY","신호_취소율","신호_미처리율","신호_할인율","신호_월변동","신호_공백","신호_승인누락","신호_단가추세"]
    base["건전도총점"] = base[signals].apply(
        lambda row: sum(score(s) for s in row if score(s) is not None), axis=1)
    base["평가가능지표수"] = base[signals].apply(
        lambda row: sum(1 for s in row if score(s) is not None), axis=1)
    base["건전도평균"] = (base["건전도총점"] / base["평가가능지표수"].replace(0, np.nan)).round(2)

    # 지입/제외 플래그
    base["지입여부"] = base["NO_BIZ"].map(JIIP_CUST_BIZ).fillna("")
    base["제외플래그"] = base["NO_BIZ"].map({b: "제외" for b in EXCLUDE_BIZ}).fillna("")

    return base


# ───────── 5. 출력 ─────────
def format_sheet(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    d["ERP매출(억)"]     = (d["매출_합산B"] / 1e8).round(2)
    d["세금매출(억)"]    = (d["세금_순매출"] / 1e8).round(2)
    d["갭(억)"]          = d["갭_절대_억"]
    cols = [
        "소속사", "NO_BIZ", "거래처명",
        "데이터신뢰도", "ERP매출(억)", "세금매출(억)", "갭(억)", "ERP_세금_비율",
        "건전도평균", "건전도총점", "평가가능지표수",
        "신호_YoY", "YoY_24_25_pct",
        "신호_취소율", "취소율_pct",
        "신호_미처리율", "미처리율_pct",
        "신호_할인율", "평균할인율",
        "신호_월변동", "ERP_월변동계수",
        "신호_공백", "거래공백일",
        "신호_승인누락", "승인없이확정_pct",
        "신호_단가추세", "단가추세",
        "매출_취소N", "매출_미처리NULL", "매출_승인없이확정",
        "건수_전체", "건수_확정", "건수_취소", "건수_미처리",
        "지입여부", "제외플래그",
    ]
    for c in cols:
        if c not in d.columns:
            d[c] = pd.NA
    return d[cols]


def main():
    base = build_profile()

    # 분리
    scorable = base[
        base["매출_합산B"].fillna(0) >= MIN_SALES_FOR_SCORING
    ].copy()
    tiny = base[
        base["매출_합산B"].fillna(0) < MIN_SALES_FOR_SCORING
    ].copy()

    # 신뢰도별 분리
    reliable = scorable[scorable["데이터신뢰도"].isin(["🟢 신뢰", "🟡 주의"])].copy()
    unreliable = scorable[~scorable["데이터신뢰도"].isin(["🟢 신뢰", "🟡 주의"])].copy()

    # 건전도 나쁜 순 정렬
    reliable = reliable.sort_values(["건전도평균", "매출_합산B"], ascending=[True, False])
    top30 = reliable.head(30)
    bottom30 = reliable.sort_values(["건전도평균", "매출_합산B"], ascending=[False, False]).head(30)

    # 지입만
    jiip = reliable[reliable["지입여부"] != ""].copy()

    # 검증 매트릭스 (정적)
    verif = pd.DataFrame({
        "지표": ["매출 절대값 2025", "거래 빈도", "월별 변동성", "YoY 24→25",
                 "3년 단가추세", "평균 할인율", "취소율", "미처리율", "승인없이확정",
                 "거래 공백일수", "지입 플래그"],
        "세금계산서 검증": ["✅ 직접","✅ 직접","🟡 조건부","❌ (24년 미수령)",
                          "❌ (24·23 미수령)","❌","❌","❌","❌",
                          "✅ 직접 (최근거래)","❌"],
        "해석": [
            "NO_BIZ 매칭, ERP/세금 비율로 신뢰도 등급 산출",
            "월별 건수 비교 가능",
            "월별 집계 비교 (세금계산서 일자 기반)",
            "24년 세금계산서 수령 시 재검증",
            "23·24·25 모두 받으면 교차 검증",
            "세금계산서엔 할인 정보 없음. 매출 신뢰도로 간접 판단",
            "세금계산서엔 취소 기록 있음 (음수 대변) — 향후 비교 가능",
            "세금계산서는 발행된 것만 — 미처리는 ERP 내부 상태",
            "ERP 승인 플래그 전용 지표",
            "세금계산서 최근 발행일 vs ERP 최근거래",
            "사업자번호 기반, ERP와 무관",
        ],
    })

    # 요약 시트
    sum_by_firm = base.groupby("소속사", dropna=False).agg(
        거래처수=("NO_BIZ", "nunique"),
        세금매출_억=("세금_순매출", lambda s: round(s.sum()/1e8, 1)),
        ERP매출_억=("매출_합산B", lambda s: round(s.sum()/1e8, 1)),
    ).reset_index()
    sum_by_firm["갭_억"] = (sum_by_firm["ERP매출_억"] - sum_by_firm["세금매출_억"]).round(1)
    sum_by_firm["ERP_세금_비율"] = (sum_by_firm["ERP매출_억"] / sum_by_firm["세금매출_억"].replace(0, np.nan)).round(2)

    rel_summary = scorable["데이터신뢰도"].value_counts().reset_index()
    rel_summary.columns = ["데이터신뢰도", "거래처수"]

    meta = pd.DataFrame({
        "항목": ["분석 기간", "ERP CD_FIRM", "세금계산서 원본", "24·23년 파일",
                 "규모 하한 (건전도 평가)", "Top N", "라벨"],
        "값": [f"{YEAR_START}~{YEAR_END}", FIRM,
                str(INVOICE_XLS.name), "아직 미수령 (YoY·단가추세 검증 불가)",
                f"{MIN_SALES_FOR_SCORING:,}", 30,
                "초안 — 영업자 확인 전"],
    })

    # 엑셀 출력
    with pd.ExcelWriter(OUT_XLSX, engine="openpyxl") as w:
        meta.to_excel(w, sheet_name="0_메타", index=False)
        sum_by_firm.to_excel(w, sheet_name="0_요약_소속사별갭", index=False)
        rel_summary.to_excel(w, sheet_name="0_요약_신뢰도분포", index=False)
        format_sheet(top30).to_excel(w, sheet_name="1_Top30_건전도나쁨", index=False)
        format_sheet(bottom30).to_excel(w, sheet_name="1b_Top30_건전도좋음", index=False)
        format_sheet(unreliable).to_excel(w, sheet_name="2_신뢰도부실_판단보류", index=False)
        format_sheet(reliable).to_excel(w, sheet_name="3_전체_평가가능", index=False)
        format_sheet(jiip).to_excel(w, sheet_name="4_지입거래처_별도", index=False)
        format_sheet(tiny).to_excel(w, sheet_name="5_소규모_1억미만", index=False)
        verif.to_excel(w, sheet_name="6_지표별_검증가능성", index=False)

    print(f"\n📤 {OUT_XLSX}")
    print("=" * 90)
    print("▼ 3사 요약 (ERP vs 세금계산서)")
    print(sum_by_firm.to_string(index=False))
    print("\n▼ 신뢰도 분포")
    print(rel_summary.to_string(index=False))
    print("\n▼ Top 10 건전도 나쁨 (미리보기)")
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 220)
    preview = format_sheet(top30.head(10))[[
        "소속사", "거래처명", "데이터신뢰도", "ERP매출(억)", "세금매출(억)",
        "건전도평균", "신호_YoY", "신호_취소율", "신호_미처리율", "신호_할인율", "신호_월변동", "신호_공백", "신호_단가추세",
    ]]
    print(preview.to_string(index=False))


if __name__ == "__main__":
    main()
