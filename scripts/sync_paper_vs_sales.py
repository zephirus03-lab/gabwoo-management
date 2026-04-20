"""
용지원가 vs 판매단가 월별 집계 → Supabase paper_vs_sales_monthly 동기화.

Feature 1 (Layer 2) 데이터 파이프라인:
  - 용지 매입 단가: ERP `viewGabwoo_마감` (그룹 공통, 3사 분리 불가)
  - 판매 단가     : ERP `SAL_SALESH × SAL_SALESL` (CD_CUST_OWN별 분리 가능)
  - 기간          : 2023-01 ~ 현재 (+30일 여유)

Supabase 테이블 레코드:
  company 코드 = 'all' / '10000'(갑우) / '20000'(비피) / '30000'(더원)
  용지 수치는 그룹 공통이므로 모든 company 행에 동일값 반복 저장
  (프론트가 탭 전환 시 단순 필터만 하도록)

2026-04-16 작성 — gabwooceo 대시보드 용지원가 vs 판매단가 추세 블록 대응
"""

import sys
import time
from pathlib import Path
from datetime import datetime, timedelta

try:
    import pymssql
    import requests
except ImportError as e:
    print(f"❌ 필요 패키지 미설치: {e}")
    print("   pip3 install pymssql requests")
    sys.exit(1)

SCRIPT_DIR = Path(__file__).parent
ENV_FILE = Path("/Users/jack/dev/gabwoo/견적계산기/.env.local")

DASHBOARD_PROJECT_REF = "btbqzbrtsmwoolurpqgx"
JOB_NAME = "paper_vs_sales"

# 범위: 2023-01-01 ~ 현재 + 45일 (미래 오입력 데이터 차단)
YEAR_START = "20230101"
YEAR_END = (datetime.now() + timedelta(days=45)).strftime("%Y%m%d")
# viewGabwoo_마감 용 ISO 형식
YEAR_END_ISO = (datetime.now() + timedelta(days=45)).strftime("%Y-%m-%d")

# ── 이상치 필터 정책 ─────────────────────────────────────────────
# 라인 단가(AM/QT)가 다음 범위 밖이면 극단 이상치로 보고 집계에서 제외합니다.
# (용지 단가 수백원~수만원, 판매 단가도 수백~수만원 대가 정상)
LINE_UM_MIN = 10        # 원 — 1원짜리 수량 보정 라인 제외
LINE_UM_MAX = 100000    # 원 — 샘플/가공비 한 건만 끼어있는 이상 라인 제외

# 월별 집계가 통계적으로 의미 있으려면 라인 수·수량이 이 이상이어야 합니다.
MIN_LINES_PER_MONTH = 10       # 라인 미만 월은 판매·용지 집계에서 제외
MIN_QTY_PER_MONTH = 1000       # 수량 미만 월도 제외 (월 초반/말 불완전 데이터)

FIRM = "7000"
COMPANIES = [
    ("all", None),        # 3사 합산
    ("10000", "10000"),   # 갑우
    ("20000", "20000"),   # 비피
    ("30000", "30000"),   # 더원
]

# 용지 카테고리 — Top 4 + 기타 (PRT_ITEM.NM_ITEM 텍스트 기반)
# 3년 매입 금액 기준 상위: 백상지(31.3%) / 특수지(18.1%) / SW(12.5%) / 아트지(8.2%)
# 나머지는 "기타"로 묶음 (합지·뉴플러스·미색백상·캠퍼스·매트 등 30%)
TOP_CATEGORIES = ["백상지", "특수지", "SW(스노우)", "아트지", "기타"]


def classify_paper(nm_item: str) -> str:
    """PRT_ITEM.NM_ITEM 텍스트로 용지 카테고리 분류.
    예: '70 미색백상지 788*545' → '기타'(미색백상은 Top 4 외)
        '100 SW 880*625' → 'SW(스노우)'
        '80 백상지 788*545' → '백상지'
        '90 하이큐매트미스틱 939*636' → '기타'
        '350 네오뷰티팩 788*545' → '특수지'
    """
    n = str(nm_item or "")
    # 미색백상지는 '백상지' 아님 (별도 분류) — Top 4에 없음
    if "미색백상" in n or "미색 백상" in n:
        return "기타"
    if "하이큐" in n or "매트" in n:
        return "기타"
    if "SW" in n:
        return "SW(스노우)"
    if "아트지" in n or "전단아트" in n:
        return "아트지"
    if "백상" in n:
        return "백상지"
    # 특수지 — 고급 프리미엄 용지군
    if any(k in n for k in [
        "아르떼", "랑데뷰", "앙상블", "비브릴리언트", "스타드림",
        "오로지", "매직", "몽블랑", "AB플러스", "AB라이트",
        "네오뷰티", "화인코트", "Volume", "젠틀", "메탈릭", "CW",
    ]):
        return "특수지"
    # 합지/뉴플러스/캠퍼스/모조 등은 "기타"로 흡수
    return "기타"


def load_env(env_path: Path) -> dict:
    env = {}
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        env[k.strip()] = v.strip()
    return env


def get_service_key(access_token: str, project_ref: str) -> str:
    resp = requests.get(
        f"https://api.supabase.com/v1/projects/{project_ref}/api-keys",
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=30,
    )
    resp.raise_for_status()
    for k in resp.json():
        if k["name"] == "service_role":
            return k["api_key"]
    raise RuntimeError("service_role key 발급 실패")


def fetch_paper(conn) -> dict:
    """viewGabwoo_마감: 월별 (수량, 공급가액) → 가중평균 단가.
    라인별 단가(공급가액/수량)가 극단 범위 밖이면 제외. 월별 라인/수량이 기준 미달이면 null."""
    cur = conn.cursor(as_dict=True)
    cur.execute(f"""
        SELECT
            CONVERT(varchar(7), [일자], 23) AS ym,
            SUM(CAST([수량] AS FLOAT))     AS qty,
            SUM(CAST([공급가액] AS FLOAT)) AS amount,
            COUNT(*)                        AS line_cnt
        FROM [viewGabwoo_마감]
        WHERE [일자] >= '{YEAR_START[:4]}-{YEAR_START[4:6]}-{YEAR_START[6:]}'
          AND [일자] <= '{YEAR_END_ISO}'
          AND CAST([수량] AS FLOAT) > 0
          AND CAST([공급가액] AS FLOAT) > 0
          AND (CAST([공급가액] AS FLOAT) / NULLIF(CAST([수량] AS FLOAT), 0))
              BETWEEN {LINE_UM_MIN} AND {LINE_UM_MAX}
        GROUP BY CONVERT(varchar(7), [일자], 23)
        ORDER BY ym
    """)
    out = {}
    for r in cur.fetchall():
        ym = r["ym"]
        qty = float(r["qty"] or 0)
        amt = float(r["amount"] or 0)
        cnt = int(r["line_cnt"] or 0)
        # 월별 최소 샘플 기준 미달 → 집계 제외 (차트에서 끊김)
        if cnt < MIN_LINES_PER_MONTH or qty < MIN_QTY_PER_MONTH:
            continue
        out[ym] = {
            "paper_qty": qty,
            "paper_amount": amt,
            "paper_um_avg": (amt / qty) if qty > 0 else 0,
        }
    return out


def fetch_paper_by_category(conn) -> dict:
    """viewGabwoo_마감 ↔ PUR_POL ↔ PRT_ITEM: 월별 × 용지 카테고리별 가중평균 단가.
    반환: {(ym, category): {qty, amount, um_avg}}
    """
    cur = conn.cursor(as_dict=True)
    cur.execute(f"""
        SELECT
            CONVERT(varchar(7), v.[일자], 23) AS ym,
            CAST(i.NM_ITEM AS VARCHAR(200))     AS nm_item,
            SUM(CAST(v.[수량] AS FLOAT))        AS qty,
            SUM(CAST(v.[공급가액] AS FLOAT))    AS amount,
            COUNT(*)                             AS line_cnt
        FROM [viewGabwoo_마감] v
        JOIN PUR_POL l
          ON l.NO_PO=v.CustKey AND l.NO_LINE=v.NO_LINE AND l.CD_FIRM='{FIRM}'
        JOIN PRT_ITEM i
          ON i.CD_ITEM=l.CD_ITEM AND i.CD_FIRM='{FIRM}'
        WHERE v.[일자] >= '{YEAR_START[:4]}-{YEAR_START[4:6]}-{YEAR_START[6:]}'
          AND v.[일자] <= '{YEAR_END_ISO}'
          AND CAST(v.[수량] AS FLOAT) > 0
          AND CAST(v.[공급가액] AS FLOAT) > 0
          AND (CAST(v.[공급가액] AS FLOAT) / NULLIF(CAST(v.[수량] AS FLOAT), 0))
              BETWEEN {LINE_UM_MIN} AND {LINE_UM_MAX}
        GROUP BY CONVERT(varchar(7), v.[일자], 23), CAST(i.NM_ITEM AS VARCHAR(200))
    """)
    # (ym, cat) → (qty, amt) 누적
    agg = {}
    for r in cur.fetchall():
        ym = r["ym"]
        cat = classify_paper(r["nm_item"])
        qty = float(r["qty"] or 0)
        amt = float(r["amount"] or 0)
        key = (ym, cat)
        if key not in agg:
            agg[key] = {"qty": 0.0, "amount": 0.0, "line_cnt": 0}
        agg[key]["qty"] += qty
        agg[key]["amount"] += amt
        agg[key]["line_cnt"] += int(r["line_cnt"] or 0)
    out = {}
    for (ym, cat), d in agg.items():
        # 카테고리별 월 기준: 라인 3개 이상 + 수량 100 이상
        if d["line_cnt"] < 3 or d["qty"] < 100:
            continue
        out[(ym, cat)] = {
            "paper_qty": d["qty"],
            "paper_amount": d["amount"],
            "paper_um_avg": (d["amount"] / d["qty"]) if d["qty"] > 0 else 0,
        }
    return out


def fetch_sales(conn, cust_own):
    """SAL_SALESH × SAL_SALESL: 월별 × (소속사) 판매 가중평균 단가.
    라인별 단가(AM/QT) 극단 범위 제외. 월별 최소 샘플 기준 미달 시 null 처리."""
    where_own = f"AND h.CD_CUST_OWN='{cust_own}'" if cust_own else ""
    cur = conn.cursor(as_dict=True)
    cur.execute(f"""
        SELECT
            LEFT(h.DT_SALES, 6) AS ym_raw,
            SUM(CAST(l.QT AS FLOAT)) AS qty,
            SUM(CAST(l.AM AS FLOAT)) AS amount,
            COUNT(*)                  AS line_cnt
        FROM SAL_SALESH h
        JOIN SAL_SALESL l ON h.CD_FIRM=l.CD_FIRM AND h.NO_SALES=l.NO_SALES
        WHERE h.CD_FIRM='{FIRM}'
          AND h.DT_SALES BETWEEN '{YEAR_START}' AND '{YEAR_END}'
          AND (h.ST_SALES='Y' OR h.ST_SALES IS NULL)
          AND l.QT > 0 AND l.AM > 0
          AND (CAST(l.AM AS FLOAT) / NULLIF(CAST(l.QT AS FLOAT), 0))
              BETWEEN {LINE_UM_MIN} AND {LINE_UM_MAX}
          {where_own}
        GROUP BY LEFT(h.DT_SALES, 6)
        ORDER BY ym_raw
    """)
    out = {}
    for r in cur.fetchall():
        ym_raw = r["ym_raw"]   # 'YYYYMM'
        if not ym_raw or len(ym_raw) != 6:
            continue
        ym = f"{ym_raw[:4]}-{ym_raw[4:6]}"
        qty = float(r["qty"] or 0)
        amt = float(r["amount"] or 0)
        cnt = int(r["line_cnt"] or 0)
        if cnt < MIN_LINES_PER_MONTH or qty < MIN_QTY_PER_MONTH:
            continue
        out[ym] = {
            "sales_qty": qty,
            "sales_amount": amt,
            "sales_um_avg": (amt / qty) if qty > 0 else 0,
        }
    return out


def build_records(paper_by_ym: dict, sales_by_company_ym: dict) -> list:
    """Supabase에 올릴 레코드 리스트 생성."""
    # 모든 ym 수집
    all_yms = set(paper_by_ym.keys())
    for cym in sales_by_company_ym.values():
        all_yms.update(cym.keys())
    all_yms = sorted(all_yms)

    records = []
    now_iso = datetime.utcnow().isoformat() + "Z"
    for ym in all_yms:
        paper = paper_by_ym.get(ym, {})
        for company_code, _ in COMPANIES:
            sales = sales_by_company_ym[company_code].get(ym, {})
            records.append({
                "ym": ym,
                "company": company_code,
                "paper_um_avg": round(paper.get("paper_um_avg", 0), 2),
                "paper_qty": round(paper.get("paper_qty", 0), 2),
                "paper_amount": round(paper.get("paper_amount", 0), 2),
                "sales_um_avg": round(sales.get("sales_um_avg", 0), 2),
                "sales_qty": round(sales.get("sales_qty", 0), 2),
                "sales_amount": round(sales.get("sales_amount", 0), 2),
                "updated_at": now_iso,
            })
    return records


def cleanup_out_of_range(supabase_url: str, service_key: str):
    """유효 범위 밖(미래/과거 오입력) 레코드 삭제."""
    headers = {
        "apikey": service_key,
        "Authorization": f"Bearer {service_key}",
    }
    cut = YEAR_END_ISO[:7]  # 'YYYY-MM'
    resp = requests.delete(
        f"{supabase_url}/rest/v1/paper_vs_sales_monthly?ym=gt.{cut}",
        headers=headers, timeout=30,
    )
    if resp.ok:
        print(f"   🧹 미래 이상 레코드 청소 완료 (> {cut})")
    else:
        print(f"   ⚠️ 청소 실패 (무시): {resp.status_code} {resp.text[:120]}")


def build_category_records(paper_by_cat: dict) -> list:
    """카테고리별 월별 레코드를 Supabase 업로드 형식으로."""
    now_iso = datetime.utcnow().isoformat() + "Z"
    records = []
    for (ym, cat), d in paper_by_cat.items():
        records.append({
            "ym": ym,
            "category": cat,
            "paper_qty": round(d["paper_qty"], 2),
            "paper_amount": round(d["paper_amount"], 2),
            "paper_um_avg": round(d["paper_um_avg"], 2),
            "updated_at": now_iso,
        })
    return records


def upsert_categories(rows, supabase_url: str, service_key: str):
    """paper_category_monthly 테이블에 upsert (ym+category 기준)."""
    headers = {
        "apikey": service_key,
        "Authorization": f"Bearer {service_key}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }
    # 미래 데이터 청소
    cut = YEAR_END_ISO[:7]
    requests.delete(
        f"{supabase_url}/rest/v1/paper_category_monthly?ym=gt.{cut}",
        headers={"apikey": service_key, "Authorization": f"Bearer {service_key}"},
        timeout=30,
    )
    CHUNK = 200
    for i in range(0, len(rows), CHUNK):
        batch = rows[i:i + CHUNK]
        url = f"{supabase_url}/rest/v1/paper_category_monthly?on_conflict=ym,category"
        resp = requests.post(url, json=batch, headers=headers, timeout=60)
        if not resp.ok:
            print(f"❌ 카테고리 배치 {i // CHUNK + 1} 실패: {resp.status_code} {resp.text[:200]}")
            resp.raise_for_status()
        print(f"   ✅ 카테고리 배치 {i // CHUNK + 1}: {len(batch)}건")


def upsert(rows, supabase_url: str, service_key: str):
    """Supabase REST API upsert (ym+company 기준 on_conflict)."""
    headers = {
        "apikey": service_key,
        "Authorization": f"Bearer {service_key}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }
    CHUNK = 200
    for i in range(0, len(rows), CHUNK):
        batch = rows[i:i + CHUNK]
        url = f"{supabase_url}/rest/v1/paper_vs_sales_monthly?on_conflict=ym,company"
        resp = requests.post(url, json=batch, headers=headers, timeout=60)
        if not resp.ok:
            print(f"❌ 배치 {i // CHUNK + 1} 실패: {resp.status_code} {resp.text[:200]}")
            resp.raise_for_status()
        print(f"   ✅ 배치 {i // CHUNK + 1}: {len(batch)}건")


def record_sync_log(job: str, status: str, rows: int, supabase_url: str, key: str):
    """sync_log 테이블에 기록 (존재하면)."""
    try:
        requests.post(
            f"{supabase_url}/rest/v1/sync_log",
            json={
                "job_name": job,
                "status": status,
                "rows": rows,
                "ran_at": datetime.utcnow().isoformat() + "Z",
            },
            headers={
                "apikey": key,
                "Authorization": f"Bearer {key}",
                "Content-Type": "application/json",
                "Prefer": "return=minimal",
            },
            timeout=10,
        )
    except Exception as e:
        print(f"   ⚠️ sync_log 기록 실패 (무시): {e}")


def main():
    env = load_env(ENV_FILE)
    t0 = time.time()

    print(f"▶ ERP 연결 ({env['ERP_HOST']}:{env.get('ERP_PORT', 1433)})")
    conn = pymssql.connect(
        server=env["ERP_HOST"],
        port=int(env.get("ERP_PORT", 1433)),
        user=env["ERP_USER"],
        password=env["ERP_PASSWORD"],
        database=env["ERP_DATABASE"],
        login_timeout=15,
    )

    print("▶ 용지 매입 월별 집계 (viewGabwoo_마감)...")
    paper = fetch_paper(conn)
    print(f"   → {len(paper)} 개월")

    print("▶ 용지 카테고리별 월별 집계 (PRT_ITEM 조인)...")
    paper_by_cat = fetch_paper_by_category(conn)
    cat_counts = {}
    for (ym, cat) in paper_by_cat.keys():
        cat_counts[cat] = cat_counts.get(cat, 0) + 1
    print(f"   → {len(paper_by_cat)} (ym, category) 조합")
    for cat, n in sorted(cat_counts.items(), key=lambda x: -x[1]):
        print(f"     {cat}: {n}개월")

    print("▶ 판매 단가 월별 × 소속사 집계 (SAL_SALESH/L)...")
    sales_by_company = {}
    for code, cust_own in COMPANIES:
        s = fetch_sales(conn, cust_own)
        sales_by_company[code] = s
        print(f"   [{code}] → {len(s)} 개월")

    conn.close()

    records = build_records(paper, sales_by_company)
    print(f"▶ 총 {len(records)}건 레코드 생성")

    print(f"▶ Supabase 업서트 (프로젝트: {DASHBOARD_PROJECT_REF})")
    access_token = env.get("SUPABASE_ACCESS_TOKEN") or env.get("SUPABASE_SERVICE_TOKEN")
    if not access_token:
        print("❌ SUPABASE_ACCESS_TOKEN 없음 — .env.local 확인")
        sys.exit(1)
    service_key = get_service_key(access_token, DASHBOARD_PROJECT_REF)
    supabase_url = f"https://{DASHBOARD_PROJECT_REF}.supabase.co"
    cleanup_out_of_range(supabase_url, service_key)
    upsert(records, supabase_url, service_key)

    # 카테고리별 업서트 (Top 4 + 기타)
    cat_records = build_category_records(paper_by_cat)
    print(f"▶ 카테고리 레코드 {len(cat_records)}건 업서트")
    upsert_categories(cat_records, supabase_url, service_key)

    record_sync_log(JOB_NAME, "ok", len(records) + len(cat_records), supabase_url, service_key)

    elapsed = time.time() - t0
    print(f"✅ 완료 ({elapsed:.1f}s)")


if __name__ == "__main__":
    main()
