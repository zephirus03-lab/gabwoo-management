"""
ERP(MSSQL) → Supabase(경영 대시보드 백엔드) 동기화 스크립트입니다.

경영 대시보드(index.html)는 Supabase btbqzbrtsmwoolurpqgx 프로젝트의
erp_quotes / erp_quote_lines 테이블을 읽습니다. 원래는 사용자가 엑셀을
업로드해야 했지만, 이 스크립트가 ERP DB를 직접 조회해서 upsert하므로
수동 업로드 없이 대시보드가 최신 데이터를 보여줄 수 있게 합니다.

동기화 범위: 최근 40개월 (KPI-4 "과거 3년" 신규/기존 판별 + 여유)

v1 개선 (2026-04-15, CTO 리뷰 반영):
- TOP-2: 라인 교체를 replace_quote_lines RPC로 트랜잭션화 (DELETE+INSERT 원자적)
- TOP-1: 배치 시작/종료 시 sync_log 테이블에 기록 → 대시보드 상단 "데이터 기준" 배너
"""

import json
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

# 경영 대시보드가 쓰는 Supabase 프로젝트 ID
DASHBOARD_PROJECT_REF = "btbqzbrtsmwoolurpqgx"

# 매핑 테이블
COMPANY_MAP = {"7000": "갑우문화사", "8000": "비피앤피"}
APPROVAL_MAP = {"R": "승인", "P": "작성", "F": "확정"}

# 동기화 범위 (오늘 기준 N개월 이전부터)
# v1: KPI-4 "과거 3년 신규/기존 판별" + KPI-5 "직전 2년 누적" 모두 커버하도록 40개월
SYNC_MONTHS_BACK = 40

# sync_log 테이블에 기록할 때 쓰는 작업 이름
JOB_NAME = "erp_quotes"


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


def get_supabase_keys(access_token: str, project_ref: str) -> tuple[str, str]:
    """Management API로 anon/service_role 키를 가져옵니다."""
    resp = requests.get(
        f"https://api.supabase.com/v1/projects/{project_ref}/api-keys",
        headers={"Authorization": f"Bearer {access_token}"},
    )
    resp.raise_for_status()
    anon, svc = "", ""
    for k in resp.json():
        if k["name"] == "anon":
            anon = k["api_key"]
        elif k["name"] == "service_role":
            svc = k["api_key"]
    return anon, svc


def fetch_erp_data(env: dict, cutoff_date: str) -> tuple[dict, list]:
    """PRT_ESTH + PRT_ESTL + MAS_EMP를 JOIN해서 견적 데이터를 가져옵니다.

    Returns:
        quotes_dict: {quote_number: header_row_dict}
        lines_list: [line_row_dict, ...]
    """
    conn = pymssql.connect(
        server=env["ERP_HOST"], port=int(env.get("ERP_PORT", 1433)),
        user=env["ERP_USER"], password=env["ERP_PASSWORD"],
        database=env["ERP_DATABASE"], login_timeout=10,
    )
    cursor = conn.cursor(as_dict=True)

    # 1) 헤더
    # AM_K는 항상 0이라 쓰지 않고, AM_SUM(VAT 포함 견적 총액)을 사용합니다.
    cursor.execute(f"""
        SELECT
            h.NO_EST, h.CD_FIRM, h.DT_EST, h.NM_EST, h.NM_PARTNER,
            h.CD_EMP, e.NM_EMP AS NM_EMP,
            h.CD_DEPT, h.QT, h.AM_SUM, h.AM_SUPPLY,
            h.YN_APP, h.NO_SO, h.TP_ITEM, h.FG_BIND
        FROM PRT_ESTH h
        LEFT JOIN MAS_EMP e ON h.CD_EMP = e.CD_EMP
        WHERE h.DT_EST >= '{cutoff_date}'
    """)
    quotes = {}
    for r in cursor.fetchall():
        qnum = r["NO_EST"]
        if not qnum or qnum in quotes:
            continue
        qdate = None
        dt = str(r["DT_EST"] or "").strip()
        if len(dt) == 8 and dt.isdigit():
            qdate = f"{dt[:4]}-{dt[4:6]}-{dt[6:8]}"
        quotes[qnum] = {
            "quote_number": qnum,
            "quote_date": qdate,
            "customer_name": (r["NM_PARTNER"] or "").strip() or None,
            "sales_person": (r["NM_EMP"] or "").strip() or None,
            "department": (r["CD_DEPT"] or "").strip() or None,
            "company": COMPANY_MAP.get(str(r["CD_FIRM"] or "").strip(), "갑우문화사"),
            "product_type": (r["TP_ITEM"] or "").strip() or None,
            "binding_name": (r["FG_BIND"] or "").strip() or None,
            "copies": int(r["QT"]) if r["QT"] else None,
            # 견적금액: AM_SUM(VAT 포함) 우선, 없으면 AM_SUPPLY(공급가)로 폴백
            "quote_amount": float(r["AM_SUM"] or r["AM_SUPPLY"] or 0),
            "quote_title": (r["NM_EST"] or "").strip() or None,
            "product_name": (r["NM_EST"] or "").strip() or None,
            "order_number": (r["NO_SO"] or "").strip() or None,
            "approval_status": APPROVAL_MAP.get(str(r["YN_APP"] or "").strip(), "작성"),
            "source_file": "ERP DB sync",
        }
    print(f"   → 견적 헤더: {len(quotes):,}건")

    # 2) 라인
    cursor.execute(f"""
        SELECT
            l.NO_EST, l.NO_LINE,
            l.FG_EST, l.CD_ITEM, l.NM_ITEM, l.DC_ITEM_SPEC,
            l.DC_ITEM_UNIT, l.QT_DASU, l.QT, l.UM, l.AM,
            l.RT_DISCOUNT, l.AM_SUPPLY, l.DC_RMK
        FROM PRT_ESTL l
        JOIN PRT_ESTH h ON l.NO_EST = h.NO_EST
                       AND l.CD_FIRM = h.CD_FIRM
                       AND l.NO_HST = h.NO_HST
        WHERE h.DT_EST >= '{cutoff_date}'
    """)
    lines = []
    for r in cursor.fetchall():
        qnum = r["NO_EST"]
        if qnum not in quotes:
            continue
        lines.append({
            "_quote_number": qnum,  # upsert 후 quote_id 매핑에 사용
            "line_seq": int(r["NO_LINE"] or 0) + 1,
            "category": (r["FG_EST"] or "").strip() or None,
            "usage_type": None,
            "item": (r["CD_ITEM"] or "").strip() or None,
            "spec": (r["DC_ITEM_SPEC"] or "").strip() or None,
            "unit": (r["DC_ITEM_UNIT"] or "").strip() or None,
            "base_qty": None,
            "sheets": float(r["QT_DASU"]) if r["QT_DASU"] else None,
            "quantity_r": float(r["QT"]) if r["QT"] else None,
            "colors": None,
            "unit_price": float(r["UM"]) if r["UM"] else None,
            "amount": float(r["AM"]) if r["AM"] else None,
            "discount_rate": float(r["RT_DISCOUNT"]) if r["RT_DISCOUNT"] else None,
            "final_amount": float(r["AM_SUPPLY"]) if r["AM_SUPPLY"] else None,
            "note": (r["DC_RMK"] or "").strip() or None,
        })
    conn.close()
    print(f"   → 견적 라인: {len(lines):,}건")
    return quotes, lines


def chunk_iter(iterable, size):
    buf = []
    for item in iterable:
        buf.append(item)
        if len(buf) >= size:
            yield buf
            buf = []
    if buf:
        yield buf


def log_sync(project_ref: str, service_key: str, job_name: str, status: str,
             rows: int = None, duration_sec: float = None, error_msg: str = None):
    """sync_log 테이블에 배치 실행 결과를 기록합니다.

    대시보드 상단 "데이터 기준: YYYY-MM-DD HH:MM" 배너가 이 값을 읽습니다.
    실패해도 main flow를 깨지 않도록 예외는 조용히 무시합니다.
    """
    try:
        payload = {
            "job_name": job_name,
            "status": status,
            "rows_affected": rows,
            "duration_sec": round(duration_sec, 2) if duration_sec is not None else None,
            "error_msg": (error_msg or "")[:500] if error_msg else None,
        }
        resp = requests.post(
            f"https://{project_ref}.supabase.co/rest/v1/sync_log",
            headers={
                "apikey": service_key,
                "Authorization": f"Bearer {service_key}",
                "Content-Type": "application/json",
                "Prefer": "return=minimal",
            },
            json=payload,
            timeout=15,
        )
        if resp.status_code not in (200, 201, 204):
            print(f"   ⚠️ sync_log 기록 실패 ({resp.status_code}): {resp.text[:200]}")
    except Exception as e:
        print(f"   ⚠️ sync_log 기록 중 예외 (무시): {e}")


def supabase_request(method: str, url: str, service_key: str, max_retries: int = 5, **kwargs) -> requests.Response:
    """재시도 로직 포함한 Supabase REST 요청입니다. 502/503/504는 백오프 재시도."""
    import time
    headers = kwargs.pop("headers", {})
    headers.update({
        "apikey": service_key,
        "Authorization": f"Bearer {service_key}",
        "Content-Type": "application/json",
    })
    for attempt in range(max_retries):
        try:
            resp = requests.request(method, url, headers=headers, timeout=60, **kwargs)
            if resp.status_code in (502, 503, 504, 429):
                wait = 2 ** attempt  # 1, 2, 4, 8, 16초
                print(f"\n   ⏳ {resp.status_code} — {wait}초 후 재시도 ({attempt+1}/{max_retries})")
                time.sleep(wait)
                continue
            return resp
        except (requests.Timeout, requests.ConnectionError) as e:
            wait = 2 ** attempt
            print(f"\n   ⏳ 네트워크 오류 ({e}) — {wait}초 후 재시도 ({attempt+1}/{max_retries})")
            time.sleep(wait)
    # 마지막 시도 실패 시 예외
    resp.raise_for_status()
    return resp


def upsert_quotes(quotes: dict, project_ref: str, service_key: str) -> dict:
    """erp_quotes에 upsert하고 quote_number → id 매핑을 반환합니다."""
    base = f"https://{project_ref}.supabase.co/rest/v1"
    rows = list(quotes.values())

    # Upsert (onConflict=quote_number)
    total = 0
    for chunk in chunk_iter(rows, 500):
        resp = supabase_request(
            "POST", f"{base}/erp_quotes?on_conflict=quote_number",
            service_key,
            headers={"Prefer": "resolution=merge-duplicates,return=minimal"},
            json=chunk,
        )
        if resp.status_code not in (200, 201, 204):
            print(f"❌ upsert 실패 ({resp.status_code}): {resp.text[:500]}")
            sys.exit(1)
        total += len(chunk)
        print(f"   upsert 진행: {total:,} / {len(rows):,}건", end="\r")
    print(f"\n   ✅ {total:,}건 upsert 완료")

    # quote_number → id 매핑 (최근 N개월치 전체를 페이징으로 조회)
    print("   id 매핑 조회 중...")
    id_map = {}
    page_from = 0
    page_size = 1000
    target_set = set(quotes.keys())
    while True:
        resp = supabase_request(
            "GET",
            f"{base}/erp_quotes?select=id,quote_number&order=quote_number.asc",
            service_key,
            headers={"Range": f"{page_from}-{page_from + page_size - 1}"},
        )
        if resp.status_code not in (200, 206):
            print(f"❌ id 조회 실패 ({resp.status_code}): {resp.text[:300]}")
            sys.exit(1)
        data = resp.json()
        if not data:
            break
        for row in data:
            if row["quote_number"] in target_set:
                id_map[row["quote_number"]] = row["id"]
        if len(data) < page_size:
            break
        page_from += page_size
    print(f"   ✅ id 매핑 {len(id_map):,}건 (대상 {len(target_set):,}건)")
    return id_map


def replace_lines(lines: list, id_map: dict, project_ref: str, service_key: str):
    """quote별 라인을 트랜잭션 안전하게 교체합니다 (CTO 리뷰 TOP-2 대응).

    기존 구현: DELETE 배치 → INSERT 배치 2단계. 중간 실패 시 라인 영구 손실.
    v1 개선: replace_quote_lines RPC 호출. Postgres 트랜잭션으로 DELETE+INSERT 원자화.
    청크 크기는 한 번에 전송 가능한 JSON 크기를 고려해 quote_id 150개씩.
    """
    base = f"https://{project_ref}.supabase.co/rest/v1"

    # quote_number → 매핑된 id로 변환 + _quote_number 제거
    lines_by_qid: dict = {}
    skipped = 0
    for line in lines:
        qid = id_map.get(line["_quote_number"])
        if not qid:
            skipped += 1
            continue
        row = {k: v for k, v in line.items() if k != "_quote_number"}
        row["quote_id"] = qid
        lines_by_qid.setdefault(qid, []).append(row)

    quote_ids_all = list(lines_by_qid.keys())
    total_lines = sum(len(v) for v in lines_by_qid.values())
    print(f"   라인 교체 대상: {len(quote_ids_all):,}개 견적 / {total_lines:,}개 라인 (스킵 {skipped})")

    # RPC에 한 번에 보내는 청크 크기 (quote_id 기준)
    CHUNK_SIZE = 150
    total_deleted = 0
    total_inserted = 0

    for i, chunk in enumerate(chunk_iter(quote_ids_all, CHUNK_SIZE), 1):
        payload = {
            "quote_ids": chunk,
            "lines": [row for qid in chunk for row in lines_by_qid[qid]],
        }
        resp = supabase_request(
            "POST",
            f"{base}/rpc/replace_quote_lines",
            service_key,
            json={"p_payload": payload},
        )
        if resp.status_code not in (200, 201, 204):
            # 트랜잭션 덕분에 이 청크는 자동 롤백됨. 전체 배치는 실패 처리.
            raise RuntimeError(
                f"replace_quote_lines RPC 실패 ({resp.status_code}): {resp.text[:500]}"
            )
        result = resp.json()
        if isinstance(result, list) and result:
            total_deleted += result[0].get("deleted_count", 0)
            total_inserted += result[0].get("inserted_count", 0)

        done = min(i * CHUNK_SIZE, len(quote_ids_all))
        print(f"   라인 교체 진행: {done:,} / {len(quote_ids_all):,}개 견적", end="\r")

    print(f"\n   ✅ 삭제 {total_deleted:,}건 / 신규 {total_inserted:,}건 (트랜잭션 안전)")


def fetch_erp_sales(env: dict, cutoff_date: str) -> list:
    """SAL_SALESH + MAS_CUST + MAS_EMP JOIN으로 실제 매출 데이터를 가져옵니다."""
    conn = pymssql.connect(
        server=env["ERP_HOST"], port=int(env.get("ERP_PORT", 1433)),
        user=env["ERP_USER"], password=env["ERP_PASSWORD"],
        database=env["ERP_DATABASE"], login_timeout=10,
    )
    cursor = conn.cursor(as_dict=True)
    cursor.execute(f"""
        SELECT
            h.NO_SALES, h.CD_FIRM, h.DT_SALES,
            h.CD_CUST, c.NM_CUST AS CUST_NAME,
            h.CD_EMP, e.NM_EMP AS EMP_NAME,
            h.CD_DEPT,
            h.AM, h.AM_VAT, h.AM_K,
            h.ST_SALES, h.YN_APP
        FROM SAL_SALESH h
        LEFT JOIN MAS_CUST c ON h.CD_CUST = c.CD_CUST AND h.CD_FIRM = c.CD_FIRM
        LEFT JOIN MAS_EMP e ON h.CD_EMP = e.CD_EMP
        WHERE h.DT_SALES >= '{cutoff_date}'
    """)
    rows = []
    for r in cursor.fetchall():
        if not r["NO_SALES"]:
            continue
        # 매출일 YYYYMMDD → YYYY-MM-DD
        dt = str(r["DT_SALES"] or "").strip()
        if len(dt) != 8 or not dt.isdigit():
            continue
        sdate = f"{dt[:4]}-{dt[4:6]}-{dt[6:8]}"

        rows.append({
            "sales_number": r["NO_SALES"],
            "sales_date": sdate,
            "customer_code": (r["CD_CUST"] or "").strip() or None,
            "customer_name": (r["CUST_NAME"] or "").strip() or None,
            "sales_person_code": (r["CD_EMP"] or "").strip() or None,
            "sales_person": (r["EMP_NAME"] or "").strip() or None,
            "department": (r["CD_DEPT"] or "").strip() or None,
            "company": COMPANY_MAP.get(str(r["CD_FIRM"] or "").strip(), "갑우문화사"),
            "supply_amount": float(r["AM"] or 0),
            "vat_amount": float(r["AM_VAT"] or 0),
            "total_amount": float(r["AM_K"] or 0),
            "sales_status": (r["ST_SALES"] or "").strip() or None,
            "approval_status": (r["YN_APP"] or "").strip() or None,
            "firm_code": str(r["CD_FIRM"] or "").strip() or None,
        })
    conn.close()
    return rows


def upsert_sales(rows: list, project_ref: str, service_key: str):
    """erp_sales 테이블에 upsert합니다."""
    base = f"https://{project_ref}.supabase.co/rest/v1"
    total = 0
    for chunk in chunk_iter(rows, 500):
        resp = supabase_request(
            "POST", f"{base}/erp_sales?on_conflict=sales_number",
            service_key,
            headers={"Prefer": "resolution=merge-duplicates,return=minimal"},
            json=chunk,
        )
        if resp.status_code not in (200, 201, 204):
            print(f"❌ upsert 실패 ({resp.status_code}): {resp.text[:500]}")
            sys.exit(1)
        total += len(chunk)
        print(f"   upsert 진행: {total:,} / {len(rows):,}건", end="\r")
    print(f"\n   ✅ {total:,}건 upsert 완료")


def sync():
    """ERP → Supabase 동기화 메인.

    전체를 try/except로 감싸서 성공·실패 모두 sync_log에 기록합니다.
    대시보드 상단 배너가 이 로그를 읽어 "데이터 기준 시각"을 표시합니다.
    """
    if not ENV_FILE.exists():
        print(f"❌ .env.local 없음: {ENV_FILE}")
        sys.exit(1)
    env = load_env(ENV_FILE)

    # 1. Supabase 키 확보 (sync_log 기록에도 필요)
    print("🔑 Supabase 키 조회 중...")
    access_token = env["SUPABASE_ACCESS_TOKEN"]
    anon_key, service_key = get_supabase_keys(access_token, DASHBOARD_PROJECT_REF)
    if not service_key:
        print("❌ service_role 키를 가져올 수 없습니다.")
        sys.exit(1)
    print(f"   ✅ {DASHBOARD_PROJECT_REF} 프로젝트 접근 확인")

    # 2. 본 작업 (타이머 + sync_log 기록)
    t_start = time.time()
    try:
        cutoff_dt = (datetime.now() - timedelta(days=SYNC_MONTHS_BACK * 31)).strftime("%Y%m%d")
        print(f"\n📥 ERP 조회 중 (견적일 >= {cutoff_dt}, 최근 {SYNC_MONTHS_BACK}개월)...")
        quotes, lines = fetch_erp_data(env, cutoff_dt)

        # 3. Supabase 견적/라인 동기화
        print(f"\n📤 Supabase 견적 upsert 중...")
        id_map = upsert_quotes(quotes, DASHBOARD_PROJECT_REF, service_key)

        print(f"\n📤 Supabase 견적라인 교체 중 (트랜잭션 RPC)...")
        replace_lines(lines, id_map, DASHBOARD_PROJECT_REF, service_key)

        # 4. SAL_SALESH(실제 매출) 동기화
        print(f"\n📥 ERP 매출(SAL_SALESH) 조회 중 (매출일 >= {cutoff_dt})...")
        sales_rows = fetch_erp_sales(env, cutoff_dt)
        print(f"   → 매출 헤더: {len(sales_rows):,}건")

        print(f"\n📤 Supabase erp_sales upsert 중...")
        upsert_sales(sales_rows, DASHBOARD_PROJECT_REF, service_key)

        duration = time.time() - t_start
        print(f"\n✅ 동기화 완료! ({duration:.1f}초)")
        print(f"   견적 {len(quotes):,}건 / 견적라인 {len(lines):,}건 / 매출 {len(sales_rows):,}건")

        # 성공 로그 (대시보드 배너가 이 시각을 "데이터 기준"으로 표시)
        log_sync(
            DASHBOARD_PROJECT_REF, service_key,
            job_name=JOB_NAME, status="success",
            rows=len(quotes) + len(sales_rows), duration_sec=duration,
        )

    except Exception as e:
        duration = time.time() - t_start
        err_msg = f"{type(e).__name__}: {e}"
        print(f"\n❌ 동기화 실패 ({duration:.1f}초): {err_msg}")

        # 실패 로그 (배너가 🔴 경고 표시)
        log_sync(
            DASHBOARD_PROJECT_REF, service_key,
            job_name=JOB_NAME, status="failed",
            rows=None, duration_sec=duration, error_msg=err_msg,
        )
        raise  # daily_update.sh가 exit code로 실패 감지할 수 있도록


if __name__ == "__main__":
    sync()
