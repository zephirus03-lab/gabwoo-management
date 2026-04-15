"""
ERP(MSSQL) → Supabase(경영 대시보드 백엔드) 동기화 스크립트입니다.

경영 대시보드(index.html)는 Supabase btbqzbrtsmwoolurpqgx 프로젝트의
erp_quotes / erp_quote_lines 테이블을 읽습니다. 원래는 사용자가 엑셀을
업로드해야 했지만, 이 스크립트가 ERP DB를 직접 조회해서 upsert하므로
수동 업로드 없이 대시보드가 최신 데이터를 보여줄 수 있게 합니다.

동기화 범위: 최근 13개월(1년 + 버퍼)
"""

import json
import sys
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
SYNC_MONTHS_BACK = 13


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
    """해당 quote_id의 기존 라인을 삭제하고 새 라인을 insert합니다."""
    base = f"https://{project_ref}.supabase.co/rest/v1"

    # 대상 quote_id 수집
    quote_ids = list(set(id_map[l["_quote_number"]] for l in lines if l["_quote_number"] in id_map))

    # 1) 기존 라인 삭제
    print(f"   기존 라인 삭제 중... ({len(quote_ids):,}개 견적)")
    deleted = 0
    for chunk in chunk_iter(quote_ids, 200):
        id_list = ",".join(f'"{qid}"' for qid in chunk)
        resp = supabase_request(
            "DELETE",
            f"{base}/erp_quote_lines?quote_id=in.({id_list})",
            service_key,
        )
        if resp.status_code not in (200, 204):
            print(f"❌ 삭제 실패 ({resp.status_code}): {resp.text[:500]}")
            sys.exit(1)
        deleted += len(chunk)
        print(f"   삭제 진행: {deleted:,} / {len(quote_ids):,}개 견적", end="\r")
    print()

    # 2) 새 라인 insert
    insert_rows = []
    skipped = 0
    for line in lines:
        qid = id_map.get(line["_quote_number"])
        if not qid:
            skipped += 1
            continue
        row = {k: v for k, v in line.items() if k != "_quote_number"}
        row["quote_id"] = qid
        insert_rows.append(row)

    print(f"   라인 insert 중... ({len(insert_rows):,}건, 스킵 {skipped})")
    total = 0
    for chunk in chunk_iter(insert_rows, 500):
        resp = supabase_request(
            "POST", f"{base}/erp_quote_lines",
            service_key,
            headers={"Prefer": "return=minimal"},
            json=chunk,
        )
        if resp.status_code not in (200, 201, 204):
            print(f"❌ insert 실패 ({resp.status_code}): {resp.text[:500]}")
            sys.exit(1)
        total += len(chunk)
        print(f"   insert 진행: {total:,} / {len(insert_rows):,}건", end="\r")
    print(f"\n   ✅ {total:,}건 insert 완료")


def sync():
    if not ENV_FILE.exists():
        print(f"❌ .env.local 없음: {ENV_FILE}")
        sys.exit(1)
    env = load_env(ENV_FILE)

    # 1. Supabase 키 확보
    print("🔑 Supabase 키 조회 중...")
    access_token = env["SUPABASE_ACCESS_TOKEN"]
    anon_key, service_key = get_supabase_keys(access_token, DASHBOARD_PROJECT_REF)
    if not service_key:
        print("❌ service_role 키를 가져올 수 없습니다.")
        sys.exit(1)
    print(f"   ✅ {DASHBOARD_PROJECT_REF} 프로젝트 접근 확인")

    # 2. ERP 데이터 조회
    cutoff_dt = (datetime.now() - timedelta(days=SYNC_MONTHS_BACK * 31)).strftime("%Y%m%d")
    print(f"\n📥 ERP 조회 중 (견적일 >= {cutoff_dt})...")
    quotes, lines = fetch_erp_data(env, cutoff_dt)

    # 3. Supabase 동기화
    print(f"\n📤 Supabase upsert 중...")
    id_map = upsert_quotes(quotes, DASHBOARD_PROJECT_REF, service_key)

    print(f"\n📤 Supabase 라인 교체 중...")
    replace_lines(lines, id_map, DASHBOARD_PROJECT_REF, service_key)

    print("\n✅ 동기화 완료!")
    print(f"   견적 {len(quotes):,}건 / 라인 {len(lines):,}건 → {DASHBOARD_PROJECT_REF}")


if __name__ == "__main__":
    sync()
