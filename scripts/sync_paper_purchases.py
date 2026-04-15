"""
ERP(MSSQL) dbo.viewGabwoo_마감 → Supabase erp_paper_purchases 동기화 스크립트.

KPI-6(월별 용지 매입액) 데이터 원천입니다.
viewGabwoo_마감 뷰가 월별 용지 매입을 깔끔하게 집계해주므로 그대로 옮겨 담습니다.

동기화 범위: 최근 40개월 (sync_erp_to_supabase.py와 동일)

2026-04-15 작성 · CTO 리뷰 TOP-1, KPI-6 대응
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

DASHBOARD_PROJECT_REF = "btbqzbrtsmwoolurpqgx"
SYNC_MONTHS_BACK = 40
JOB_NAME = "paper_purchases"


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
    return ""


def chunk_iter(iterable, size):
    buf = []
    for item in iterable:
        buf.append(item)
        if len(buf) >= size:
            yield buf
            buf = []
    if buf:
        yield buf


def supabase_request(method, url, service_key, max_retries=5, **kwargs):
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
                wait = 2 ** attempt
                print(f"   ⏳ {resp.status_code} — {wait}초 후 재시도 ({attempt+1}/{max_retries})")
                time.sleep(wait)
                continue
            return resp
        except (requests.Timeout, requests.ConnectionError) as e:
            wait = 2 ** attempt
            print(f"   ⏳ 네트워크 오류 ({e}) — {wait}초 후 재시도 ({attempt+1}/{max_retries})")
            time.sleep(wait)
    resp.raise_for_status()
    return resp


def log_sync(project_ref, service_key, status, rows=None, duration_sec=None, error_msg=None):
    """sync_log 테이블 기록. 실패해도 main flow 깨지 않음."""
    try:
        payload = {
            "job_name": JOB_NAME,
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
            print(f"   ⚠️ sync_log 기록 실패 ({resp.status_code})")
    except Exception as e:
        print(f"   ⚠️ sync_log 기록 중 예외 (무시): {e}")


def fetch_paper_purchases(env: dict, cutoff_date: datetime) -> list:
    """viewGabwoo_마감에서 용지 매입 데이터를 가져옵니다."""
    conn = pymssql.connect(
        server=env["ERP_HOST"], port=int(env.get("ERP_PORT", 1433)),
        user=env["ERP_USER"], password=env["ERP_PASSWORD"],
        database=env["ERP_DATABASE"], login_timeout=15,
    )
    cursor = conn.cursor(as_dict=True)

    cursor.execute(
        """
        SELECT
            CAST(일자 AS date) AS purchase_date,
            CustKey AS po_number,
            NO_LINE AS line_seq,
            지종 AS paper_type,
            제조사명 AS maker_name,
            가로 AS width_mm,
            세로 AS height_mm,
            수량 AS quantity,
            단가 AS unit_price,
            공급가액 AS supply_amount,
            표준가 AS standard_price
        FROM dbo.viewGabwoo_마감
        WHERE 일자 >= %s AND CustKey IS NOT NULL AND NO_LINE IS NOT NULL
        """,
        (cutoff_date.strftime("%Y-%m-%d"),),
    )

    rows = []
    for r in cursor.fetchall():
        # 필수 필드 검증
        if not r["purchase_date"] or not r["po_number"]:
            continue
        rows.append({
            "purchase_date": r["purchase_date"].isoformat() if hasattr(r["purchase_date"], "isoformat") else str(r["purchase_date"]),
            "po_number": str(r["po_number"]).strip(),
            "line_seq": int(r["line_seq"]),
            "paper_type": int(r["paper_type"]) if r["paper_type"] is not None else None,
            "maker_name": (r["maker_name"] or "").strip() or None,
            "width_mm": float(r["width_mm"]) if r["width_mm"] is not None else None,
            "height_mm": float(r["height_mm"]) if r["height_mm"] is not None else None,
            "quantity": float(r["quantity"]) if r["quantity"] is not None else None,
            "unit_price": float(r["unit_price"]) if r["unit_price"] is not None else None,
            "supply_amount": float(r["supply_amount"]) if r["supply_amount"] is not None else None,
            "standard_price": float(r["standard_price"]) if r["standard_price"] is not None else None,
        })
    conn.close()
    return rows


def upsert_paper(rows: list, project_ref: str, service_key: str) -> int:
    """erp_paper_purchases에 upsert. 충돌 키: (po_number, line_seq).

    뷰가 같은 (po_number, line_seq)에 대해 중복 행을 반환할 수 있어
    같은 키 내에서 마지막 값이 이기도록 미리 dedup합니다.
    """
    # (po_number, line_seq) → row 최신값으로 dedup
    dedup_map: dict = {}
    for row in rows:
        key = (row["po_number"], row["line_seq"])
        dedup_map[key] = row
    deduped = list(dedup_map.values())
    dup_count = len(rows) - len(deduped)
    if dup_count > 0:
        print(f"   ℹ️ 중복 {dup_count:,}행 제거 (뷰에서 동일 키로 여러 행 반환)")

    base = f"https://{project_ref}.supabase.co/rest/v1"
    total = 0
    rows = deduped  # 이하 청크 루프가 dedup된 리스트를 쓰도록
    for chunk in chunk_iter(rows, 500):
        resp = supabase_request(
            "POST",
            f"{base}/erp_paper_purchases?on_conflict=po_number,line_seq",
            service_key,
            headers={"Prefer": "resolution=merge-duplicates,return=minimal"},
            json=chunk,
        )
        if resp.status_code not in (200, 201, 204):
            raise RuntimeError(
                f"erp_paper_purchases upsert 실패 ({resp.status_code}): {resp.text[:500]}"
            )
        total += len(chunk)
        print(f"   upsert 진행: {total:,} / {len(rows):,}건", end="\r")
    if rows:
        print()
    return total


def sync():
    if not ENV_FILE.exists():
        print(f"❌ .env.local 없음: {ENV_FILE}")
        sys.exit(1)
    env = load_env(ENV_FILE)

    print("🔑 Supabase 키 조회 중...")
    access_token = env["SUPABASE_ACCESS_TOKEN"]
    service_key = get_service_key(access_token, DASHBOARD_PROJECT_REF)
    if not service_key:
        print("❌ service_role 키를 가져올 수 없습니다.")
        sys.exit(1)
    print(f"   ✅ {DASHBOARD_PROJECT_REF} 접근 확인")

    t_start = time.time()
    try:
        cutoff = datetime.now() - timedelta(days=SYNC_MONTHS_BACK * 31)
        print(f"\n📥 ERP 용지매입 조회 중 (일자 >= {cutoff.strftime('%Y-%m-%d')}, 최근 {SYNC_MONTHS_BACK}개월)...")
        rows = fetch_paper_purchases(env, cutoff)
        print(f"   → 용지 매입: {len(rows):,}행")

        if rows:
            print(f"\n📤 Supabase erp_paper_purchases upsert 중...")
            total = upsert_paper(rows, DASHBOARD_PROJECT_REF, service_key)
        else:
            total = 0

        duration = time.time() - t_start
        print(f"\n✅ 용지매입 동기화 완료! ({duration:.1f}초, {total:,}건)")

        log_sync(DASHBOARD_PROJECT_REF, service_key, "success",
                 rows=total, duration_sec=duration)

    except Exception as e:
        duration = time.time() - t_start
        err_msg = f"{type(e).__name__}: {e}"
        print(f"\n❌ 용지매입 동기화 실패 ({duration:.1f}초): {err_msg}")
        log_sync(DASHBOARD_PROJECT_REF, service_key, "failed",
                 rows=None, duration_sec=duration, error_msg=err_msg)
        raise


if __name__ == "__main__":
    sync()
