"""
매출 집중도 + 영업자 키맨 리스크 집계 스크립트.

Supabase erp_sales_confirmed 뷰(경영 대시보드와 동일 데이터 소스)에서
거래처/영업자별 매출을 집계하여 sales_concentration_{period}.json 파일로 출력합니다.

영업자 대시보드(sales-dashboard.html, 공개 JSON 구독 구조)가 이 JSON을 읽어
매출 규모 쏠림 경보를 상단에 표시합니다.

기간: 1개월 / 3개월 / 6개월 / 1년 (pricing_audit와 동일)
2026-04-15 작성
"""
import json
import sys
from pathlib import Path
from datetime import datetime, timedelta

try:
    import requests
except ImportError:
    print("❌ requests 패키지 필요: pip3 install requests")
    sys.exit(1)

SCRIPT_DIR = Path(__file__).parent
OUTPUT_DIR = SCRIPT_DIR / "output"
ENV_FILE = Path("/Users/jack/dev/gabwoo/견적계산기/.env.local")
DASHBOARD_PROJECT_REF = "btbqzbrtsmwoolurpqgx"  # 경영 대시보드 Supabase (erp_sales_confirmed 뷰)

# 집계 기간 (pricing_audit와 동일)
PERIODS = [
    ("1m", 30),
    ("3m", 92),
    ("6m", 183),
    ("1y", 365),
]

# 경보 임계값 (v1 스펙과 동일)
CONCENTRATION_TOP1_WARN = 20.0     # Top 1 거래처 >= 20% → 주황
CONCENTRATION_TOP5_WARN = 60.0     # Top 5 합계 >= 60% → 주황
KEYMAN_TOP1_WARN = 25.0            # 1인 영업자 >= 25% → 주황


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
    raise RuntimeError("service_role 키를 찾을 수 없습니다")


def fetch_sales(service_key: str, date_from: str) -> list:
    """erp_sales_confirmed 뷰에서 date_from 이후 모든 매출 페이징 조회."""
    base = f"https://{DASHBOARD_PROJECT_REF}.supabase.co/rest/v1/erp_sales_confirmed"
    headers = {"apikey": service_key, "Authorization": f"Bearer {service_key}"}
    all_rows = []
    page_from = 0
    page_size = 1000
    while True:
        params = {
            "select": "customer_name,sales_person,supply_amount,sales_date",
            "sales_date": f"gte.{date_from}",
            "supply_amount": "gt.0",
            "order": "sales_number.asc",
        }
        r = requests.get(base, headers={**headers, "Range": f"{page_from}-{page_from + page_size - 1}"},
                         params=params, timeout=60)
        r.raise_for_status()
        page = r.json()
        all_rows.extend(page)
        if len(page) < page_size:
            break
        page_from += page_size
    return all_rows


def aggregate(rows: list) -> dict:
    """거래처별 / 영업자별 집계."""
    cust_map = {}
    emp_map = {}
    total = 0.0
    for r in rows:
        amt = float(r.get("supply_amount") or 0)
        if amt <= 0:
            continue
        total += amt
        cn = (r.get("customer_name") or "").strip()
        if cn:
            cust_map[cn] = cust_map.get(cn, 0.0) + amt
        sp = (r.get("sales_person") or "").strip()
        if sp:
            emp_map[sp] = emp_map.get(sp, 0.0) + amt

    def top_n(m, n=10):
        items = sorted(m.items(), key=lambda x: -x[1])[:n]
        return [{"name": k, "amount": round(v), "pct": round(v / total * 100, 1) if total > 0 else 0}
                for k, v in items]

    cust_top = top_n(cust_map, 10)
    emp_top = top_n(emp_map, 10)

    cust_top1_pct = cust_top[0]["pct"] if cust_top else 0
    cust_top5_pct = round(sum(x["pct"] for x in cust_top[:5]), 1)
    cust_top10_pct = round(sum(x["pct"] for x in cust_top[:10]), 1)

    emp_top1 = emp_top[0] if emp_top else None
    emp_top1_pct = emp_top1["pct"] if emp_top1 else 0

    # HHI (허핀달지수) — 거래처 기준. 1만 이하(분산)/1만~1.8만(중간)/1.8만+(집중)
    hhi = round(sum((v / total * 100) ** 2 for v in cust_map.values()), 0) if total > 0 else 0

    return {
        "total_amount": round(total),
        "customer_count": len(cust_map),
        "salesperson_count": len(emp_map),
        "customer_top10": cust_top,
        "customer_top1_pct": cust_top1_pct,
        "customer_top5_pct": cust_top5_pct,
        "customer_top10_pct": cust_top10_pct,
        "customer_hhi": hhi,
        "customer_alert": cust_top1_pct >= CONCENTRATION_TOP1_WARN or cust_top5_pct >= CONCENTRATION_TOP5_WARN,
        "salesperson_top10": emp_top,
        "salesperson_top1_pct": emp_top1_pct,
        "salesperson_top1_name": emp_top1["name"] if emp_top1 else None,
        "keyman_alert": emp_top1_pct >= KEYMAN_TOP1_WARN,
    }


def main():
    if not ENV_FILE.exists():
        print(f"❌ .env.local 없음: {ENV_FILE}")
        sys.exit(1)
    env = load_env(ENV_FILE)

    print("🔑 Supabase 키 조회 중...")
    service_key = get_service_key(env["SUPABASE_ACCESS_TOKEN"], DASHBOARD_PROJECT_REF)
    print(f"   ✅ {DASHBOARD_PROJECT_REF} 접근 확인")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    now = datetime.now()

    for period_key, days in PERIODS:
        date_from = (now - timedelta(days=days)).date().isoformat()
        print(f"\n📥 [{period_key}] 매출 조회 중 ({date_from} 이후)...")
        rows = fetch_sales(service_key, date_from)
        print(f"   → {len(rows):,}건")

        if not rows:
            print(f"   ⚠️ 데이터 없음, {period_key} 스킵")
            continue

        agg = aggregate(rows)
        out = {
            "_meta": {
                "generated": now.strftime("%Y-%m-%d %H:%M"),
                "period_key": period_key,
                "period_days": days,
                "date_from": date_from,
                "date_to": now.date().isoformat(),
                "source": "Supabase erp_sales_confirmed (경영 대시보드와 동일)",
            },
            **agg,
        }

        out_file = OUTPUT_DIR / f"sales_concentration_{period_key}.json"
        out_file.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"   📝 {out_file.name}  총 {agg['total_amount']:,}원  "
              f"Top1 {agg['customer_top1_pct']}%  Top5 {agg['customer_top5_pct']}%  "
              f"키맨 {agg['salesperson_top1_name']} {agg['salesperson_top1_pct']}%")

    print("\n✅ 매출 집중도 집계 완료")


if __name__ == "__main__":
    main()
