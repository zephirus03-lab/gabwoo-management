"""
YoY(전년 동월) 매출 변화를 분석해서 경영 대시보드 상단에 표시할
"3줄 해석"을 자동 생성하는 스크립트입니다.

- ERP가 아니라 이미 Supabase erp_sales에 동기화된 데이터를 분석합니다.
- 분석 관점:
  1. 거래처 이탈/축소 — 작년 동기에 있었는데 올해 사라지거나 크게 줄은 거래처 상위
  2. 영업자별 실적 변동 — 올해 전담 영업자의 매출 변화
  3. 평균 객단가 변화 — 거래처당·건당 평균 매출
- 결과는 dashboard_insights 테이블에 upsert됩니다.

생성 기준일: p_base_date (기본: 오늘)
비교 기간: 최근 2개월 (2개월 전부터 1개월 전까지) vs 전년 동기 2개월
"""

import json
import sys
from pathlib import Path
from datetime import datetime, date, timedelta
from collections import defaultdict

try:
    import requests
except ImportError:
    print("❌ requests 미설치: pip3 install requests")
    sys.exit(1)

ENV_FILE = Path("/Users/jack/dev/gabwoo/견적계산기/.env.local")
PROJECT_REF = "btbqzbrtsmwoolurpqgx"


def load_env(p: Path) -> dict:
    env = {}
    for line in p.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        env[k.strip()] = v.strip()
    return env


def get_service_key(access_token: str) -> str:
    resp = requests.get(
        f"https://api.supabase.com/v1/projects/{PROJECT_REF}/api-keys",
        headers={"Authorization": f"Bearer {access_token}"},
    )
    resp.raise_for_status()
    for k in resp.json():
        if k["name"] == "service_role":
            return k["api_key"]
    return ""


def sb_query(sql: str, access_token: str) -> list:
    """Management API로 SQL 실행 — 복잡한 집계에 사용."""
    resp = requests.post(
        f"https://api.supabase.com/v1/projects/{PROJECT_REF}/database/query",
        headers={"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"},
        json={"query": sql},
    )
    resp.raise_for_status()
    return resp.json()


def add_months(d: date, months: int) -> date:
    """월 단위 날짜 이동 (day=1로 정규화)."""
    y = d.year + (d.month - 1 + months) // 12
    m = (d.month - 1 + months) % 12 + 1
    return date(y, m, 1)


def fmt_won(n: float) -> str:
    n = float(n or 0)
    if abs(n) >= 1e8:
        return f"{n/1e8:.1f}억원"
    if abs(n) >= 1e4:
        return f"{n/1e4:,.0f}만원"
    return f"{n:,.0f}원"


def fmt_pct(cur: float, prev: float) -> str:
    if not prev:
        return "신규" if cur else "변동 없음"
    pct = (cur - prev) / prev * 100
    sign = "+" if pct >= 0 else ""
    return f"{sign}{pct:.1f}%"


def analyze(access_token: str, base_date: date, company: str = None) -> dict:
    """비교 기간: base_date 기준 최근 2개월 윈도우 vs 전년 동기."""
    # 올해 윈도우: 2개월 전 1일 ~ 이번달 1일
    this_end = date(base_date.year, base_date.month, 1)
    this_start = add_months(this_end, -2)
    # 작년 동기
    last_end = date(this_end.year - 1, this_end.month, 1)
    last_start = add_months(last_end, -2)

    company_clause = f"AND company = '{company}'" if company else ""

    # 전체 요약
    summary_sql = f"""
        SELECT
            (SELECT COALESCE(SUM(supply_amount),0) FROM erp_sales
             WHERE sales_status='Y' AND supply_amount>0 {company_clause}
             AND sales_date >= '{this_start}' AND sales_date < '{this_end}') AS this_total,
            (SELECT COALESCE(SUM(supply_amount),0) FROM erp_sales
             WHERE sales_status='Y' AND supply_amount>0 {company_clause}
             AND sales_date >= '{last_start}' AND sales_date < '{last_end}') AS last_total,
            (SELECT COUNT(*) FROM erp_sales
             WHERE sales_status='Y' AND supply_amount>0 {company_clause}
             AND sales_date >= '{this_start}' AND sales_date < '{this_end}') AS this_cnt,
            (SELECT COUNT(*) FROM erp_sales
             WHERE sales_status='Y' AND supply_amount>0 {company_clause}
             AND sales_date >= '{last_start}' AND sales_date < '{last_end}') AS last_cnt
    """
    s = sb_query(summary_sql, access_token)[0]
    this_total = float(s["this_total"])
    last_total = float(s["last_total"])
    this_cnt = int(s["this_cnt"])
    last_cnt = int(s["last_cnt"])

    total_delta = this_total - last_total
    total_pct = (total_delta / last_total * 100) if last_total else 0

    # 거래처별 YoY
    cust_sql = f"""
        WITH this_cust AS (
            SELECT customer_name, SUM(supply_amount) AS amt, COUNT(*) AS cnt
            FROM erp_sales
            WHERE sales_status='Y' AND supply_amount>0 {company_clause}
              AND sales_date >= '{this_start}' AND sales_date < '{this_end}'
            GROUP BY customer_name
        ),
        last_cust AS (
            SELECT customer_name, SUM(supply_amount) AS amt, COUNT(*) AS cnt
            FROM erp_sales
            WHERE sales_status='Y' AND supply_amount>0 {company_clause}
              AND sales_date >= '{last_start}' AND sales_date < '{last_end}'
            GROUP BY customer_name
        )
        SELECT
            COALESCE(t.customer_name, l.customer_name) AS name,
            COALESCE(t.amt, 0) AS this_amt,
            COALESCE(l.amt, 0) AS last_amt,
            COALESCE(t.cnt, 0) AS this_cnt,
            COALESCE(l.cnt, 0) AS last_cnt
        FROM this_cust t
        FULL OUTER JOIN last_cust l ON t.customer_name = l.customer_name
    """
    cust_rows = sb_query(cust_sql, access_token)
    # 감소 금액 기준 정렬
    cust_rows = [r for r in cust_rows if r.get("name")]
    for r in cust_rows:
        r["this_amt"] = float(r["this_amt"])
        r["last_amt"] = float(r["last_amt"])
        r["delta"] = r["this_amt"] - r["last_amt"]
    cust_rows.sort(key=lambda r: r["delta"])  # 가장 줄어든 순

    # 영업자별 YoY
    emp_sql = cust_sql.replace("customer_name", "sales_person")
    emp_rows = sb_query(emp_sql, access_token)
    emp_rows = [r for r in emp_rows if r.get("name")]
    for r in emp_rows:
        r["this_amt"] = float(r["this_amt"])
        r["last_amt"] = float(r["last_amt"])
        r["delta"] = r["this_amt"] - r["last_amt"]
    emp_rows.sort(key=lambda r: r["delta"])

    # 건당 평균(객단가) 변화
    this_avg = this_total / this_cnt if this_cnt else 0
    last_avg = last_total / last_cnt if last_cnt else 0

    insights = []

    # 인사이트 1: 총매출 YoY + 주요 원인 (이탈 거래처)
    top_declined = [r for r in cust_rows if r["delta"] < 0][:3]
    top_declined_sum = sum(r["delta"] for r in top_declined)
    lost_customers = [r for r in cust_rows if r["last_amt"] > 0 and r["this_amt"] == 0][:5]
    lost_total = sum(r["last_amt"] for r in lost_customers)

    period_label = f"{this_start.year}.{this_start.month:02d}~{(this_end - timedelta(days=1)).month:02d}월"
    if total_delta < 0:
        direction = "down"
        title = f"매출 {fmt_won(total_total := abs(total_delta))} 감소 ({fmt_pct(this_total, last_total)})"
        body_parts = [f"최근 2개월({period_label}) 확정 매출이 작년 동기 {fmt_won(last_total)} → 올해 {fmt_won(this_total)}로 줄었습니다."]
        if top_declined:
            names = ", ".join([f"{r['name'][:12]}({fmt_won(r['delta'])})" for r in top_declined[:2]])
            body_parts.append(f"가장 크게 줄어든 거래처: {names}.")
        insights.append({"title": title, "body": " ".join(body_parts), "direction": direction})
    else:
        direction = "up"
        title = f"매출 {fmt_won(total_delta)} 증가 ({fmt_pct(this_total, last_total)})"
        body_parts = [f"최근 2개월({period_label}) 확정 매출이 작년 동기 대비 늘었습니다."]
        top_grown = [r for r in sorted(cust_rows, key=lambda r: -r['delta']) if r["delta"] > 0][:2]
        if top_grown:
            names = ", ".join([f"{r['name'][:12]}(+{fmt_won(r['delta'])})" for r in top_grown])
            body_parts.append(f"견인 거래처: {names}.")
        insights.append({"title": title, "body": " ".join(body_parts), "direction": direction})

    # 인사이트 2: 이탈/급감 거래처 구체 금액
    if lost_customers:
        names = ", ".join([r["name"][:15] for r in lost_customers[:3]])
        insights.append({
            "title": f"거래 중단 {len(lost_customers)}곳 — {fmt_won(lost_total)} 공백",
            "body": f"작년 동기에는 매출이 있었지만 올해 같은 기간 매출이 0원인 거래처: {names} 등. "
                    f"영업 재접촉 또는 이탈 사유 확인이 필요합니다.",
            "direction": "down",
        })
    elif top_declined:
        r = top_declined[0]
        insights.append({
            "title": f"{r['name'][:15]} 매출 {fmt_pct(r['this_amt'], r['last_amt'])} 축소",
            "body": f"작년 {fmt_won(r['last_amt'])} → 올해 {fmt_won(r['this_amt'])}. "
                    f"주요 거래처의 물량 축소가 총매출 하락에 기여했습니다.",
            "direction": "down",
        })

    # 인사이트 3: 객단가 or 영업자 성과 변화
    if this_avg and last_avg:
        avg_pct = (this_avg - last_avg) / last_avg * 100
        if abs(avg_pct) > 5:
            direction = "down" if avg_pct < 0 else "up"
            title = f"건당 평균 매출 {fmt_pct(this_avg, last_avg)} — {fmt_won(this_avg)}"
            body = (f"작년 동기 건당 {fmt_won(last_avg)} → 올해 {fmt_won(this_avg)}. "
                    f"거래 건수는 {last_cnt}→{this_cnt}건. ")
            if avg_pct < 0:
                body += "동일 거래처 내 물량 감소 또는 단가 인하 가능성이 있습니다."
            else:
                body += "고가 수주 비중이 늘어나는 긍정 신호입니다."
            insights.append({"title": title, "body": body, "direction": direction})
        else:
            # 영업자 성과 변화
            worst_emp = emp_rows[0] if emp_rows and emp_rows[0]["delta"] < 0 else None
            if worst_emp:
                insights.append({
                    "title": f"영업담당 {worst_emp['name']} 매출 {fmt_pct(worst_emp['this_amt'], worst_emp['last_amt'])}",
                    "body": f"작년 동기 {fmt_won(worst_emp['last_amt'])} → 올해 {fmt_won(worst_emp['this_amt'])}. "
                            f"담당 거래처 상태 및 활동 점검이 필요합니다.",
                    "direction": "down",
                })
    # 최소 3개 보장 (너무 적으면 기본 문구)
    while len(insights) < 3:
        insights.append({
            "title": "추가 분석 필요",
            "body": "세부 지표(단가·물량·거래처별)를 추가 확인하면 더 정확한 해석이 가능합니다.",
            "direction": "neutral",
        })
    insights = insights[:3]

    meta = {
        "period_this": f"{this_start} ~ {this_end - timedelta(days=1)}",
        "period_last": f"{last_start} ~ {last_end - timedelta(days=1)}",
        "this_total": this_total,
        "last_total": last_total,
        "total_delta_pct": total_pct,
        "this_cnt": this_cnt,
        "last_cnt": last_cnt,
        "base_date": str(base_date),
    }

    return {"insights": insights, "meta": meta, "base_date": str(base_date), "company": company}


def upsert_insights(result: dict, service_key: str):
    base = f"https://{PROJECT_REF}.supabase.co/rest/v1"
    payload = {
        "base_date": result["base_date"],
        "company_filter": result["company"],
        "insights": result["insights"],
        "meta": result["meta"],
    }
    # 같은 base_date + company 조합은 덮어쓰기 대신 새로 insert — 이력 남기기
    resp = requests.post(
        f"{base}/dashboard_insights",
        headers={
            "apikey": service_key,
            "Authorization": f"Bearer {service_key}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal",
        },
        json=payload,
    )
    if resp.status_code not in (200, 201, 204):
        print(f"❌ insights insert 실패 ({resp.status_code}): {resp.text[:500]}")
        sys.exit(1)


def main():
    env = load_env(ENV_FILE)
    access_token = env["SUPABASE_ACCESS_TOKEN"]
    service_key = get_service_key(access_token)
    base_date = datetime.now().date()

    print(f"🔍 YoY 분석 중 (기준일 {base_date})...")
    result = analyze(access_token, base_date, company=None)

    print(f"\n📊 생성된 인사이트 (전체):")
    for i, it in enumerate(result["insights"], 1):
        print(f"  [{i}] {it['title']}")
        print(f"      {it['body']}\n")

    print("📤 Supabase dashboard_insights에 저장 중...")
    upsert_insights(result, service_key)
    print("   ✅ 완료")


if __name__ == "__main__":
    main()
