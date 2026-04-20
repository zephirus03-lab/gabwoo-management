"""
한국조폐공사 2026 Q1 매출 수동 보정 — 담당자 ERP 입력 누락분 반영

배경 (2026-04-20):
  한국조폐공사(V1389, 비피앤피 디지털사업부)는 담당자(김세미) 실수로
  2026년 1·2·3월 매출이 ERP에 입력되지 않아 대시보드에 빵꾸가 있음.
  영업이사가 월별 매출(부가세 포함) 숫자를 별도로 제공 →
  매출현황(디지털사업부_조폐공사)25년~.xlsx
  이 스크립트는 해당 월별 합산 금액을 erp_sales에 "MANUAL-" 시리얼로 1건씩
  upsert해 ERP 시스템을 오염시키지 않고 대시보드 수치만 보정합니다.

원칙:
  - 원본 ERP DB는 건드리지 않는다 (읽기 전용)
  - erp_sales에는 source_file='manual-kostamp-2026q1' 로 명시
  - sales_number 고유: MANUAL-KOSTAMP-2026-01/02/03
  - 부가세 포함 금액을 제공받았으므로 공급가액 = 총액 ÷ 1.1
  - 이후 ERP에 실제 입력되면 이 MANUAL 레코드는 사용자가 수동 삭제

실행 후:
  - 대시보드 Top20 월별 타일에 26.01~03 조폐공사 셀이 채워짐
  - 매출 집중도 / YoY 방향 지표가 정상 반영
"""
from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path

try:
    import requests
except ImportError:
    print("❌ pip3 install requests"); sys.exit(1)

ENV_FILE = Path("/Users/jack/dev/gabwoo/견적계산기/.env.local")
PROJECT = "btbqzbrtsmwoolurpqgx"

# 영업이사 제공 수치 (부가세 포함)
MONTHLY_VAT_INCLUDED = {
    "2026-01": 224_797_030,
    "2026-02": 130_672_260,
    "2026-03": 86_946_732,
}

CUSTOMER = {
    "customer_code": "V1389",
    "customer_name": "한국조폐공사",
    "company": "비피앤피",
    "firm_code": "7000",
    "sales_person": "김세미",
    "sales_person_code": "2018004",
    "department": "0105005",
}


def load_env():
    env = {}
    for line in ENV_FILE.read_text().splitlines():
        if "=" in line and not line.startswith("#"):
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip()
    return env


def get_service_key(token):
    r = requests.get(
        f"https://api.supabase.com/v1/projects/{PROJECT}/api-keys",
        headers={"Authorization": f"Bearer {token}"}, timeout=30,
    )
    r.raise_for_status()
    return next(k["api_key"] for k in r.json() if k["name"] == "service_role")


def build_rows():
    rows = []
    for ym, vat_incl in MONTHLY_VAT_INCLUDED.items():
        year, month = ym.split("-")
        # 월 마지막 날짜로 기록 (월합산 성격)
        from calendar import monthrange
        last_day = monthrange(int(year), int(month))[1]
        supply = round(vat_incl / 1.1)  # 공급가액
        vat = vat_incl - supply
        rows.append({
            "sales_number": f"MANUAL-KOSTAMP-{ym}",
            "sales_date": f"{year}-{month}-{last_day:02d}",
            "customer_code": CUSTOMER["customer_code"],
            "customer_name": CUSTOMER["customer_name"],
            "sales_person": CUSTOMER["sales_person"],
            "sales_person_code": CUSTOMER["sales_person_code"],
            "department": CUSTOMER["department"],
            "company": CUSTOMER["company"],
            "supply_amount": supply,
            "vat_amount": vat,
            "total_amount": vat_incl,
            "sales_status": "Y",
            "approval_status": "Y",
            "firm_code": CUSTOMER["firm_code"],
            "source_file": f"manual-kostamp-2026q1-{datetime.now():%Y%m%d}",
        })
    return rows


def upsert(rows, service_key):
    url = f"https://{PROJECT}.supabase.co/rest/v1/erp_sales?on_conflict=sales_number"
    headers = {
        "apikey": service_key,
        "Authorization": f"Bearer {service_key}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=representation",
    }
    r = requests.post(url, headers=headers, json=rows, timeout=60)
    if r.status_code >= 400:
        print(f"❌ 실패 ({r.status_code}): {r.text[:500]}")
        sys.exit(1)
    data = r.json()
    print(f"✅ upsert 성공: {len(data)}건")
    return data


def verify(service_key):
    url = f"https://{PROJECT}.supabase.co/rest/v1/erp_sales_confirmed"
    headers = {"apikey": service_key, "Authorization": f"Bearer {service_key}"}
    params = {
        "select": "sales_number,sales_date,customer_name,supply_amount,total_amount,source_file",
        "customer_name": "like.*조폐*",
        "sales_date": "gte.2026-01-01",
        "order": "sales_date.asc",
    }
    r = requests.get(url, headers=headers, params=params, timeout=30)
    r.raise_for_status()
    rows = r.json()
    print(f"\n📊 확정 뷰(erp_sales_confirmed) 조폐공사 2026년 레코드: {len(rows)}건")
    total_supply = 0
    for row in rows:
        print(f"  {row['sales_date']} | {row['sales_number']:25s} | 공급 {row['supply_amount']:>12,.0f} | 총액 {row['total_amount']:>12,.0f} | {row['source_file']}")
        total_supply += row["supply_amount"]
    print(f"  ─────────────────────────────────────────────────────────────────────")
    print(f"  공급가액 합: {total_supply:,.0f}원  ({total_supply/1e8:.2f}억)")


def main():
    env = load_env()
    service_key = get_service_key(env["SUPABASE_ACCESS_TOKEN"])
    print("🔑 service_role OK")
    rows = build_rows()
    print(f"\n입력 데이터 ({len(rows)}건):")
    for r in rows:
        print(f"  {r['sales_date']} | {r['sales_number']} | 공급 {r['supply_amount']:>12,} | VAT {r['vat_amount']:>11,} | 총 {r['total_amount']:>12,}")
    print()
    upsert(rows, service_key)
    verify(service_key)


if __name__ == "__main__":
    main()
