"""
교원 ERP NM_ITEM 패턴 분석 — 재단지시서 vs ERP 매출의 표기 차이 파악.
"""
from pathlib import Path
import re
import pymssql

ENV = Path("/Users/jack/dev/gabwoo/견적계산기/.env.local")


def load_env():
    env = {}
    for line in ENV.read_text().splitlines():
        line = line.strip()
        if line and "=" in line and not line.startswith("#"):
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip()
    return env


def main():
    env = load_env()
    conn = pymssql.connect(
        server=env["ERP_HOST"], port=int(env.get("ERP_PORT", 1433)),
        user=env["ERP_USER"], password=env["ERP_PASSWORD"],
        database=env["ERP_DATABASE"], login_timeout=10,
    )
    cur = conn.cursor(as_dict=True)

    print("=" * 80)
    print("교원 ERP 매출 라인 NM_ITEM 샘플 (2025-2026, 30개)")
    print("=" * 80)
    cur.execute("""
        SELECT TOP 30 h.DT_SALES, c.NM_CUST, i.NM_ITEM, CAST(l.AM AS BIGINT) am
        FROM SAL_SALESL l
        INNER JOIN SAL_SALESH h ON h.NO_SALES = l.NO_SALES
        INNER JOIN MAS_CUST c ON c.CD_CUST = h.CD_CUST
        LEFT JOIN PRT_ITEM i ON i.CD_ITEM = l.CD_ITEM
        WHERE (h.DT_SALES LIKE '2025%' OR h.DT_SALES LIKE '2026%')
          AND h.CD_CUST_OWN='10000' AND h.ST_SALES='Y' AND h.AM > 0
          AND c.NM_CUST LIKE '%교원%'
        ORDER BY h.DT_SALES DESC
    """)
    rows = cur.fetchall()
    for r in rows:
        print(f"  [{r['DT_SALES']}] {r['NM_CUST']:<10} ₩{int(r['am'] or 0):>12,} {r['NM_ITEM']}")

    # 정교재/전집/구몬 등 키워드별 분포
    print("\n" + "=" * 80)
    print("교원 NM_ITEM 키워드별 라인 수 (2025-2026)")
    print("=" * 80)
    keywords = ["정교재", "전집", "구몬", "한자", "수학", "일어", "중국어", "성장노트", "호시탐탐", "월간", "월일자"]
    for kw in keywords:
        cur.execute("""
            SELECT COUNT(*) cnt
            FROM SAL_SALESL l
            INNER JOIN SAL_SALESH h ON h.NO_SALES = l.NO_SALES
            INNER JOIN MAS_CUST c ON c.CD_CUST = h.CD_CUST
            LEFT JOIN PRT_ITEM i ON i.CD_ITEM = l.CD_ITEM
            WHERE (h.DT_SALES LIKE '2025%' OR h.DT_SALES LIKE '2026%')
              AND h.CD_CUST_OWN='10000' AND h.ST_SALES='Y' AND h.AM > 0
              AND c.NM_CUST LIKE '%교원%'
              AND i.NM_ITEM LIKE %s
        """, (f"%{kw}%",))
        n = cur.fetchone()["cnt"]
        print(f"   '{kw}': {n}건")

    # 교원 NM_ITEM 전체 다운로드 (매칭 룰 설계용)
    print("\n" + "=" * 80)
    print("교원 NM_ITEM 전체 (2025-2026) — 첫 50개 + 마지막 20개")
    print("=" * 80)
    cur.execute("""
        SELECT i.NM_ITEM, COUNT(*) cnt, SUM(CAST(l.AM AS BIGINT)) am
        FROM SAL_SALESL l
        INNER JOIN SAL_SALESH h ON h.NO_SALES = l.NO_SALES
        INNER JOIN MAS_CUST c ON c.CD_CUST = h.CD_CUST
        LEFT JOIN PRT_ITEM i ON i.CD_ITEM = l.CD_ITEM
        WHERE (h.DT_SALES LIKE '2025%' OR h.DT_SALES LIKE '2026%')
          AND h.CD_CUST_OWN='10000' AND h.ST_SALES='Y' AND h.AM > 0
          AND c.NM_CUST LIKE '%교원%'
          AND i.NM_ITEM IS NOT NULL
        GROUP BY i.NM_ITEM
        ORDER BY SUM(CAST(l.AM AS BIGINT)) DESC
    """)
    all_items = cur.fetchall()
    print(f"\n   교원 distinct NM_ITEM: {len(all_items)}개")
    print(f"\n   매출 Top 30 NM_ITEM:")
    for r in all_items[:30]:
        print(f"      ₩{int(r['am'] or 0):>14,} ({r['cnt']:>3}건) {r['NM_ITEM']}")

    conn.close()


if __name__ == "__main__":
    main()
