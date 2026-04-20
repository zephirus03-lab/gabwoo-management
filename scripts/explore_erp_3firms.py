"""
ERP CD_CUST_OWN 기준 3사 분리 (10000=갑우 / 20000=비피앤피 / 30000=더원).
2025년 매출을 시나리오별로 뽑아 PDF(세금계산서)·감사매출과 3층 비교.
"""
from pathlib import Path
import pymssql

ENV = Path("/Users/jack/dev/gabwoo/견적계산기/.env.local")

OWN_NAME = {"10000": "갑우문화사", "20000": "비피앤피", "30000": "더원프린팅"}

# 감사보고서 2025 (백만원 → 원)
AUDIT = {"10000": 16_433_000_000, "20000": 22_158_000_000, "30000": None}

# PDF 세금계산서 2025 12월 누계 (원)
PDF_TOTAL = {"10000": 15_144_286_566, "20000": 21_806_851_511, "30000": 2_026_587_554}


def load_env():
    env = {}
    for line in ENV.read_text().splitlines():
        line = line.strip()
        if line and "=" in line and not line.startswith("#"):
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip()
    return env


def connect():
    env = load_env()
    return pymssql.connect(
        server=env["ERP_HOST"], port=int(env.get("ERP_PORT", 1433)),
        user=env["ERP_USER"], password=env["ERP_PASSWORD"],
        database=env["ERP_DATABASE"], login_timeout=10,
    )


def fmt(n):
    if n is None:
        return "-"
    return f"{n:>16,}"


def pct(a, b):
    if not b or not a:
        return ""
    return f"({a/b*100:.1f}%)"


def main():
    conn = connect()
    cur = conn.cursor(as_dict=True)

    # 1. CD_CUST_OWN 값 분포 확인
    print("=== CD_CUST_OWN 분포 (SAL_SALESH 2025) ===")
    cur.execute("""
        SELECT CD_CUST_OWN, COUNT(*) cnt, SUM(CAST(AM AS BIGINT)) am_all
        FROM SAL_SALESH
        WHERE DT_SALES LIKE '2025%'
        GROUP BY CD_CUST_OWN ORDER BY SUM(CAST(AM AS BIGINT)) DESC
    """)
    rows = cur.fetchall()
    for r in rows:
        name = OWN_NAME.get(str(r["CD_CUST_OWN"]) if r["CD_CUST_OWN"] else "", "?")
        print(f"  CD_CUST_OWN={str(r['CD_CUST_OWN']):>8} [{name:<10}] | 건수={r['cnt']:>6,} | AM합={int(r['am_all'] or 0):>18,}")

    # 2. 3사 × 시나리오 매트릭스
    print("\n=== 2025년 매출 3층 비교 (3사 × 시나리오) ===")
    print(f"{'회사':<10}{'시나리오':<30}{'ERP 합계':>17}{'vs PDF':>10}{'vs 감사':>10}")
    print("-" * 80)

    scenarios = [
        ("A. ST=Y AND AM>0 (현행)", "ST_SALES='Y' AND AM>0"),
        ("B. ST=Y|NULL AND AM>0", "(ST_SALES='Y' OR ST_SALES IS NULL) AND AM>0"),
        ("C. ST<>N AND AM>0", "(ST_SALES IS NULL OR ST_SALES<>'N') AND AM>0"),
        ("D. AM<>0 전체", "AM<>0"),
        ("E. ST<>N 전체 부호포함", "(ST_SALES IS NULL OR ST_SALES<>'N')"),
    ]

    for own in ["10000", "20000", "30000"]:
        name = OWN_NAME[own]
        pdf = PDF_TOTAL.get(own)
        audit = AUDIT.get(own)
        print(f"\n{name} (OWN={own})")
        print(f"  [참조] PDF 세금계산서 누계: {fmt(pdf)}")
        print(f"  [참조] 감사매출:         {fmt(audit)}")
        for label, where in scenarios:
            cur.execute(f"""
                SELECT SUM(CAST(AM AS BIGINT)) s
                FROM SAL_SALESH
                WHERE CD_CUST_OWN='{own}' AND DT_SALES LIKE '2025%' AND {where}
            """)
            s = int(cur.fetchone()["s"] or 0)
            print(f"  {label:<30}{fmt(s)}  {pct(s, pdf):>8}{pct(s, audit):>10}")

    # 3. 월별 추이 (시나리오 B 기준)
    print("\n=== 월별 ERP 매출 추이 (시나리오 B: ST=Y|NULL AND AM>0) ===")
    print(f"{'월':<8}{'갑우(10000)':>17}{'비피(20000)':>17}{'더원(30000)':>17}{'합계':>17}")
    for m in range(1, 13):
        ym = f"2025{m:02d}"
        row = {"10000": 0, "20000": 0, "30000": 0}
        for own in row:
            cur.execute(f"""
                SELECT SUM(CAST(AM AS BIGINT)) s
                FROM SAL_SALESH
                WHERE CD_CUST_OWN='{own}' AND DT_SALES LIKE '{ym}%'
                  AND (ST_SALES='Y' OR ST_SALES IS NULL) AND AM>0
            """)
            row[own] = int(cur.fetchone()["s"] or 0)
        tot = sum(row.values())
        print(f"{ym:<8}{fmt(row['10000'])}{fmt(row['20000'])}{fmt(row['30000'])}{fmt(tot)}")


if __name__ == "__main__":
    main()
