"""
U1 v3: v2에서 NULL로 죽어있는 컬럼을 빼고, 의미 있는 후보 3개만 깊이 본다.
- TP_ITEM (100/200/300) — 매출 라인 × 매출 분포
- MAS_CUST.DC_CUST_TYPE — 거래처 업종 분류 (인쇄/출판/화장품)
- 품목명 키워드 — 패키지 거래처(코스맥스 등) 매출 라인의 실제 NM_ITEM 패턴

목표: 가장 신뢰 가능한 분류 룰 1개 채택.
"""
from pathlib import Path
import pymssql

ENV = Path("/Users/jack/dev/gabwoo/견적계산기/.env.local")
OUT = Path(__file__).parent / "output" / "u1_classification_v3.txt"

# 알려진 패키지(화장품) 주요 거래처 (회사 정보 CLAUDE.md 기준)
KNOWN_PACKAGE_KEYWORDS = ["코스맥스", "코스메카", "정샘물", "에뛰드", "지담"]


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


class Tee:
    def __init__(self, path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.f = open(path, "w", encoding="utf-8")

    def __call__(self, *args):
        msg = " ".join(str(a) for a in args)
        print(msg)
        self.f.write(msg + "\n")

    def close(self):
        self.f.close()


def section(log, title):
    log("\n" + "=" * 80)
    log(title)
    log("=" * 80)


def main():
    log = Tee(OUT)
    conn = connect()
    cur = conn.cursor(as_dict=True)

    # ─── 1. 2025 갑우 매출 × TP_ITEM 분포 ───────────────────────────
    section(log, "1. 2025 갑우(OWN=10000) 매출 × PRT_ITEM.TP_ITEM")
    cur.execute("""
        SELECT i.TP_ITEM,
               COUNT(*) cnt,
               SUM(CAST(l.AM AS BIGINT)) am_sum
        FROM SAL_SALESL l
        INNER JOIN SAL_SALESH h ON h.NO_SALES = l.NO_SALES
        LEFT JOIN PRT_ITEM i ON i.CD_ITEM = l.CD_ITEM
        WHERE h.DT_SALES LIKE '2025%'
          AND h.CD_CUST_OWN = '10000'
          AND h.ST_SALES = 'Y'
          AND h.AM > 0
        GROUP BY i.TP_ITEM
        ORDER BY SUM(CAST(l.AM AS BIGINT)) DESC
    """)
    for r in cur.fetchall():
        log(f"   TP_ITEM={str(r['TP_ITEM']):<10} 라인수={r['cnt']:>8,}  매출(AM합)={int(r['am_sum'] or 0):>17,}")

    # ─── 2. 2025 갑우 매출 × DC_CUST_TYPE ───────────────────────────
    section(log, "2. 2025 갑우 매출 × MAS_CUST.DC_CUST_TYPE (거래처 업종)")
    cur.execute("""
        SELECT c.DC_CUST_TYPE,
               COUNT(DISTINCT h.NO_SALES) sales_cnt,
               SUM(CAST(h.AM AS BIGINT)) am_sum
        FROM SAL_SALESH h
        LEFT JOIN MAS_CUST c ON c.CD_CUST = h.CD_CUST
        WHERE h.DT_SALES LIKE '2025%'
          AND h.CD_CUST_OWN = '10000'
          AND h.ST_SALES = 'Y'
          AND h.AM > 0
        GROUP BY c.DC_CUST_TYPE
        ORDER BY SUM(CAST(h.AM AS BIGINT)) DESC
    """)
    for r in cur.fetchall():
        log(f"   업종='{str(r['DC_CUST_TYPE']):<15}' 견적건수={r['sales_cnt']:>6,}  매출={int(r['am_sum'] or 0):>17,}")

    # ─── 3. 알려진 패키지(화장품) 거래처 매출 분포 ──────────────────
    section(log, "3. 알려진 패키지 거래처 매출 (이름 LIKE)")
    for kw in KNOWN_PACKAGE_KEYWORDS:
        cur.execute("""
            SELECT c.CD_CUST, c.NM_CUST,
                   COUNT(DISTINCT h.NO_SALES) cnt,
                   SUM(CAST(h.AM AS BIGINT)) am
            FROM SAL_SALESH h
            INNER JOIN MAS_CUST c ON c.CD_CUST = h.CD_CUST
            WHERE h.DT_SALES LIKE '2025%'
              AND h.CD_CUST_OWN = '10000'
              AND h.ST_SALES = 'Y' AND h.AM > 0
              AND c.NM_CUST LIKE '%' + %s + '%'
            GROUP BY c.CD_CUST, c.NM_CUST
            ORDER BY SUM(CAST(h.AM AS BIGINT)) DESC
        """, (kw,))
        rows = cur.fetchall()
        if rows:
            log(f"\n   '{kw}' 매칭 거래처:")
            for r in rows:
                log(f"      {r['CD_CUST']} {r['NM_CUST']:<30} 건수={r['cnt']:>4} 매출={int(r['am'] or 0):>15,}")
        else:
            log(f"\n   '{kw}' 매칭 거래처 0건")

    # ─── 4. 패키지 거래처 매출 라인의 실제 NM_ITEM 샘플 ─────────────
    section(log, "4. 패키지 거래처(코스맥스 등) 매출 라인의 NM_ITEM 샘플 — 패턴 추출")
    pkg_like = " OR ".join([f"c.NM_CUST LIKE '%{kw}%'" for kw in KNOWN_PACKAGE_KEYWORDS])
    cur.execute(f"""
        SELECT TOP 20 c.NM_CUST, i.NM_ITEM, i.TP_ITEM, l.CD_ITEM
        FROM SAL_SALESL l
        INNER JOIN SAL_SALESH h ON h.NO_SALES = l.NO_SALES
        INNER JOIN MAS_CUST c ON c.CD_CUST = h.CD_CUST
        LEFT JOIN PRT_ITEM i ON i.CD_ITEM = l.CD_ITEM
        WHERE h.DT_SALES LIKE '2025%' AND h.CD_CUST_OWN='10000' AND h.ST_SALES='Y'
          AND ({pkg_like})
        ORDER BY NEWID()
    """)
    for r in cur.fetchall():
        log(f"   {r['NM_CUST']:<25} TP={str(r['TP_ITEM']):<5} {r['NM_ITEM']}")

    # ─── 5. 출판 거래처(이투스에듀, 에듀윌 등) NM_ITEM 패턴 ─────────
    section(log, "5. 출판 대표 거래처 매출 라인의 NM_ITEM 샘플")
    PUB_KW = ["이투스", "에듀윌", "필통", "동행복권", "HSAD"]
    pub_like = " OR ".join([f"c.NM_CUST LIKE '%{kw}%'" for kw in PUB_KW])
    cur.execute(f"""
        SELECT TOP 20 c.NM_CUST, i.NM_ITEM, i.TP_ITEM, l.CD_ITEM
        FROM SAL_SALESL l
        INNER JOIN SAL_SALESH h ON h.NO_SALES = l.NO_SALES
        INNER JOIN MAS_CUST c ON c.CD_CUST = h.CD_CUST
        LEFT JOIN PRT_ITEM i ON i.CD_ITEM = l.CD_ITEM
        WHERE h.DT_SALES LIKE '2025%' AND h.CD_CUST_OWN='10000' AND h.ST_SALES='Y'
          AND ({pub_like})
        ORDER BY NEWID()
    """)
    for r in cur.fetchall():
        log(f"   {r['NM_CUST']:<25} TP={str(r['TP_ITEM']):<5} {r['NM_ITEM']}")

    # ─── 6. DC_CUST_TYPE='화장품' 거래처 vs 패키지 거래처 일치도 ────
    section(log, "6. DC_CUST_TYPE='화장품' 거래처 목록 (매출 발생 기준)")
    cur.execute("""
        SELECT c.CD_CUST, c.NM_CUST, c.DC_CUST_TYPE,
               COUNT(DISTINCT h.NO_SALES) cnt, SUM(CAST(h.AM AS BIGINT)) am
        FROM SAL_SALESH h
        INNER JOIN MAS_CUST c ON c.CD_CUST = h.CD_CUST
        WHERE h.DT_SALES LIKE '2025%' AND h.CD_CUST_OWN='10000'
          AND h.ST_SALES='Y' AND h.AM > 0
          AND c.DC_CUST_TYPE = '화장품'
        GROUP BY c.CD_CUST, c.NM_CUST, c.DC_CUST_TYPE
        ORDER BY SUM(CAST(h.AM AS BIGINT)) DESC
    """)
    for r in cur.fetchall():
        log(f"   {r['CD_CUST']} {r['NM_CUST']:<30} 건수={r['cnt']:>4} 매출={int(r['am'] or 0):>15,}")

    # ─── 7. 화장품 외에도 패키지 의심되는 거래처 — TP_ITEM과 교차 ────
    section(log, "7. 거래처별 TP_ITEM 혼재 정도 (Top 20 거래처)")
    cur.execute("""
        SELECT TOP 20
               c.NM_CUST,
               c.DC_CUST_TYPE,
               SUM(CASE WHEN i.TP_ITEM='100' THEN CAST(l.AM AS BIGINT) ELSE 0 END) am_100,
               SUM(CASE WHEN i.TP_ITEM='200' THEN CAST(l.AM AS BIGINT) ELSE 0 END) am_200,
               SUM(CASE WHEN i.TP_ITEM='300' THEN CAST(l.AM AS BIGINT) ELSE 0 END) am_300,
               SUM(CAST(l.AM AS BIGINT)) am_total
        FROM SAL_SALESL l
        INNER JOIN SAL_SALESH h ON h.NO_SALES = l.NO_SALES
        INNER JOIN MAS_CUST c ON c.CD_CUST = h.CD_CUST
        LEFT JOIN PRT_ITEM i ON i.CD_ITEM = l.CD_ITEM
        WHERE h.DT_SALES LIKE '2025%' AND h.CD_CUST_OWN='10000'
          AND h.ST_SALES='Y' AND h.AM > 0
        GROUP BY c.NM_CUST, c.DC_CUST_TYPE
        ORDER BY SUM(CAST(l.AM AS BIGINT)) DESC
    """)
    log(f"   {'거래처':<25}{'업종':<10}{'TP=100':>13}{'TP=200':>13}{'TP=300':>13}{'합계':>15}")
    for r in cur.fetchall():
        log(f"   {r['NM_CUST'][:24]:<25}{str(r['DC_CUST_TYPE'])[:9]:<10}"
            f"{int(r['am_100'] or 0):>13,}{int(r['am_200'] or 0):>13,}"
            f"{int(r['am_300'] or 0):>13,}{int(r['am_total'] or 0):>15,}")

    log.close()
    conn.close()


if __name__ == "__main__":
    main()
