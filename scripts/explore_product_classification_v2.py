"""
U1 v2: 1차 탐색에서 떠오른 강력 후보 4개를 깊이 본다.
- PRT_ITEM.FG_BIZ        ('사업 구분 Flag'로 추정)
- PRT_ITEM.TP_ITEM       (100/200/300 코드 의미)
- PRT_ITEM.CD_GROUP1/2   (그룹 분류)
- SAL_SALESL.CD_ITEM_PACK (이름에 PACK — 패키지 전용?)
- MAS_CUST.DC_CUST_TYPE  (거래처 업종: 인쇄/출판/화장품)

목표: 2025년 갑우(10000) ST_SALES='Y' 매출 라인을 출판 vs 패키지로 분류 가능한지 확인.
"""
from pathlib import Path
import pymssql

ENV = Path("/Users/jack/dev/gabwoo/견적계산기/.env.local")
OUT = Path(__file__).parent / "output" / "u1_classification_v2.txt"


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

    # ─── 1. PRT_ITEM.FG_BIZ — 가장 강력 후보 ─────────────────────────
    section(log, "1. PRT_ITEM.FG_BIZ — 사업 구분 Flag (전체 분포)")
    cur.execute("""
        SELECT FG_BIZ v, COUNT(*) c
        FROM PRT_ITEM GROUP BY FG_BIZ ORDER BY COUNT(*) DESC
    """)
    for r in cur.fetchall():
        log(f"   FG_BIZ={str(r['v']):<15} {r['c']:>10,}")

    # FG_BIZ 값별 샘플 품목명 5개씩
    cur.execute("SELECT DISTINCT FG_BIZ FROM PRT_ITEM WHERE FG_BIZ IS NOT NULL")
    vals = [r["FG_BIZ"] for r in cur.fetchall()]
    for v in vals:
        log(f"\n   [FG_BIZ='{v}'] 샘플 품목명 5개")
        cur.execute(f"""
            SELECT TOP 5 NM_ITEM, CD_ITEM, TP_ITEM
            FROM PRT_ITEM
            WHERE FG_BIZ = %s AND NM_ITEM IS NOT NULL AND NM_ITEM <> ''
            ORDER BY NEWID()
        """, (v,))
        for r in cur.fetchall():
            log(f"      {r['CD_ITEM']:<18} TP={r['TP_ITEM']:<5} {r['NM_ITEM']}")

    # ─── 2. PRT_ITEM.TP_ITEM (100/200/300) 의미 추정 ─────────────────
    section(log, "2. PRT_ITEM.TP_ITEM 100/200/300 — 샘플 품목명")
    for tp in ["100", "200", "300"]:
        log(f"\n   [TP_ITEM='{tp}'] 샘플 5개")
        cur.execute(f"""
            SELECT TOP 5 NM_ITEM, CD_ITEM, FG_BIZ
            FROM PRT_ITEM
            WHERE TP_ITEM = %s AND NM_ITEM IS NOT NULL AND NM_ITEM <> ''
            ORDER BY NEWID()
        """, (tp,))
        for r in cur.fetchall():
            log(f"      {r['CD_ITEM']:<18} BIZ={str(r['FG_BIZ']):<5} {r['NM_ITEM']}")

    # ─── 3. PRT_ITEM.CD_GROUP1 / CD_GROUP2 ───────────────────────────
    for gcol in ["CD_GROUP1", "CD_GROUP2"]:
        section(log, f"3. PRT_ITEM.{gcol} — Top 20 분포")
        try:
            cur.execute(f"""
                SELECT TOP 20 {gcol} v, COUNT(*) c
                FROM PRT_ITEM GROUP BY {gcol} ORDER BY COUNT(*) DESC
            """)
            for r in cur.fetchall():
                log(f"   {gcol}={str(r['v']):<25} {r['c']:>10,}")
        except Exception as e:
            log(f"   [ERR] {e}")

    # ─── 4. SAL_SALESL.CD_ITEM_PACK 채워진 비율 ──────────────────────
    section(log, "4. SAL_SALESL.CD_ITEM_PACK — 채워진 라인 비율 (2025년 전체)")
    cur.execute("""
        SELECT
            COUNT(*) total_lines,
            SUM(CASE WHEN CD_ITEM_PACK IS NOT NULL AND CD_ITEM_PACK <> '' THEN 1 ELSE 0 END) pack_filled,
            SUM(CASE WHEN CD_ITEM IS NOT NULL AND CD_ITEM <> '' THEN 1 ELSE 0 END) item_filled
        FROM SAL_SALESL l
        WHERE EXISTS (
            SELECT 1 FROM SAL_SALESH h
            WHERE h.NO_SALES = l.NO_SALES AND h.DT_SALES LIKE '2025%'
        )
    """)
    r = cur.fetchone()
    log(f"   2025년 전체 매출 라인: {r['total_lines']:>10,}")
    log(f"   CD_ITEM 채워짐:        {r['item_filled']:>10,}")
    log(f"   CD_ITEM_PACK 채워짐:   {r['pack_filled']:>10,}")

    # PACK이 채워진 샘플
    cur.execute("""
        SELECT TOP 5 l.CD_ITEM, l.CD_ITEM_PACK, l.NM_ITEM
        FROM SAL_SALESL l
        WHERE l.CD_ITEM_PACK IS NOT NULL AND l.CD_ITEM_PACK <> ''
        ORDER BY NEWID()
    """)
    log("\n   CD_ITEM_PACK 채워진 라인 샘플 5개:")
    for r in cur.fetchall():
        log(f"      CD_ITEM={r['CD_ITEM']} PACK={r['CD_ITEM_PACK']} NM={r.get('NM_ITEM')}")

    # ─── 5. 2025 갑우 매출 라인을 PRT_ITEM JOIN해서 FG_BIZ별 분포 ──────
    section(log, "5. 2025 갑우(OWN=10000) 매출 라인 × PRT_ITEM.FG_BIZ — 매출 분포")
    cur.execute("""
        SELECT i.FG_BIZ,
               COUNT(*) cnt,
               SUM(CAST(l.AM AS BIGINT)) am_sum
        FROM SAL_SALESL l
        INNER JOIN SAL_SALESH h ON h.NO_SALES = l.NO_SALES
        LEFT JOIN PRT_ITEM i ON i.CD_ITEM = l.CD_ITEM
        WHERE h.DT_SALES LIKE '2025%'
          AND h.CD_CUST_OWN = '10000'
          AND h.ST_SALES = 'Y'
          AND h.AM > 0
        GROUP BY i.FG_BIZ
        ORDER BY SUM(CAST(l.AM AS BIGINT)) DESC
    """)
    for r in cur.fetchall():
        log(f"   FG_BIZ={str(r['FG_BIZ']):<15} 건수={r['cnt']:>8,}  매출(AM합)={int(r['am_sum'] or 0):>17,}")

    # ─── 6. 2025 갑우 매출 라인 × TP_ITEM 분포 ───────────────────────
    section(log, "6. 2025 갑우 매출 라인 × PRT_ITEM.TP_ITEM — 매출 분포")
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
        log(f"   TP_ITEM={str(r['TP_ITEM']):<10} 건수={r['cnt']:>8,}  매출(AM합)={int(r['am_sum'] or 0):>17,}")

    # ─── 7. 2025 갑우 매출 × MAS_CUST.DC_CUST_TYPE 분포 ──────────────
    section(log, "7. 2025 갑우 매출 × MAS_CUST.DC_CUST_TYPE — 거래처 업종별 분포")
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
        log(f"   업종='{str(r['DC_CUST_TYPE']):<15}' 건수={r['sales_cnt']:>6,}  매출={int(r['am_sum'] or 0):>17,}")

    # ─── 8. FG_BIZ × TP_ITEM 교차표 ──────────────────────────────────
    section(log, "8. PRT_ITEM 마스터 — FG_BIZ × TP_ITEM 교차표")
    cur.execute("""
        SELECT FG_BIZ, TP_ITEM, COUNT(*) c
        FROM PRT_ITEM
        WHERE FG_BIZ IS NOT NULL OR TP_ITEM IS NOT NULL
        GROUP BY FG_BIZ, TP_ITEM
        ORDER BY COUNT(*) DESC
    """)
    log(f"   {'FG_BIZ':<10}{'TP_ITEM':<10}{'count':>10}")
    for r in cur.fetchall():
        log(f"   {str(r['FG_BIZ']):<10}{str(r['TP_ITEM']):<10}{r['c']:>10,}")

    log.close()
    conn.close()


if __name__ == "__main__":
    main()
