"""
Track C 1단계: 인쇄기별 매출 데이터 탐색.

목표:
- PRT_EQUIP 설비 마스터 확인 (Heidelberg/Roland/Komori 8대)
- PRT_WO ↔ SAL_SALESH/L 연결 컬럼 (견적번호 NO_EST? 수주번호 NO_SO? 작업지시 NO_WO?)
- 작업지시 → 인쇄기 매핑 (PRT_WOPROC_EQUIP)
- 인쇄기 1대당 2025년 매출 추정 (1차 러프)

본부장 말씀: "견적번호가 있어서 최초 등록부터 매출 등록까지 쭉 따라가서 어렵지가 않아요"
"""
from pathlib import Path
import pymssql

ENV = Path("/Users/jack/dev/gabwoo/견적계산기/.env.local")
OUT = Path(__file__).parent / "output" / "machine_revenue_explore.txt"


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


def list_columns(cur, table):
    cur.execute("""
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = %s
        ORDER BY ORDINAL_POSITION
    """, (table,))
    return [(r["COLUMN_NAME"], r["DATA_TYPE"]) for r in cur.fetchall()]


def main():
    log = Tee(OUT)
    conn = connect()
    cur = conn.cursor(as_dict=True)

    # ─── 1. PRT_EQUIP 설비 마스터 ────────────────────────────────────
    section(log, "1. PRT_EQUIP 설비 마스터 (전체)")
    cols = list_columns(cur, "PRT_EQUIP")
    log(f"   컬럼 ({len(cols)}): {[c[0] for c in cols]}")
    name_col = "NM_EQUIP" if any(c[0] == "NM_EQUIP" for c in cols) else None
    cd_col = "CD_EQUIP" if any(c[0] == "CD_EQUIP" for c in cols) else None
    if name_col and cd_col:
        cur.execute(f"SELECT TOP 80 * FROM PRT_EQUIP ORDER BY {cd_col}")
        for r in cur.fetchall():
            cd = r.get(cd_col, "?")
            nm = r.get(name_col, "?")
            extra = {k: v for k, v in r.items() if k not in (cd_col, name_col) and v not in (None, "", 0)}
            log(f"   {cd:<15} {str(nm):<30} {dict(list(extra.items())[:4])}")

    # ─── 2. SAL_SALESH ↔ PRT_SOH ↔ PRT_WO 연결 컬럼 찾기 ─────────────
    section(log, "2. SAL_SALESH 컬럼 — 견적번호/수주번호/작업지시번호 후보")
    cols_h = list_columns(cur, "SAL_SALESH")
    no_cols = [c for c in cols_h if c[0].startswith("NO_") or c[0].startswith("CD_")]
    log(f"   NO_*/CD_* 컬럼: {[c[0] for c in no_cols]}")

    # 2025 한 건 샘플로 어떤 NO_* 컬럼이 채워져 있는지
    cur.execute("""
        SELECT TOP 1 *
        FROM SAL_SALESH
        WHERE DT_SALES LIKE '2025%' AND CD_CUST_OWN='10000' AND ST_SALES='Y'
    """)
    sample = cur.fetchone()
    if sample:
        log("\n   샘플 1건 — 채워진 NO_*/CD_* 컬럼:")
        for c, _ in no_cols:
            v = sample.get(c)
            if v not in (None, "", 0, "0"):
                log(f"      {c}={v}")

    section(log, "3. SAL_SALESL 컬럼 — 라인의 작업지시 연결 후보")
    cols_l = list_columns(cur, "SAL_SALESL")
    no_l = [c for c in cols_l if c[0].startswith("NO_") or c[0].startswith("CD_")]
    log(f"   NO_*/CD_* 컬럼: {[c[0] for c in no_l]}")
    cur.execute("""
        SELECT TOP 1 *
        FROM SAL_SALESL l
        WHERE EXISTS(
          SELECT 1 FROM SAL_SALESH h
          WHERE h.NO_SALES=l.NO_SALES AND h.DT_SALES LIKE '2025%' AND h.CD_CUST_OWN='10000' AND h.ST_SALES='Y'
        )
    """)
    sample = cur.fetchone()
    if sample:
        log("\n   샘플 1건 — 채워진 NO_*/CD_* 컬럼:")
        for c, _ in no_l:
            v = sample.get(c)
            if v not in (None, "", 0, "0"):
                log(f"      {c}={v}")

    # ─── 4. PRT_WO 컬럼 — 매출/수주/품목 연결 ───────────────────────
    section(log, "4. PRT_WO (작업지시) 컬럼")
    cols_wo = list_columns(cur, "PRT_WO")
    no_wo = [c for c in cols_wo if c[0].startswith("NO_") or c[0].startswith("CD_") or "DT_" in c[0]]
    log(f"   NO_*/CD_*/DT_* 컬럼: {[c[0] for c in no_wo]}")
    cur.execute("SELECT TOP 1 * FROM PRT_WO WHERE DT_REG LIKE '2025%' OR DT_WO LIKE '2025%'")
    sample = cur.fetchone()
    if sample:
        log("\n   샘플 1건 — 채워진 NO_*/CD_* 컬럼:")
        for c, _ in no_wo:
            v = sample.get(c)
            if v not in (None, "", 0, "0"):
                log(f"      {c}={v}")

    # ─── 5. PRT_WOPROC_EQUIP — 작업×설비 매핑 ────────────────────────
    section(log, "5. PRT_WOPROC_EQUIP (작업공정×설비 매핑) 컬럼")
    cols_eq = list_columns(cur, "PRT_WOPROC_EQUIP")
    log(f"   전체 컬럼: {[c[0] for c in cols_eq]}")
    cur.execute("SELECT TOP 1 * FROM PRT_WOPROC_EQUIP")
    sample = cur.fetchone()
    if sample:
        log("\n   샘플 1건:")
        for k, v in sample.items():
            if v not in (None, "", 0, "0"):
                log(f"      {k}={v}")

    # ─── 6. 인쇄기 그룹별 (Heidelberg/Roland/Komori) 가동 통계 ──────
    section(log, "6. 2025년 설비별 작업공정 횟수 (PRT_WOPROC_EQUIP)")
    cur.execute("""
        SELECT TOP 30 e.CD_EQUIP, eq.NM_EQUIP, COUNT(*) cnt
        FROM PRT_WOPROC_EQUIP e
        LEFT JOIN PRT_EQUIP eq ON eq.CD_EQUIP = e.CD_EQUIP
        WHERE e.DT_REG LIKE '2025%'
        GROUP BY e.CD_EQUIP, eq.NM_EQUIP
        ORDER BY COUNT(*) DESC
    """)
    rows = cur.fetchall()
    if not rows:
        log("   (DT_REG 컬럼 없음 — 다른 날짜 컬럼 시도 필요)")
    for r in rows:
        log(f"   {str(r.get('CD_EQUIP')):<15} {str(r.get('NM_EQUIP')):<30} {r['cnt']:>10,}")

    # ─── 7. 매출 ↔ 작업지시 연결 가능성 검증 ─────────────────────────
    section(log, "7. SAL_SALESL → 매출 라인이 PRT_WO·PRT_SOL 어느 컬럼으로 연결되나")
    # 후보: NO_SO (수주번호), NO_WO, NO_EST (견적)
    candidates = [c[0] for c in cols_l if c[0] in ("NO_SO", "NO_WO", "NO_EST", "NO_SOH", "NO_SOL")]
    log(f"   SAL_SALESL의 매출-수주-작업 연결 후보 컬럼: {candidates}")

    for col in candidates:
        cur.execute(f"""
            SELECT
                COUNT(*) total,
                SUM(CASE WHEN {col} IS NOT NULL AND {col} <> '' THEN 1 ELSE 0 END) filled
            FROM SAL_SALESL l
            WHERE EXISTS(
                SELECT 1 FROM SAL_SALESH h
                WHERE h.NO_SALES=l.NO_SALES AND h.DT_SALES LIKE '2025%'
                  AND h.CD_CUST_OWN='10000' AND h.ST_SALES='Y'
            )
        """)
        r = cur.fetchone()
        log(f"   {col}: 채워짐 {r['filled']:>6,} / 전체 {r['total']:>6,}")

    log.close()
    conn.close()


if __name__ == "__main__":
    main()
