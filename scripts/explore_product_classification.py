"""
U1: SNOTES ERP에서 출판/패키지 사업부 구분 가능 여부 탐색.

목적:
- 본부장 가설("BOM 다르니 분명 ERP에 구분되어 있을 것") 검증
- SAL_SALESH/L · PRT_ITEM · PRT_BOM · PRT_BOMH 의 컬럼 + 분류성 값 덤프
- 매출 라인을 출판 vs 패키지로 분류할 수 있는 룰 후보 도출

출력:
- 콘솔 + scripts/output/u1_product_classification.txt
"""
from pathlib import Path
import pymssql

ENV = Path("/Users/jack/dev/gabwoo/견적계산기/.env.local")
OUT = Path(__file__).parent / "output" / "u1_product_classification.txt"

# 분류 의심 키워드 (컬럼명에 포함되면 후보)
CLASSIFY_KEYWORDS = [
    "GUBUN", "GBN", "TYPE", "KIND", "CATEGORY", "CLS", "DIV",
    "BIZ", "DEPT", "SECTION", "BU", "GROUP", "GRP", "CD_ITEM",
    "PRODUCT", "ITEM", "BOM"
]


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


def list_columns(cur, table):
    cur.execute("""
        SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = %s
        ORDER BY ORDINAL_POSITION
    """, (table,))
    return cur.fetchall()


def distinct_values(cur, table, col, top=20):
    """샘플 값 + 분포 (NULL 포함)."""
    try:
        cur.execute(f"""
            SELECT TOP {top} {col} v, COUNT(*) c
            FROM {table}
            GROUP BY {col}
            ORDER BY COUNT(*) DESC
        """)
        return cur.fetchall()
    except Exception as e:
        return [{"v": f"[ERR {e.__class__.__name__}]", "c": 0}]


def main():
    log = Tee(OUT)
    conn = connect()
    cur = conn.cursor(as_dict=True)

    # ─── 1. 핵심 후보 테이블 컬럼 덤프 ────────────────────────────────
    candidates = [
        "SAL_SALESH", "SAL_SALESL",
        "PRT_ITEM", "PRT_BOM", "PRT_BOMH",
        "PRT_SOH", "PRT_SOL",
    ]
    log("=" * 80)
    log("STEP 1. 후보 테이블 컬럼 목록 (분류 의심 컬럼 ★ 표시)")
    log("=" * 80)
    classify_hits = {}  # {table: [col, ...]}
    for t in candidates:
        cols = list_columns(cur, t)
        if not cols:
            log(f"\n[{t}] (테이블 없음)")
            continue
        log(f"\n[{t}] {len(cols)} columns")
        hits = []
        for c in cols:
            name = c["COLUMN_NAME"]
            mark = ""
            up = name.upper()
            if any(k in up for k in CLASSIFY_KEYWORDS):
                mark = " ★"
                hits.append(name)
            dtype = c["DATA_TYPE"]
            ln = c["CHARACTER_MAXIMUM_LENGTH"]
            ln_str = f"({ln})" if ln else ""
            log(f"   {name:<25} {dtype}{ln_str}{mark}")
        if hits:
            classify_hits[t] = hits

    # ─── 2. 분류 의심 컬럼의 실제 값 분포 ────────────────────────────
    log("\n" + "=" * 80)
    log("STEP 2. 분류 의심 컬럼 — 실제 값 분포 (Top 20)")
    log("=" * 80)
    for table, cols in classify_hits.items():
        for col in cols:
            log(f"\n[{table}.{col}] 값 분포")
            rows = distinct_values(cur, table, col, top=20)
            for r in rows:
                v = r["v"]
                c = r["c"]
                log(f"   {str(v):<40} {c:>10,}")

    # ─── 3. SAL_SALESL ↔ 품목/제품 코드 연결 추적 ────────────────────
    log("\n" + "=" * 80)
    log("STEP 3. SAL_SALESL 라인에서 품목/제품 코드 컬럼 확인")
    log("=" * 80)
    sal_l_cols = list_columns(cur, "SAL_SALESL")
    item_like = [c["COLUMN_NAME"] for c in sal_l_cols
                 if any(k in c["COLUMN_NAME"].upper() for k in ["ITEM", "PRODUCT", "BOM", "CD_"])]
    log(f"SAL_SALESL의 ITEM/PRODUCT/BOM/CD_ 계열 컬럼: {item_like}")

    # 샘플 라인 5개 + 어떤 코드들이 채워져 있는지
    if item_like:
        cols_str = ", ".join(item_like[:8])
        log(f"\nSAL_SALESL 샘플 (2025년) — 컬럼: {cols_str}")
        try:
            cur.execute(f"""
                SELECT TOP 5 {cols_str}
                FROM SAL_SALESL l
                WHERE EXISTS (
                    SELECT 1 FROM SAL_SALESH h
                    WHERE h.NO_SALES = l.NO_SALES AND h.DT_SALES LIKE '2025%'
                      AND h.CD_CUST_OWN='10000' AND h.ST_SALES='Y'
                )
            """)
            for r in cur.fetchall():
                log(f"   {r}")
        except Exception as e:
            log(f"   [ERR] {e}")

    # ─── 4. PRT_ITEM 마스터에 사업부 구분 컬럼 있는지 확인 ──────────
    log("\n" + "=" * 80)
    log("STEP 4. PRT_ITEM 마스터 — 사업부/제품유형 추정 컬럼 샘플")
    log("=" * 80)
    item_cols = list_columns(cur, "PRT_ITEM")
    classify_item_cols = [c["COLUMN_NAME"] for c in item_cols
                          if any(k in c["COLUMN_NAME"].upper() for k in CLASSIFY_KEYWORDS)
                          and "CD_ITEM" not in c["COLUMN_NAME"].upper()]
    log(f"PRT_ITEM 분류 의심 컬럼: {classify_item_cols}")
    for col in classify_item_cols[:10]:
        log(f"\n[PRT_ITEM.{col}] 값 분포 (Top 10)")
        rows = distinct_values(cur, "PRT_ITEM", col, top=10)
        for r in rows:
            log(f"   {str(r['v']):<40} {r['c']:>10,}")

    # ─── 5. PRT_BOM/BOMH 헤더에 사업부 구분 있는지 ───────────────────
    log("\n" + "=" * 80)
    log("STEP 5. PRT_BOMH — BOM 헤더 분류 컬럼")
    log("=" * 80)
    for tbl in ["PRT_BOMH", "PRT_BOM"]:
        cols = list_columns(cur, tbl)
        if not cols:
            continue
        cls_cols = [c["COLUMN_NAME"] for c in cols
                    if any(k in c["COLUMN_NAME"].upper() for k in CLASSIFY_KEYWORDS)]
        log(f"\n[{tbl}] 분류 의심 컬럼: {cls_cols}")
        for col in cls_cols[:8]:
            log(f"   [{tbl}.{col}] Top 5")
            rows = distinct_values(cur, tbl, col, top=5)
            for r in rows:
                log(f"      {str(r['v']):<40} {r['c']:>10,}")

    # ─── 6. 거래처 마스터에 사업부 구분 있는지 ───────────────────────
    log("\n" + "=" * 80)
    log("STEP 6. 거래처 마스터 (MAS_CUST / CUST_UP$) — 사업부 분류")
    log("=" * 80)
    for tbl in ["MAS_CUST", "CUST_UP$"]:
        cols = list_columns(cur, tbl)
        if not cols:
            log(f"\n[{tbl}] (없음)")
            continue
        cls_cols = [c["COLUMN_NAME"] for c in cols
                    if any(k in c["COLUMN_NAME"].upper() for k in CLASSIFY_KEYWORDS + ["BIZ"])]
        log(f"\n[{tbl}] 분류 의심 컬럼: {cls_cols}")
        for col in cls_cols[:8]:
            log(f"   [{tbl}.{col}] Top 5")
            try:
                t = f"[{tbl}]" if "$" in tbl else tbl
                rows = distinct_values(cur, t, col, top=5)
                for r in rows:
                    log(f"      {str(r['v']):<40} {r['c']:>10,}")
            except Exception as e:
                log(f"      [ERR] {e}")

    log("\n" + "=" * 80)
    log("탐색 완료. 다음 결정 포인트:")
    log("- 위 분류 컬럼 중 출판/패키지 구분이 명확한 게 있는지 사용자 확인")
    log("- 없다면 거래처 단위 매핑 / 품목코드 prefix 룰 / 영업자 매핑 등 우회로 검토")
    log("=" * 80)
    log.close()
    conn.close()


if __name__ == "__main__":
    main()
