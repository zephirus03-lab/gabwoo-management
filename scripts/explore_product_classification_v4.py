"""
U1 v4: 모호 Top 거래처 분류 마무리 + 거래처 화이트리스트 자동 생성.

전제:
- ERP에 사업부 구분 컬럼 없음 (v2/v3에서 확인)
- 거래처 단위 분류가 가장 신뢰 가능
- NM_ITEM 키워드("단상자/리필/인박스/케이스" vs "쇄/표지/본문/시리즈/핸드북")로 분류

목표:
1. 2025 갑우 매출 Top 100 거래처를 NM_ITEM 키워드 비율로 자동 분류
2. 모호 거래처(키워드 비율 30~70%)는 수동 확인 대상으로 분리
3. JSON 화이트리스트 출력 → data/customer_classification_draft.json
"""
from pathlib import Path
import json
import pymssql

ENV = Path("/Users/jack/dev/gabwoo/견적계산기/.env.local")
OUT_TXT = Path(__file__).parent / "output" / "u1_classification_v4.txt"
OUT_JSON = Path(__file__).parent / "output" / "customer_classification_draft.json"

# 패키지(화장품·단상자) 시그니처 키워드
PACKAGE_KW = [
    "단상자", "리필", "인박스", "케이스", "브러쉬", "팩트", "튜브", "용기",
    "포장", "쇼핑백", "쿠션", "립스틱", "파운데이션", "에센스", "세럼",
    "지함", "OP", "받침", "뚜껑", "카톤", "파우치", "발효액",
]
# 출판 시그니처 키워드
PUBLISH_KW = [
    "쇄", "호", "표지", "본문", "시리즈", "핸드북", "양장", "무선",
    "교재", "문제집", "워크북", "기본서", "전집", "도서", "잡지",
    "리플렛", "다이어리", "달력", "카탈로그", "팸플릿", "상장",
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


def classify_by_kw(name):
    """NM_ITEM 한 건을 패키지/출판/모호로 분류."""
    if not name:
        return "unknown"
    n = str(name)
    pkg_hit = sum(1 for k in PACKAGE_KW if k in n)
    pub_hit = sum(1 for k in PUBLISH_KW if k in n)
    if pkg_hit > 0 and pub_hit == 0:
        return "package"
    if pub_hit > 0 and pkg_hit == 0:
        return "publish"
    if pkg_hit > 0 and pub_hit > 0:
        return "mixed"
    return "unknown"


def main():
    OUT_TXT.parent.mkdir(parents=True, exist_ok=True)
    log_lines = []

    def log(*args):
        msg = " ".join(str(a) for a in args)
        print(msg)
        log_lines.append(msg)

    conn = connect()
    cur = conn.cursor(as_dict=True)

    # 2025 갑우 매출 Top 100 거래처
    log("=" * 80)
    log("2025 갑우 매출 Top 100 거래처 — NM_ITEM 키워드 분류")
    log("=" * 80)
    cur.execute("""
        SELECT TOP 100 h.CD_CUST, c.NM_CUST, c.DC_CUST_TYPE,
               COUNT(DISTINCT h.NO_SALES) sales_cnt,
               SUM(CAST(h.AM AS BIGINT)) am_total
        FROM SAL_SALESH h
        LEFT JOIN MAS_CUST c ON c.CD_CUST = h.CD_CUST
        WHERE h.DT_SALES LIKE '2025%' AND h.CD_CUST_OWN='10000'
          AND h.ST_SALES='Y' AND h.AM > 0
        GROUP BY h.CD_CUST, c.NM_CUST, c.DC_CUST_TYPE
        ORDER BY SUM(CAST(h.AM AS BIGINT)) DESC
    """)
    top_custs = cur.fetchall()

    results = []
    for cust in top_custs:
        cd = cust["CD_CUST"]
        nm = cust["NM_CUST"] or "(이름없음)"
        # 해당 거래처의 매출 라인 NM_ITEM 분포
        cur.execute("""
            SELECT i.NM_ITEM, CAST(l.AM AS BIGINT) am
            FROM SAL_SALESL l
            INNER JOIN SAL_SALESH h ON h.NO_SALES = l.NO_SALES
            LEFT JOIN PRT_ITEM i ON i.CD_ITEM = l.CD_ITEM
            WHERE h.DT_SALES LIKE '2025%' AND h.CD_CUST_OWN='10000'
              AND h.ST_SALES='Y' AND h.AM > 0
              AND h.CD_CUST = %s
        """, (cd,))
        lines = cur.fetchall()
        am_pkg = am_pub = am_mix = am_unk = 0
        for ln in lines:
            cls = classify_by_kw(ln["NM_ITEM"])
            am = int(ln["am"] or 0)
            if cls == "package":
                am_pkg += am
            elif cls == "publish":
                am_pub += am
            elif cls == "mixed":
                am_mix += am
            else:
                am_unk += am
        total = am_pkg + am_pub + am_mix + am_unk
        if total == 0:
            continue
        pkg_ratio = am_pkg / total
        pub_ratio = am_pub / total
        # 분류 판정
        if pkg_ratio >= 0.7:
            verdict = "PACKAGE"
        elif pub_ratio >= 0.7:
            verdict = "PUBLISH"
        elif pkg_ratio + pub_ratio < 0.3:
            verdict = "UNKNOWN_TOO_NOISY"
        else:
            verdict = "MIXED_REVIEW"

        results.append({
            "cd_cust": cd,
            "nm_cust": nm,
            "dc_cust_type": cust.get("DC_CUST_TYPE"),
            "sales_cnt": cust["sales_cnt"],
            "am_total": int(cust["am_total"] or 0),
            "am_package": am_pkg,
            "am_publish": am_pub,
            "am_mixed": am_mix,
            "am_unknown": am_unk,
            "pkg_ratio": round(pkg_ratio, 3),
            "pub_ratio": round(pub_ratio, 3),
            "verdict": verdict,
        })

    # 보고
    log(f"\n{'순위':<4}{'거래처':<28}{'업종':<12}{'매출(M)':>8}"
        f"{'pkg%':>7}{'pub%':>7}{'판정':>20}")
    for i, r in enumerate(results, 1):
        log(f"{i:<4}{r['nm_cust'][:27]:<28}{str(r['dc_cust_type'])[:11]:<12}"
            f"{r['am_total']/1_000_000:>8.0f}"
            f"{r['pkg_ratio']*100:>6.1f}%{r['pub_ratio']*100:>6.1f}%"
            f"{r['verdict']:>20}")

    # 사업부별 합계
    log("\n" + "=" * 80)
    log("Top 100 거래처 분류 결과 — 매출 합계 기준")
    log("=" * 80)
    by_v = {}
    for r in results:
        by_v.setdefault(r["verdict"], []).append(r)
    for v, lst in sorted(by_v.items(), key=lambda x: -sum(r["am_total"] for r in x[1])):
        am = sum(r["am_total"] for r in lst)
        log(f"   {v:<20} 거래처 {len(lst):>3}개 / 매출합 {am:>15,} ({am/1_000_000:.0f}M)")

    # JSON 저장
    OUT_JSON.write_text(
        json.dumps(results, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    log(f"\n→ JSON 저장: {OUT_JSON}")

    OUT_TXT.write_text("\n".join(log_lines), encoding="utf-8")
    conn.close()


if __name__ == "__main__":
    main()
