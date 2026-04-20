"""
U1 v5: 3단계 분류 + 모호 Top 거래처(씨디유 등) NM_ITEM 까기.

분류 우선순위:
  1단계 — 거래처명 화이트리스트 (가장 강력)
  2단계 — DC_CUST_TYPE 키워드 정규화
  3단계 — NM_ITEM 키워드 비율 (보조)

부가 작업: 모호 Top 5 거래처(씨디유디자인/한성피앤아이/알래스카애드/워터멜론/투엘)
의 NM_ITEM 샘플 30개씩 출력 → 보고서에 첨부할 정체 확인 자료.
"""
from pathlib import Path
import json
import pymssql

ENV = Path("/Users/jack/dev/gabwoo/견적계산기/.env.local")
OUT_TXT = Path(__file__).parent / "output" / "u1_classification_v5.txt"
OUT_JSON = Path(__file__).parent / "output" / "customer_classification_v5.json"
OUT_SAMPLES = Path(__file__).parent / "output" / "ambiguous_customer_samples.txt"

# ─── 1단계: 거래처명 화이트리스트 ────────────────────────────────────
# 회사명 한 단어가 들어있으면 강제 매핑 (대소문자 무시)
PACKAGE_NAME_KW = [
    "코스맥스", "코스메카", "코스알엑스", "코스메틱",
    "정샘물", "라샘", "매그니프", "콜마", "인터코스", "브이티피엘",
    "VTP", "VTPL", "와니코스", "노디너리", "샤르드", "신미글로벌",
    "아이다", "지피클럽", "한솔생명", "아이비엘", "서울화장품",
    "에스테르", "뷰티", "코스메", "코스매", "에스티비",
    "샴푸", "립", "바이오", "스킨", "팩토리",
]
PUBLISH_NAME_KW = [
    "출판", "북스", "도서", "교재", "교과서",
    "에듀", "edu", "EDU", "필통", "에듀윌", "에듀토셀",
    "교원", "기문당", "자유아카데미", "한미의학",
    "잡지", "정기간행물", "신문", "이비에스미디어", "EBS",
    "학습", "해커스", "디딤돌", "미래엔", "가야미디어",
    "그래서음악", "스몰빅미디어", "지구문화사", "도서출판",
    "이오문화사", "객석컴퍼니", "원교재사", "학토재", "토이트론",
    "코딩앤플레이", "한국가이던스", "기프트서울", "한국판촉선물",
]

# ─── 2단계: DC_CUST_TYPE 키워드 ──────────────────────────────────────
PACKAGE_TYPE_KW = ["화장품", "코스메", "단상자", "지함", "패키지"]
PUBLISH_TYPE_KW = [
    "출판", "도서", "교재", "교과서", "서적", "잡지",
    "정기간행물", "학습", "신문", "인쇄", "경인쇄", "옵셋",
]

# ─── 3단계: NM_ITEM 키워드 (v4와 동일) ────────────────────────────────
PACKAGE_ITEM_KW = [
    "단상자", "리필", "인박스", "케이스", "브러쉬", "팩트", "튜브",
    "용기", "포장", "쇼핑백", "쿠션", "립스틱", "파운데이션",
    "에센스", "세럼", "지함", "OP", "받침", "뚜껑", "카톤", "파우치",
    "마스카라", "쉐도우", "아이펜슬", "아이브로우", "립밤",
]
PUBLISH_ITEM_KW = [
    "쇄", "호", "표지", "본문", "시리즈", "핸드북", "양장", "무선",
    "교재", "문제집", "워크북", "기본서", "전집", "도서", "잡지",
    "리플렛", "다이어리", "달력", "카탈로그", "팸플릿", "상장",
    "교과서", "참고서", "OMR", "표지교환", "정교재",
]

# 모호 거래처 정체 확인 대상
AMBIGUOUS_TARGETS = ["씨디유디자인", "한성피앤아이", "알래스카애드",
                     "워터멜론", "투엘미디어"]


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


def name_classify(nm_cust):
    """1단계 — 거래처명 키워드 매칭."""
    if not nm_cust:
        return None
    n = nm_cust
    pkg = any(k in n for k in PACKAGE_NAME_KW)
    pub = any(k in n for k in PUBLISH_NAME_KW)
    if pkg and not pub:
        return "PACKAGE"
    if pub and not pkg:
        return "PUBLISH"
    return None


def type_classify(dc_type):
    """2단계 — DC_CUST_TYPE 키워드."""
    if not dc_type:
        return None
    t = dc_type
    pkg = any(k in t for k in PACKAGE_TYPE_KW)
    pub = any(k in t for k in PUBLISH_TYPE_KW)
    if pkg and not pub:
        return "PACKAGE"
    if pub and not pkg:
        return "PUBLISH"
    if pkg and pub:
        return None  # 둘 다 → 다음 단계로
    return None


def item_classify(am_pkg, am_pub, am_total):
    """3단계 — NM_ITEM 비율."""
    if am_total == 0:
        return None
    pr = am_pkg / am_total
    pr2 = am_pub / am_total
    if pr >= 0.5:
        return "PACKAGE"
    if pr2 >= 0.5:
        return "PUBLISH"
    return None


def main():
    OUT_TXT.parent.mkdir(parents=True, exist_ok=True)
    log_lines = []

    def log(*args):
        msg = " ".join(str(a) for a in args)
        print(msg)
        log_lines.append(msg)

    conn = connect()
    cur = conn.cursor(as_dict=True)

    # ─── A. Top 100 거래처 3단계 분류 ───────────────────────────────
    log("=" * 80)
    log("v5 — 3단계 분류 (거래처명 → 업종 → NM_ITEM)")
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
        dc_type = cust.get("DC_CUST_TYPE")
        am_total = int(cust["am_total"] or 0)

        # 라인 NM_ITEM 분포
        cur.execute("""
            SELECT i.NM_ITEM, CAST(l.AM AS BIGINT) am
            FROM SAL_SALESL l
            INNER JOIN SAL_SALESH h ON h.NO_SALES = l.NO_SALES
            LEFT JOIN PRT_ITEM i ON i.CD_ITEM = l.CD_ITEM
            WHERE h.DT_SALES LIKE '2025%' AND h.CD_CUST_OWN='10000'
              AND h.ST_SALES='Y' AND h.AM > 0
              AND h.CD_CUST = %s
        """, (cd,))
        am_pkg = am_pub = 0
        line_total = 0
        for ln in cur.fetchall():
            n = ln["NM_ITEM"] or ""
            am = int(ln["am"] or 0)
            line_total += am
            if any(k in n for k in PACKAGE_ITEM_KW):
                am_pkg += am
            elif any(k in n for k in PUBLISH_ITEM_KW):
                am_pub += am

        # 3단계 판정
        v1 = name_classify(nm)
        v2 = type_classify(dc_type)
        v3 = item_classify(am_pkg, am_pub, line_total)

        verdict = v1 or v2 or v3
        if not verdict:
            verdict = "REVIEW"  # 수동 검토 필요
        source = "name" if v1 else ("type" if v2 else ("item" if v3 else "none"))

        results.append({
            "rank": len(results) + 1,
            "cd_cust": cd,
            "nm_cust": nm,
            "dc_cust_type": dc_type,
            "am_total": am_total,
            "verdict": verdict,
            "source": source,
            "by_name": v1,
            "by_type": v2,
            "by_item": v3,
            "pkg_ratio": round(am_pkg / line_total, 3) if line_total else 0,
            "pub_ratio": round(am_pub / line_total, 3) if line_total else 0,
        })

    # 보고
    log(f"\n{'순위':<4}{'거래처':<28}{'업종':<13}{'매출(M)':>8}{'판정':>10}{'근거':>8}{'pkg%':>7}{'pub%':>7}")
    for r in results:
        log(f"{r['rank']:<4}{r['nm_cust'][:27]:<28}"
            f"{(str(r['dc_cust_type'])[:12] if r['dc_cust_type'] else '-'):<13}"
            f"{r['am_total']/1_000_000:>8.0f}"
            f"{r['verdict']:>10}{r['source']:>8}"
            f"{r['pkg_ratio']*100:>6.1f}%{r['pub_ratio']*100:>6.1f}%")

    # 합계
    log("\n" + "=" * 80)
    log("v5 분류 결과 — 매출 합계 기준")
    log("=" * 80)
    by_v = {}
    for r in results:
        by_v.setdefault(r["verdict"], []).append(r)
    for v, lst in sorted(by_v.items(), key=lambda x: -sum(r["am_total"] for r in x[1])):
        am = sum(r["am_total"] for r in lst)
        log(f"   {v:<10} 거래처 {len(lst):>3}개 / 매출합 {am:>15,} ({am/1_000_000:.0f}M)")

    # 근거별 분포
    log("\n근거(source) 분포:")
    by_s = {}
    for r in results:
        by_s.setdefault(r["source"], []).append(r)
    for s, lst in sorted(by_s.items(), key=lambda x: -len(x[1])):
        am = sum(r["am_total"] for r in lst)
        log(f"   {s:<8} 거래처 {len(lst):>3}개 / 매출합 {am:>15,}")

    # JSON 저장
    OUT_JSON.write_text(
        json.dumps(results, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    log(f"\n→ JSON: {OUT_JSON}")

    # ─── B. 모호 Top 거래처 NM_ITEM 샘플 ─────────────────────────────
    log("\n" + "=" * 80)
    log("B. 모호 거래처 NM_ITEM 샘플 30개씩 (정체 확인)")
    log("=" * 80)
    sample_lines = []

    def slog(*args):
        msg = " ".join(str(a) for a in args)
        print(msg)
        log_lines.append(msg)
        sample_lines.append(msg)

    for kw in AMBIGUOUS_TARGETS:
        slog(f"\n── '{kw}' ──────────────────────────────────")
        cur.execute(f"""
            SELECT TOP 30 c.NM_CUST, c.DC_CUST_TYPE, i.NM_ITEM, CAST(l.AM AS BIGINT) am
            FROM SAL_SALESL l
            INNER JOIN SAL_SALESH h ON h.NO_SALES = l.NO_SALES
            INNER JOIN MAS_CUST c ON c.CD_CUST = h.CD_CUST
            LEFT JOIN PRT_ITEM i ON i.CD_ITEM = l.CD_ITEM
            WHERE h.DT_SALES LIKE '2025%' AND h.CD_CUST_OWN='10000'
              AND h.ST_SALES='Y' AND h.AM > 0
              AND c.NM_CUST LIKE '%' + %s + '%'
            ORDER BY CAST(l.AM AS BIGINT) DESC
        """, (kw,))
        rows = cur.fetchall()
        if not rows:
            slog(f"   매칭 0건")
            continue
        slog(f"   거래처: {rows[0]['NM_CUST']} / 업종: {rows[0].get('DC_CUST_TYPE')}")
        for r in rows:
            am = int(r["am"] or 0)
            slog(f"   ₩{am:>11,}  {r['NM_ITEM']}")

    OUT_SAMPLES.write_text("\n".join(sample_lines), encoding="utf-8")
    OUT_TXT.write_text("\n".join(log_lines), encoding="utf-8")
    log(f"\n→ 샘플: {OUT_SAMPLES}")
    conn.close()


if __name__ == "__main__":
    main()
