"""
ERP ↔ PDF 매칭 재조사.
목표: ERP에서 어떤 조건으로 뽑아야 PDF(실제 세금계산서 발행)와 가장 가까운가.

전략:
1. 필터 조건 후보별로 총합 / 거래처별 일치율 비교
2. (날짜, 거래처, 금액) 3-key로 레코드 매칭 시도
3. 내부 그룹 거래(갑우/비피앤피 자기거래) 구분
"""
import re
from pathlib import Path
from collections import defaultdict
import pypdf
import pymssql

PDF = Path("/Users/jack/dev/gabwoo/PDF_25년 제품매출현황(갑우,비피,더원).pdf")
ENV = Path("/Users/jack/dev/gabwoo/견적계산기/.env.local")

BIZ = re.compile(r"(\d{3}-\d{2}-\d{5})")
DATE = re.compile(r"(20\d{2}/\d{2}/\d{2})")


def parse_pdf_rows():
    reader = pypdf.PdfReader(str(PDF))
    rows = []  # dict(date_ymd, bizno, amount)
    for page in reader.pages:
        txt = page.extract_text() or ""
        for line in txt.splitlines():
            mb = BIZ.search(line)
            if not mb:
                continue
            md = DATE.search(line)
            if not md:
                continue
            bizno = mb.group(1).replace("-", "")
            date_ymd = md.group(1).replace("/", "")
            tail = line[mb.end():]
            nums = re.findall(r"-?[\d,]+", tail)
            amt = None
            for n in nums:
                v = n.replace(",", "")
                if v.lstrip("-").isdigit() and abs(int(v)) >= 1000:
                    amt = int(v)
                    break
            if amt is not None:
                rows.append({"date": date_ymd, "biz": bizno, "am": amt})
    return rows


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


def scenario_test(conn, pdf_total, pdf_by_biz):
    """여러 ERP 필터 조건별 2025 합계 테스트"""
    cur = conn.cursor(as_dict=True)
    scenarios = [
        ("A. ST='Y' AND AM>0 (현재 대시보드)", "ST_SALES='Y' AND AM>0"),
        ("B. ST IN ('Y', NULL) AND AM>0", "(ST_SALES='Y' OR ST_SALES IS NULL) AND AM>0"),
        ("C. ST <> 'N' AND AM>0", "(ST_SALES IS NULL OR ST_SALES<>'N') AND AM>0"),
        ("D. 모든 레코드 (AM<>0)", "AM<>0"),
        ("E. ST<>'N' 모든 부호 (NULL+Y, 취소차감 포함)", "(ST_SALES IS NULL OR ST_SALES<>'N')"),
    ]
    print("\n=== ERP 필터 조건별 2025 총합 vs PDF 38,945M ===")
    print(f"{'시나리오':<50}{'합계':>18}{'PDF대비':>10}")
    for name, where in scenarios:
        cur.execute(f"SELECT SUM(CAST(AM AS BIGINT)) s FROM SAL_SALESH WHERE CD_FIRM='7000' AND DT_SALES LIKE '2025%' AND {where}")
        s = cur.fetchone()["s"] or 0
        ratio = s / pdf_total * 100
        print(f"{name:<50}{s:>18,}{ratio:>9.1f}%")


def per_customer_scenarios(conn, pdf_by_biz):
    """주요 거래처별로 시나리오별 일치율"""
    cur = conn.cursor(as_dict=True)
    cur.execute("""
        SELECT c.NO_BIZ bz, c.NM_CUST nm,
          SUM(CASE WHEN s.ST_SALES='Y' AND s.AM>0 THEN CAST(s.AM AS BIGINT) ELSE 0 END) a_only_y_pos,
          SUM(CASE WHEN (s.ST_SALES='Y' OR s.ST_SALES IS NULL) AND s.AM>0 THEN CAST(s.AM AS BIGINT) ELSE 0 END) b_y_null_pos,
          SUM(CASE WHEN s.ST_SALES IS NULL OR s.ST_SALES<>'N' THEN CAST(s.AM AS BIGINT) ELSE 0 END) c_not_n_all,
          SUM(CAST(s.AM AS BIGINT)) d_all
        FROM SAL_SALESH s
        JOIN MAS_CUST c ON c.CD_CUST=s.CD_CUST AND c.CD_FIRM=s.CD_FIRM
        WHERE s.CD_FIRM='7000' AND s.DT_SALES LIKE '2025%' AND c.NO_BIZ IS NOT NULL
        GROUP BY c.NO_BIZ, c.NM_CUST
    """)
    erp_map = {r["bz"]: r for r in cur.fetchall()}

    priority = [
        "4968602009",  # 교원
        "6678702103",  # 교원구몬
        "7228502352",  # 이투스에듀
        "1198154852",  # 에듀윌
        "1448120381",  # 코스알엑스
        "1058129368",  # HS애드
        "2118804526",  # 씨디유디자인
        "7608801434",  # 필통북스
        "1168118745",  # GS리테일
        "1438119635",  # 코스맥스
        "3078138444",  # 한국콜마
        "3148200583",  # 조폐공사
        "7818801805",  # 지담디앤피
        "8668700833",  # 동행복권
        "1058131452",  # 지학사
        "1108132211",  # 다락원
    ]
    print("\n=== 거래처별 시나리오 일치율 (PDF 대비) ===")
    print(f"{'거래처':<20}{'PDF':>13}{'A(Y+)':>13}{'B(Y/null+)':>13}{'C(≠N all)':>13}{'D(all)':>13}  최적")
    for bz in priority:
        if bz not in pdf_by_biz or bz not in erp_map:
            continue
        p = pdf_by_biz[bz]
        e = erp_map[bz]
        name = e["nm"][:18]
        a, b, c, d = int(e["a_only_y_pos"]), int(e["b_y_null_pos"]), int(e["c_not_n_all"]), int(e["d_all"])
        # 가장 p에 가까운 시나리오
        cands = [("A", a), ("B", b), ("C", c), ("D", d)]
        best = min(cands, key=lambda x: abs(x[1] - p))
        print(f"{name:<20}{p:>13,}{a:>13,}{b:>13,}{c:>13,}{d:>13,}  {best[0]}({best[1]/p*100 if p else 0:.0f}%)")


def record_matching(conn, pdf_rows):
    """레코드 단위 매칭: (날짜, 사업자번호, 절대금액) 3-key"""
    cur = conn.cursor(as_dict=True)
    # ERP 2025 전체 레코드 (AM<>0)
    cur.execute("""
        SELECT s.DT_SALES dt, c.NO_BIZ bz, CAST(s.AM AS BIGINT) am, s.ST_SALES st, s.NO_SALES nos
        FROM SAL_SALESH s
        LEFT JOIN MAS_CUST c ON c.CD_CUST=s.CD_CUST AND c.CD_FIRM=s.CD_FIRM
        WHERE s.CD_FIRM='7000' AND s.DT_SALES LIKE '2025%' AND s.AM<>0
    """)
    erp = cur.fetchall()

    # 키: (date, biz, am_abs) / 값: [레코드]
    erp_key = defaultdict(list)
    for r in erp:
        if r["bz"]:
            erp_key[(r["dt"], r["bz"], abs(r["am"]))].append(r)

    matched, pdf_only, amt_matched, pdf_total = 0, 0, 0, 0
    for pr in pdf_rows:
        pdf_total += 1
        key = (pr["date"], pr["biz"], abs(pr["am"]))
        if key in erp_key and erp_key[key]:
            matched += 1
            amt_matched += abs(pr["am"])
        else:
            pdf_only += 1

    print(f"\n=== 레코드 단위 매칭 (날짜+사업자번호+|금액| 정확일치) ===")
    print(f"PDF 전체: {pdf_total:,}건 / ERP 전체: {len(erp):,}건")
    print(f"  정확 매칭: {matched:,}건 ({matched/pdf_total*100:.1f}%)")
    print(f"  PDF에만:  {pdf_only:,}건")


def main():
    print("PDF 파싱 중...")
    pdf_rows = parse_pdf_rows()
    pdf_by_biz = defaultdict(int)
    for r in pdf_rows:
        pdf_by_biz[r["biz"]] += r["am"]
    pdf_total = sum(pdf_by_biz.values())
    print(f"  PDF 합: {pdf_total:,} ({len(pdf_rows):,}건, {len(pdf_by_biz)}개 거래처)")

    conn = connect()
    scenario_test(conn, pdf_total, pdf_by_biz)
    per_customer_scenarios(conn, pdf_by_biz)
    record_matching(conn, pdf_rows)


if __name__ == "__main__":
    main()
