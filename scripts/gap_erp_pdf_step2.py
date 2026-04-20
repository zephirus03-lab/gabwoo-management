"""
Step 2: ERP(B) vs PDF(세금계산서) 갭 분해.
목표:
  (1) 월별 갭: 어느 달에 집중?
  (2) 거래처별 갭: 어느 거래처가 주범?
  (3) PDF-only 레코드 성격: 사업자번호/금액 패턴
  (4) ERP-only 레코드 성격: 마이너스(취소)/특수
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

# PDF 내에서 어느 회사의 매출인지는 페이지 구분자로 안 나와서, 전체 파싱 후 월별 누계 라인으로 페이지 경계 추정.
# 대신 레코드 매칭 시 갑우 사업자(107-81-40772)와 관계된 건은 제외하고 보자.
# 실제로는 PDF 전체를 "갑우 발행 세금계산서"로 간주하고 ERP OWN=10000과 매칭.


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


def parse_pdf_by_firm():
    """
    PDF에는 갑우·비피·더원이 섞여 있음. 월계/누계 라인 간격으로 회사 경계 추정.
    3사 각각 [월계] 12개 + [누계] 12개 패턴. PDF 구조상 갑우가 먼저(1-77p), 비피(78-149p), 더원(150-155p) 순.
    """
    reader = pypdf.PdfReader(str(PDF))
    # 회사별 페이지 범위: 누계값으로 자동 판별
    # 갑우 12월 누계: 15,144,286,566
    # 비피 12월 누계: 21,806,851,511
    # 더원 12월 누계: 2,026,587,554
    totals_by_page = []
    for i, page in enumerate(reader.pages):
        txt = page.extract_text() or ""
        m = re.search(r"\[누\s*계\].*?([\d,]{10,})", txt)
        if m:
            v = int(m.group(1).replace(",", ""))
            totals_by_page.append((i, v))

    # 각 회사의 마지막 누계 = 연 누계. 누계가 리셋되는 지점이 회사 경계.
    bounds = []  # [(firm, start_page, end_page)]
    firm_names = ["갑우", "비피", "더원"]
    annual_totals = [15_144_286_566, 21_806_851_511, 2_026_587_554]
    # 각 annual에 해당하는 페이지 찾기
    last_page = 0
    for fi, tot in enumerate(annual_totals):
        for p, v in totals_by_page:
            if v == tot and p >= last_page:
                bounds.append((firm_names[fi], last_page, p))
                last_page = p + 1
                break

    # 각 범위 페이지 → 레코드 파싱
    firm_rows = {"갑우": [], "비피": [], "더원": []}
    for firm, sp, ep in bounds:
        for pi in range(sp, ep + 1):
            txt = reader.pages[pi].extract_text() or ""
            for line in txt.splitlines():
                mb = BIZ.search(line)
                md = DATE.search(line)
                if not (mb and md):
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
                    firm_rows[firm].append({"date": date_ymd, "biz": bizno, "am": amt, "line": line[:120]})
    return firm_rows


def month_of(date_ymd):
    return date_ymd[:6]


def analyze_gap(firm_name, pdf_rows, erp_rows):
    print(f"\n{'='*80}")
    print(f"🔍 {firm_name} — ERP(B) vs PDF 레코드 매칭 분석")
    print(f"{'='*80}")

    # 키: (date, biz, |am|)
    erp_by_key = defaultdict(list)
    for r in erp_rows:
        erp_by_key[(r["dt"], r["bz"] or "", abs(r["am"]))].append(r)

    pdf_by_key = defaultdict(list)
    for r in pdf_rows:
        pdf_by_key[(r["date"], r["biz"], abs(r["am"]))].append(r)

    # 매칭
    matched_keys = set(erp_by_key.keys()) & set(pdf_by_key.keys())
    pdf_only_keys = set(pdf_by_key.keys()) - matched_keys
    erp_only_keys = set(erp_by_key.keys()) - matched_keys

    matched_amt = sum(abs(pdf_rows[0]["am"]) for _ in range(0))  # init
    matched_amt = 0
    for k in matched_keys:
        matched_amt += k[2] * min(len(pdf_by_key[k]), len(erp_by_key[k]))

    pdf_only_amt = sum(abs(r["am"]) for k in pdf_only_keys for r in pdf_by_key[k])
    erp_only_amt = sum(abs(r["am"]) for k in erp_only_keys for r in erp_by_key[k])
    pdf_total = sum(abs(r["am"]) for r in pdf_rows)
    erp_total = sum(r["am"] for r in erp_rows if r["am"] > 0)

    print(f"  PDF 총건수: {len(pdf_rows):,}건  / 합: {pdf_total:>15,}")
    print(f"  ERP 총건수: {len(erp_rows):,}건  / 합(>0): {erp_total:>15,}")
    print(f"  정확매칭:  {len(matched_keys):,}키 / 합: {matched_amt:>15,}")
    print(f"  PDF only: {len(pdf_only_keys):,}키 / 합: {pdf_only_amt:>15,}  ← ERP에 없는 세금계산서")
    print(f"  ERP only: {len(erp_only_keys):,}키 / 합: {erp_only_amt:>15,}  ← PDF에 없는 ERP 레코드")

    # 월별 PDF-only
    print(f"\n  [월별 PDF-only 금액 — ERP에 없는 세금계산서]")
    by_month = defaultdict(int)
    for k in pdf_only_keys:
        for r in pdf_by_key[k]:
            by_month[month_of(r["date"])] += abs(r["am"])
    for m in sorted(by_month):
        print(f"    {m}: {by_month[m]:>13,}")

    # PDF-only 거래처 TOP10
    print(f"\n  [PDF-only 거래처 TOP10 — ERP 등록 누락 의심]")
    by_biz = defaultdict(lambda: {"amt": 0, "cnt": 0})
    for k in pdf_only_keys:
        for r in pdf_by_key[k]:
            by_biz[r["biz"]]["amt"] += abs(r["am"])
            by_biz[r["biz"]]["cnt"] += 1
    top = sorted(by_biz.items(), key=lambda x: -x[1]["amt"])[:10]
    for bz, v in top:
        print(f"    사업자={bz}  건수={v['cnt']:>4}  금액={v['amt']:>13,}")

    # ERP-only 중 마이너스(취소) 분리
    print(f"\n  [ERP-only 성격 분해]")
    neg_amt, pos_amt, neg_cnt, pos_cnt = 0, 0, 0, 0
    no_biz_amt, no_biz_cnt = 0, 0
    for k in erp_only_keys:
        for r in erp_by_key[k]:
            if not r["bz"]:
                no_biz_amt += abs(r["am"])
                no_biz_cnt += 1
            if r["am"] < 0:
                neg_amt += abs(r["am"])
                neg_cnt += 1
            else:
                pos_amt += r["am"]
                pos_cnt += 1
    print(f"    (+)금액 레코드: {pos_cnt:,}건 / {pos_amt:>13,}  ← ERP에만 있는 양의 매출")
    print(f"    (-)금액 레코드: {neg_cnt:,}건 / {neg_amt:>13,}  ← 취소/할인/반품")
    print(f"    사업자번호 없음: {no_biz_cnt:,}건 / {no_biz_amt:>13,}  ← 현금판매/내부")

    return {
        "pdf_total": pdf_total, "erp_total": erp_total,
        "matched_amt": matched_amt, "pdf_only_amt": pdf_only_amt, "erp_only_amt": erp_only_amt,
    }


def main():
    print("PDF 3사 분리 파싱 중...")
    firm_rows = parse_pdf_by_firm()
    for firm, rs in firm_rows.items():
        print(f"  {firm}: {len(rs):,}건 / {sum(abs(r['am']) for r in rs):,}원")

    conn = connect()
    cur = conn.cursor(as_dict=True)

    firm_to_own = {"갑우": "10000", "비피": "20000", "더원": "30000"}

    for firm, pdf_list in firm_rows.items():
        own = firm_to_own[firm]
        cur.execute(f"""
            SELECT s.DT_SALES dt, c.NO_BIZ bz, CAST(s.AM AS BIGINT) am, s.ST_SALES st, s.NO_SALES nos
            FROM SAL_SALESH s
            LEFT JOIN MAS_CUST c ON c.CD_CUST=s.CD_CUST AND c.CD_FIRM=s.CD_FIRM
            WHERE s.CD_CUST_OWN='{own}' AND s.DT_SALES LIKE '2025%'
              AND ((s.ST_SALES='Y' OR s.ST_SALES IS NULL) AND s.AM>0)
        """)
        erp_list = cur.fetchall()
        erp_list = [{"dt": r["dt"], "bz": (r["bz"] or "").replace("-", ""), "am": int(r["am"])} for r in erp_list]
        analyze_gap(firm, pdf_list, erp_list)


if __name__ == "__main__":
    main()
