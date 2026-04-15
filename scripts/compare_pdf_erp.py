"""
PDF(세금계산서 발행 실제) vs ERP(SAL_SALESH 추정치) 거래처별 2025 매출 비교
- PDF 컬럼: 날짜 / 적요 / 거래처명 / 사업자번호 / 대변(금액) / 프로젝트명
- ERP: CD_FIRM='7000', DT_SALES LIKE '2025%', NO_BIZ 기준 매칭
"""
import re, sys
from pathlib import Path
from collections import defaultdict
import pypdf
import pymssql

PDF = Path("/Users/jack/dev/gabwoo/PDF_25년 제품매출현황(갑우,비피,더원).pdf")
ENV = Path("/Users/jack/dev/gabwoo/견적계산기/.env.local")

# PDF 한 행 패턴: 날짜 ... 사업자번호(10자리) 금액
BIZ = re.compile(r"(\d{3}-\d{2}-\d{5})")
AMT = re.compile(r"(-?[\d,]+)\s*(?:인쇄사업부|패키지사업부|콘텐츠사업부|출판사업부|디지털사업|KGM|인쇄사업|패키지사업|출판사업|$)")


def parse_pdf():
    """PDF에서 (사업자번호, 금액) 추출. 한 줄에 사업자번호 + 그 뒤 첫 숫자 = 대변."""
    reader = pypdf.PdfReader(str(PDF))
    rows = []  # (bizno, amount_int)
    for page in reader.pages:
        txt = page.extract_text() or ""
        for line in txt.splitlines():
            m = BIZ.search(line)
            if not m:
                continue
            bizno = m.group(1)
            tail = line[m.end():]
            # tail에서 첫 음수/양수 콤마숫자
            nums = re.findall(r"-?[\d,]+", tail)
            # 금액은 보통 마지막에서 두 번째 또는 끝 쪽
            amt = None
            for n in nums:
                v = n.replace(",", "")
                if v.lstrip("-").isdigit() and abs(int(v)) >= 1000:
                    amt = int(v)
                    break
            if amt is not None:
                rows.append((bizno, amt))
    return rows


def load_env(path):
    env = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        env[k.strip()] = v.strip()
    return env


def fetch_erp(bizno_set):
    env = load_env(ENV)
    conn = pymssql.connect(
        server=env["ERP_HOST"], port=int(env.get("ERP_PORT", 1433)),
        user=env["ERP_USER"], password=env["ERP_PASSWORD"],
        database=env["ERP_DATABASE"], login_timeout=10,
    )
    cur = conn.cursor(as_dict=True)
    # ERP: 거래처 × 2025 매출 (모든 ST_SALES, 총액 AM)
    cur.execute("""
        SELECT c.NO_BIZ AS bizno, c.NM_CUST AS name,
               SUM(CAST(s.AM AS BIGINT)) AS am_all,
               SUM(CASE WHEN s.ST_SALES='Y' AND s.AM>0 THEN CAST(s.AM AS BIGINT) ELSE 0 END) AS am_y,
               COUNT(*) AS cnt
        FROM SAL_SALESH s
        JOIN MAS_CUST c ON c.CD_CUST = s.CD_CUST AND c.CD_FIRM = s.CD_FIRM
        WHERE s.CD_FIRM='7000' AND s.DT_SALES LIKE '2025%'
        GROUP BY c.NO_BIZ, c.NM_CUST
    """)
    return cur.fetchall()


def main():
    print("PDF 파싱 중...")
    pdf_rows = parse_pdf()
    print(f"  PDF 행: {len(pdf_rows):,}")

    pdf_by_biz = defaultdict(int)
    pdf_cnt = defaultdict(int)
    for biz, amt in pdf_rows:
        key = biz.replace("-", "")  # ERP는 하이픈 없이 저장
        pdf_by_biz[key] += amt
        pdf_cnt[key] += 1
    print(f"  고유 사업자번호: {len(pdf_by_biz):,}")
    print(f"  PDF 합계: {sum(pdf_by_biz.values()):,}")

    print("\nERP 조회 중...")
    erp_rows = fetch_erp(set(pdf_by_biz.keys()))
    erp_by_biz = {r["bizno"]: r for r in erp_rows if r["bizno"]}
    print(f"  ERP 거래처 행: {len(erp_by_biz):,}")

    # 주요 거래처 목록 (PDF 합계 상위 + 사용자 관심)
    priority = [
        "496-86-02009",  # 교원
        "667-87-02103",  # 교원구몬
        "101-81-39767",  # 교원프라퍼티
        "104-86-30469",  # 교원라이프
        "433-87-00730",  # 교원더오름/헬스케어
        "129-81-67558",  # 교원위즈
        "722-85-02352",  # 이투스에듀 서초지점
        "119-81-54852",  # 에듀윌
        "144-81-20381",  # 코스알엑스
        "105-81-29368",  # 에이치에스애드
        "211-88-04526",  # 씨디유디자인
        "760-88-01434",  # 필통북스
        "220-82-00051",  # 한국산업기술진흥협회
        "116-81-18745",  # GS리테일
        "261-81-20935",  # 정샘물뷰티
        "143-81-19635",  # 코스맥스
        "303-81-24119",  # 코스메카코리아
        "211-86-27452",  # 코스맥스네오
        "307-81-38444",  # 한국콜마
        "101-86-92296",  # 인터코스코리아
        "209-81-54776",  # 알래스카애드
        "684-86-01005",  # 라샘코스메틱
        "297-81-00210",  # 매그니프
        "141-81-15138",  # 비피앤피(인쇄대)
        "101-82-04644",  # 한국금융연수원
        "214-88-59980",  # 핑크퐁
        "220-88-38559",  # EBS미디어
        "110-81-32211",  # 다락원
        "105-87-80649",  # 비즈니스북스
        "225-87-01399",  # 노머스
        "106-82-02511",  # 대한의사협회
        "314-82-00583",  # 한국조폐공사
        "866-87-00833",  # 동행복권
        "109-82-05569",  # 새마을금고복지회
        "157-86-01653",  # 케이랩
        "105-81-31452",  # 지학사
        "781-88-01805",  # 지담디앤피
    ]

    print("\n=== 주요 거래처 2025 매출: PDF vs ERP ===\n")
    print(f"{'사업자번호':<14} {'거래처명':<28} {'PDF':>15} {'ERP(전체)':>15} {'ERP(Y만)':>15} {'PDF-ERP전체':>15} {'일치율':>7}")
    print("-" * 125)
    pdf_total, erp_total = 0, 0
    for biz in priority:
        key = biz.replace("-", "")
        pdf_amt = pdf_by_biz.get(key, 0)
        erp = erp_by_biz.get(key)
        erp_all = int(erp["am_all"]) if erp else 0
        erp_y = int(erp["am_y"]) if erp else 0
        name = erp["name"] if erp else "(ERP에 없음)"
        diff = pdf_amt - erp_all
        ratio = (min(pdf_amt, erp_all) / max(pdf_amt, erp_all) * 100) if max(pdf_amt, erp_all) > 0 else 0
        print(f"{biz:<14} {name[:28]:<28} {pdf_amt:>15,} {erp_all:>15,} {erp_y:>15,} {diff:>15,} {ratio:>6.1f}%")
        pdf_total += pdf_amt
        erp_total += erp_all
    print("-" * 125)
    print(f"{'소계':<14} {'':<28} {pdf_total:>15,} {erp_total:>15,} {'':>15} {pdf_total-erp_total:>15,}")

    # PDF에 있는데 ERP에 없는 TOP 10
    print("\n=== PDF에 있으나 ERP에 없는 거래처 TOP 10 (ERP 누락) ===")
    missing = [(b, a) for b, a in pdf_by_biz.items() if b not in erp_by_biz]
    missing.sort(key=lambda x: -abs(x[1]))
    for b, a in missing[:10]:
        print(f"  {b}  {a:>15,}")


if __name__ == "__main__":
    main()
