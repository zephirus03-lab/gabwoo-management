"""
매입(외주) 단가표(출판).numbers → standard_pricing.json 변환 스크립트

출처: 견적 및 단가표 예시/01_매입(외주) 단가표(출판).numbers
경영진 확인(2026-04-13): 출판사업부는 인쇄 외 전부 외주 → 이 파일이 실질 표준단가표
"""

import json
from numbers_parser import Document

SRC = "../견적 및 단가표 예시/01_매입(외주) 단가표(출판).numbers"
OUT = "standard_pricing.json"

def cell_val(table, r, c):
    """셀 값을 가져옵니다. 범위 밖이면 None을 반환합니다."""
    try:
        v = table.cell(r, c).value
        return v
    except:
        return None

def num(v):
    """숫자로 변환합니다. 실패하면 None을 반환합니다."""
    if v is None:
        return None
    try:
        return float(v)
    except:
        return None

def parse_printing(table):
    """인쇄 시트를 파싱합니다 (본문 + 표지)."""
    # 본문 단가: 국전(행3~14), 4×6(행15~27)
    # 컬럼: [0]구분 [1]부수 [2]1도 [3]2도 [4]4도
    body = {}
    for size_name, start, end in [("국전", 3, 15), ("4x6", 15, 28)]:
        matrix = {}
        for r in range(start, end):
            qty_raw = cell_val(table, r, 1)
            c1 = num(cell_val(table, r, 2))
            c2 = num(cell_val(table, r, 3))
            c4_raw = cell_val(table, r, 4)
            c4 = num(c4_raw)

            if qty_raw is None and c1 is None:
                continue

            qty_str = str(qty_raw).strip() if qty_raw else ""

            # 부수 구간 정규화
            qty_key = qty_str
            qty_num = num(qty_raw)
            if qty_num and qty_num >= 100000:
                qty_key = f"{int(qty_num/1000)}K이상"
            elif qty_num:
                qty_key = str(int(qty_num))

            entry = {}
            if c1 is not None:
                entry["1도"] = int(c1)
            if c2 is not None:
                entry["2도"] = int(c2)
            if c4 is not None:
                entry["4도"] = int(c4)
            elif c4_raw and "별도" in str(c4_raw):
                entry["4도"] = "별도협의"

            if entry:
                matrix[qty_key] = entry

        body[size_name] = matrix

    # 표지 단가: 국전(행3~7), 4×6(행8~12)
    # 컬럼: [6]구분 [7]수량 [8]단가
    cover = {}
    for size_name, start, end in [("국전", 3, 8), ("4x6", 8, 13)]:
        prices = {}
        for r in range(start, end):
            qty_raw = cell_val(table, r, 7)
            price = num(cell_val(table, r, 8))
            if qty_raw and price:
                prices[str(qty_raw).strip()] = int(price)
        if prices:
            cover[size_name] = prices

    # 면지 단가 (행14 비고)
    cover["면지"] = 4000

    return {"본문": body, "표지": cover}


def parse_finishing(table):
    """후가공 시트를 파싱합니다."""
    result = {"일반": {}, "교원_이투스": {}, "특수": {}, "박": {}, "톰슨": {}}

    # 일반 코팅 (행3~12)
    for r in range(3, 13):
        coating_type = cell_val(table, r, 1)
        r_range = cell_val(table, r, 2)
        p86 = num(cell_val(table, r, 3))  # 8×6
        p_guk = num(cell_val(table, r, 4))  # 국전
        p46 = num(cell_val(table, r, 5))  # 4×6

        if coating_type is None and r_range is None:
            continue

        coating_str = str(coating_type).strip() if coating_type else ""
        range_str = str(r_range).strip() if r_range else ""

        if not range_str or not p_guk:
            continue

        key = f"{coating_str}_{range_str}" if coating_str else range_str
        # 이전 행의 coating_type 이어받기
        if not coating_str:
            # 위 행에서 coating_type 찾기
            for prev_r in range(r - 1, 2, -1):
                prev_ct = cell_val(table, prev_r, 1)
                if prev_ct:
                    coating_str = str(prev_ct).strip()
                    key = f"{coating_str}_{range_str}"
                    break

        entry = {}
        if p86: entry["8x6"] = int(p86)
        if p_guk: entry["국전"] = int(p_guk)
        if p46: entry["4x6"] = int(p46)

        if entry:
            if "일반" not in result:
                result["일반"] = {}
            result["일반"][key] = entry

    # 써멀/UV/엠보/이지스킨/소프트/에폭시 (행13~22)
    special = {}
    for r in range(13, 23):
        name = cell_val(table, r, 1)
        sub = cell_val(table, r, 2)
        p86 = num(cell_val(table, r, 3))
        p_guk = num(cell_val(table, r, 4))
        p46 = num(cell_val(table, r, 5))
        note = cell_val(table, r, 6)

        name_str = str(name).strip() if name else ""
        sub_str = str(sub).strip() if sub else ""

        key = f"{name_str}_{sub_str}" if sub_str else name_str
        if not key:
            continue

        entry = {}
        if p86: entry["8x6"] = int(p86)
        if p_guk: entry["국전"] = int(p_guk)
        if p46: entry["4x6"] = int(p46)
        if note: entry["비고"] = str(note).strip()

        if entry:
            special[key] = entry

    result["특수"] = special

    # 박 (행23~30)
    foil = {}
    for r in range(23, 31):
        sub = cell_val(table, r, 2)
        price_raw = cell_val(table, r, 3)
        if sub:
            sub_str = str(sub).strip()
            price_str = str(price_raw).strip() if price_raw else ""
            foil[sub_str] = price_str

    result["박"] = foil

    # 교원/이투스 코팅 (행31~39)
    kyowon = {}
    for r in range(31, 40):
        coating = cell_val(table, r, 1)
        r_range = cell_val(table, r, 2)
        p86 = num(cell_val(table, r, 3))
        p_guk = num(cell_val(table, r, 4))
        p46 = num(cell_val(table, r, 5))

        coating_str = str(coating).strip() if coating else ""
        range_str = str(r_range).strip() if r_range else ""

        if not range_str or not p_guk:
            continue

        if not coating_str:
            for prev_r in range(r - 1, 30, -1):
                prev_ct = cell_val(table, prev_r, 1)
                if prev_ct:
                    coating_str = str(prev_ct).strip()
                    break

        key = f"{coating_str}_{range_str}"
        entry = {}
        if p86: entry["8x6"] = int(p86)
        if p_guk: entry["국전"] = int(p_guk)
        if p46: entry["4x6"] = int(p46)

        if entry:
            kyowon[key] = entry

    result["교원_이투스"] = kyowon

    return result


def parse_binding(table):
    """제본 시트를 파싱합니다."""
    result = {}

    # 무선제본 (행2~22)
    wireless = {}

    # 기본단가
    base_prices = {}
    for r in [3, 4]:
        bind_type = cell_val(table, r, 1)
        if bind_type and ("무선" in str(bind_type)):
            for c, size in [(3, "8절"), (4, "신국판"), (5, "4x6배판"), (6, "국배판")]:
                v = num(cell_val(table, r, c))
                if v and v > 0:
                    btype = str(bind_type).strip()
                    if btype not in base_prices:
                        base_prices[btype] = {}
                    base_prices[btype][size] = int(v)

    wireless["기본단가"] = base_prices

    # 부당기본 (행7)
    per_unit = {}
    for c, size in [(4, "신국판"), (5, "4x6배판"), (6, "국배판")]:
        v = num(cell_val(table, 7, c))
        if v:
            per_unit[size] = int(v)
    wireless["부당기본"] = per_unit

    # 페이지당 단가 (행8)
    per_page = {}
    for c, size in [(3, "8절"), (4, "신국판"), (5, "4x6배판"), (6, "국배판")]:
        v = num(cell_val(table, 8, c))
        if v:
            per_page[size] = v
    wireless["100P_이상_페이지당"] = per_page

    # 표지날개/띠지/커버/CD 등 (행16~21)
    extras = {}
    for r in range(16, 23):
        name = cell_val(table, r, 1)
        note = cell_val(table, r, 7)
        if name:
            name_str = str(name).strip()
            prices = {}
            for c, size in [(4, "신국판"), (5, "4x6배판"), (6, "국배판")]:
                v = num(cell_val(table, r, c))
                if v:
                    prices[size] = v if v < 1 else int(v)
            if prices:
                entry = {"단가": prices}
                if note:
                    entry["비고"] = str(note).strip()
                extras[name_str] = entry

    wireless["부가작업"] = extras
    result["무선"] = wireless

    # 양장 (행23~29)
    hardcover = {}
    # 기본단가
    hc_base = {}
    for c, size in [(3, "46판"), (4, "신국판"), (5, "4x6배판"), (6, "국배판")]:
        v = num(cell_val(table, 24, c))
        if v:
            hc_base[size] = int(v)
    hardcover["기본단가"] = hc_base

    # 페이지당 단가
    hc_per_page = {}
    for r in range(26, 30):
        work_type = cell_val(table, r, 2)
        if work_type:
            work_str = str(work_type).strip()
            prices = {}
            for c, size in [(3, "46판"), (4, "신국판"), (5, "4x6배판"), (6, "국배판")]:
                v = num(cell_val(table, r, c))
                if v:
                    prices[size] = v
            if prices:
                hc_per_page[work_str] = prices
    hardcover["100P_이상_페이지당"] = hc_per_page
    result["양장"] = hardcover

    # PUR (행30~33)
    pur = {}
    pur_base = {}
    for c, size in [(4, "신국판"), (5, "4x6배"), (6, "국배판")]:
        v = num(cell_val(table, 32, c))
        if v:
            pur_base[size] = int(v)
    pur["기본단가"] = pur_base
    pur["날개추가"] = 50000

    pur_pp = {}
    for c, size in [(4, "신국판"), (5, "4x6배"), (6, "국배판")]:
        v = num(cell_val(table, 33, c))
        if v:
            pur_pp[size] = v
    pur["100P_이상_페이지당"] = pur_pp
    result["PUR"] = pur

    # 중철 (행34~37)
    saddle = {}
    for r in range(35, 38):
        name = cell_val(table, r, 1)
        if name:
            name_str = str(name).strip()
            prices = {}
            for c, size in [(4, "신국판"), (5, "4x6배"), (6, "국배판")]:
                v = num(cell_val(table, r, c))
                if v:
                    prices[size] = v if v < 1 else int(v)
            if prices:
                saddle[name_str] = prices
    result["중철"] = saddle

    return result


def parse_logistics(table):
    """물류비 시트를 파싱합니다."""
    # 컬럼: [0]지역 [1]다마스 [2]라보 [3]1톤 [4]1.4톤 [5]2.5톤 [6]3.5톤 [7]5톤 [8]5톤축 [9]회수비용
    vehicle_types = ["다마스", "라보", "1톤", "1.4톤", "2.5톤", "3.5톤", "5톤", "5톤축"]
    result = {}

    for r in range(4, 40):
        region = cell_val(table, r, 0)
        if not region:
            continue
        region_str = str(region).strip()

        # 안내 문구 건너뛰기
        if "(" in region_str and "DC" in region_str:
            continue

        prices = {}
        for c in range(1, 9):
            v = num(cell_val(table, r, c))
            if v:
                prices[vehicle_types[c - 1]] = int(v)

        # 회수비용
        pickup = num(cell_val(table, r, 9))
        if pickup:
            prices["회수비용"] = int(pickup)

        if prices:
            result[region_str] = prices

    return result


def parse_paper_discount(table):
    """용지할인율 시트를 파싱합니다."""
    result = {}
    current_category = ""

    for r in range(4, 40):
        cat = cell_val(table, r, 0)
        paper = cell_val(table, r, 1)
        spec = cell_val(table, r, 2)
        maker = cell_val(table, r, 3)
        old_rate = num(cell_val(table, r, 4))
        new_rate = num(cell_val(table, r, 5))

        if cat:
            current_category = str(cat).strip()

        if not paper:
            continue

        paper_str = str(paper).strip()
        rate = new_rate if new_rate else old_rate

        entry = {
            "분류": current_category,
            "용지사양": str(spec).strip() if spec else "전체",
        }
        if maker:
            entry["제지사"] = str(maker).strip()
        if rate:
            entry["할인율"] = round(rate, 3)

        key = f"{current_category}_{paper_str}"
        result[key] = entry

    return result


def parse_internal(table):
    """내부거래 단가표를 파싱합니다."""
    # 인쇄비는 일반 인쇄와 동일 구조, 판비만 추출
    plate_costs = {}

    for r in range(3, 6):
        name = cell_val(table, r, 6)
        sub = cell_val(table, r, 7)
        price = num(cell_val(table, r, 8))

        if name and price:
            name_str = str(name).strip()
            if sub:
                name_str += f"_{str(sub).strip()}"
            plate_costs[name_str] = int(price)

    return {"판비": plate_costs}


def main():
    doc = Document(SRC)
    sheets = {s.name: s for s in doc.sheets}

    pricing = {
        "_meta": {
            "source": "견적 및 단가표 예시/01_매입(외주) 단가표(출판).numbers",
            "label": "갑우문화사 출판사업부 매입(외주) 표준단가표",
            "note": "출판사업부는 인쇄 외 전부 외주. 이 단가표가 실질 표준단가 (경영진 확인 2026-04-13)",
            "updated": "2026-04-13",
            "supersedes": "기존 단가표.xlsx 기반 standard_pricing.json"
        }
    }

    # 1. 인쇄
    if "인쇄" in sheets:
        pricing["인쇄"] = parse_printing(sheets["인쇄"].tables[0])
        pricing["인쇄"]["_note"] = "외주 인쇄 표준 단가. 거래처별 오버라이드 가능"

    # 2. 후가공
    if "후가공" in sheets:
        pricing["후가공"] = parse_finishing(sheets["후가공"].tables[0])
        pricing["후가공"]["_note"] = "코팅, 박, 톰슨 등. 교원/이투스는 별도 단가 적용"

    # 3. 제본
    if "제본" in sheets:
        pricing["제본"] = parse_binding(sheets["제본"].tables[0])
        pricing["제본"]["_note"] = "무선/양장/PUR/중철. 더원프린팅 별도 단가표 있음"

    # 4. 내부거래 (판비)
    if "내부거래" in sheets:
        pricing["내부거래"] = parse_internal(sheets["내부거래"].tables[0])
        pricing["내부거래"]["_note"] = "자체 인쇄 시 판비(CTF/CTP)"

    # 5. 물류비
    if "물류비" in sheets:
        pricing["물류비"] = parse_logistics(sheets["물류비"].tables[0])
        pricing["물류비"]["_note"] = "지역별 차량 크기별 배송 단가"

    # 6. 용지할인율
    if "용지할인율" in sheets:
        pricing["용지할인율"] = parse_paper_discount(sheets["용지할인율"].tables[0])
        pricing["용지할인율"]["_note"] = "신승지류 기준 2025-07-01. 직송 시 1~2% 추가 할인"

    # 7. 거래처별 오버라이드 (별도 시트들)
    overrides = {}

    # 이투스 윤전
    if "이투스_국윤전" in sheets:
        t = sheets["이투스_국윤전"].tables[0]
        etus = {"CTP": 6000, "국윤전": {}, "46윤전": {}}

        current_color = ""
        for r in range(9, 40):
            color = cell_val(t, r, 0)
            range_val = cell_val(t, r, 1)
            price_guk = num(cell_val(t, r, 4))
            price_46 = num(cell_val(t, r, 9))

            if color:
                current_color = str(color).strip().replace("\n", "")

            if range_val and price_guk:
                range_str = str(range_val).strip()
                key = f"{current_color}_{range_str}"
                etus["국윤전"][key] = int(price_guk)
                if price_46:
                    etus["46윤전"][key] = int(price_46)

        overrides["이투스에듀"] = etus

    # 금하 인쇄
    if "금하" in sheets:
        t = sheets["금하"].tables[0]
        overrides["금하"] = {"_note": "외주인쇄(금하) — 일반 단가표와 유사, 4도 국전 10K~30K에서 900원 차이"}

    # 프린트뱅크
    if "프린트뱅크" in sheets:
        overrides["프린트뱅크"] = {"_note": "외주인쇄(프린트뱅크) — 일반 대비 약 10% 할증"}

    if overrides:
        pricing["거래처_오버라이드"] = overrides

    # 기존 ERP 항목코드 매핑 (호환성 유지)
    pricing["ERP_항목코드_매핑"] = {
        "703002": {"name": "CTP(제판)", "category": "내부거래.판비.CTP", "standard_price": 5000},
        "303001": {"name": "인쇄", "category": "인쇄.본문", "note": "부수/색도/용지 3변수 룩업"},
        "303002": {"name": "인쇄(2종)", "category": "인쇄.본문", "note": "303001과 동일 매트릭스"},
        "_note": "ERP 63,522건 분석에서 추출한 항목코드 → 단가표 섹션 매핑"
    }

    # 저장
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(pricing, f, ensure_ascii=False, indent=2)

    print(f"✅ {OUT} 생성 완료")
    print(f"   섹션: {[k for k in pricing if not k.startswith('_')]}")

    # 주요 숫자 검증 출력
    if "인쇄" in pricing:
        body = pricing["인쇄"]["본문"]
        print(f"\n📊 인쇄 본문 검증:")
        if "국전" in body and "1000" in body["국전"]:
            print(f"   국전 1,000부 1도: {body['국전']['1000'].get('1도', '-')}원")
        if "국전" in body and "10000" in body["국전"]:
            print(f"   국전 10,000부 4도: {body['국전']['10000'].get('4도', '-')}원")

    if "제본" in pricing:
        bind = pricing["제본"]
        if "무선" in bind and "기본단가" in bind["무선"]:
            base = bind["무선"]["기본단가"]
            print(f"\n📊 제본 검증:")
            for btype, prices in base.items():
                print(f"   {btype}: {prices}")

    if "후가공" in pricing:
        finish = pricing["후가공"]
        if "특수" in finish:
            print(f"\n📊 후가공 특수 항목: {list(finish['특수'].keys())}")


if __name__ == "__main__":
    main()
