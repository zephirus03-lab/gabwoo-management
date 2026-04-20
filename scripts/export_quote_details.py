"""
ERP에서 견적 상세현황을 엑셀로 추출합니다.
견적서 상세현황_(20250601~20260410).xlsx 와 동일한 29개 컬럼 포맷입니다.

사용법:
    python3 export_quote_details.py 20240101 20251231
    python3 export_quote_details.py            # 기본 2024-01-01 ~ 2025-12-31
"""
import sys
from pathlib import Path
from datetime import datetime

import pymssql
import openpyxl

ENV_FILE = Path("/Users/jack/dev/gabwoo/견적계산기/.env.local")
OUT_DIR = Path(__file__).parent / "output"
OUT_DIR.mkdir(exist_ok=True)

COLUMNS = [
    "승인여부", "영업담당", "귀속회사", "견적번호", "견적일", "거래처",
    "제품종류", "제본명", "부수", "견적금액", "건수", "견적명", "품명",
    "수주번호", "부서명", "구분", "용도", "항목", "규격", "단위", "기본",
    "대수", "수량(R)", "색도", "단가", "금액", "할인율", "최종금액", "비고",
]

COMPANY_MAP = {"7000": "갑우문화사", "8000": "비피앤피"}
APPROVAL_MAP = {"R": "승인", "P": "작성", "F": "확정"}


def load_env(path: Path) -> dict:
    env = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        env[k.strip()] = v.strip()
    return env


def fetch_rows(env: dict, dt_from: str, dt_to: str):
    conn = pymssql.connect(
        server=env["ERP_HOST"], port=int(env.get("ERP_PORT", 1433)),
        user=env["ERP_USER"], password=env["ERP_PASSWORD"],
        database=env["ERP_DATABASE"], login_timeout=10,
    )
    cur = conn.cursor(as_dict=True)
    sql = f"""
        SELECT
            h.YN_APP, e.NM_EMP, h.CD_FIRM, h.NO_EST, h.DT_EST, h.NM_PARTNER,
            h.TP_ITEM, h.FG_BIND, h.QT AS H_QT, h.AM_SUM,
            h.NM_EST, h.NO_SO, h.CD_DEPT,
            l.FG_EST, l.NM_ITEM, l.CD_ITEM, l.DC_ITEM_SPEC, l.DC_ITEM_UNIT,
            l.YN_BASE, l.QT_DASU, l.QT AS L_QT, l.QT_DOSU,
            l.UM, l.AM, l.RT_DISCOUNT, l.AM_SUPPLY, l.DC_RMK, l.NO_LINE
        FROM PRT_ESTH h
        LEFT JOIN MAS_EMP e
               ON e.CD_EMP = h.CD_EMP
        LEFT JOIN PRT_ESTL l
               ON l.CD_FIRM = h.CD_FIRM
              AND l.NO_EST  = h.NO_EST
              AND l.NO_HST  = h.NO_HST
        WHERE h.DT_EST >= '{dt_from}' AND h.DT_EST <= '{dt_to}'
        ORDER BY h.DT_EST, h.NO_EST, l.NO_LINE
    """
    cur.execute(sql)
    for r in cur:
        yield r
    conn.close()


def norm(v):
    if v is None:
        return ""
    if isinstance(v, str):
        return v.strip()
    return v


def main():
    if len(sys.argv) >= 3:
        dt_from, dt_to = sys.argv[1], sys.argv[2]
    else:
        dt_from, dt_to = "20240101", "20251231"

    env = load_env(ENV_FILE)
    print(f"추출 범위: {dt_from} ~ {dt_to}")
    t0 = datetime.now()

    wb = openpyxl.Workbook(write_only=True)
    ws = wb.create_sheet("Export")
    ws.append(COLUMNS)

    n = 0
    for r in fetch_rows(env, dt_from, dt_to):
        row = [
            APPROVAL_MAP.get(str(norm(r["YN_APP"])), norm(r["YN_APP"])),
            norm(r["NM_EMP"]),
            COMPANY_MAP.get(str(norm(r["CD_FIRM"])), norm(r["CD_FIRM"])),
            norm(r["NO_EST"]),
            norm(r["DT_EST"]),
            norm(r["NM_PARTNER"]),
            norm(r["TP_ITEM"]),
            norm(r["FG_BIND"]),
            r["H_QT"] or 0,
            r["AM_SUM"] or 0,
            "1",
            norm(r["NM_EST"]),
            norm(r["NM_EST"]),
            norm(r["NO_SO"]),
            norm(r["CD_DEPT"]),
            norm(r["FG_EST"]),
            norm(r["NM_ITEM"]),
            norm(r["CD_ITEM"]),
            norm(r["DC_ITEM_SPEC"]),
            norm(r["DC_ITEM_UNIT"]),
            norm(r["YN_BASE"]) or "N",
            r["QT_DASU"] or 0,
            r["L_QT"] or 0,
            r["QT_DOSU"] or 0,
            r["UM"] or 0,
            r["AM"] or 0,
            r["RT_DISCOUNT"] or 0,
            r["AM_SUPPLY"] or 0,
            norm(r["DC_RMK"]),
        ]
        ws.append(row)
        n += 1
        if n % 10000 == 0:
            print(f"  ... {n:,} 행")

    out_path = OUT_DIR / f"견적서_상세현황_({dt_from}~{dt_to}).xlsx"
    wb.save(out_path)
    dt = (datetime.now() - t0).total_seconds()
    print(f"✅ 완료: {n:,}행 · {dt:.1f}s · {out_path}")


if __name__ == "__main__":
    main()
