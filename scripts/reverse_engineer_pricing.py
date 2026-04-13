"""
ERP 견적 상세현황에서 영업자별 단가 적정성을 분석하는 스크립트입니다.

핵심 질문:
  1. 각 영업자가 표준단가 대비 제대로 된 견적을 내고 있는가?
  2. 각 공정(제판/인쇄/제본) 단가가 거래처별로 공정하게 매겨지고 있는가?
  3. 대량 주문 할인이 합리적 수준인가?

⚠️ 라벨: 초안(ERP 역산) — 영업자 확인 전. 실제 단가표와 대조 필요.

입력: 견적서 상세현황_(20250601~20260410).xlsx (63,523행, 읽기 전용)
출력: scripts/output/pricing_audit.json, pricing_audit.csv
"""

import json
import os
from pathlib import Path

import numpy as np
import pandas as pd

# --- 경로 설정 ---
SCRIPT_DIR = Path(__file__).parent
PROJECT_DIR = SCRIPT_DIR.parent
ERP_FILE = PROJECT_DIR / "견적 상세 현황" / "견적서 상세현황_(20250601~20260410).xlsx"
STANDARD_PRICING_FILE = SCRIPT_DIR / "standard_pricing.json"
OUTPUT_DIR = SCRIPT_DIR / "output"
OUTPUT_JSON = OUTPUT_DIR / "pricing_audit.json"
OUTPUT_CSV = OUTPUT_DIR / "pricing_audit.csv"

# 표준단가 매칭 가능한 주요 항목코드
# 기준: 매입(외주) 단가표(출판).numbers — 경영진 확인 2026-04-13
# 출판사업부는 인쇄 외 전부 외주이므로 외주 매입 단가가 실질 표준
KEY_ITEMS = {
    "703002": {"name": "CTP(제판)", "standard": 5000},
    "303001": {"name": "인쇄", "standard": 1500},  # 대표값 (부수 구간별 변동)
    "303002": {"name": "인쇄(2종)", "standard": 1500},
}

# 본문 인쇄 표준단가 매트릭스 (국전 4도 기준)
# 출처: 매입(외주) 단가표(출판).numbers > 인쇄 시트
PRINT_STANDARD_BY_VOLUME = [
    (1000, 3000),    # 1,000부 (국전 4도)
    (2000, 2000),    # 2,000부
    (3000, 1500),    # 3,000부
    (4000, 1400),    # 4,000부
    (5000, 1300),    # 5,000부
    (6000, 1200),    # 6,000부
    (7000, 1100),    # 7,000부
    (8000, 1000),    # 8,000부 ~ 10,000부
    (10000, 1000),   # 10,000부
]


def get_print_standard_price(volume):
    """부수(R)에 따른 인쇄 표준단가를 반환합니다. (국전 4도 기준, 외주 매입 단가)"""
    if pd.isna(volume) or volume <= 0:
        return 1500  # 기본 대표값
    for threshold, price in PRINT_STANDARD_BY_VOLUME:
        if volume < threshold:
            return price
    return 900  # 100,000부 이상


def load_and_prepare():
    """ERP 데이터를 로드하고 분석용으로 준비합니다."""
    print("📂 ERP 파일 로딩...")
    df = pd.read_excel(ERP_FILE, engine="openpyxl")
    print(f"   → {len(df):,}행 로드")

    # 항목 컬럼 정리
    df["항목"] = df["항목"].astype(str).str.strip()
    df.loc[df["항목"].isin(["nan", "None", ""]), "항목"] = pd.NA

    # 출판사업부(갑우문화사) + 영업담당 있는 행만 필터
    mask = (
        df["영업담당"].notna()
        & df["항목"].notna()
        & df["단가"].notna()
        & (df["단가"] > 0)
    )
    filtered = df[mask].copy()
    print(f"📊 분석 대상: {len(filtered):,}건 (영업담당 있고 + 항목 있고 + 단가 > 0)")
    print(f"   영업담당 {filtered['영업담당'].nunique()}명, 거래처 {filtered['거래처'].nunique()}곳")

    # 표준단가 컬럼 추가
    filtered["표준단가"] = filtered.apply(calc_standard_price, axis=1)
    filtered["편차(%)"] = filtered.apply(
        lambda r: round((r["단가"] - r["표준단가"]) / r["표준단가"] * 100, 1)
        if pd.notna(r["표준단가"]) and r["표준단가"] > 0 else None,
        axis=1
    )

    return df, filtered


def calc_standard_price(row):
    """행별 표준단가를 계산합니다."""
    item = str(row["항목"])
    if item not in KEY_ITEMS:
        return None

    if item == "703002":  # CTP — 고정 (외주 매입 단가)
        return 5000

    if item in ("303001", "303002"):  # 인쇄 — 부수 기반
        volume = row.get("수량(R)", None)
        return get_print_standard_price(volume)

    return KEY_ITEMS[item]["standard"]


def build_salesperson_scorecard(filtered):
    """영업자별 업무 패턴를 만듭니다."""
    print("\n📋 영업자별 업무 패턴 생성 중...")

    # 표준단가 매칭 가능한 행만
    with_std = filtered[filtered["표준단가"].notna()].copy()

    scorecards = {}
    for name, group in with_std.groupby("영업담당"):
        # 기본 통계
        total_rows = len(group)
        avg_diff = group["편차(%)"].mean()
        median_diff = group["편차(%)"].median()

        # 이상 건 (표준 대비 ±30% 초과)
        anomalies = group[group["편차(%)"].abs() > 30]
        anomaly_rate = len(anomalies) / total_rows * 100 if total_rows > 0 else 0

        # 할인 건 vs 할증 건
        discount_rows = group[group["편차(%)"] < -5]
        premium_rows = group[group["편차(%)"] > 5]
        normal_rows = group[(group["편차(%)"] >= -5) & (group["편차(%)"] <= 5)]

        # 담당 거래처
        top_clients = group.groupby("거래처").agg(
            건수=("단가", "count"),
            평균편차=("편차(%)", "mean"),
        ).sort_values("건수", ascending=False).head(5)

        # 항목별 분석
        item_stats = group.groupby("항목").agg(
            건수=("단가", "count"),
            단가_중앙값=("단가", "median"),
            표준단가=("표준단가", "first"),
            평균편차=("편차(%)", "mean"),
        ).sort_values("건수", ascending=False)

        scorecards[name] = {
            "name": name,
            "total_rows": int(total_rows),
            "avg_diff_pct": round(float(avg_diff), 1),
            "median_diff_pct": round(float(median_diff), 1),
            "anomaly_count": int(len(anomalies)),
            "anomaly_rate_pct": round(float(anomaly_rate), 1),
            "discount_count": int(len(discount_rows)),
            "premium_count": int(len(premium_rows)),
            "normal_count": int(len(normal_rows)),
            "top_clients": [
                {
                    "name": c,
                    "count": int(r["건수"]),
                    "avg_diff_pct": round(float(r["평균편차"]), 1),
                }
                for c, r in top_clients.iterrows()
            ],
            "item_stats": [
                {
                    "item_code": item,
                    "item_name": KEY_ITEMS.get(item, {}).get("name", item),
                    "count": int(r["건수"]),
                    "median_price": float(r["단가_중앙값"]),
                    "standard_price": float(r["표준단가"]),
                    "avg_diff_pct": round(float(r["평균편차"]), 1),
                }
                for item, r in item_stats.iterrows()
            ],
        }

    # 각 영업자에 패턴 해석 추가
    for name, sc in scorecards.items():
        sc["pattern_analysis"] = generate_pattern_analysis(sc)

    return scorecards


def generate_pattern_analysis(sc):
    """영업자 데이터를 기반으로 영업 패턴을 자동 해석합니다."""
    lines = []
    name = sc["name"]
    total = sc["total_rows"]
    avg = sc["avg_diff_pct"]
    anomaly_rate = sc["anomaly_rate_pct"]
    discount_pct = sc["discount_count"] / total * 100 if total > 0 else 0
    premium_pct = sc["premium_count"] / total * 100 if total > 0 else 0
    normal_pct = sc["normal_count"] / total * 100 if total > 0 else 0

    if total < 10:
        return "분석 건수가 10건 미만으로 패턴 판단이 어렵습니다."

    # 1. 전체 경향
    if avg < -10:
        lines.append(f"전체적으로 표준 대비 평균 {avg:+.1f}% 낮은 단가를 적용하고 있습니다. 할인 위주의 영업 패턴입니다.")
    elif avg > 20:
        lines.append(f"전체적으로 표준 대비 평균 {avg:+.1f}% 높은 단가를 적용하고 있습니다. 소량·특수 건이 많거나 단가 입력 방식을 확인할 필요가 있습니다.")
    elif avg > 5:
        lines.append(f"표준 대비 평균 {avg:+.1f}% 소폭 높은 단가를 적용하고 있습니다.")
    elif avg > -5:
        lines.append(f"표준단가와 거의 일치하는 단가를 적용하고 있습니다 (평균 {avg:+.1f}%).")
    else:
        lines.append(f"표준 대비 평균 {avg:+.1f}% 소폭 낮은 단가를 적용하고 있습니다.")

    # 2. 구성 비율 해석
    if discount_pct > 60:
        lines.append(f"견적의 {discount_pct:.0f}%가 할인 건으로, 대량 거래처 중심이거나 공격적 가격 정책을 취하고 있습니다.")
    elif premium_pct > 60:
        lines.append(f"견적의 {premium_pct:.0f}%가 할증 건으로, 소량·특수 건 비중이 높거나 ERP 단가 입력 단위를 확인할 필요가 있습니다.")
    elif normal_pct > 50:
        lines.append(f"견적의 {normal_pct:.0f}%가 정상 범위로, 표준단가를 비교적 충실히 따르고 있습니다.")
    else:
        lines.append(f"할인 {discount_pct:.0f}% / 정상 {normal_pct:.0f}% / 할증 {premium_pct:.0f}%로, 거래처별 단가 편차가 큰 편입니다.")

    # 3. 이상비율
    if anomaly_rate > 30:
        lines.append(f"이상 건 비율이 {anomaly_rate:.1f}%로 높습니다. 특수 조건 거래가 많거나, 단가 입력 오류 가능성을 점검할 필요가 있습니다.")
    elif anomaly_rate > 15:
        lines.append(f"이상 건 비율이 {anomaly_rate:.1f}%로, 일부 거래처에서 표준과 크게 다른 단가가 적용되고 있습니다.")

    # 4. 거래처 패턴
    clients = sc["top_clients"]
    if clients:
        top = clients[0]
        if len(clients) == 1 or (top["count"] > sum(c["count"] for c in clients[1:]) * 2):
            lines.append(f"주력 거래처 '{top['name']}'에 집중된 영업 구조입니다 ({top['count']}건, 편차 {top['avg_diff_pct']:+.1f}%).")
        else:
            client_names = [c["name"] for c in clients[:3]]
            lines.append(f"주요 거래처: {', '.join(client_names)} 등 다수 거래처를 담당하고 있습니다.")

    # 5. 항목별 특이점
    for item in sc["item_stats"]:
        if item["count"] >= 5 and abs(item["avg_diff_pct"]) > 30:
            direction = "높게" if item["avg_diff_pct"] > 0 else "낮게"
            lines.append(f"'{item['item_name']}' 항목에서 표준 대비 {abs(item['avg_diff_pct']):.1f}% {direction} 적용 — 확인 필요.")

    return " ".join(lines)


def build_process_fairness(filtered):
    """공정별 거래처 간 공정성 분석을 합니다."""
    print("⚖️  공정별 공정성 분석 중...")

    results = {}
    for item_code, item_info in KEY_ITEMS.items():
        item_data = filtered[filtered["항목"] == item_code].copy()
        if len(item_data) == 0:
            continue

        # 거래처별 집계
        by_client = item_data.groupby("거래처").agg(
            건수=("단가", "count"),
            중앙값=("단가", "median"),
            최빈값=("단가", lambda x: x.mode().iloc[0] if len(x.mode()) > 0 else x.median()),
            최소=("단가", "min"),
            최대=("단가", "max"),
            평균편차=("편차(%)", "mean"),
            영업담당=("영업담당", "first"),
        ).sort_values("건수", ascending=False)

        # 전체 통계
        all_median = float(item_data["단가"].median())
        all_std = float(item_data["단가"].std())
        standard = item_info["standard"]

        clients_list = []
        for client, r in by_client.iterrows():
            clients_list.append({
                "client": client,
                "salesperson": r["영업담당"],
                "count": int(r["건수"]),
                "median": float(r["중앙값"]),
                "mode": float(r["최빈값"]),
                "min": float(r["최소"]),
                "max": float(r["최대"]),
                "avg_diff_pct": round(float(r["평균편차"]), 1) if pd.notna(r["평균편차"]) else None,
            })

        results[item_code] = {
            "name": item_info["name"],
            "standard_price": standard,
            "total_rows": len(item_data),
            "overall_median": all_median,
            "overall_std": round(all_std, 1),
            "clients": clients_list,
        }

    return results


def find_anomalies(filtered, threshold_pct=30):
    """표준 대비 이상 건을 탐지합니다."""
    print(f"🔍 이상 건 탐지 중 (±{threshold_pct}% 초과)...")

    with_std = filtered[filtered["편차(%)"].notna()].copy()
    anomalies = with_std[with_std["편차(%)"].abs() > threshold_pct].copy()

    def rows_to_list(df_slice):
        result = []
        for _, row in df_slice.iterrows():
            result.append({
                "견적번호": str(row.get("견적번호", "")),
                "수주번호": str(row.get("수주번호", "")) if pd.notna(row.get("수주번호")) else "",
                "견적일": str(row.get("견적일", "")),
                "영업담당": str(row["영업담당"]),
                "거래처": str(row["거래처"]),
                "품명": str(row.get("품명", "")) if pd.notna(row.get("품명")) else "",
                "항목": str(row["항목"]),
                "항목명": KEY_ITEMS.get(str(row["항목"]), {}).get("name", str(row["항목"])),
                "단가": float(row["단가"]),
                "표준단가": float(row["표준단가"]),
                "편차(%)": float(row["편차(%)"]),
                "수량R": float(row["수량(R)"]) if pd.notna(row.get("수량(R)")) else None,
                "승인여부": str(row.get("승인여부", "")),
            })
        return result

    # 할증 (편차 > +30%) — 편차 큰 순 30건
    premium_anomalies = anomalies[anomalies["편차(%)"] > threshold_pct].sort_values("편차(%)", ascending=False).head(30)
    # 할인 (편차 < -30%) — 편차 큰 순(음수이므로 오름차순) 30건
    discount_anomalies = anomalies[anomalies["편차(%)"] < -threshold_pct].sort_values("편차(%)").head(30)

    print(f"   → 이상 건: {len(anomalies):,}건 (전체 {len(with_std):,}건 중 {len(anomalies)/len(with_std)*100:.1f}%)")
    print(f"      할증 이상: {len(anomalies[anomalies['편차(%)'] > threshold_pct]):,}건 / 할인 이상: {len(anomalies[anomalies['편차(%)'] < -threshold_pct]):,}건")

    return {
        "premium": rows_to_list(premium_anomalies),
        "discount": rows_to_list(discount_anomalies),
        "total_count": len(anomalies),
        "premium_total": int(len(anomalies[anomalies["편차(%)"] > threshold_pct])),
        "discount_total": int(len(anomalies[anomalies["편차(%)"] < -threshold_pct])),
    }


def build_volume_discount_analysis(filtered):
    """대량 주문 할인의 합리성을 분석합니다."""
    print("📦 대량 주문 할인 분석 중...")

    # 인쇄 항목만 (303001, 303002)
    printing = filtered[filtered["항목"].isin(["303001", "303002"])].copy()
    printing = printing[printing["수량(R)"].notna() & (printing["수량(R)"] > 0)]

    if len(printing) == 0:
        return {}

    # 부수 구간별 집계
    bins = [0, 1000, 2000, 3000, 4000, 5000, 7000, 10000, float("inf")]
    labels = ["~1천", "1~2천", "2~3천", "3~4천", "4~5천", "5~7천", "7~1만", "1만+"]
    printing["부수구간"] = pd.cut(printing["수량(R)"], bins=bins, labels=labels, right=False)

    # 전체 구간별
    overall = printing.groupby("부수구간", observed=True).agg(
        건수=("단가", "count"),
        단가_중앙값=("단가", "median"),
        단가_평균=("단가", "mean"),
    ).reset_index()

    # 영업담당별 × 부수구간
    by_sales = printing.groupby(["영업담당", "부수구간"], observed=True).agg(
        건수=("단가", "count"),
        단가_중앙값=("단가", "median"),
    ).reset_index()

    # 표준 구간별 단가
    standard_by_bin = {
        "~1천": "판갯수x5000",
        "1~2천": 3500, "2~3천": 3000, "3~4천": 2500,
        "4~5천": 2200, "5~7천": 2000, "7~1만": 1800, "1만+": 1600,
    }

    return {
        "total_rows": len(printing),
        "overall": [
            {
                "range": str(r["부수구간"]),
                "count": int(r["건수"]),
                "median_price": round(float(r["단가_중앙값"])),
                "avg_price": round(float(r["단가_평균"])),
                "standard": standard_by_bin.get(str(r["부수구간"])),
            }
            for _, r in overall.iterrows()
        ],
        "by_salesperson": {},
    }


def save_all(scorecards, fairness, anomalies, volume, filtered):
    """모든 결과를 저장합니다."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    output = {
        "_meta": {
            "label": "초안(ERP 역산) — 영업자 확인 전. 실제 단가표와 대조 필요.",
            "source": ERP_FILE.name,
            "generated": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M"),
            "analysis_scope": "영업담당 있고 + 항목 있고 + 단가 > 0인 행",
            "total_analyzed": len(filtered),
        },
        "scorecards": scorecards,
        "process_fairness": fairness,
        "anomalies": anomalies,
        "volume_discount": volume,
    }

    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"\n💾 JSON 저장: {OUTPUT_JSON}")

    # CSV도 저장 (간단 버전)
    rows = []
    for name, sc in scorecards.items():
        rows.append({
            "영업담당": name,
            "분석건수": sc["total_rows"],
            "평균편차(%)": sc["avg_diff_pct"],
            "중앙편차(%)": sc["median_diff_pct"],
            "이상건수": sc["anomaly_count"],
            "이상비율(%)": sc["anomaly_rate_pct"],
            "할인건": sc["discount_count"],
            "할증건": sc["premium_count"],
            "정상건": sc["normal_count"],
        })
    pd.DataFrame(rows).to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
    print(f"💾 CSV 저장: {OUTPUT_CSV}")


def print_summary(scorecards, anomalies):
    """핵심 요약을 출력합니다."""
    print("\n" + "=" * 70)
    print("📋 영업자별 업무 패턴 요약 (초안 — ERP 역산)")
    print("=" * 70)

    # 정렬: 이상비율 높은 순
    sorted_cards = sorted(scorecards.values(), key=lambda x: x["anomaly_rate_pct"], reverse=True)

    print(f"\n{'영업담당':<10} {'건수':>6} {'평균편차':>8} {'이상건':>6} {'이상비율':>8} {'할인':>5} {'정상':>5} {'할증':>5}")
    print("-" * 70)
    for sc in sorted_cards:
        flag = " ⚠️" if sc["anomaly_rate_pct"] > 20 else ""
        print(f"{sc['name']:<10} {sc['total_rows']:>6,} {sc['avg_diff_pct']:>+7.1f}% {sc['anomaly_count']:>6,} {sc['anomaly_rate_pct']:>7.1f}% {sc['discount_count']:>5,} {sc['normal_count']:>5,} {sc['premium_count']:>5,}{flag}")

    if anomalies:
        print(f"\n🔍 할증 이상 TOP 5 (표준 대비 +30% 초과):")
        for a in anomalies["premium"][:5]:
            print(f"  {a['영업담당']} × {a['거래처'][:15]} | {a['항목명']} | "
                  f"단가 {a['단가']:,.0f} vs 표준 {a['표준단가']:,.0f} ({a['편차(%)']:+.1f}%)")
        print(f"\n🔍 할인 이상 TOP 5 (표준 대비 -30% 초과):")
        for a in anomalies["discount"][:5]:
            print(f"  {a['영업담당']} × {a['거래처'][:15]} | {a['항목명']} | "
                  f"단가 {a['단가']:,.0f} vs 표준 {a['표준단가']:,.0f} ({a['편차(%)']:+.1f}%)")


def main():
    df_raw, filtered = load_and_prepare()
    scorecards = build_salesperson_scorecard(filtered)
    fairness = build_process_fairness(filtered)
    anomalies = find_anomalies(filtered)
    volume = build_volume_discount_analysis(filtered)
    save_all(scorecards, fairness, anomalies, volume, filtered)
    print_summary(scorecards, anomalies)

    print(f"\n✅ 완료!")
    print("   ⚠️ 라벨: 초안(ERP 역산) — 영업자 확인 전. 실제 단가표와 대조 필요.")


if __name__ == "__main__":
    main()
