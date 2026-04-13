#!/usr/bin/env python3
"""
ERP 견적 상세현황 엑셀 파일 분석 스크립트 (읽기 전용)
원본 파일을 절대 수정하지 않습니다.
"""
import pandas as pd
import sys

FILE = "/Users/jack/dev/gabwoo/견적계산기/견적 상세 현황/견적서 상세현황_(20250601~20260410).xlsx"

print("=" * 70)
print("ERP 견적 상세현황 분석")
print("=" * 70)

print("\n파일 로딩 중...")
df = pd.read_excel(FILE, engine="openpyxl")

# 1. 컬럼명
print("\n[1] 컬럼명 (총 {}개)".format(len(df.columns)))
for i, col in enumerate(df.columns, 1):
    print(f"  {i:2d}. {col}")

# 2. 총 행 수
print(f"\n[2] 총 행 수: {len(df):,}행")

# 3. 첫 5행 샘플
print("\n[3] 첫 5행 샘플")
pd.set_option("display.max_columns", None)
pd.set_option("display.width", 250)
pd.set_option("display.max_colwidth", 30)
print(df.head(5).to_string())

# 4. 고유값 수
print("\n[4] 주요 컬럼 고유값 수")
for col in ["영업담당", "거래처", "항목", "견적번호"]:
    if col in df.columns:
        print(f"  {col}: {df[col].nunique():,}개")
    else:
        print(f"  {col}: 컬럼 없음")

# 5. 항목 Top 10
print("\n[5] 항목 Top 10")
if "항목" in df.columns:
    top_items = df["항목"].value_counts().head(10)
    for item, cnt in top_items.items():
        print(f"  {item}: {cnt:,}건")

# 6. 거래처 Top 10
print("\n[6] 거래처 Top 10")
if "거래처" in df.columns:
    top_clients = df["거래처"].value_counts().head(10)
    for client, cnt in top_clients.items():
        print(f"  {client}: {cnt:,}건")

# 7. 데이터 타입
print("\n[7] 데이터 타입")
for col in df.columns:
    non_null = df[col].notna().sum()
    print(f"  {col}: {df[col].dtype} (non-null: {non_null:,}/{len(df):,})")

# 8. 단가/금액 non-null 행 수
print("\n[8] 단가·금액 non-null 행 수")
for col in ["단가", "금액", "최종금액"]:
    if col in df.columns:
        non_null = df[col].notna().sum()
        non_zero = (df[col].fillna(0) != 0).sum()
        print(f"  {col}: non-null {non_null:,}건, non-zero {non_zero:,}건")
    else:
        print(f"  {col}: 컬럼 없음")

# 단가 AND 금액 동시 non-null
if "단가" in df.columns and "금액" in df.columns:
    both = (df["단가"].notna() & df["금액"].notna()).sum()
    both_nonzero = ((df["단가"].fillna(0) != 0) & (df["금액"].fillna(0) != 0)).sum()
    print(f"  단가+금액 모두 non-null: {both:,}건")
    print(f"  단가+금액 모두 non-zero: {both_nonzero:,}건")

print("\n" + "=" * 70)
print("분석 완료")
print("=" * 70)
