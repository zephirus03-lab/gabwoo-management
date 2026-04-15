# 견적 대시보드 v0.1 Design Document

> **Summary**: ERP 견적 데이터를 업로드하여 경영진이 월별 매출을 한눈에 볼 수 있는 대시보드
>
> **Project**: 갑우문화사 견적 산출기
> **Author**: Jack (AX 퍼실리테이터) + Claude Code CTO팀
> **Date**: 2026-04-10
> **Status**: Draft
> **Planning Doc**: [dashboard-v0.1.plan.md](../01-plan/features/dashboard-v0.1.plan.md)

---

## Context Anchor

| Key | Value |
|-----|-------|
| **WHY** | 대표이사가 이번달 예상 매출을 구조적으로 알 수 없다. 세금계산서 발행 전까지 ERP 어디에도 실시간 매출 예측이 없다. |
| **WHO** | 1차: 대표이사(열람), 2차: Jack(업로드), 3차: 이수현 본부장(열람) |
| **RISK** | ① RLS 미설정 시 영업 단가 전체 노출 ② 비피앤피 60.5% 데이터로 매출 부풀림 ③ 거래처명 표기 불일치로 피벗 깨짐 ④ 63K건 엑셀 업로드 시 청크 실패 |
| **SUCCESS** | ① 대표이사가 브라우저에서 3패널 매출 확인 ② 거래처/영업자별 순위 확인 ③ 엑셀 업로드→반영 1분 이내 |
| **SCOPE** | v0.1: 엑셀 업로드+파싱, 3패널 대시보드, 피벗 차트, 기존 인증 재활용. 제외: 견적 생성, 단가 엔진, ERP 일괄 등록. |

---

## 1. Overview

### 1.1 Design Goals

1. **신뢰할 수 있는 숫자** — 데이터 정합성 검증을 내장하여 "이 숫자 맞아?"라는 질문에 답할 수 있게
2. **1분 이내 갱신** — 엑셀 업로드부터 대시보드 반영까지 체감 1분
3. **프린트 가능** — 대표이사가 주간 회의에 종이로 가져갈 수 있는 레이아웃
4. **확장 가능한 데이터 모델** — 향후 견적 생성·단가 엔진 추가 시 같은 DB를 활용

### 1.2 Design Principles

- **기존 패턴 재활용**: gabwoo 생산현황 서비스의 HTML 단일파일 + Supabase 패턴 유지
- **데이터 정확성 우선**: 한 건의 오집계도 프로젝트 전체 신뢰를 깨뜨린다
- **비개발자 유지보수**: Jack이 Claude Code로 수정할 수 있는 구조

---

## 2. Architecture

### 2.0 Architecture Comparison

| Criteria | Option A: Minimal | Option B: Clean | Option C: Pragmatic |
|----------|:-:|:-:|:-:|
| **Approach** | HTML 1파일, 구조 없음 | HTML/CSS/JS 파일 분리 | 1파일, IIFE 모듈 분리 |
| **New Files** | 1 | 6 | 1 |
| **Complexity** | Low | High | Medium |
| **Maintainability** | Low (2000줄+이면 혼란) | High | High |
| **Effort** | Low | Medium (Vercel 설정) | Low |
| **Risk** | Low (단기), High (장기) | Low | Low |

**Selected**: Option C — **Rationale**: 기존 gabwoo 서비스(3,356줄)가 이미 같은 패턴으로 안정적으로 운영 중. 비개발자(Jack)가 로컬 서버/번들러 없이 배포 가능. 내부적으로 IIFE 모듈로 관심사를 분리하여 유지보수성 확보.

### 2.1 Component Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                  quote-dashboard.html (단일 파일)                │
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ <style>                                                  │   │
│  │   /* Design Tokens */ — 색상, 폰트, 간격 CSS 변수         │   │
│  │   /* Layout */        — 그리드, 반응형 breakpoint         │   │
│  │   /* Components */    — 카드, 뱃지, 버튼, 테이블          │   │
│  │   /* Dashboard */     — 3패널, 차트 영역                  │   │
│  │   /* Upload */        — 드래그&드롭, 프로그레스바           │   │
│  │   /* Print */         — @media print 전용                │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ <body>                                                   │   │
│  │   #login-section     — 로그인 (기존 gabwoo 패턴 재활용)   │   │
│  │   #app-container     — 로그인 후 보이는 영역              │   │
│  │     #upload-panel    — 접이식 엑셀 업로드 패널            │   │
│  │     #dashboard-section — 3패널 KPI 카드                  │   │
│  │     #charts-section  — 거래처/영업자 차트                 │   │
│  │     #data-section    — 상세 데이터 테이블 (선택적)        │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ <script>                                                 │   │
│  │   const CONFIG = { supabaseUrl, supabaseKey, ... }       │   │
│  │   const Auth     = (function(){ ... })()  // 인증 모듈    │   │
│  │   const Upload   = (function(){ ... })()  // 엑셀 파싱    │   │
│  │   const Dashboard= (function(){ ... })()  // 대시보드     │   │
│  │   const Charts   = (function(){ ... })()  // 차트 렌더링  │   │
│  │   const DB       = (function(){ ... })()  // Supabase DB │   │
│  │   const App      = { init() { ... } }     // 앱 초기화    │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘

CDN Dependencies:
  ├── @supabase/supabase-js@2       (Supabase 클라이언트)
  ├── xlsx (SheetJS)                (엑셀 파싱)
  └── chart.js                     (차트 렌더링)
```

### 2.2 Data Flow

```
[1] 로그인 (기존 gabwoo Auth 재활용)
     │
     ▼
[2] 대시보드 로드 (DB에서 기존 데이터 조회)
     │
     ├─── Supabase RPC: get_dashboard_summary(기준월)
     │     → 3패널 KPI 데이터
     │
     ├─── Supabase Query: 거래처별 TOP 10
     │     → Charts.renderCustomerChart()
     │
     └─── Supabase Query: 영업자별 매출
           → Charts.renderSalesChart()

[3] 엑셀 업로드 (Jack만 사용)
     │
     ├─ 파일 선택 / 드래그&드롭
     ▼
     SheetJS 파싱 (브라우저)
     │
     ├─ 컬럼 매핑 검증 (견적번호 컬럼 존재 확인)
     ├─ 헤더/라인 분리 (견적번호 기준 GROUP)
     ├─ 데이터 정합성 검증 (NULL, 이상치)
     ▼
     500행 청크 배치 upsert → Supabase DB
     │
     ├─ 프로그레스바 업데이트
     ├─ 실패 건 기록
     ▼
     완료 → 대시보드 자동 갱신
```

### 2.3 Dependencies

| Component | Depends On | Purpose |
|-----------|-----------|---------|
| Auth | Supabase Auth + approved_users | 로그인·권한 확인 |
| Upload | SheetJS CDN | 브라우저 엑셀 파싱 |
| Dashboard | Supabase DB (erp_quotes) | 월별 매출 집계 |
| Charts | Chart.js CDN + Supabase DB | 피벗 차트 렌더링 |
| DB | Supabase JS Client CDN | DB CRUD + RPC |

---

## 3. Data Model

### 3.1 Entity Definition (실데이터 검증 반영)

63,523행 실데이터 분석 결과를 반영한 스키마입니다.

```
[erp_quotes] 1건 = 견적 1건 (9,032건)
     │
     └── 1:N ── [erp_quote_lines] 견적 라인 (63,522건, 평균 7라인/건)
```

### 3.2 Database Schema

```sql
-- ============================================
-- 견적 헤더 (견적번호 1개 = 1행)
-- 실데이터 검증: 모든 라인에 동일 견적금액 반복 확인됨
-- ============================================
CREATE TABLE erp_quotes (
  id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  quote_number    text NOT NULL UNIQUE,   -- 견적번호 (EST20260121-0004 형식)
  quote_date      date,                    -- 견적일 (원본은 문자열, 파싱 시 변환)
  customer_name   text,                    -- 거래처 (419개 고유값)
  sales_person    text,                    -- 영업담당 (20명, 39.4% NULL — 주로 비피앤피)
  department      text,                    -- 부서명
  company         text NOT NULL DEFAULT '갑우문화사', -- 귀속회사 (갑우/비피앤피/더원)
  product_type    text,                    -- 제품종류
  binding_name    text,                    -- 제본명
  copies          integer,                 -- 부수
  quote_amount    numeric DEFAULT 0,       -- 견적금액 (합계, 0원 675건 존재)
  item_count      integer,                 -- 건수
  quote_title     text,                    -- 견적명
  product_name    text,                    -- 품명
  order_number    text,                    -- 수주번호 (2.5% NULL)
  approval_status text DEFAULT '작성',     -- 승인여부: 승인(89.8%)/작성(5.7%)/확정(4.5%)
  source_file     text,                    -- 원본 파일명 (감사 추적용)
  uploaded_at     timestamptz DEFAULT now(),
  created_at      timestamptz DEFAULT now()
);

-- 인덱스: 대시보드 쿼리 성능
CREATE INDEX idx_quotes_date ON erp_quotes(quote_date);
CREATE INDEX idx_quotes_company ON erp_quotes(company);
CREATE INDEX idx_quotes_customer ON erp_quotes(customer_name);
CREATE INDEX idx_quotes_sales ON erp_quotes(sales_person);
CREATE INDEX idx_quotes_approval ON erp_quotes(approval_status);

-- ============================================
-- 견적 라인 (견적 1건당 평균 7라인)
-- ============================================
CREATE TABLE erp_quote_lines (
  id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  quote_id        uuid NOT NULL REFERENCES erp_quotes(id) ON DELETE CASCADE,
  line_seq        integer NOT NULL,        -- 라인 순번 (엑셀 행 순서 기반 생성)
  category        text,                    -- 구분 (제판&소부, 인쇄, 후가공, 제본, 용지대 등)
  usage_type      text,                    -- 용도 ("usage"는 SQL 예약어 가능성)
  item            text,                    -- 항목 (845종 항목 코드)
  spec            text,                    -- 규격
  unit            text,                    -- 단위
  base_qty        text,                    -- 기본 (숫자+문자 혼재 가능)
  sheets          numeric,                 -- 대수
  quantity_r      numeric,                 -- 수량(R)
  colors          text,                    -- 색도
  unit_price      numeric,                 -- 단가
  amount          numeric,                 -- 금액
  discount_rate   numeric,                 -- 할인율 (%, 1~65 범위, 640건만 값 있음)
  final_amount    numeric,                 -- 최종금액
  note            text,                    -- 비고
  UNIQUE(quote_id, line_seq)
);

CREATE INDEX idx_lines_quote ON erp_quote_lines(quote_id);
CREATE INDEX idx_lines_category ON erp_quote_lines(category);

-- ============================================
-- RLS 정책 (보안 아키텍트 권고 — Critical)
-- 기존 gabwoo의 approved_users 테이블 재활용
-- ============================================
ALTER TABLE erp_quotes ENABLE ROW LEVEL SECURITY;
ALTER TABLE erp_quote_lines ENABLE ROW LEVEL SECURITY;

-- 인증된 사용자만 읽기
CREATE POLICY "authenticated_read_quotes" ON erp_quotes
  FOR SELECT USING (
    auth.uid() IS NOT NULL
    AND EXISTS (SELECT 1 FROM approved_users WHERE email = auth.jwt()->>'email')
  );

-- 인증된 사용자만 입력 (업로드)
CREATE POLICY "authenticated_insert_quotes" ON erp_quotes
  FOR INSERT WITH CHECK (
    auth.uid() IS NOT NULL
    AND EXISTS (SELECT 1 FROM approved_users WHERE email = auth.jwt()->>'email')
  );

-- 라인 테이블도 동일 정책
CREATE POLICY "authenticated_read_lines" ON erp_quote_lines
  FOR SELECT USING (
    auth.uid() IS NOT NULL
    AND EXISTS (SELECT 1 FROM approved_users WHERE email = auth.jwt()->>'email')
  );

CREATE POLICY "authenticated_insert_lines" ON erp_quote_lines
  FOR INSERT WITH CHECK (
    auth.uid() IS NOT NULL
    AND EXISTS (SELECT 1 FROM approved_users WHERE email = auth.jwt()->>'email')
  );

-- ============================================
-- 대시보드용 RPC 함수 (DB에서 집계하여 성능 확보)
-- ============================================
CREATE OR REPLACE FUNCTION get_dashboard_summary(
  p_company text DEFAULT NULL,
  p_base_date date DEFAULT CURRENT_DATE
)
RETURNS TABLE (
  period text,
  label text,
  total_amount numeric,
  quote_count bigint
) AS $$
BEGIN
  RETURN QUERY
  -- 지난달 (확정)
  SELECT
    '지난달'::text AS period,
    '확정'::text AS label,
    COALESCE(SUM(quote_amount), 0) AS total_amount,
    COUNT(DISTINCT quote_number) AS quote_count
  FROM erp_quotes
  WHERE approval_status = '승인'
    AND quote_date >= (date_trunc('month', p_base_date) - interval '1 month')::date
    AND quote_date < date_trunc('month', p_base_date)::date
    AND (p_company IS NULL OR company = p_company)
    AND quote_amount > 0

  UNION ALL

  -- 이번달 (예상)
  SELECT
    '이번달'::text,
    '예상'::text,
    COALESCE(SUM(quote_amount), 0),
    COUNT(DISTINCT quote_number)
  FROM erp_quotes
  WHERE quote_date >= date_trunc('month', p_base_date)::date
    AND quote_date < (date_trunc('month', p_base_date) + interval '1 month')::date
    AND (p_company IS NULL OR company = p_company)
    AND quote_amount > 0

  UNION ALL

  -- 다음달 (예상)
  SELECT
    '다음달'::text,
    '예상'::text,
    COALESCE(SUM(quote_amount), 0),
    COUNT(DISTINCT quote_number)
  FROM erp_quotes
  WHERE quote_date >= (date_trunc('month', p_base_date) + interval '1 month')::date
    AND quote_date < (date_trunc('month', p_base_date) + interval '2 month')::date
    AND (p_company IS NULL OR company = p_company)
    AND quote_amount > 0;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
```

### 3.3 데이터 검증 규칙 (파싱 시 적용)

| # | 규칙 | 처리 |
|---|---|---|
| V1 | 견적번호가 NULL/빈값인 행 | **스킵** (합계행 또는 빈 행) |
| V2 | 견적일이 문자열 | `YYYY-MM-DD` 또는 `YYYY/MM/DD` 파싱 → DATE 변환 |
| V3 | 견적금액이 0 이하 | 저장하되 대시보드 집계에서 제외 (`quote_amount > 0`) |
| V4 | 견적금액 음수 (24건) | 저장하되 별도 라벨 표시 (반품/환불) |
| V5 | 동일 견적번호 재업로드 | upsert (ON CONFLICT quote_number DO UPDATE) |
| V6 | 거래처명 정규화 | trim + 전각→반각. v0.1은 원본 보존, 정규화는 v1 |
| V7 | 할인율 | 퍼센트 단위 (1~65) 확인됨. 그대로 저장 |

---

## 4. API Specification

### 4.1 Supabase 직접 쿼리 (서버리스 — API 서버 없음)

정적 HTML에서 Supabase JS Client로 직접 DB 접근합니다. 별도 API 서버가 없습니다.

| 용도 | 방식 | 설명 |
|---|---|---|
| 대시보드 요약 | `supabase.rpc('get_dashboard_summary', {p_company, p_base_date})` | 3패널 데이터 |
| 거래처 TOP 10 | `supabase.from('erp_quotes').select(...)` + JS 집계 | 거래처별 매출 합계 |
| 영업자별 매출 | `supabase.from('erp_quotes').select(...)` + JS 집계 | 영업자별 매출 합계 |
| 엑셀 업로드 | `supabase.from('erp_quotes').upsert(batch)` | 500행 청크 배치 |
| 라인 저장 | `supabase.from('erp_quote_lines').insert(batch)` | 헤더 저장 후 라인 저장 |

### 4.2 주요 쿼리 상세

#### 거래처별 매출 TOP 10

```javascript
// JS에서 집계 (Supabase에서 GROUP BY + SUM이 직접 안 되므로)
const { data } = await supabase
  .from('erp_quotes')
  .select('customer_name, quote_amount')
  .eq('company', selectedCompany)
  .gte('quote_date', startDate)
  .lte('quote_date', endDate)
  .gt('quote_amount', 0);

// JS로 집계
const grouped = data.reduce((acc, row) => {
  acc[row.customer_name] = (acc[row.customer_name] || 0) + row.quote_amount;
  return acc;
}, {});
const top10 = Object.entries(grouped)
  .sort((a, b) => b[1] - a[1])
  .slice(0, 10);
```

---

## 5. UI/UX Design

### 5.1 Screen Layout

```
┌─────────────────────────────────────────────────────────────┐
│  갑우문화사 견적 대시보드                    [귀속회사 ▼] [로그아웃] │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ▶ 데이터 업로드 (접이식 — 기본 접힌 상태)                      │
│  ┌─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐ │
│  │  [파일을 드래그하세요 또는 클릭하여 선택]                    │ │
│  │  ██████████░░░░░░  23,500 / 63,522건 (37%)              │ │
│  └─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘ │
│                                                             │
│  ┌───────────────┐ ┌───────────────┐ ┌───────────────┐     │
│  │   3월 매출     │ │   4월 매출     │ │   5월 매출     │     │
│  │               │ │               │ │               │     │
│  │  ₩12.3억      │ │  ₩8.7억       │ │  ₩5.2억       │     │
│  │  156건        │ │  112건        │ │  67건         │     │
│  │  [확정]       │ │  [예상]       │ │  [예상]       │     │
│  └───────────────┘ └───────────────┘ └───────────────┘     │
│                                                             │
│  ┌──────────────────────┐ ┌──────────────────────┐         │
│  │ 거래처별 매출 TOP 10  │ │ 영업자별 매출 비교     │         │
│  │                      │ │                      │         │
│  │  ████████ 교원구몬    │ │  ████████ 유영선      │         │
│  │  ██████ 이투스에듀    │ │  ██████ 조기석        │         │
│  │  █████ 지담디앤피    │ │  █████ 방진혁         │         │
│  │  ████ 알래스카애드   │ │  ████ 김영훈          │         │
│  │  ...                 │ │  ...                 │         │
│  └──────────────────────┘ └──────────────────────┘         │
│                                                             │
│  마지막 업데이트: 2026-04-10 14:30 | 데이터: 9,032건 견적     │
└─────────────────────────────────────────────────────────────┘
```

### 5.2 User Flow

```
[로그인] → [대시보드 자동 로드] → [귀속회사 필터 변경] → [차트 갱신]
                                       │
                                       └→ [업로드 패널 열기] → [엑셀 드래그&드롭]
                                            → [파싱+저장 프로그레스바]
                                            → [완료 → 대시보드 자동 갱신]
```

### 5.3 Component List (JS 모듈 기준)

| Module | 역할 | 주요 함수 |
|--------|------|---------|
| `CONFIG` | 설정값 (Supabase URL/Key, 컬럼 매핑) | - |
| `Auth` | 로그인/로그아웃, 세션 관리 | `login()`, `logout()`, `checkSession()` |
| `DB` | Supabase 클라이언트 래퍼 | `query()`, `upsert()`, `rpc()` |
| `Upload` | 엑셀 파싱, 검증, 청크 업로드 | `parseExcel()`, `validate()`, `uploadChunks()` |
| `Dashboard` | 3패널 렌더링, 귀속회사 필터 | `loadSummary()`, `renderPanels()`, `filterByCompany()` |
| `Charts` | Chart.js 차트 생성/갱신 | `renderCustomerChart()`, `renderSalesChart()` |
| `App` | 초기화, 이벤트 바인딩 | `init()` |

### 5.4 Page UI Checklist

#### 로그인 화면

- [ ] Input: 이메일 입력 필드
- [ ] Input: 비밀번호 입력 필드
- [ ] Button: 로그인 버튼
- [ ] Text: 에러 메시지 표시 영역
- [ ] Text: "갑우문화사 견적 대시보드" 타이틀

#### 대시보드 메인

- [ ] Dropdown: 귀속회사 필터 (전체 / 갑우문화사 / 비피앤피 / 더원프린팅)
- [ ] Button: 로그아웃
- [ ] Details/Summary: 업로드 패널 (기본 접힌 상태)
- [ ] Card: 지난달 매출 — 금액(₩), 건수, [확정] 뱃지
- [ ] Card: 이번달 매출 — 금액(₩), 건수, [예상] 뱃지
- [ ] Card: 다음달 매출 — 금액(₩), 건수, [예상] 뱃지
- [ ] Chart: 거래처별 매출 TOP 10 (수평 막대)
- [ ] Chart: 영업자별 매출 비교 (수평 막대)
- [ ] Text: 마지막 업데이트 시간
- [ ] Text: 총 데이터 건수

#### 업로드 패널 (펼친 상태)

- [ ] DropZone: 파일 드래그&드롭 영역
- [ ] Input: 파일 선택 버튼 (input type=file)
- [ ] Progress: 프로그레스바 (파싱 중... / 저장 중... N/M건)
- [ ] Text: 완료/실패 메시지
- [ ] Button: 실패 건 CSV 다운로드 (실패 시에만 표시)
- [ ] Text: 검증 결과 요약 (총 행수, 스킵 건수, 저장 건수)

---

## 6. Error Handling

### 6.1 에러 시나리오 및 사용자 메시지

| 시나리오 | 사용자 메시지 (비기술적 언어) | 기술적 처리 |
|---------|--------------------------|-----------|
| 잘못된 엑셀 형식 | "엑셀 파일에서 '견적번호' 컬럼을 찾을 수 없습니다. ERP에서 내려받은 견적 상세현황 파일이 맞는지 확인해주세요." | SheetJS 파싱 후 컬럼 매핑 검증 실패 |
| 파싱 중 에러 | "N행에서 데이터 오류가 발생했습니다. 해당 행을 건너뛰고 나머지를 처리합니다." | try-catch로 행 단위 에러 격리 |
| DB 저장 실패 (청크) | "23,500건 저장 완료 / 1,200건 실패. 실패 건 목록을 다운로드할 수 있습니다." | 실패 청크 번호 기록, CSV 내보내기 |
| DB 연결 실패 | "데이터를 불러오지 못했습니다. 잠시 후 다시 시도해주세요." | 재시도 버튼 (fetch만 재호출) |
| 로그인 실패 | "이메일 또는 비밀번호가 올바르지 않습니다." | Supabase Auth 에러 매핑 |
| 파일 크기 초과 | "파일 크기가 50MB를 초과합니다. 기간을 나누어 여러 번 업로드해주세요." | `file.size` 체크 |
| 중복 업로드 | "이미 등록된 견적 N건이 업데이트되었습니다." (에러가 아닌 정보) | upsert 결과 메시지 |

### 6.2 에러 표시 방식

- **토스트 메시지**: 화면 상단 고정, 5초 후 자동 닫힘, 닫기 버튼 포함
- **색상**: 성공=초록, 경고=주황, 에러=빨강
- **기술 용어 금지**: "대표이사님이 받아도 이상하지 않은" 수준의 언어

---

## 7. Security Considerations (보안 아키텍트 분석 반영)

- [x] **RLS 필수**: `erp_quotes`, `erp_quote_lines` 모두 RLS 활성화. anon key 노출 상태에서 유일한 DB 보호 수단
- [x] **기존 Auth 재활용**: gabwoo 서비스의 `approved_users` 테이블 + Supabase Auth 그대로 사용
- [x] **XSS 방지**: DOM 삽입 시 반드시 `esc()` 함수 또는 `textContent` 사용. `innerHTML`에 사용자 데이터 직접 삽입 금지
- [x] **파일 크기 제한**: 50MB 이하만 허용
- [x] **감사 추적**: `source_file` + `uploaded_at` 필드로 업로드 이력 보존
- [x] **upsert 중복 방지**: `quote_number` UNIQUE 제약으로 동일 데이터 2배 적재 방지
- [ ] *(v1)* RBAC: 영업자는 본인 견적만, 경영진은 전체 조회
- [ ] *(v1)* CSP 헤더 추가
- [ ] *(v1)* CORS origin 제한

---

## 8. Test Plan

### 8.1 Test Scope

| Type | Target | 방법 | Phase |
|------|--------|------|-------|
| T1: 데이터 정합성 | 업로드 결과가 원본 엑셀과 일치하는지 | 수동 비교 (대조표) | Do |
| T2: 대시보드 정확성 | 3패널 숫자가 DB 쿼리 결과와 일치하는지 | Supabase SQL 직접 실행 → 화면 비교 | Do |
| T3: UI 동작 | 로그인, 업로드, 필터, 차트 | 수동 테스트 체크리스트 | Do |
| T4: 엣지케이스 | 0원, 음수, 빈 파일, 큰 파일 | 수동 테스트 | Do |

### 8.2 데이터 정합성 테스트 (가장 중요)

| # | 테스트 | 검증 방법 | 기대 결과 |
|---|--------|---------|---------|
| 1 | 업로드 행 수 | DB `SELECT COUNT(*)` vs 엑셀 행 수 | 일치 (NULL 행 제외) |
| 2 | 견적금액 합계 | DB `SUM(quote_amount)` vs 엑셀 피벗 합계 | 일치 |
| 3 | 귀속회사별 비율 | DB 갑우 39.4% / 비피앤피 60.5% | 원본 비율과 일치 |
| 4 | 승인여부 분포 | DB 승인 89.8% / 작성 5.7% / 확정 4.5% | 원본 분포와 일치 |
| 5 | 견적번호 중복 | DB `SELECT quote_number, COUNT(*) HAVING COUNT > 1` | 0건 (헤더 테이블에서) |
| 6 | 샘플 20건 견적금액 vs 최종금액합 | 이전 검증과 동일 | 16건 일치, 4건 소액 차이 |

### 8.3 UI 테스트 체크리스트

| # | 화면 | 동작 | 기대 결과 |
|---|------|------|---------|
| 1 | 로그인 | 올바른 계정 입력 | 대시보드 표시 |
| 2 | 로그인 | 잘못된 비밀번호 | 에러 메시지 |
| 3 | 대시보드 | 페이지 로드 | 3패널에 숫자 표시 (스켈레톤 아님) |
| 4 | 대시보드 | 귀속회사 필터 변경 | 3패널 + 차트 갱신 |
| 5 | 업로드 | 올바른 엑셀 드래그 | 프로그레스바 → 완료 → 대시보드 갱신 |
| 6 | 업로드 | 잘못된 파일 (PDF) | 에러 메시지 |
| 7 | 업로드 | 동일 파일 재업로드 | "N건 업데이트" 메시지 (중복 아님) |
| 8 | 차트 | 거래처 TOP 10 | 막대 차트에 10개 거래처 |
| 9 | 차트 | 영업자별 | 막대 차트에 영업자 목록 |
| 10 | 인쇄 | Ctrl+P | 업로드 패널 숨김, 차트 이미지, 깔끔한 레이아웃 |

### 8.4 Seed Data

| Entity | 데이터 | 출처 |
|--------|--------|------|
| erp_quotes | 9,032건 | 견적서 상세현황 엑셀 63,522행에서 추출 |
| erp_quote_lines | 63,522건 | 위 엑셀의 전체 행 |
| approved_users | 기존 3명 | 대표이사, Jack, 이수현 본부장 |

---

## 9. Performance

### 9.1 엑셀 파싱 성능

| 단계 | 예상 시간 | 전략 |
|---|---|---|
| SheetJS 파싱 (63K행) | 1~3초 | 브라우저 메인 스레드, Web Worker 보류 |
| 헤더/라인 분리 | < 1초 | JS reduce로 견적번호 기준 그룹핑 |
| DB upsert (9K 헤더) | 10~20초 | 500건 청크 × 18회 배치 |
| DB insert (63K 라인) | 30~60초 | 1000건 청크 × 64회 배치 |
| **전체** | **약 1분** | 프로그레스바로 진행률 표시 |

### 9.2 대시보드 로드 성능

| 쿼리 | 예상 시간 | 전략 |
|---|---|---|
| RPC get_dashboard_summary | < 200ms | DB 함수에서 집계, 인덱스 활용 |
| 거래처 TOP 10 | < 500ms | 9K 헤더 조회 → JS 집계 |
| 영업자별 매출 | < 300ms | 9K 헤더 조회 → JS 집계 |
| **전체 로드** | **< 1초** | 병렬 fetch |

---

## 10. Print Design (프론트엔드 아키텍트 권고)

### 10.1 인쇄 CSS 요구사항

```
@media print:
  - 업로드 패널, 로그아웃 버튼, 필터 → display: none
  - 3패널 KPI → grid-template-columns: repeat(3, 1fr) 강제
  - Chart.js canvas → toBase64Image()로 이미지 변환 (canvas는 인쇄 시 흐릿함)
  - 배경색 강제 출력: -webkit-print-color-adjust: exact
  - 폰트 크기: 10pt 기준
  - 페이지 하단: "갑우문화사 견적 대시보드 | 출력일: {날짜}" 자동 삽입
```

---

## 11. Implementation Guide

### 11.1 File Structure

```
견적계산기/
├── web/
│   └── quote-dashboard.html     ← 대시보드 (단일 파일, Vercel 배포)
├── docs/
│   ├── 01-plan/features/dashboard-v0.1.plan.md
│   └── 02-design/features/dashboard-v0.1.design.md  ← 본 문서
├── scripts/
│   └── verify_upload.sql        ← 업로드 후 데이터 정합성 검증 SQL
└── CLAUDE.md
```

### 11.2 Implementation Order

1. [ ] Supabase 테이블 생성 + RLS 정책 적용
2. [ ] HTML 뼈대 + CSS Design Tokens + 로그인 UI
3. [ ] Auth 모듈 (기존 gabwoo 패턴 복사)
4. [ ] 엑셀 업로드 + SheetJS 파싱 + 헤더/라인 분리 + 청크 upsert
5. [ ] 데이터 정합성 검증 (T1~T6)
6. [ ] 대시보드 3패널 + RPC 호출
7. [ ] Chart.js 피벗 차트 (거래처 TOP 10, 영업자별)
8. [ ] 귀속회사 필터 + 차트 갱신
9. [ ] 에러 처리 + 토스트 메시지
10. [ ] 인쇄 CSS + canvas→img 변환
11. [ ] Vercel 배포 + 실데이터 업로드 테스트

### 11.3 Session Guide

#### Module Map

| Module | Scope Key | Description | Estimated Turns |
|--------|-----------|-------------|:---------------:|
| DB 세팅 + 인증 | `module-1` | Supabase 테이블/RLS 생성, 로그인 UI/Auth 모듈 | 20~25 |
| 엑셀 업로드 | `module-2` | SheetJS 파싱, 헤더/라인 분리, 청크 upsert, 검증 | 30~40 |
| 대시보드 + 차트 | `module-3` | 3패널 KPI, Chart.js 피벗, 필터, 인쇄 CSS | 25~35 |
| 테스트 + 배포 | `module-4` | 데이터 정합성 검증, 에러 처리, Vercel 배포 | 15~20 |

#### Recommended Session Plan

| Session | Scope | 산출물 |
|---------|-------|--------|
| Session 1 | `--scope module-1` | Supabase 테이블 생성, 로그인 동작 확인 |
| Session 2 | `--scope module-2` | 63,522건 엑셀 업로드 성공, 정합성 검증 통과 |
| Session 3 | `--scope module-3` | 대시보드 3패널 + 차트 + 필터 + 인쇄 |
| Session 4 | `--scope module-4` | 실데이터 테스트, 에러 처리, Vercel 배포 |

---

## Version History

| Version | Date | Changes | Author |
|---------|------|---------|--------|
| 0.1 | 2026-04-10 | Initial draft — CTO팀 분석(프론트엔드/보안/데이터) 반영 | Jack + Claude CTO팀 |
