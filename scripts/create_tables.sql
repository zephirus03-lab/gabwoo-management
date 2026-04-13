-- ============================================
-- 견적 대시보드 v0.1 — Supabase 테이블 생성 스크립트
-- 실행 방법: Supabase 대시보드 > SQL Editor에 붙여넣기 > Run
-- Design Ref: §3 Data Model
-- ============================================

-- 1. 견적 헤더 테이블
-- 견적번호 1개 = 견적 1건. ERP 엑셀에서 견적번호로 GROUP하여 헤더를 추출합니다.
-- 실데이터: 9,032건 견적, 견적금액은 모든 라인에 동일값 반복 확인됨
CREATE TABLE IF NOT EXISTS erp_quotes (
  id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  quote_number    text NOT NULL UNIQUE,
  quote_date      date,
  customer_name   text,
  sales_person    text,            -- 39.4% NULL (주로 비피앤피 소속)
  department      text,
  company         text NOT NULL DEFAULT '갑우문화사',  -- 갑우 39.4% / 비피앤피 60.5% / 더원 0.1%
  product_type    text,
  binding_name    text,
  copies          integer,
  quote_amount    numeric DEFAULT 0,  -- 0원 675건, 음수 24건 존재
  item_count      integer,
  quote_title     text,
  product_name    text,
  order_number    text,
  approval_status text DEFAULT '작성',  -- 승인(89.8%) / 작성(5.7%) / 확정(4.5%)
  source_file     text,
  uploaded_at     timestamptz DEFAULT now(),
  created_at      timestamptz DEFAULT now()
);

-- 2. 견적 라인 테이블
-- 견적 1건당 평균 7라인 (제판&소부, 인쇄, 후가공, 제본, 용지대 등)
CREATE TABLE IF NOT EXISTS erp_quote_lines (
  id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  quote_id        uuid NOT NULL REFERENCES erp_quotes(id) ON DELETE CASCADE,
  line_seq        integer NOT NULL,
  category        text,            -- 구분
  usage_type      text,            -- 용도
  item            text,            -- 항목 (845종)
  spec            text,
  unit            text,
  base_qty        text,            -- 숫자+문자 혼재 가능하여 text
  sheets          numeric,
  quantity_r      numeric,
  colors          text,
  unit_price      numeric,
  amount          numeric,
  discount_rate   numeric,         -- 퍼센트 단위 (1~65), 640건만 값 있음
  final_amount    numeric,
  note            text,
  UNIQUE(quote_id, line_seq)
);

-- 3. 인덱스 (대시보드 쿼리 성능)
CREATE INDEX IF NOT EXISTS idx_quotes_date ON erp_quotes(quote_date);
CREATE INDEX IF NOT EXISTS idx_quotes_company ON erp_quotes(company);
CREATE INDEX IF NOT EXISTS idx_quotes_customer ON erp_quotes(customer_name);
CREATE INDEX IF NOT EXISTS idx_quotes_sales ON erp_quotes(sales_person);
CREATE INDEX IF NOT EXISTS idx_quotes_approval ON erp_quotes(approval_status);
CREATE INDEX IF NOT EXISTS idx_lines_quote ON erp_quote_lines(quote_id);

-- ============================================
-- 4. RLS (Row Level Security) — Critical
-- anon key가 HTML에 노출되므로 RLS가 유일한 DB 보호 수단입니다.
-- 기존 gabwoo 서비스의 approved_users 테이블을 재활용합니다.
-- ============================================

ALTER TABLE erp_quotes ENABLE ROW LEVEL SECURITY;
ALTER TABLE erp_quote_lines ENABLE ROW LEVEL SECURITY;

-- 인증된 승인 사용자만 읽기
CREATE POLICY "approved_users_read_quotes" ON erp_quotes
  FOR SELECT USING (
    auth.uid() IS NOT NULL
    AND EXISTS (SELECT 1 FROM approved_users WHERE email = auth.jwt()->>'email')
  );

-- 인증된 승인 사용자만 입력 (업로드)
CREATE POLICY "approved_users_insert_quotes" ON erp_quotes
  FOR INSERT WITH CHECK (
    auth.uid() IS NOT NULL
    AND EXISTS (SELECT 1 FROM approved_users WHERE email = auth.jwt()->>'email')
  );

-- 인증된 승인 사용자만 수정 (upsert 시 필요)
CREATE POLICY "approved_users_update_quotes" ON erp_quotes
  FOR UPDATE USING (
    auth.uid() IS NOT NULL
    AND EXISTS (SELECT 1 FROM approved_users WHERE email = auth.jwt()->>'email')
  );

-- 라인 테이블 RLS
CREATE POLICY "approved_users_read_lines" ON erp_quote_lines
  FOR SELECT USING (
    auth.uid() IS NOT NULL
    AND EXISTS (SELECT 1 FROM approved_users WHERE email = auth.jwt()->>'email')
  );

CREATE POLICY "approved_users_insert_lines" ON erp_quote_lines
  FOR INSERT WITH CHECK (
    auth.uid() IS NOT NULL
    AND EXISTS (SELECT 1 FROM approved_users WHERE email = auth.jwt()->>'email')
  );

CREATE POLICY "approved_users_delete_lines" ON erp_quote_lines
  FOR DELETE USING (
    auth.uid() IS NOT NULL
    AND EXISTS (SELECT 1 FROM approved_users WHERE email = auth.jwt()->>'email')
  );

-- ============================================
-- 5. 대시보드 RPC 함수
-- 3패널 데이터를 한 번의 호출로 가져옵니다.
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

  -- 지난달 (확정: 승인 상태만)
  SELECT
    '지난달'::text AS period,
    '확정'::text AS label,
    COALESCE(SUM(q.quote_amount), 0) AS total_amount,
    COUNT(DISTINCT q.quote_number) AS quote_count
  FROM erp_quotes q
  WHERE q.approval_status = '승인'
    AND q.quote_date >= (date_trunc('month', p_base_date) - interval '1 month')::date
    AND q.quote_date < date_trunc('month', p_base_date)::date
    AND (p_company IS NULL OR q.company = p_company)
    AND q.quote_amount > 0

  UNION ALL

  -- 이번달 (승인 기준)
  SELECT
    '이번달'::text,
    '승인'::text,
    COALESCE(SUM(q.quote_amount), 0),
    COUNT(DISTINCT q.quote_number)
  FROM erp_quotes q
  WHERE q.approval_status = '승인'
    AND q.quote_date >= date_trunc('month', p_base_date)::date
    AND q.quote_date < (date_trunc('month', p_base_date) + interval '1 month')::date
    AND (p_company IS NULL OR q.company = p_company)
    AND q.quote_amount > 0

  UNION ALL

  -- 다음달 (승인 기준)
  SELECT
    '다음달'::text,
    '승인'::text,
    COALESCE(SUM(q.quote_amount), 0),
    COUNT(DISTINCT q.quote_number)
  FROM erp_quotes q
  WHERE q.approval_status = '승인'
    AND q.quote_date >= (date_trunc('month', p_base_date) + interval '1 month')::date
    AND q.quote_date < (date_trunc('month', p_base_date) + interval '2 month')::date
    AND (p_company IS NULL OR q.company = p_company)
    AND q.quote_amount > 0;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- ============================================
-- 완료! 이 스크립트 실행 후 확인사항:
-- 1. Supabase > Table Editor에서 erp_quotes, erp_quote_lines 테이블 확인
-- 2. Supabase > Authentication > Policies에서 RLS 정책 6개 확인
-- 3. Supabase > Database > Functions에서 get_dashboard_summary 확인
-- ============================================
