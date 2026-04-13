-- ============================================
-- 테이블 초기화 + 재생성 (한 번에 복붙해서 Run)
-- 기존 테이블/정책/함수를 지우고 새로 만듭니다
-- ============================================

-- 기존 테이블 삭제 (데이터도 함께 삭제됨 — 엑셀 재업로드 필요)
DROP TABLE IF EXISTS erp_quote_lines CASCADE;
DROP TABLE IF EXISTS erp_quotes CASCADE;
DROP FUNCTION IF EXISTS get_dashboard_summary;

-- 1. 견적 헤더
CREATE TABLE erp_quotes (
  id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  quote_number    text NOT NULL UNIQUE,
  quote_date      date,
  customer_name   text,
  sales_person    text,
  department      text,
  company         text NOT NULL DEFAULT '갑우문화사',
  product_type    text,
  binding_name    text,
  copies          integer,
  quote_amount    numeric DEFAULT 0,
  item_count      integer,
  quote_title     text,
  product_name    text,
  order_number    text,
  approval_status text DEFAULT '작성',
  source_file     text,
  uploaded_at     timestamptz DEFAULT now(),
  created_at      timestamptz DEFAULT now()
);

-- 2. 견적 라인
CREATE TABLE erp_quote_lines (
  id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  quote_id        uuid NOT NULL REFERENCES erp_quotes(id) ON DELETE CASCADE,
  line_seq        integer NOT NULL,
  category        text,
  usage_type      text,
  item            text,
  spec            text,
  unit            text,
  base_qty        text,
  sheets          numeric,
  quantity_r      numeric,
  colors          text,
  unit_price      numeric,
  amount          numeric,
  discount_rate   numeric,
  final_amount    numeric,
  note            text,
  UNIQUE(quote_id, line_seq)
);

-- 3. 인덱스
CREATE INDEX idx_quotes_date ON erp_quotes(quote_date);
CREATE INDEX idx_quotes_company ON erp_quotes(company);
CREATE INDEX idx_quotes_customer ON erp_quotes(customer_name);
CREATE INDEX idx_quotes_sales ON erp_quotes(sales_person);
CREATE INDEX idx_quotes_approval ON erp_quotes(approval_status);
CREATE INDEX idx_lines_quote ON erp_quote_lines(quote_id);

-- 4. RLS
ALTER TABLE erp_quotes ENABLE ROW LEVEL SECURITY;
ALTER TABLE erp_quote_lines ENABLE ROW LEVEL SECURITY;

CREATE POLICY "read_quotes" ON erp_quotes FOR SELECT USING (
  auth.uid() IS NOT NULL AND EXISTS (SELECT 1 FROM approved_users WHERE email = auth.jwt()->>'email')
);
CREATE POLICY "insert_quotes" ON erp_quotes FOR INSERT WITH CHECK (
  auth.uid() IS NOT NULL AND EXISTS (SELECT 1 FROM approved_users WHERE email = auth.jwt()->>'email')
);
CREATE POLICY "update_quotes" ON erp_quotes FOR UPDATE USING (
  auth.uid() IS NOT NULL AND EXISTS (SELECT 1 FROM approved_users WHERE email = auth.jwt()->>'email')
);
CREATE POLICY "read_lines" ON erp_quote_lines FOR SELECT USING (
  auth.uid() IS NOT NULL AND EXISTS (SELECT 1 FROM approved_users WHERE email = auth.jwt()->>'email')
);
CREATE POLICY "insert_lines" ON erp_quote_lines FOR INSERT WITH CHECK (
  auth.uid() IS NOT NULL AND EXISTS (SELECT 1 FROM approved_users WHERE email = auth.jwt()->>'email')
);
CREATE POLICY "delete_lines" ON erp_quote_lines FOR DELETE USING (
  auth.uid() IS NOT NULL AND EXISTS (SELECT 1 FROM approved_users WHERE email = auth.jwt()->>'email')
);

-- 5. RPC 함수 (전부 승인 기준)
CREATE OR REPLACE FUNCTION get_dashboard_summary(
  p_company text DEFAULT NULL,
  p_base_date date DEFAULT CURRENT_DATE
)
RETURNS TABLE (period text, label text, total_amount numeric, quote_count bigint)
AS $$
BEGIN
  RETURN QUERY
  SELECT '지난달'::text, '승인'::text,
    COALESCE(SUM(q.quote_amount), 0), COUNT(DISTINCT q.quote_number)
  FROM erp_quotes q
  WHERE q.approval_status = '승인'
    AND q.quote_date >= (date_trunc('month', p_base_date) - interval '1 month')::date
    AND q.quote_date < date_trunc('month', p_base_date)::date
    AND (p_company IS NULL OR q.company = p_company)
    AND q.quote_amount > 0
  UNION ALL
  SELECT '이번달'::text, '승인'::text,
    COALESCE(SUM(q.quote_amount), 0), COUNT(DISTINCT q.quote_number)
  FROM erp_quotes q
  WHERE q.approval_status = '승인'
    AND q.quote_date >= date_trunc('month', p_base_date)::date
    AND q.quote_date < (date_trunc('month', p_base_date) + interval '1 month')::date
    AND (p_company IS NULL OR q.company = p_company)
    AND q.quote_amount > 0
  UNION ALL
  SELECT '다음달'::text, '승인'::text,
    COALESCE(SUM(q.quote_amount), 0), COUNT(DISTINCT q.quote_number)
  FROM erp_quotes q
  WHERE q.approval_status = '승인'
    AND q.quote_date >= (date_trunc('month', p_base_date) + interval '1 month')::date
    AND q.quote_date < (date_trunc('month', p_base_date) + interval '2 month')::date
    AND (p_company IS NULL OR q.company = p_company)
    AND q.quote_amount > 0;
END;
$$ LANGUAGE plpgsql SECURITY INVOKER;
