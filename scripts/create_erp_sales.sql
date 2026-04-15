-- 실제 매출 테이블 (SAL_SALESH 기반)
-- 기존 erp_quotes는 견적 단계, 이 테이블은 세금계산서 발행 완료된 확정 매출
-- 경영 대시보드가 매출(확정) 기준으로 판단하도록 이 테이블을 사용합니다.

CREATE TABLE IF NOT EXISTS erp_sales (
  id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  sales_number    text NOT NULL UNIQUE,       -- NO_SALES
  sales_date      date,                        -- DT_SALES
  customer_code   text,                        -- CD_CUST
  customer_name   text,                        -- MAS_CUST.NM_CUST
  sales_person_code text,                      -- CD_EMP
  sales_person    text,                        -- MAS_EMP.NM_EMP
  department      text,                        -- CD_DEPT
  company         text NOT NULL DEFAULT '갑우문화사',
  supply_amount   numeric DEFAULT 0,           -- AM (공급가, VAT 제외)
  vat_amount      numeric DEFAULT 0,           -- AM_VAT
  total_amount    numeric DEFAULT 0,           -- AM_K (VAT 포함)
  sales_status    text,                        -- ST_SALES: Y/N/null
  approval_status text,                        -- YN_APP
  firm_code       text,                        -- CD_FIRM (7000 갑우, 8000 비피앤피)
  source_file     text DEFAULT 'ERP DB sync',
  uploaded_at     timestamptz DEFAULT now(),
  created_at      timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_sales_date ON erp_sales(sales_date);
CREATE INDEX IF NOT EXISTS idx_sales_customer ON erp_sales(customer_name);
CREATE INDEX IF NOT EXISTS idx_sales_person ON erp_sales(sales_person);
CREATE INDEX IF NOT EXISTS idx_sales_company ON erp_sales(company);
CREATE INDEX IF NOT EXISTS idx_sales_status ON erp_sales(sales_status);

ALTER TABLE erp_sales ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "approved_users_read_sales" ON erp_sales;
CREATE POLICY "approved_users_read_sales" ON erp_sales
  FOR SELECT USING (
    auth.uid() IS NOT NULL
    AND EXISTS (SELECT 1 FROM approved_users WHERE email = auth.jwt()->>'email')
  );

-- 인사이트 저장용 테이블 (AI가 분석한 YoY 해석 3줄)
CREATE TABLE IF NOT EXISTS dashboard_insights (
  id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  generated_at    timestamptz NOT NULL DEFAULT now(),
  base_date       date NOT NULL,
  company_filter  text,                        -- null = 전체
  insights        jsonb NOT NULL,              -- [{title, body, direction}] × 3
  meta            jsonb                        -- 비교 기간, 기준 숫자 등
);

CREATE INDEX IF NOT EXISTS idx_insights_generated ON dashboard_insights(generated_at DESC);

ALTER TABLE dashboard_insights ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "approved_users_read_insights" ON dashboard_insights;
CREATE POLICY "approved_users_read_insights" ON dashboard_insights
  FOR SELECT USING (
    auth.uid() IS NOT NULL
    AND EXISTS (SELECT 1 FROM approved_users WHERE email = auth.jwt()->>'email')
  );
