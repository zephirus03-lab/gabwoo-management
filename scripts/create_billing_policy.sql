-- 거래처별 과금 정책 테이블과 확정 매출 뷰
-- 배경: 교원/교원구몬/에이치에스애드 같은 특수 거래처는 월말 일괄 세금계산서 방식이라
--       ERP의 SAL_SALESH.ST_SALES가 NULL인 레코드도 유효한 매출임.
--       일반 거래처는 ST_SALES='Y' 확정만 매출로 인정.
-- 해결: 정책 테이블로 거래처별 타입을 관리하고, 뷰에서 필터 로직을 캡슐화.

-- 1. 과금 정책 테이블
CREATE TABLE IF NOT EXISTS customer_billing_policy (
  id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  customer_code text NOT NULL,           -- erp_sales.customer_code와 매칭
  customer_name text,                    -- 참조용(표시)
  billing_type  text NOT NULL DEFAULT 'standard',
  -- 'standard'     : ST_SALES='Y'만 확정 매출로 인정 (기본)
  -- 'monthly_batch': 월말 일괄 세금계산서 거래처, ST_SALES가 NULL도 인정
  -- 'excluded'     : 아예 대시보드 집계에서 제외
  note          text,
  created_at    timestamptz DEFAULT now(),
  UNIQUE(customer_code)
);

CREATE INDEX IF NOT EXISTS idx_billing_policy_code ON customer_billing_policy(customer_code);

ALTER TABLE customer_billing_policy ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "approved_users_read_billing_policy" ON customer_billing_policy;
CREATE POLICY "approved_users_read_billing_policy" ON customer_billing_policy
  FOR SELECT USING (
    auth.uid() IS NOT NULL
    AND EXISTS (SELECT 1 FROM approved_users WHERE email = auth.jwt()->>'email')
  );

-- 2. 특수 거래처 등록 (월말 일괄 세금계산서 방식)
INSERT INTO customer_billing_policy (customer_code, customer_name, billing_type, note) VALUES
  ('V00661', '(주)교원구몬', 'monthly_batch', '월말 일괄 세금계산서 발행 거래처'),
  ('V1253',  '(주)교원구몬', 'monthly_batch', '월말 일괄 세금계산서 발행 거래처'),
  ('V00712', '(주)교원',     'monthly_batch', '월말 일괄 세금계산서 발행 거래처'),
  ('V1251',  '(주)교원',     'monthly_batch', '월말 일괄 세금계산서 발행 거래처'),
  ('V2163',  '(주)에이치에스애드', 'monthly_batch', '월말 일괄 세금계산서 발행 거래처'),
  ('V00919', '(주)지담디앤피', 'monthly_batch', '월말 일괄 세금계산서 발행 거래처 (혼합)')
ON CONFLICT (customer_code) DO UPDATE
  SET billing_type = EXCLUDED.billing_type,
      customer_name = EXCLUDED.customer_name,
      note = EXCLUDED.note;

-- 3. 확정 매출 뷰 — 모든 집계 쿼리의 단일 진실 소스
--    특수: NULL도 확정으로 인정. 일반: Y만 인정. 모두 N은 제외.
DROP VIEW IF EXISTS erp_sales_confirmed;

CREATE VIEW erp_sales_confirmed
WITH (security_invoker = true)  -- 뷰는 호출자의 RLS를 따름 (erp_sales의 RLS 적용)
AS
SELECT s.*
FROM erp_sales s
LEFT JOIN customer_billing_policy p
  ON s.customer_code = p.customer_code
WHERE s.supply_amount > 0
  AND (s.sales_status IS NULL OR s.sales_status != 'N')  -- 취소는 항상 제외
  AND p.billing_type IS DISTINCT FROM 'excluded'          -- 정책상 제외되지 않음
  AND (
    -- 특수 거래처: ST_SALES가 NULL이거나 Y이면 인정
    (p.billing_type = 'monthly_batch' AND (s.sales_status IS NULL OR s.sales_status = 'Y'))
    OR
    -- 일반 거래처(정책 미등록 포함): ST_SALES='Y'만 인정
    ((p.billing_type IS NULL OR p.billing_type = 'standard') AND s.sales_status = 'Y')
  );

-- 4. 확인 쿼리 (실행 직후 결과 확인용 — Supabase Studio에서 수동 실행)
-- SELECT billing_type, COUNT(*) FROM customer_billing_policy GROUP BY billing_type;
-- SELECT
--   DATE_TRUNC('month', sales_date) AS month,
--   COUNT(*) cnt, SUM(supply_amount) total
-- FROM erp_sales_confirmed
-- WHERE sales_date >= '2026-01-01'
-- GROUP BY 1 ORDER BY 1;
