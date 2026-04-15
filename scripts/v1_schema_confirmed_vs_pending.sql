-- ============================================
-- 확정(세금계산서 발행) vs 진행(미발행) 두 라벨 분리를 위한 스키마 확장
-- 실행: Supabase SQL Editor 붙여넣기 Run
-- 2026-04-15 작성
-- ============================================
--
-- 배경:
-- - 기존 erp_sales_confirmed 뷰는 customer_billing_policy 정책에 따라
--   monthly_batch 거래처의 ST_SALES=NULL을 매출로 인정했으나,
-- - 실제 데이터 확인 결과 NULL 건들은 NO_INV(세금계산서 번호)가 전혀 발급되지 않은
--   "ERP 등록 but 세금계산서 미발행" 상태로 확인됨.
-- - CLAUDE.md 면책 라벨링 원칙에 따라 "확정"과 "진행"을 분리 표시해야 함.
--
-- 해결:
-- 1. erp_sales에 invoice_number, invoice_issue_date, etax_status 컬럼 추가
-- 2. status_label 컬럼을 계산하는 뷰 재정의 (확정 / 진행 / 취소)
-- 3. RPC get_dashboard_summary 확장: confirmed_amount, pending_amount 별도 반환

-- ============================================
-- 1. erp_sales 컬럼 추가
-- ============================================
ALTER TABLE erp_sales ADD COLUMN IF NOT EXISTS invoice_number text;
ALTER TABLE erp_sales ADD COLUMN IF NOT EXISTS invoice_issue_date date;
ALTER TABLE erp_sales ADD COLUMN IF NOT EXISTS etax_status text;

CREATE INDEX IF NOT EXISTS idx_erp_sales_invoice ON erp_sales(invoice_number);


-- ============================================
-- 2. erp_sales_confirmed 뷰 재정의
-- ============================================
-- status_label 계산 규칙:
--   'canceled' : ST_SALES = 'N' (항상 제외 대상)
--   'confirmed': NO_INV IS NOT NULL AND NO_INV <> '' (세금계산서 발행됨 — 진짜 확정)
--   'pending'  : NO_INV 없음 AND ST_SALES <> 'N' (등록만 됨, 발행 대기)
--
-- 기존 빌링 정책 로직은 제거 (너무 낙관적이었음). 대신 status_label로 세분화.
-- 뷰는 취소(canceled)를 제외하고 confirmed + pending만 반환.

DROP VIEW IF EXISTS erp_sales_confirmed CASCADE;

CREATE VIEW erp_sales_confirmed
WITH (security_invoker = true)
AS
SELECT
  s.*,
  CASE
    WHEN s.sales_status = 'N' THEN 'canceled'
    WHEN s.invoice_number IS NOT NULL AND s.invoice_number <> '' THEN 'confirmed'
    ELSE 'pending'
  END AS status_label
FROM erp_sales s
WHERE COALESCE(s.sales_status, '') <> 'N';
-- 취소(N)는 절대 포함 안함. confirmed + pending만 남음.


-- ============================================
-- 3. RPC get_dashboard_summary 확장 — 확정/진행 분리
-- ============================================
DROP FUNCTION IF EXISTS get_dashboard_summary(text, date);

CREATE OR REPLACE FUNCTION get_dashboard_summary(
  p_company text DEFAULT NULL,
  p_base_date date DEFAULT CURRENT_DATE
)
RETURNS TABLE (
  period text,
  year_type text,
  label text,
  confirmed_amount numeric,
  pending_amount numeric,
  total_amount numeric,
  quote_count bigint,
  period_start date,
  period_end date
) AS $$
BEGIN
  IF auth.role() != 'service_role' AND NOT EXISTS (
    SELECT 1 FROM approved_users WHERE email = auth.jwt()->>'email'
  ) THEN
    RAISE EXCEPTION 'Unauthorized';
  END IF;

  RETURN QUERY
  WITH periods AS (
    SELECT '3개월전'::text p, '올해'::text y,
           (date_trunc('month', p_base_date) - interval '3 month')::date ps,
           (date_trunc('month', p_base_date) - interval '2 month' - interval '1 day')::date pe
    UNION ALL SELECT '3개월전','작년',
           (date_trunc('month', p_base_date) - interval '1 year' - interval '3 month')::date,
           (date_trunc('month', p_base_date) - interval '1 year' - interval '2 month' - interval '1 day')::date
    UNION ALL SELECT '2개월전','올해',
           (date_trunc('month', p_base_date) - interval '2 month')::date,
           (date_trunc('month', p_base_date) - interval '1 month' - interval '1 day')::date
    UNION ALL SELECT '2개월전','작년',
           (date_trunc('month', p_base_date) - interval '1 year' - interval '2 month')::date,
           (date_trunc('month', p_base_date) - interval '1 year' - interval '1 month' - interval '1 day')::date
    UNION ALL SELECT '1개월전','올해',
           (date_trunc('month', p_base_date) - interval '1 month')::date,
           (date_trunc('month', p_base_date) - interval '1 day')::date
    UNION ALL SELECT '1개월전','작년',
           (date_trunc('month', p_base_date) - interval '1 year' - interval '1 month')::date,
           (date_trunc('month', p_base_date) - interval '1 year' - interval '1 day')::date
  )
  SELECT
    pr.p AS period,
    pr.y AS year_type,
    '확정+진행'::text AS label,
    COALESCE(SUM(CASE WHEN v.status_label = 'confirmed' THEN v.supply_amount ELSE 0 END), 0) AS confirmed_amount,
    COALESCE(SUM(CASE WHEN v.status_label = 'pending'   THEN v.supply_amount ELSE 0 END), 0) AS pending_amount,
    COALESCE(SUM(v.supply_amount), 0) AS total_amount,
    COUNT(DISTINCT v.sales_number)::bigint AS quote_count,
    pr.ps AS period_start,
    pr.pe AS period_end
  FROM periods pr
  LEFT JOIN erp_sales_confirmed v
    ON v.sales_date >= pr.ps
   AND v.sales_date <= pr.pe
   AND v.supply_amount > 0
   AND (p_company IS NULL OR v.company = p_company)
  GROUP BY pr.p, pr.y, pr.ps, pr.pe
  ORDER BY
    CASE pr.p WHEN '3개월전' THEN 1 WHEN '2개월전' THEN 2 WHEN '1개월전' THEN 3 END,
    pr.y;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;


-- ============================================
-- 완료!
-- Supabase 대시보드 > Table Editor에서 erp_sales에 3개 컬럼 추가 확인
-- Supabase > Database > Views에서 erp_sales_confirmed 재정의 확인
-- 이후 sync_erp_to_supabase.py 1회 실행해서 invoice_number 채우기
-- ============================================
