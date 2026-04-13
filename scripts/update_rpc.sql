-- 이 SQL만 Supabase SQL Editor에 붙여넣고 Run 하세요
-- 기존 함수를 덮어씁니다 (테이블은 건드리지 않음)

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

  SELECT
    '지난달'::text AS period,
    '승인'::text AS label,
    COALESCE(SUM(q.quote_amount), 0) AS total_amount,
    COUNT(DISTINCT q.quote_number) AS quote_count
  FROM erp_quotes q
  WHERE q.approval_status = '승인'
    AND q.quote_date >= (date_trunc('month', p_base_date) - interval '1 month')::date
    AND q.quote_date < date_trunc('month', p_base_date)::date
    AND (p_company IS NULL OR q.company = p_company)
    AND q.quote_amount > 0

  UNION ALL

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
$$ LANGUAGE plpgsql SECURITY INVOKER;
