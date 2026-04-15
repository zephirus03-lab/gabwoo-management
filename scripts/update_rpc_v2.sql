-- get_dashboard_summary RPC v2
-- 기존: 지난달(확정) / 이번달(예상) / 다음달(예상)
-- 변경: 2개월전(확정) / 1개월전(확정) / 이번달(진행중)
-- 이유: 경영진은 확정된 실적을 중심으로 의사결정하므로 예측보다 과거 2개월 실적 비교가 유용함

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

  -- 2개월전 (확정: 승인)
  SELECT
    '2개월전'::text AS period,
    '확정'::text AS label,
    COALESCE(SUM(q.quote_amount), 0) AS total_amount,
    COUNT(DISTINCT q.quote_number) AS quote_count
  FROM erp_quotes q
  WHERE q.approval_status = '승인'
    AND q.quote_date >= (date_trunc('month', p_base_date) - interval '2 month')::date
    AND q.quote_date < (date_trunc('month', p_base_date) - interval '1 month')::date
    AND (p_company IS NULL OR q.company = p_company)
    AND q.quote_amount > 0

  UNION ALL

  -- 1개월전 (확정: 승인)
  SELECT
    '1개월전'::text,
    '확정'::text,
    COALESCE(SUM(q.quote_amount), 0),
    COUNT(DISTINCT q.quote_number)
  FROM erp_quotes q
  WHERE q.approval_status = '승인'
    AND q.quote_date >= (date_trunc('month', p_base_date) - interval '1 month')::date
    AND q.quote_date < date_trunc('month', p_base_date)::date
    AND (p_company IS NULL OR q.company = p_company)
    AND q.quote_amount > 0

  UNION ALL

  -- 이번달 (진행중: 승인된 견적 누적)
  SELECT
    '이번달'::text,
    '진행중'::text,
    COALESCE(SUM(q.quote_amount), 0),
    COUNT(DISTINCT q.quote_number)
  FROM erp_quotes q
  WHERE q.approval_status = '승인'
    AND q.quote_date >= date_trunc('month', p_base_date)::date
    AND q.quote_date < (date_trunc('month', p_base_date) + interval '1 month')::date
    AND (p_company IS NULL OR q.company = p_company)
    AND q.quote_amount > 0;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
