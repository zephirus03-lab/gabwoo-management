-- 매출 기반 RPC: erp_sales 테이블을 참조하도록 대시보드 서머리를 덮어씁니다.
-- 기존 get_dashboard_summary는 견적 기반이었으나 이제 확정 매출 기반으로 전환합니다.
-- 집계 필터: sales_status = 'Y' (취소 제외) + supply_amount > 0
-- 금액 컬럼: supply_amount (공급가, VAT 제외) — 경영진 관점의 매출액

DROP FUNCTION IF EXISTS get_dashboard_summary(text, date);

CREATE OR REPLACE FUNCTION get_dashboard_summary(
  p_company text DEFAULT NULL,
  p_base_date date DEFAULT CURRENT_DATE
)
RETURNS TABLE (
  period text,
  year_type text,
  label text,
  total_amount numeric,
  quote_count bigint
) AS $$
DECLARE
  m_this_start date := date_trunc('month', p_base_date)::date;
  m_last_start date := (date_trunc('month', p_base_date) - interval '1 month')::date;
  m_two_start date := (date_trunc('month', p_base_date) - interval '2 month')::date;
  m_next_start date := (date_trunc('month', p_base_date) + interval '1 month')::date;
BEGIN
  RETURN QUERY

  SELECT '2개월전'::text, '올해'::text, '확정'::text,
         COALESCE(SUM(s.supply_amount), 0), COUNT(*)::bigint
  FROM erp_sales s
  WHERE s.sales_status = 'Y' AND s.supply_amount > 0
    AND s.sales_date >= m_two_start AND s.sales_date < m_last_start
    AND (p_company IS NULL OR s.company = p_company)

  UNION ALL

  SELECT '2개월전'::text, '작년'::text, '확정'::text,
         COALESCE(SUM(s.supply_amount), 0), COUNT(*)::bigint
  FROM erp_sales s
  WHERE s.sales_status = 'Y' AND s.supply_amount > 0
    AND s.sales_date >= (m_two_start - interval '1 year')::date
    AND s.sales_date < (m_last_start - interval '1 year')::date
    AND (p_company IS NULL OR s.company = p_company)

  UNION ALL

  SELECT '1개월전'::text, '올해'::text, '확정'::text,
         COALESCE(SUM(s.supply_amount), 0), COUNT(*)::bigint
  FROM erp_sales s
  WHERE s.sales_status = 'Y' AND s.supply_amount > 0
    AND s.sales_date >= m_last_start AND s.sales_date < m_this_start
    AND (p_company IS NULL OR s.company = p_company)

  UNION ALL

  SELECT '1개월전'::text, '작년'::text, '확정'::text,
         COALESCE(SUM(s.supply_amount), 0), COUNT(*)::bigint
  FROM erp_sales s
  WHERE s.sales_status = 'Y' AND s.supply_amount > 0
    AND s.sales_date >= (m_last_start - interval '1 year')::date
    AND s.sales_date < (m_this_start - interval '1 year')::date
    AND (p_company IS NULL OR s.company = p_company)

  UNION ALL

  SELECT '이번달'::text, '올해'::text, '진행중'::text,
         COALESCE(SUM(s.supply_amount), 0), COUNT(*)::bigint
  FROM erp_sales s
  WHERE s.sales_status = 'Y' AND s.supply_amount > 0
    AND s.sales_date >= m_this_start AND s.sales_date < m_next_start
    AND (p_company IS NULL OR s.company = p_company)

  UNION ALL

  SELECT '이번달'::text, '작년'::text, '확정'::text,
         COALESCE(SUM(s.supply_amount), 0), COUNT(*)::bigint
  FROM erp_sales s
  WHERE s.sales_status = 'Y' AND s.supply_amount > 0
    AND s.sales_date >= (m_this_start - interval '1 year')::date
    AND s.sales_date < (m_next_start - interval '1 year')::date
    AND (p_company IS NULL OR s.company = p_company);
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
