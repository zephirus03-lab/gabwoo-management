-- v3 (2026-04-15): 3개월전 케이스 추가
-- v2에서 2개월전/1개월전/이번달만 반환해서 프론트 1패널(3개월전)이 0원으로 표시되던 버그 수정.
-- 프론트는 logicalBase 기준 m3/m2/m1 3패널 구조.

DROP FUNCTION IF EXISTS get_dashboard_summary(text, date);
DROP FUNCTION IF EXISTS get_dashboard_summary(text, date, text);

CREATE OR REPLACE FUNCTION get_dashboard_summary(
  p_company       text DEFAULT NULL,
  p_base_date     date DEFAULT CURRENT_DATE,
  p_business_unit text DEFAULT NULL
)
RETURNS TABLE (
  period text,
  year_type text,
  label text,
  total_amount numeric,
  quote_count bigint
) AS $$
DECLARE
  m_this_start   date := date_trunc('month', p_base_date)::date;
  m_1ago_start   date := (date_trunc('month', p_base_date) - interval '1 month')::date;
  m_2ago_start   date := (date_trunc('month', p_base_date) - interval '2 month')::date;
  m_3ago_start   date := (date_trunc('month', p_base_date) - interval '3 month')::date;
  m_next_start   date := (date_trunc('month', p_base_date) + interval '1 month')::date;
BEGIN
  IF auth.jwt()->>'email' IS NOT NULL AND NOT EXISTS (
    SELECT 1 FROM approved_users WHERE email = auth.jwt()->>'email'
  ) THEN
    RAISE EXCEPTION 'Unauthorized';
  END IF;

  RETURN QUERY

  -- 3개월 전 (올해)
  SELECT '3개월전'::text, '올해'::text, '확정'::text,
         COALESCE(SUM(s.supply_amount), 0), COUNT(*)::bigint
  FROM erp_sales s
  WHERE s.supply_amount <> 0
    AND s.sales_date >= m_3ago_start AND s.sales_date < m_2ago_start
    AND (p_company IS NULL OR s.company = p_company)
    AND (p_business_unit IS NULL OR COALESCE(s.business_unit, '') = p_business_unit)

  UNION ALL

  SELECT '3개월전'::text, '작년'::text, '확정'::text,
         COALESCE(SUM(s.supply_amount), 0), COUNT(*)::bigint
  FROM erp_sales s
  WHERE s.supply_amount <> 0
    AND s.sales_date >= (m_3ago_start - interval '1 year')::date
    AND s.sales_date <  (m_2ago_start - interval '1 year')::date
    AND (p_company IS NULL OR s.company = p_company)
    AND (p_business_unit IS NULL OR COALESCE(s.business_unit, '') = p_business_unit)

  UNION ALL

  -- 2개월 전
  SELECT '2개월전'::text, '올해'::text, '확정'::text,
         COALESCE(SUM(s.supply_amount), 0), COUNT(*)::bigint
  FROM erp_sales s
  WHERE s.supply_amount <> 0
    AND s.sales_date >= m_2ago_start AND s.sales_date < m_1ago_start
    AND (p_company IS NULL OR s.company = p_company)
    AND (p_business_unit IS NULL OR COALESCE(s.business_unit, '') = p_business_unit)

  UNION ALL

  SELECT '2개월전'::text, '작년'::text, '확정'::text,
         COALESCE(SUM(s.supply_amount), 0), COUNT(*)::bigint
  FROM erp_sales s
  WHERE s.supply_amount <> 0
    AND s.sales_date >= (m_2ago_start - interval '1 year')::date
    AND s.sales_date <  (m_1ago_start - interval '1 year')::date
    AND (p_company IS NULL OR s.company = p_company)
    AND (p_business_unit IS NULL OR COALESCE(s.business_unit, '') = p_business_unit)

  UNION ALL

  -- 1개월 전 (가장 최근 확정월)
  SELECT '1개월전'::text, '올해'::text, '확정'::text,
         COALESCE(SUM(s.supply_amount), 0), COUNT(*)::bigint
  FROM erp_sales s
  WHERE s.supply_amount <> 0
    AND s.sales_date >= m_1ago_start AND s.sales_date < m_this_start
    AND (p_company IS NULL OR s.company = p_company)
    AND (p_business_unit IS NULL OR COALESCE(s.business_unit, '') = p_business_unit)

  UNION ALL

  SELECT '1개월전'::text, '작년'::text, '확정'::text,
         COALESCE(SUM(s.supply_amount), 0), COUNT(*)::bigint
  FROM erp_sales s
  WHERE s.supply_amount <> 0
    AND s.sales_date >= (m_1ago_start - interval '1 year')::date
    AND s.sales_date <  (m_this_start - interval '1 year')::date
    AND (p_company IS NULL OR s.company = p_company)
    AND (p_business_unit IS NULL OR COALESCE(s.business_unit, '') = p_business_unit)

  UNION ALL

  -- 이번 달 (진행중)
  SELECT '이번달'::text, '올해'::text, '진행중'::text,
         COALESCE(SUM(s.supply_amount), 0), COUNT(*)::bigint
  FROM erp_sales s
  WHERE s.supply_amount <> 0
    AND s.sales_date >= m_this_start AND s.sales_date < m_next_start
    AND (p_company IS NULL OR s.company = p_company)
    AND (p_business_unit IS NULL OR COALESCE(s.business_unit, '') = p_business_unit)

  UNION ALL

  SELECT '이번달'::text, '작년'::text, '확정'::text,
         COALESCE(SUM(s.supply_amount), 0), COUNT(*)::bigint
  FROM erp_sales s
  WHERE s.supply_amount <> 0
    AND s.sales_date >= (m_this_start - interval '1 year')::date
    AND s.sales_date <  (m_next_start - interval '1 year')::date
    AND (p_company IS NULL OR s.company = p_company)
    AND (p_business_unit IS NULL OR COALESCE(s.business_unit, '') = p_business_unit);
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

GRANT EXECUTE ON FUNCTION get_dashboard_summary(text, date, text) TO authenticated, anon, service_role;
