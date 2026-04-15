-- 경영 대시보드 RPC: 최근 확정 3개월만 반환
-- 배경: 매달 10일에 전월 세금계산서가 마감되므로 "이번달 진행중"은 신뢰할 수 없음.
-- 프론트엔드가 base_date를 "마지막 확정월 + 1" (= 이번달 초, day>10이면 / 지난달 초, day<=10이면)로 전달.
-- RPC는 그 base_date 직전 3개월을 반환.

DROP FUNCTION IF EXISTS get_dashboard_summary(text, date);

CREATE OR REPLACE FUNCTION get_dashboard_summary(
  p_company text DEFAULT NULL,
  p_base_date date DEFAULT CURRENT_DATE
)
RETURNS TABLE (
  period text,       -- '3개월전' / '2개월전' / '1개월전'  (모두 확정)
  year_type text,    -- '올해' / '작년'
  label text,        -- '확정'
  total_amount numeric,
  quote_count bigint,
  period_start date,
  period_end date
) AS $$
DECLARE
  m0 date := date_trunc('month', p_base_date)::date;         -- base_date 속 월의 1일
  m1 date := (m0 - interval '1 month')::date;                -- 1개월전 시작
  m2 date := (m0 - interval '2 month')::date;                -- 2개월전 시작
  m3 date := (m0 - interval '3 month')::date;                -- 3개월전 시작
  ly_m0 date := (m0 - interval '1 year')::date;
  ly_m1 date := (m1 - interval '1 year')::date;
  ly_m2 date := (m2 - interval '1 year')::date;
  ly_m3 date := (m3 - interval '1 year')::date;
BEGIN
  IF auth.jwt()->>'email' IS NOT NULL AND NOT EXISTS (
    SELECT 1 FROM approved_users WHERE email = auth.jwt()->>'email'
  ) THEN
    RAISE EXCEPTION 'Unauthorized';
  END IF;

  RETURN QUERY

  -- 3개월전 올해
  SELECT '3개월전'::text, '올해'::text, '확정'::text,
         COALESCE(SUM(s.supply_amount), 0), COUNT(*)::bigint, m3, (m2 - interval '1 day')::date
  FROM erp_sales s
  WHERE s.sales_status = 'Y' AND s.supply_amount > 0
    AND s.sales_date >= m3 AND s.sales_date < m2
    AND (p_company IS NULL OR s.company = p_company)

  UNION ALL

  -- 3개월전 작년
  SELECT '3개월전'::text, '작년'::text, '확정'::text,
         COALESCE(SUM(s.supply_amount), 0), COUNT(*)::bigint, ly_m3, (ly_m2 - interval '1 day')::date
  FROM erp_sales s
  WHERE s.sales_status = 'Y' AND s.supply_amount > 0
    AND s.sales_date >= ly_m3 AND s.sales_date < ly_m2
    AND (p_company IS NULL OR s.company = p_company)

  UNION ALL

  -- 2개월전 올해
  SELECT '2개월전'::text, '올해'::text, '확정'::text,
         COALESCE(SUM(s.supply_amount), 0), COUNT(*)::bigint, m2, (m1 - interval '1 day')::date
  FROM erp_sales s
  WHERE s.sales_status = 'Y' AND s.supply_amount > 0
    AND s.sales_date >= m2 AND s.sales_date < m1
    AND (p_company IS NULL OR s.company = p_company)

  UNION ALL

  -- 2개월전 작년
  SELECT '2개월전'::text, '작년'::text, '확정'::text,
         COALESCE(SUM(s.supply_amount), 0), COUNT(*)::bigint, ly_m2, (ly_m1 - interval '1 day')::date
  FROM erp_sales s
  WHERE s.sales_status = 'Y' AND s.supply_amount > 0
    AND s.sales_date >= ly_m2 AND s.sales_date < ly_m1
    AND (p_company IS NULL OR s.company = p_company)

  UNION ALL

  -- 1개월전 올해 (가장 최근 확정월)
  SELECT '1개월전'::text, '올해'::text, '확정'::text,
         COALESCE(SUM(s.supply_amount), 0), COUNT(*)::bigint, m1, (m0 - interval '1 day')::date
  FROM erp_sales s
  WHERE s.sales_status = 'Y' AND s.supply_amount > 0
    AND s.sales_date >= m1 AND s.sales_date < m0
    AND (p_company IS NULL OR s.company = p_company)

  UNION ALL

  -- 1개월전 작년
  SELECT '1개월전'::text, '작년'::text, '확정'::text,
         COALESCE(SUM(s.supply_amount), 0), COUNT(*)::bigint, ly_m1, (ly_m0 - interval '1 day')::date
  FROM erp_sales s
  WHERE s.sales_status = 'Y' AND s.supply_amount > 0
    AND s.sales_date >= ly_m1 AND s.sales_date < ly_m0
    AND (p_company IS NULL OR s.company = p_company);
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
