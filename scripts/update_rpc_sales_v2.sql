-- v2 (2026-04-15): PDF(세금계산서 실 발행) 대조 결과 반영
--
-- 변경 요지:
--   1) 필터 `sales_status='Y' AND supply_amount>0`
--      → `supply_amount <> 0`
--      근거: PDF 2025년 합계 38,945M 대비
--        - 기존 필터: 23,479M (60.3%)  ← 교원구몬 5.1B 등 ST='Y' 미적용 거래처 대량 누락
--        - 신규 필터: 37,656M (96.7%)
--   2) p_business_unit 파라미터 추가 (ERP CD_BIZ)
--        - 'GABWOO' = 주력 출판/인쇄
--        - 'PKG'    = 패키지 사업부
--        - ''(빈값) = 비피앤피 계열로 추정
--      NULL 전달 시 전체 합산 (현행과 동일)
--   3) 취소/수정 마이너스건(AM<0)도 자연 차감되도록 SUM 그대로 둠
--      (PDF에서도 마이너스 행이 상쇄하는 방식이라 동일 결과)

DROP FUNCTION IF EXISTS get_dashboard_summary(text, date);
DROP FUNCTION IF EXISTS get_dashboard_summary(text, date, text);

CREATE OR REPLACE FUNCTION get_dashboard_summary(
  p_company       text DEFAULT NULL,
  p_base_date     date DEFAULT CURRENT_DATE,
  p_business_unit text DEFAULT NULL   -- v2 신규: CD_BIZ 필터
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
  m_two_start  date := (date_trunc('month', p_base_date) - interval '2 month')::date;
  m_next_start date := (date_trunc('month', p_base_date) + interval '1 month')::date;
BEGIN
  IF auth.jwt()->>'email' IS NOT NULL AND NOT EXISTS (
    SELECT 1 FROM approved_users WHERE email = auth.jwt()->>'email'
  ) THEN
    RAISE EXCEPTION 'Unauthorized';
  END IF;

  RETURN QUERY

  -- 2개월 전 (올해)
  SELECT '2개월전'::text, '올해'::text, '확정'::text,
         COALESCE(SUM(s.supply_amount), 0), COUNT(*)::bigint
  FROM erp_sales s
  WHERE s.supply_amount <> 0
    AND s.sales_date >= m_two_start AND s.sales_date < m_last_start
    AND (p_company IS NULL OR s.company = p_company)
    AND (p_business_unit IS NULL OR COALESCE(s.business_unit, '') = p_business_unit)

  UNION ALL

  -- 2개월 전 (작년 동월)
  SELECT '2개월전'::text, '작년'::text, '확정'::text,
         COALESCE(SUM(s.supply_amount), 0), COUNT(*)::bigint
  FROM erp_sales s
  WHERE s.supply_amount <> 0
    AND s.sales_date >= (m_two_start  - interval '1 year')::date
    AND s.sales_date <  (m_last_start - interval '1 year')::date
    AND (p_company IS NULL OR s.company = p_company)
    AND (p_business_unit IS NULL OR COALESCE(s.business_unit, '') = p_business_unit)

  UNION ALL

  -- 1개월 전 (올해)
  SELECT '1개월전'::text, '올해'::text, '확정'::text,
         COALESCE(SUM(s.supply_amount), 0), COUNT(*)::bigint
  FROM erp_sales s
  WHERE s.supply_amount <> 0
    AND s.sales_date >= m_last_start AND s.sales_date < m_this_start
    AND (p_company IS NULL OR s.company = p_company)
    AND (p_business_unit IS NULL OR COALESCE(s.business_unit, '') = p_business_unit)

  UNION ALL

  -- 1개월 전 (작년 동월)
  SELECT '1개월전'::text, '작년'::text, '확정'::text,
         COALESCE(SUM(s.supply_amount), 0), COUNT(*)::bigint
  FROM erp_sales s
  WHERE s.supply_amount <> 0
    AND s.sales_date >= (m_last_start - interval '1 year')::date
    AND s.sales_date <  (m_this_start - interval '1 year')::date
    AND (p_company IS NULL OR s.company = p_company)
    AND (p_business_unit IS NULL OR COALESCE(s.business_unit, '') = p_business_unit)

  UNION ALL

  -- 이번 달 (올해, 진행중)
  SELECT '이번달'::text, '올해'::text, '진행중'::text,
         COALESCE(SUM(s.supply_amount), 0), COUNT(*)::bigint
  FROM erp_sales s
  WHERE s.supply_amount <> 0
    AND s.sales_date >= m_this_start AND s.sales_date < m_next_start
    AND (p_company IS NULL OR s.company = p_company)
    AND (p_business_unit IS NULL OR COALESCE(s.business_unit, '') = p_business_unit)

  UNION ALL

  -- 이번 달 (작년 동월, 확정)
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
