-- get_dashboard_summary RPC v3
-- v2에서 추가: 전년 동기(동월) 값도 함께 반환하여 YoY 비교 가능
-- RETURNS TABLE의 컬럼이 바뀌므로 DROP 후 재생성

DROP FUNCTION IF EXISTS get_dashboard_summary(text, date);

CREATE OR REPLACE FUNCTION get_dashboard_summary(
  p_company text DEFAULT NULL,
  p_base_date date DEFAULT CURRENT_DATE
)
RETURNS TABLE (
  period text,      -- '2개월전' / '1개월전' / '이번달'
  year_type text,   -- '올해' / '작년'
  label text,       -- '확정' / '진행중'
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

  -- 올해 2개월전
  SELECT '2개월전'::text, '올해'::text, '확정'::text,
         COALESCE(SUM(q.quote_amount), 0), COUNT(DISTINCT q.quote_number)
  FROM erp_quotes q
  WHERE q.approval_status = '승인'
    AND q.quote_date >= m_two_start AND q.quote_date < m_last_start
    AND (p_company IS NULL OR q.company = p_company)
    AND q.quote_amount > 0

  UNION ALL

  -- 작년 2개월전 (동월)
  SELECT '2개월전'::text, '작년'::text, '확정'::text,
         COALESCE(SUM(q.quote_amount), 0), COUNT(DISTINCT q.quote_number)
  FROM erp_quotes q
  WHERE q.approval_status = '승인'
    AND q.quote_date >= (m_two_start - interval '1 year')::date
    AND q.quote_date < (m_last_start - interval '1 year')::date
    AND (p_company IS NULL OR q.company = p_company)
    AND q.quote_amount > 0

  UNION ALL

  -- 올해 1개월전
  SELECT '1개월전'::text, '올해'::text, '확정'::text,
         COALESCE(SUM(q.quote_amount), 0), COUNT(DISTINCT q.quote_number)
  FROM erp_quotes q
  WHERE q.approval_status = '승인'
    AND q.quote_date >= m_last_start AND q.quote_date < m_this_start
    AND (p_company IS NULL OR q.company = p_company)
    AND q.quote_amount > 0

  UNION ALL

  -- 작년 1개월전 (동월)
  SELECT '1개월전'::text, '작년'::text, '확정'::text,
         COALESCE(SUM(q.quote_amount), 0), COUNT(DISTINCT q.quote_number)
  FROM erp_quotes q
  WHERE q.approval_status = '승인'
    AND q.quote_date >= (m_last_start - interval '1 year')::date
    AND q.quote_date < (m_this_start - interval '1 year')::date
    AND (p_company IS NULL OR q.company = p_company)
    AND q.quote_amount > 0

  UNION ALL

  -- 올해 이번달 (진행중)
  SELECT '이번달'::text, '올해'::text, '진행중'::text,
         COALESCE(SUM(q.quote_amount), 0), COUNT(DISTINCT q.quote_number)
  FROM erp_quotes q
  WHERE q.approval_status = '승인'
    AND q.quote_date >= m_this_start AND q.quote_date < m_next_start
    AND (p_company IS NULL OR q.company = p_company)
    AND q.quote_amount > 0

  UNION ALL

  -- 작년 같은 달 전체 (비교 기준 — 이번달 진행중과 YoY 비교용)
  -- 이번달은 진행 중이라 month-to-date로 비교하는 게 더 공정하지만,
  -- 일단 작년 동월 전체로 비교(경영진 의사결정에는 이게 더 의미 있음)
  SELECT '이번달'::text, '작년'::text, '확정'::text,
         COALESCE(SUM(q.quote_amount), 0), COUNT(DISTINCT q.quote_number)
  FROM erp_quotes q
  WHERE q.approval_status = '승인'
    AND q.quote_date >= (m_this_start - interval '1 year')::date
    AND q.quote_date < (m_next_start - interval '1 year')::date
    AND (p_company IS NULL OR q.company = p_company)
    AND q.quote_amount > 0;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
