-- ============================================
-- v1_schema.sql 보완 패치 — service_role 키 우회 허용
-- ============================================
-- 배경: 앞서 만든 3개 RPC(replace_quote_lines, get_sync_status, get_dashboard_summary)가
--       SECURITY DEFINER 우회를 막기 위해 approved_users 체크를 합니다.
--       그런데 sync_erp_to_supabase.py는 service_role 키로 호출하고,
--       service_role에는 이메일이 없어서 체크에 걸립니다.
-- 해결: service_role은 우회 허용 (배치 스크립트 전용), 일반 사용자는 기존대로 체크.
--
-- Supabase SQL Editor에 이 파일 전체를 붙여넣고 Run 하세요.
-- ============================================


-- 1. replace_quote_lines 수정
CREATE OR REPLACE FUNCTION replace_quote_lines(p_payload jsonb)
RETURNS TABLE(deleted_count bigint, inserted_count bigint)
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
  v_quote_ids uuid[];
  v_deleted bigint := 0;
  v_inserted bigint := 0;
BEGIN
  -- service_role(배치 스크립트)은 통과, 그 외에는 approved_users 체크
  IF auth.role() != 'service_role' AND NOT EXISTS (
    SELECT 1 FROM approved_users WHERE email = auth.jwt()->>'email'
  ) THEN
    RAISE EXCEPTION 'Unauthorized: approved_users check failed';
  END IF;

  SELECT ARRAY(
    SELECT (x)::uuid FROM jsonb_array_elements_text(p_payload->'quote_ids') AS x
  ) INTO v_quote_ids;

  WITH del AS (
    DELETE FROM erp_quote_lines
    WHERE quote_id = ANY(v_quote_ids)
    RETURNING 1
  )
  SELECT count(*) INTO v_deleted FROM del;

  WITH ins AS (
    INSERT INTO erp_quote_lines (
      quote_id, line_seq, category, usage_type, item, spec, unit,
      base_qty, sheets, quantity_r, colors, unit_price, amount,
      discount_rate, final_amount, note
    )
    SELECT
      (e->>'quote_id')::uuid,
      (e->>'line_seq')::integer,
      e->>'category',
      e->>'usage_type',
      e->>'item',
      e->>'spec',
      e->>'unit',
      e->>'base_qty',
      (e->>'sheets')::numeric,
      (e->>'quantity_r')::numeric,
      e->>'colors',
      (e->>'unit_price')::numeric,
      (e->>'amount')::numeric,
      (e->>'discount_rate')::numeric,
      (e->>'final_amount')::numeric,
      e->>'note'
    FROM jsonb_array_elements(p_payload->'lines') AS e
    RETURNING 1
  )
  SELECT count(*) INTO v_inserted FROM ins;

  RETURN QUERY SELECT v_deleted, v_inserted;
END;
$$;


-- 2. get_sync_status 수정
CREATE OR REPLACE FUNCTION get_sync_status()
RETURNS TABLE (
  job_name text,
  last_run timestamptz,
  status text,
  rows_affected integer,
  hours_since_run numeric
)
LANGUAGE sql
SECURITY DEFINER
AS $$
  SELECT
    s.job_name,
    s.run_at AS last_run,
    s.status,
    s.rows_affected,
    ROUND(EXTRACT(EPOCH FROM (now() - s.run_at)) / 3600, 1) AS hours_since_run
  FROM (
    SELECT DISTINCT ON (job_name) job_name, run_at, status, rows_affected
    FROM sync_log
    ORDER BY job_name, run_at DESC
  ) s
  WHERE auth.role() = 'service_role'
     OR EXISTS (SELECT 1 FROM approved_users WHERE email = auth.jwt()->>'email');
$$;


-- 3. get_dashboard_summary 수정
DROP FUNCTION IF EXISTS get_dashboard_summary(text, date);

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
  IF auth.role() != 'service_role' AND NOT EXISTS (
    SELECT 1 FROM approved_users WHERE email = auth.jwt()->>'email'
  ) THEN
    RAISE EXCEPTION 'Unauthorized';
  END IF;

  RETURN QUERY

  SELECT
    '2개월전'::text, '확정'::text,
    COALESCE(SUM(q.quote_amount), 0),
    COUNT(DISTINCT q.quote_number)
  FROM erp_quotes q
  WHERE q.approval_status = '승인'
    AND q.quote_date >= (date_trunc('month', p_base_date) - interval '2 month')::date
    AND q.quote_date < (date_trunc('month', p_base_date) - interval '1 month')::date
    AND (p_company IS NULL OR q.company = p_company)
    AND q.quote_amount > 0

  UNION ALL

  SELECT
    '1개월전'::text, '확정'::text,
    COALESCE(SUM(q.quote_amount), 0),
    COUNT(DISTINCT q.quote_number)
  FROM erp_quotes q
  WHERE q.approval_status = '승인'
    AND q.quote_date >= (date_trunc('month', p_base_date) - interval '1 month')::date
    AND q.quote_date < date_trunc('month', p_base_date)::date
    AND (p_company IS NULL OR q.company = p_company)
    AND q.quote_amount > 0

  UNION ALL

  SELECT
    '이번달'::text, '진행중'::text,
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


-- 완료.
