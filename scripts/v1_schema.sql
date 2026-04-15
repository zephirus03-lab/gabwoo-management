-- ============================================
-- GMD 경영 대시보드 v1 — 추가 스키마
-- 실행 방법: Supabase SQL Editor에 붙여넣기 → Run
-- 기존 create_tables.sql (erp_quotes, erp_quote_lines)은 그대로 두고,
-- 이 파일은 v1에서 추가되는 테이블/기능만 담습니다.
--
-- 2026-04-15 작성 · CTO 리뷰 TOP-1, TOP-2, KPI-6 대응
-- ============================================


-- ============================================
-- 1. sync_log — 배치 실행 이력 (CTO 리뷰 TOP-1)
-- ============================================
-- 배치가 돌 때마다 한 행 기록합니다.
-- 대시보드 상단 배너가 이 테이블의 최신 행을 읽어서
-- "데이터 기준: YYYY-MM-DD HH:MM" 표시 + 24시간 이상 미갱신 시 🔴 경고.

CREATE TABLE IF NOT EXISTS sync_log (
  id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  run_at        timestamptz NOT NULL DEFAULT now(),
  job_name      text NOT NULL,       -- 'erp_quotes' / 'paper_purchases' / 'pricing_audit' 등
  status        text NOT NULL,       -- 'success' / 'failed'
  rows_affected integer,             -- upsert 건수 (실패 시 NULL 가능)
  duration_sec  numeric,             -- 실행 시간
  error_msg     text,                -- 실패 시 메시지 500자 이내
  created_at    timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_sync_log_job_time ON sync_log(job_name, run_at DESC);
CREATE INDEX IF NOT EXISTS idx_sync_log_latest ON sync_log(run_at DESC);

-- RLS: 읽기만 허용, 쓰기는 service_role만 (anon key로는 기록 불가)
ALTER TABLE sync_log ENABLE ROW LEVEL SECURITY;

CREATE POLICY "approved_users_read_sync_log" ON sync_log
  FOR SELECT USING (
    auth.uid() IS NOT NULL
    AND EXISTS (SELECT 1 FROM approved_users WHERE email = auth.jwt()->>'email')
  );


-- ============================================
-- 2. erp_paper_purchases — 용지 매입 (KPI-6)
-- ============================================
-- SNOTES.viewGabwoo_마감을 그대로 옮겨 담습니다.
-- 월별 집계는 대시보드에서 date_trunc('month', purchase_date)로 처리.

CREATE TABLE IF NOT EXISTS erp_paper_purchases (
  id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  purchase_date   date NOT NULL,
  po_number       text NOT NULL,          -- viewGabwoo_마감.CustKey (예: 'PO2025070140')
  line_seq        integer NOT NULL,       -- NO_LINE
  paper_type      integer,                -- 지종
  maker_name      text,                   -- 제조사명 (무림/한솔/…)
  width_mm        numeric,
  height_mm       numeric,
  quantity        numeric,
  unit_price      numeric,
  supply_amount   numeric,                -- 공급가액
  standard_price  numeric,                -- 표준가
  synced_at       timestamptz DEFAULT now(),
  UNIQUE(po_number, line_seq)
);

CREATE INDEX IF NOT EXISTS idx_paper_date ON erp_paper_purchases(purchase_date);
CREATE INDEX IF NOT EXISTS idx_paper_maker ON erp_paper_purchases(maker_name);

ALTER TABLE erp_paper_purchases ENABLE ROW LEVEL SECURITY;

CREATE POLICY "approved_users_read_paper" ON erp_paper_purchases
  FOR SELECT USING (
    auth.uid() IS NOT NULL
    AND EXISTS (SELECT 1 FROM approved_users WHERE email = auth.jwt()->>'email')
  );

CREATE POLICY "approved_users_insert_paper" ON erp_paper_purchases
  FOR INSERT WITH CHECK (
    auth.uid() IS NOT NULL
    AND EXISTS (SELECT 1 FROM approved_users WHERE email = auth.jwt()->>'email')
  );

CREATE POLICY "approved_users_update_paper" ON erp_paper_purchases
  FOR UPDATE USING (
    auth.uid() IS NOT NULL
    AND EXISTS (SELECT 1 FROM approved_users WHERE email = auth.jwt()->>'email')
  );


-- ============================================
-- 3. replace_quote_lines RPC — 트랜잭션 안전 라인 교체 (CTO 리뷰 TOP-2)
-- ============================================
-- 기존 sync_erp_to_supabase.py의 replace_lines()는 DELETE + INSERT 2단계라
-- 중간 실패 시 라인이 영구 손실됩니다. 이를 트랜잭션으로 감싼 RPC로 대체.
--
-- 사용법:
--   SELECT replace_quote_lines(
--     '{"quote_ids":["uuid1","uuid2"], "lines":[{...}, {...}]}'::jsonb
--   );
-- 하나의 트랜잭션에서 DELETE → INSERT 수행. 실패 시 전체 롤백.

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
  -- 승인 사용자만 허용 (SECURITY DEFINER 우회 방지 — Security 리뷰 지적)
  IF NOT EXISTS (
    SELECT 1 FROM approved_users WHERE email = auth.jwt()->>'email'
  ) THEN
    RAISE EXCEPTION 'Unauthorized: approved_users check failed';
  END IF;

  -- 1. 대상 quote_id 배열 추출
  SELECT ARRAY(
    SELECT (x)::uuid FROM jsonb_array_elements_text(p_payload->'quote_ids') AS x
  ) INTO v_quote_ids;

  -- 2. 기존 라인 삭제
  WITH del AS (
    DELETE FROM erp_quote_lines
    WHERE quote_id = ANY(v_quote_ids)
    RETURNING 1
  )
  SELECT count(*) INTO v_deleted FROM del;

  -- 3. 새 라인 insert
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


-- ============================================
-- 4. get_sync_status RPC — 대시보드 상단 배너용
-- ============================================
-- 최신 sync_log를 job_name별로 반환. HTML이 이걸 읽어서 배너 구성.

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
  -- 승인 사용자 체크 (SECURITY DEFINER 우회 방지)
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
  WHERE EXISTS (
    SELECT 1 FROM approved_users WHERE email = auth.jwt()->>'email'
  );
$$;


-- ============================================
-- 5. get_dashboard_summary에 approved_users 체크 추가 (Security 리뷰)
-- ============================================
-- 기존 함수는 SECURITY DEFINER로 RLS를 우회하므로
-- 함수 내부에서 명시적으로 권한 체크.
-- PostgreSQL은 반환 타입 변경 시 REPLACE 불가라 먼저 DROP 필요.

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
  -- 권한 체크
  IF NOT EXISTS (
    SELECT 1 FROM approved_users WHERE email = auth.jwt()->>'email'
  ) THEN
    RAISE EXCEPTION 'Unauthorized';
  END IF;

  RETURN QUERY

  -- 2개월전 (확정: 승인)
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


-- ============================================
-- 실행 확인
-- Supabase > Table Editor에서 다음이 보여야 합니다:
--   - sync_log (RLS enabled)
--   - erp_paper_purchases (RLS enabled)
-- Supabase > Database > Functions:
--   - replace_quote_lines
--   - get_sync_status
--   - get_dashboard_summary (갱신됨)
-- ============================================
