-- pnl_mailbox_schema.sql — 분기손익 자동산출기 v2 "우편함" 스키마
-- Design Ref: 분기손익산출기/docs/02-design/features/분기손익-자동산출기.design.md §3.2·§7
-- 적용: Supabase 대시보드(btbqzbrtsmwoolurpqgx) SQL Editor에서 1회 실행. (DDL은 사용자가 직접 적용)
-- 구조: 사내 엔진 → 결과 push / 프론트(gabwooceo) → 업로드·요청·렌더. 단방향(push only).
-- 보안(SC-6): 경영진(approved_users)만 읽기/업로드. anon 직접 접근 차단. 사내 엔진은 service_role로 우회.
--
-- 작성 2026-06-11 (PDCA Do module-2)

-- ───────────────────────────────────────────────────────────────────────────
-- 1) 기간 단위 산출 요청·상태 (우편함의 작업 큐)
-- ───────────────────────────────────────────────────────────────────────────
create table if not exists pnl_periods (
  id            text primary key,            -- '2026Q2' | '2026-05' | '2026-04-01_2026-06-30'
  kind          text not null,               -- 'quarter' | 'month' | 'custom'
  start_date    date not null,
  end_date      date not null,
  status        text not null default 'collecting',
                -- collecting → requested → (blocked|running) → done | error
  requested_by  uuid,                         -- 요청한 경영진(auth.users)
  ran_by        text,                         -- 실행 권한자 식별(엔진이 기록 — 감사)
  message       text,                         -- blocked/error 사유 등 상태 메시지
  updated_at    timestamptz default now()
);

comment on table  pnl_periods is '분기손익 산출 요청·상태 큐. status: collecting→requested→(blocked|running)→done|error';
comment on column pnl_periods.kind is 'quarter|month|custom. custom은 months[] 합산';
comment on column pnl_periods.ran_by is '사내 엔진 실행 권한자(누가 실행해도 결과 동일, 감사용)';

-- ───────────────────────────────────────────────────────────────────────────
-- 2) 소스 도착 현황 (현황판 데이터)
-- ───────────────────────────────────────────────────────────────────────────
create table if not exists pnl_sources (
  period_id    text not null references pnl_periods(id) on delete cascade,
  source_id    text not null,                -- config/sources.json의 source_id
  month        text not null default '',     -- 월별 소스면 '2026-05', 아니면 ''(분기일괄)
  status       text not null,                -- 'missing'|'uploaded'|'manual_empty'|'ready'
  storage_path text,                          -- Storage 내 파일 경로
  owner        text,                          -- 담당자(미수령 알림용)
  title        text,                          -- 요청 자료명
  updated_at   timestamptz default now(),
  primary key (period_id, source_id, month)
);

comment on table  pnl_sources is '소스 도착 현황. 게이트·현황판 데이터. status: missing|uploaded|manual_empty|ready';

-- ───────────────────────────────────────────────────────────────────────────
-- 3) 산출 결과 (대시보드 렌더 소스)
-- ───────────────────────────────────────────────────────────────────────────
create table if not exists pnl_results (
  period_id   text primary key references pnl_periods(id) on delete cascade,
  payload     jsonb not null,                -- 부문손익·브릿지·거래처손익·갭리포트·출처메타 일체
  engine_ver  text,                           -- cost_params version + 엔진 git hash
  gap_summary jsonb,                          -- 정답지 대조 요약
  created_at  timestamptz default now()
);

comment on table  pnl_results is '산출 결과 JSON 한 덩어리. 프론트는 이 payload만 받아 렌더(엑셀 불필요)';

-- ───────────────────────────────────────────────────────────────────────────
-- 4) Storage 버킷 (업로드 원본 — 비공개)
-- ───────────────────────────────────────────────────────────────────────────
-- 경로 규칙: {period_id}/{source_id}/{원본파일명}
insert into storage.buckets (id, name, public)
values ('pnl-uploads', 'pnl-uploads', false)
on conflict (id) do nothing;

-- ───────────────────────────────────────────────────────────────────────────
-- 5) RLS — 경영진(approved_users)만 접근 (SC-6). 기존 인증 자산 재사용.
--    사내 엔진은 service_role 키로 RLS 우회(서버측) → 별도 정책 불필요.
-- ───────────────────────────────────────────────────────────────────────────
-- 헬퍼: 현재 로그인 사용자가 승인된 경영진인가
--   (approved_users는 기존 gabwooceo 인증 테이블. email 기준 승인 관리)
create or replace function is_approved_management()
returns boolean
language sql stable
as $$
  select exists (
    select 1 from approved_users au
    where au.email = (auth.jwt() ->> 'email')
  );
$$;

alter table pnl_periods  enable row level security;
alter table pnl_sources  enable row level security;
alter table pnl_results  enable row level security;

-- 읽기: 승인된 경영진만
drop policy if exists pnl_periods_read on pnl_periods;
create policy pnl_periods_read on pnl_periods
  for select using (is_approved_management());

drop policy if exists pnl_sources_read on pnl_sources;
create policy pnl_sources_read on pnl_sources
  for select using (is_approved_management());

drop policy if exists pnl_results_read on pnl_results;
create policy pnl_results_read on pnl_results
  for select using (is_approved_management());

-- 쓰기(기간 생성/요청·소스 업로드 기록): 승인된 경영진만
drop policy if exists pnl_periods_write on pnl_periods;
create policy pnl_periods_write on pnl_periods
  for all using (is_approved_management()) with check (is_approved_management());

drop policy if exists pnl_sources_write on pnl_sources;
create policy pnl_sources_write on pnl_sources
  for all using (is_approved_management()) with check (is_approved_management());

-- pnl_results 쓰기는 사내 엔진(service_role)만 → anon/authenticated 쓰기 정책 없음(차단).

-- ───────────────────────────────────────────────────────────────────────────
-- 6) Storage RLS — pnl-uploads 버킷: 경영진만 업로드/조회. 직접 URL 추측 차단(NFR-3).
-- ───────────────────────────────────────────────────────────────────────────
drop policy if exists pnl_uploads_read on storage.objects;
create policy pnl_uploads_read on storage.objects
  for select using (bucket_id = 'pnl-uploads' and is_approved_management());

drop policy if exists pnl_uploads_write on storage.objects;
create policy pnl_uploads_write on storage.objects
  for insert with check (bucket_id = 'pnl-uploads' and is_approved_management());

-- ───────────────────────────────────────────────────────────────────────────
-- 7) 갱신 트리거 — updated_at 자동
-- ───────────────────────────────────────────────────────────────────────────
create or replace function touch_updated_at()
returns trigger language plpgsql as $$
begin new.updated_at = now(); return new; end; $$;

drop trigger if exists trg_pnl_periods_touch on pnl_periods;
create trigger trg_pnl_periods_touch before update on pnl_periods
  for each row execute function touch_updated_at();

drop trigger if exists trg_pnl_sources_touch on pnl_sources;
create trigger trg_pnl_sources_touch before update on pnl_sources
  for each row execute function touch_updated_at();

-- 끝. 적용 후 분기손익산출기/src/io/supabase_io.py가 이 스키마를 우편함으로 사용.
