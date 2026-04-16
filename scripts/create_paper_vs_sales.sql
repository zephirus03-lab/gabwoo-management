-- 용지원가 vs 판매단가 2년 월별 추세 테이블
-- Feature 1 (Layer 2): gabwooceo.vercel.app 대시보드에서 사용
--
-- 소스:
--   용지 매입 단가 = viewGabwoo_마감 (그룹 공통, CD_CUST_OWN 없음 — 3사 합산만 가능)
--   판매 단가     = SAL_SALESL × SAL_SALESH (AM/QT 가중평균, CD_CUST_OWN별 분리 가능)
--
-- company 코드:
--   'all'   = 3사 합산 (기본 표시)
--   '10000' = 갑우문화사
--   '20000' = 비피앤피
--   '30000' = 더원프린팅
--
-- 용지 데이터는 그룹 공통이므로 모든 company 행에 동일 paper_um_avg 값을 넣어서
-- 프론트가 탭 전환 시 단순 필터만 하도록 설계.

CREATE TABLE IF NOT EXISTS paper_vs_sales_monthly (
  id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  ym              text NOT NULL,               -- 'YYYY-MM'
  company         text NOT NULL,               -- 'all' / '10000' / '20000' / '30000'
  paper_um_avg    numeric,                     -- 용지 가중평균 단가 (그룹 공통)
  paper_qty       numeric,                     -- 총 수량 (장)
  paper_amount    numeric,                     -- 용지 매입 총액 (공급가)
  sales_um_avg    numeric,                     -- 판매 가중평균 단가 (company별)
  sales_qty       numeric,                     -- 판매 수량
  sales_amount    numeric,                     -- 판매 매출 총액 (공급가)
  updated_at      timestamptz DEFAULT now(),
  UNIQUE (ym, company)
);

CREATE INDEX IF NOT EXISTS idx_pvs_ym ON paper_vs_sales_monthly(ym);
CREATE INDEX IF NOT EXISTS idx_pvs_company ON paper_vs_sales_monthly(company);

ALTER TABLE paper_vs_sales_monthly ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "approved_users_read_pvs" ON paper_vs_sales_monthly;
CREATE POLICY "approved_users_read_pvs" ON paper_vs_sales_monthly
  FOR SELECT USING (
    auth.uid() IS NOT NULL
    AND EXISTS (SELECT 1 FROM approved_users WHERE email = auth.jwt()->>'email')
  );

-- 삽입·갱신은 service_role만 (동기화 스크립트용)
DROP POLICY IF EXISTS "service_write_pvs" ON paper_vs_sales_monthly;
CREATE POLICY "service_write_pvs" ON paper_vs_sales_monthly
  FOR ALL USING (auth.role() = 'service_role')
  WITH CHECK (auth.role() = 'service_role');
