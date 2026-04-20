-- 용지 카테고리별 월별 매입 단가 추이 (Top 4 + 기타)
-- 2026-04-20: 기존 paper_vs_sales_monthly는 전체 가중평균 한 줄이라
-- 종류별 가격 추이를 보여주지 못해 새 테이블을 분리.
-- 카테고리는 PRT_ITEM.NM_ITEM을 기반으로 sync_paper_vs_sales.py의 classify_paper()가 산출.

CREATE TABLE IF NOT EXISTS paper_category_monthly (
  ym            text NOT NULL,             -- 'YYYY-MM'
  category      text NOT NULL,             -- '백상지' / '특수지' / 'SW(스노우)' / '아트지' / '기타'
  paper_qty     numeric,                   -- 해당 월 카테고리의 매입 수량 합
  paper_amount  numeric,                   -- 공급가액 합
  paper_um_avg  numeric,                   -- 가중평균 단가 (amount / qty)
  updated_at    timestamptz DEFAULT now(),
  PRIMARY KEY (ym, category)
);

CREATE INDEX IF NOT EXISTS idx_paper_cat_ym ON paper_category_monthly(ym);

ALTER TABLE paper_category_monthly ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "approved_users_read_paper_cat" ON paper_category_monthly;
CREATE POLICY "approved_users_read_paper_cat" ON paper_category_monthly
  FOR SELECT USING (
    auth.uid() IS NOT NULL
    AND EXISTS (SELECT 1 FROM approved_users WHERE email = auth.jwt()->>'email')
  );
