-- v2 스키마 패치 (2026-04-15)
-- PDF(세금계산서 실 발행) vs ERP 검증 결과 반영:
--   1) ST_SALES='Y' 필터로는 PDF의 60%만 커버 → 필터 완화
--   2) ERP CD_BIZ 컬럼으로 사업부 구분 가능 (GABWOO/PKG/'')
-- 이 패치는 erp_sales에 business_unit 컬럼을 추가합니다.

ALTER TABLE erp_sales
  ADD COLUMN IF NOT EXISTS business_unit text;  -- CD_BIZ: 'GABWOO' | 'PKG' | NULL(비피앤피 계열)

CREATE INDEX IF NOT EXISTS idx_sales_business_unit ON erp_sales(business_unit);

COMMENT ON COLUMN erp_sales.business_unit IS
  'ERP SAL_SALESH.CD_BIZ. GABWOO=주력 출판/인쇄, PKG=패키지, NULL(빈값)=비피앤피 계열 추정';
