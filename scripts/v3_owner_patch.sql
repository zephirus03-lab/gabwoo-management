-- v3 (2026-04-15): CD_CUST_OWN 기반 3사 분리
-- 결정적 발견: ERP SAL_SALESH.CD_CUST_OWN 값으로 3사 구분 가능
--   '10000' → 갑우문화사   (2025 14,025M, PDF 15,144M 대비 92.6%)
--   '20000' → 비피앤피     (2025 22,968M, PDF 21,807M 대비 94.9%)
--   '30000' → 더원프린팅   (2025    663M, PDF  2,026M 대비 32.7% ← 제본대 미등록 건)
--
-- 기존 erp_sales.company 필드는 CD_FIRM(='7000')만 봐서 전부 "갑우문화사"로 잘못
-- 채워져 있었음. sync 스크립트를 고쳐 CD_CUST_OWN으로 재매핑.

ALTER TABLE erp_sales
  ADD COLUMN IF NOT EXISTS owner_code text;  -- CD_CUST_OWN 원본 (10000/20000/30000)

CREATE INDEX IF NOT EXISTS idx_sales_owner ON erp_sales(owner_code);

COMMENT ON COLUMN erp_sales.owner_code IS
  'ERP SAL_SALESH.CD_CUST_OWN. 10000=갑우문화사, 20000=비피앤피, 30000=더원프린팅';
COMMENT ON COLUMN erp_sales.company IS
  'CD_CUST_OWN 매핑 결과. company 필터는 이 값 기준 (갑우문화사/비피앤피/더원프린팅)';
