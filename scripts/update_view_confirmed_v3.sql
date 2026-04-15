-- v3 (2026-04-15): erp_sales_confirmed 뷰를 PDF 대조 기준으로 완화
--
-- 기존: billing_policy에 따라 일반 거래처는 ST='Y' 필수 → PDF의 60%만 커버
-- 변경: supply_amount<>0만 체크 → PDF의 97% 커버
--
-- 근거: 교원구몬 5.1B 등 ST='Y' 승인 워크플로우를 안 쓰는 거래처가 대량 누락됨.
-- 취소건(sales_status='N')은 PDF에도 마이너스 행이 있어서 SUM 계산 시 자연 차감됨.

DROP VIEW IF EXISTS erp_sales_confirmed;

CREATE VIEW erp_sales_confirmed
WITH (security_invoker = true)
AS
SELECT s.*
FROM erp_sales s
WHERE s.supply_amount <> 0;

COMMENT ON VIEW erp_sales_confirmed IS
  'PDF 세금계산서 대조 기준(96.7% 매칭). ST_SALES 무관, AM<>0만 포함. 취소(N)와 확정(Y/NULL) 모두 포함 — PDF와 동일 방식으로 마이너스건이 양수건을 자연 상쇄.';
