---
title: "Composer 비용 한눈에"
status: draft
tags:
  - airflow
  - 스케줄러
  - composer
  - cost
  - summary
created: 2026-06-29
updated: 2026-06-29
---

# Composer 비용 한눈에

> [[7_Composer 비용]] 핵심만 10줄로 압축. 서울 리전 / GCP 청구액 기준 (운영 인력 비용 제외).

1. **결론**: 같은 워크로드 기준 **Composer 3가 Self-managed 대비 on-demand로 ~20~35% 비쌈**, Spot/CUD 적극 적용 시 격차는 **40~60%까지 확대**.
2. **이유**: Composer 3는 compute SKU에 **관리 마크업 ~30~40%** 가 붙고, **관리 컴포넌트에는 Spot 노드 불가 + CUD 적용도 제한적**.
3. **공통 영역**: DB(Cloud SQL) / Redis(Memorystore) / GCS / 네트워크는 두 모델 모두 **같은 단가**로 결제됨 — 차이는 결국 **compute에만** 집중.
4. **Composer 3 floor**: DAG 0개여도 scheduler / web / triggerer + Cloud SQL + Redis가 24/7 떠 있어 **최소 ~$200~300/월** 무조건 청구.
5. **Self-managed 견적 (Medium 기준)**: on-demand ~$850~1,100/월 → **Spot 워커 + CUD 1년 적용 시 ~$500~700/월** 까지 절감 가능.
6. **규모별 격차**: Small ~$100/월(비등) → Medium **~$200~500/월** → Large **~$500~1,000/월**, 규모가 클수록 Self-managed가 유리.
7. **절감 레버**: Hybrid 실행(Celery + KubernetesExecutor) · Deferrable Sensor · Spot 노드 · CUD · 로그 보존 단축 · DB 정기 cleanup · Budget alert (필수).
8. **비용 폭증 요인**: Pool/Quota 없는 Pod 무한 증식 · 장기 실행 sensor의 워커 점유 · 빈 DAG 다수로 인한 scheduler CPU 증가 · 외부 API egress.
9. **본 비교의 한계**: GCP 청구액만. **운영 인력 / 장애 시 안정성 / 마이그레이션 dual-running 중복 청구**는 [[2_Cloud Composer vs Self-managed 비교]] 에서 별도 판단.
10. **PoC 필수 항목**: 서울 리전 SKU 단가 최신 확인 · Composer 3 1개월 실측 · Spot 노드 task 재시도율 측정 · CUD 가능 SKU 범위 영업 확인.

## 관련 문서

- [[7_Composer 비용]] — 상세 비교표 / 시나리오별 견적
- [[14_Composer 3 비용 구조]] — Composer 3 컴포넌트별 과금 구조
- [[7_1_실제 스펙 산정]] / [[7_2_리소스 다이어트 포인트]]
- [[2_Cloud Composer vs Self-managed 비교]] — 인프라 외 종합 비교
