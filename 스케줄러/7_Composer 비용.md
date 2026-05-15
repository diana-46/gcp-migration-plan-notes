---
title: "Composer 비용 (Composer 3 / Self-managed)"
status: draft
tags:
  - airflow
  - 스케줄러
  - composer
  - cost
created: 2026-05-14
updated: 2026-05-15
---

# Composer 비용 (Composer 3 / Self-managed)

> Cloud Composer 3 vs Self-managed (GKE)의 **GCP 인프라 월 청구액** 비교. 서울 리전(asia-northeast3) 기준. 운영 인력 비용은 본 문서에서 제외 — 순수하게 GCP에 얼마 내는지에 집중.
>
> 정확한 수치는 [GCP Pricing Calculator](https://cloud.google.com/products/calculator) 로 최종 확인 필요.
>
> **Note**: Composer 2는 본 비교에서 제외 (Composer 3로의 마이그레이션이 권장되는 시점이고, 신규 도입 대상에서 빠짐).

## 결론 먼저 (한 줄)

> 같은 워크로드 기준, **Composer 3가 Self-managed 대비 ~20~35% 비쌈** (on-demand). Spot/CUD까지 적극 적용 시 격차는 **40~60%까지 확대**.
>
> 핵심 이유: Composer 3는 compute SKU에 **관리 마크업(~30~40%)** 이 붙고, **Spot 노드 / CUD 적용이 제한적**.

## 둘 다 결국 GCP 리소스를 쓴다 — 그럼 뭐가 다른가

Self-managed든 Composer든 결국 **GKE / GCE + Cloud SQL + Memorystore + GCS** 를 쓰는 건 똑같다. 차이는 **누가 청구서를 받느냐** 와 **얼마의 마크업이 붙느냐**.

| 항목 | Self-managed | Composer 3 | 청구 차이 |
|---|---|---|---|
| **Compute (vCPU)** | GCE 단가 직접 결제 (~$0.05/vCPU-hr 수준) | Composer SKU (~$0.074/vCPU-hr 수준) | **+30~50% 마크업** |
| **Memory** | GCE 단가 (instance에 포함) | Composer SKU (~$0.008/GB-hr) | **+15~20% 마크업** |
| **GKE 클러스터 fee** | $73/mo (Standard) | Composer가 흡수 (별도 청구 없음) | 비등 |
| **Cloud SQL (Metadata DB)** | 직접 프로비저닝 / 결제 | Composer가 관리하지만 동일 단가 청구 | **거의 같음** |
| **Memorystore Redis (Celery)** | 직접 프로비저닝 / 결제 | Composer가 관리하지만 동일 단가 청구 | **거의 같음** |
| **GCS (DAG/log)** | 직접 결제 | 동일 단가 | 거의 같음 |
| **Load Balancer / 네트워크** | 직접 결제 | Composer가 일부 흡수 | 거의 같음 |
| **Spot/Preemptible 노드** | ✅ 사용 가능 (compute -60~70%) | ❌ Composer 관리 컴포넌트에는 불가 | **Self-managed 유리** |
| **CUD (1년/3년 약정)** | ✅ compute -25%/-55% | △ 일부 SKU만 제한적 적용 | **Self-managed 유리** |

→ **결국 차이는 "compute 부분에 붙는 관리 마크업" + "할인 적용 가능 범위"**. DB/Redis/Storage는 어차피 똑같이 결제.

## Composer 3 — 사용량 기반 + 최소 floor

Composer 3는 컴포넌트별 vCPU·메모리·시간 단위 과금이지만 **0까지 내려가지는 않음**:

| 컴포넌트 | 과금 단위 | 0까지 내려가나? |
|---|---|---|
| **Worker (Celery)** | vCPU-hour, memory-hour | ✅ 최소치까지 (보통 1대) |
| **Scheduler** | vCPU-hour, memory-hour | ❌ 항상 떠있음 |
| **Web Server** | vCPU-hour, memory-hour | ❌ 항상 떠있음 |
| **Triggerer** | vCPU-hour | ❌ 항상 떠있음 |
| **Cloud SQL (Metadata DB)** | 표준 Cloud SQL 가격 | ❌ 24/7 |
| **Memorystore (Redis)** | 표준 Memorystore 가격 | ❌ 24/7 |

→ DAG 0개여도 **최소 ~$200~300/월은 무조건** 나감 (scheduler + DB + Redis floor). Worker만 부하에 따라 변동.

## Self-managed (GKE) — 구성 요소별 직접 결제

서울 리전, on-demand 가격 기준:

| 항목 | 사양 예시 | 월 비용 (대략) |
|---|---|---|
| **GKE Standard 클러스터 관리** | 1 cluster | ~$73/mo |
| **GKE 노드 (workload, on-demand)** | n2-standard-4 × 3대 (24/7) | ~$420~480/mo |
| **GKE 노드 (heavy task용 spot)** | n2-standard-8 spot × 평균 1대 | ~$80~150/mo |
| **Cloud SQL PostgreSQL (HA)** | db-custom-2-7680 (2vCPU/7.5GB), HA | ~$200~300/mo |
| **Cloud SQL 스토리지** | 100GB SSD | ~$17/mo |
| **Memorystore Redis (Celery)** | Basic Tier 1GB | ~$30~50/mo |
| **GCS (DAG / log)** | 수십 GB | ~$5/mo |
| **Cloud Logging / Monitoring** | 표준 quota 내 | ~$0~50/mo |
| **Load Balancer** | 1 LB (web UI 노출 시) | ~$20/mo |
| **Artifact Registry** | 이미지 저장 | ~$5/mo |
| **합계 (on-demand)** | | **~$850~1,100/mo** |

**할인 적용 시**:
- Spot 워커 노드 비중 ↑ → compute 비용 -50~70%
- CUD 1년 약정 → compute 비용 -25%
- 둘 다 적용 시 **~$500~700/mo** 까지 절감 가능

## 시나리오별 GCP 청구액 비교

> 한 달 기준, 서울 리전, USD. **인프라(GCP 청구액)만** — 운영 인력 비용은 제외.

### Small 규모 (DAG ~50, task ~10k/day)

| 모델 | 인프라 월 비용 |
|---|---|
| **Composer 3** | **~$300~500** |
| Self-managed (on-demand) | ~$400~550 |
| Self-managed (Spot 워커 + CUD) | ~$250~350 |

→ 차이 작음 (~$100). on-demand끼리는 거의 비등. Spot 활용 시 Self-managed가 살짝 유리.

### Medium 규모 (DAG ~200, task ~100k/day)

| 모델 | 인프라 월 비용 |
|---|---|
| **Composer 3** | **~$900~1,200** |
| Self-managed (on-demand) | ~$750~950 |
| Self-managed (Spot 워커 + CUD) | ~$500~700 |

→ Composer가 **~$200~300/월 비쌈** (관리 마크업). Spot+CUD 적용 시 **~$400~500/월 격차**.

### Large 규모 (DAG 500+, task 1M/day)

| 모델 | 인프라 월 비용 |
|---|---|
| **Composer 3** | **~$2,000~2,800** |
| Self-managed (on-demand) | ~$1,800~2,400 |
| Self-managed (Spot 워커 + CUD) | ~$1,200~1,700 |

→ on-demand끼리는 비등. **Spot+CUD 적극 활용 시 Self-managed가 ~$600~1,000/월 저렴**.

## 비용을 키우는 요인 (공통)

- **잘못된 Pod 스펙**: Pool/Quota 없이 무한 Pod → autoscaler가 노드 폭증
- **장기 실행 sensor**: Celery 워커가 점유 → worker 추가 → 비용 증가 (deferrable sensor 사용 추천)
- **DAG parse 빈도 / 빈 DAG 다수**: scheduler CPU 증가
- **로그 보존**: Cloud Logging 표준 quota 초과 시 GB당 과금
- **네트워크 egress**: 외부 API 호출 많으면 빠르게 증가
- **이미지 pull 실패 / 재시도**: Artifact Registry 트래픽

## 비용을 줄이는 방법

| 방법 | 효과 | Composer 3 | Self-managed |
|---|---|---|---|
| **Hybrid 실행** (Celery + KubernetesExecutor) | 짧은 task는 Celery로 → Pod 안 만듦 ([[4_Queue 라우팅과 Pod 스펙 설정]]) | ✅ | ✅ |
| **Deferrable Sensor / Trigger** | sensor 워커 점유 줄임 | ✅ | ✅ |
| **Spot/Preemptible 노드 풀** | 노드 비용 60~70% 절감 가능 (재시도 가능한 task만) | ❌ (관리 컴포넌트 불가) | ✅ |
| **CUD (Committed Use Discount)** | compute 1년 -25%, 3년 -55% | △ (일부 SKU만) | ✅ |
| **로그 보존 기간 조정** | 30일 → 14일로 줄이면 즉시 절감 | ✅ | ✅ |
| **scheduler/web server 최소 사양으로** | idle 시 floor 비용 ↓ | ✅ | ✅ |
| **DB 사이즈 적정화** ([[5_Metadata DB 운영]]) | 정기 cleanup + 적정 사양 | △ (Composer가 일부 통제) | ✅ |
| **Budget alert** | 예산 초과 알람 (필수) | ✅ | ✅ |

→ **할인 적용 가능 범위에서 Self-managed가 구조적으로 유리**.

## 의사결정에 주는 함의 (GCP 비용만 기준)

| 우리 규모 | 추천 (GCP 청구액만 봤을 때) |
|---|---|
| Small | 비등. Composer 3가 살짝 비싸지만 격차 작음 ($100/월 수준) |
| Medium | Self-managed가 **~$200~500/월 저렴** (Spot/CUD 적용 시) |
| Large | Self-managed가 **~$500~1,000/월 저렴** (Spot/CUD 적용 시) |

> ⚠️ 본 문서는 **GCP 청구액만** 비교. 운영 인력 / 안정성 / 마이그레이션 비용 등은 [[2_Cloud Composer vs Self-managed 비교]] 참조.

## PoC / 검증 추가 항목

- [ ] 현재 운영 DAG 수, task/day, 평균 실행 시간 통계
- [ ] Composer 3 PoC 환경 1개월 실측 — scheduler/worker 사양별 vCPU-hour 누적 측정
- [ ] Self-managed PoC: GKE 클러스터 견적 (n2 vs n2d, on-demand vs spot 혼합)
- [ ] Spot 노드 적용 시 task 재시도율 측정 (Spot preemption 영향)
- [ ] CUD 약정 가능한 compute 사양 미리 산정
- [ ] BigQuery 쿼리 비용은 별도 (Userlake에서 발생 — [[../애슬론/1_개요]])

## 미확정 / 확인 필요

- 서울 리전 정확한 Composer 3 vCPU-hour / memory-hour SKU 단가 (분기별 갱신될 수 있음)
- Composer 3 실제 청구 사례 (1개월 실측 권장)
- Composer 3 환경에서 CUD 적용 가능한 SKU 범위 (Google 공식 문서 / 영업 확인 필요)
- Composer 3 도입 시 마이그레이션 비용 (기존 환경과 dual-running 기간 동안 중복 청구)

## 관련 문서

- [[1_개요]]
- [[2_Cloud Composer vs Self-managed 비교]]
- [[3_Executor 종류 및 비교]]
- [[4_Queue 라우팅과 Pod 스펙 설정]]
- [[5_Metadata DB 운영]]
- [[6_Airflow 2 vs 3 비교]]
