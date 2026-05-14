---
title: "Composer 비용 (Composer 2 / 3 / Self-managed)"
status: draft
tags:
  - airflow
  - 스케줄러
  - composer
  - cost
created: 2026-05-14
updated: 2026-05-14
---

# Composer 비용 (Composer 2 / 3 / Self-managed)

> Cloud Composer 2 vs Composer 3 vs Self-managed (GKE)의 **월 비용** 비교. 서울 리전(asia-northeast3) 기준. 정확한 수치는 [GCP Pricing Calculator](https://cloud.google.com/products/calculator) 로 최종 확인 필요.

## 비용 구조의 차이 (한눈에)

| 모델 | 과금 방식 | 고정 비용 | 변동 비용 |
|---|---|---|---|
| **Composer 2** | 환경 패키지(Small/Medium/Large) + 워커 | 환경 패키지 (기본 인프라 포함) | 추가 워커, 스토리지, 네트워크 |
| **Composer 3** | 컴포넌트 단위 (Scheduler vCPU/mem, Worker, Web server, DB) | 없음 (실제 사용량만) | 모든 컴포넌트가 vCPU/mem 단위 |
| **Self-managed** | GCP 리소스 합산 | 거의 없음 | GKE 노드 + Cloud SQL + Memorystore + 네트워크 |

> **핵심 차이**: Composer 2는 "환경 패키지"라는 묶음 SKU. Composer 3는 컴포넌트별 vCPU/mem 단위로 더 세밀 — **트래픽 적으면 Composer 3이 더 쌀 수 있음**, 많으면 비슷하거나 더 비쌀 수도. Self-managed는 가장 유연하지만 운영 인력 비용이 별도.

## Cloud Composer 2 — 환경 패키지 (서울 리전, 대략)

> 환경 패키지는 GKE 노드, scheduler, web server, Cloud SQL, Memorystore 등이 **번들**된 SKU. 정확한 가격은 GCP 공식 페이지 확인.

| 패키지 | 대략적 월 비용 (USD) | 포함 사양 (대략) |
|---|---|---|
| **Small** | ~$350~500 | scheduler 0.5vCPU, worker 2개, DB 작은 사양 |
| **Medium** | ~$700~900 | scheduler 1vCPU, worker 3개, DB 중간 |
| **Large** | ~$1,500~2,500 | scheduler 2vCPU+, worker 6개+, DB 큼 |

여기에 추가로:
- **추가 워커** (vCPU/mem/시간 단위)
- **GCS storage** (DAG bundle, log) — 수 GB 수준이면 무시 가능
- **Cloud Logging / Monitoring** — 표준 quota 넘으면 과금
- **네트워크 egress** — 리전 외부로 나갈 때

> 위 수치는 2025-2026 시점의 시장 추정. **서울 리전은 us-central1 대비 5~15% 비쌈**. 공식 가격 변경 가능성 있어 **확정 시 견적 다시**.

## Cloud Composer 3 — 컴포넌트 단위 과금

Composer 3는 환경 패키지가 사라지고 **컴포넌트별로 vCPU·메모리·시간** 단위 과금:

| 컴포넌트 | 과금 단위 |
|---|---|
| Scheduler | vCPU-hour, memory-hour |
| Worker (Celery) | vCPU-hour, memory-hour (autoscale 됨) |
| Web Server | vCPU-hour, memory-hour |
| Triggerer (deferrable 용) | vCPU-hour |
| Cloud SQL (Metadata DB) | 표준 Cloud SQL 가격 |
| Memorystore (Celery용 Redis) | 표준 Memorystore 가격 |

### 영향

- **소규모 (DAG 수십, task 수천/일)**: Composer 2 Small보다 더 저렴할 가능성 (idle 시 줄어듦)
- **중대형**: Composer 2 Medium~Large 와 비슷한 레인지로 수렴 예상
- **트래픽 스파이크**: autoscale 되지만 vCPU-hour 누적되므로 **예측이 더 어려움** — 예산 가드(Budget alert) 필수

> Composer 3 실제 청구액 사례가 아직 적어서 **PoC 환경 1개월 띄워보고 실측**하는 것을 강력 추천.

## Self-managed (GKE) — 구성 요소별

서울 리전, on-demand 가격 기준:

| 항목 | 사양 예시 | 월 비용 (대략) |
|---|---|---|
| **GKE Standard 클러스터 관리** | 1 cluster | ~$73/mo (cluster fee) |
| **GKE 노드 (workload)** | n2-standard-4 × 3대 (24/7) | ~$420~480/mo |
| **GKE 노드 (heavy task용 spot)** | n2-standard-8 spot × 평균 1대 | ~$80~150/mo |
| **Cloud SQL PostgreSQL (HA)** | db-custom-2-7680 (2vCPU/7.5GB), HA | ~$200~300/mo |
| **Cloud SQL 스토리지** | 100GB SSD | ~$17/mo |
| **Memorystore Redis (Celery)** | Basic Tier 1GB | ~$30~50/mo |
| **GCS (DAG / log)** | 수십 GB | ~$5/mo |
| **Cloud Logging / Monitoring** | 표준 quota 내 | ~$0~50/mo |
| **Load Balancer** | 1 LB (web UI 노출 시) | ~$20/mo |
| **Container Registry / Artifact Registry** | 이미지 저장 | ~$5/mo |
| **합계 (인프라만)** | | **~$850~1,100/mo** |

여기에 **숨은 비용**:
- **운영 인력** (가장 큼): 업그레이드, 패치, 트러블슈팅, on-call 등 **0.2~0.5 FTE** 추정. 인건비 환산 시 인프라 비용 상회 가능
- **네트워크 egress**: GCP 외부로 나가는 트래픽
- **백업 / DR**: Cloud SQL 자동 백업은 포함이지만 추가 보존 / cross-region 시 별도

## 시나리오별 비교 (대략)

> 한 달 기준, 서울 리전, USD. **추정치이며 PoC 실측으로 최종 확정 필요**.

### Small 규모 (DAG ~50개, task ~10k/day)

| 모델 | 인프라 월 비용 | 운영 인력 | 합계 (인건비 제외) |
|---|---|---|---|
| Composer 2 Small | ~$400 | 거의 0 | **~$400** |
| Composer 3 (autoscale 잘 활용) | ~$300~500 | 거의 0 | **~$300~500** |
| Self-managed | ~$850~1,100 | 0.2 FTE | **~$850~1,100** + 인건비 |

→ **Small 규모는 Composer가 압도적 유리**. Self-managed는 운영 인력 비용 포함하면 2~3배.

### Medium 규모 (DAG ~200개, task ~100k/day)

| 모델 | 인프라 월 비용 | 운영 인력 | 합계 |
|---|---|---|---|
| Composer 2 Medium | ~$800 | 거의 0 | **~$800** |
| Composer 3 | ~$700~1,000 | 거의 0 | **~$700~1,000** |
| Self-managed | ~$1,200~1,500 | 0.3 FTE | **~$1,200~1,500** + 인건비 |

→ **Composer가 여전히 유리하지만 격차 줄어듦**. Self-managed는 워커 자유도 가치가 있어야 정당화.

### Large 규모 (DAG 500+, task 1M/day)

| 모델 | 인프라 월 비용 | 운영 인력 | 합계 |
|---|---|---|---|
| Composer 2 Large | ~$2,000~2,500 | ~0.1 FTE (tuning) | **~$2,000~2,500** |
| Composer 3 | ~$1,800~2,500 | ~0.1 FTE | **~$1,800~2,500** |
| Self-managed | ~$2,000~3,000 | 0.5 FTE | **~$2,000~3,000** + 인건비 |

→ **격차 미미**. Self-managed가 worker queue 분리, 커스텀 패키지 자유, multi-tenancy 등으로 가치 줄 수 있음.

## 비용을 키우는 요인

- **잘못된 Pod 스펙**: Pool/Quota 없이 무한 Pod → autoscaler가 노드 폭증
- **장기 실행 sensor**: Celery 워커가 점유 → worker 추가 → 비용 증가 (deferrable sensor 사용 추천)
- **DAG parse 빈도 / 빈 DAG 다수**: scheduler CPU 증가
- **로그 보존**: Cloud Logging 표준 quota 초과 시 GB당 과금
- **네트워크 egress**: 외부 API 호출 많으면 빠르게 증가
- **이미지 pull 실패 / 재시도**: Container Registry 트래픽

## 비용을 줄이는 방법

| 방법 | 효과 |
|---|---|
| **Hybrid 실행** (Celery + KubernetesExecutor) | 짧은 task는 Celery로 → Pod 안 만듦 ([[4_Queue 라우팅과 Pod 스펙 설정]]) |
| **Deferrable Sensor / Trigger** | sensor 워커 점유 줄임 |
| **Spot/Preemptible 노드 풀** (Self-managed) | 노드 비용 60~70% 절감 가능 (재시도 가능한 task만) |
| **로그 보존 기간 조정** | 30일 → 14일로 줄이면 즉시 절감 |
| **Composer 3 autoscale**: scheduler/web server 최소 사양으로 | idle 시 비용 ↓ |
| **DB 사이즈 적정화** ([[5_Metadata DB 운영]]) | 정기 cleanup + 적정 사양 |
| **Budget alert** | 예산 초과 알람 (필수) |

## 의사결정에 주는 함의

| 우리 규모 | 추천 |
|---|---|
| Small | Composer (운영 인력 비용 포함하면 압도적) |
| Medium | Composer 우세. Self-managed는 worker queue 분리 등 강한 이유 필요 |
| Large | 비등. 운영 자유도(자유 패키지, multi-tenancy, queue 분리) 가치로 결정 |

→ **현재 우리 운영 규모를 측정한 뒤** 어느 구간인지 매핑하는 것이 PoC 1순위 ([[1_개요]]).

## PoC / 검증 추가 항목

- [ ] 현재 운영 DAG 수, task/day, 평균 실행 시간 통계
- [ ] 위 통계로 환산한 Composer 2 Small/Medium 어느 쪽 fit
- [ ] Composer 3 PoC 환경 1개월 실측 (테스트 DAG로)
- [ ] Self-managed PoC: GKE 클러스터 견적 (n2 vs n2d vs spot 혼합)
- [ ] BigQuery 쿼리 비용은 별도 (Userlake에서 발생 — [[../애슬론/1_개요]])

## 미확정 / 확인 필요

- 서울 리전 정확한 Composer 패키지 가격 (분기별 갱신될 수 있음)
- Composer 3 실제 청구 사례 (1개월 실측 권장)
- Sustained Use Discount / Committed Use Discount 적용 시 절감폭 (Self-managed 1년 약정 시 ~25%, 3년 ~55%)
- Cloud Composer 1 → 2 / 2 → 3 마이그레이션 시 dual-running 비용

## 관련 문서

- [[1_개요]]
- [[2_Cloud Composer vs Self-managed 비교]]
- [[3_Executor 종류 및 비교]]
- [[4_Queue 라우팅과 Pod 스펙 설정]]
- [[5_Metadata DB 운영]]
- [[6_Airflow 2 vs 3 비교]]
