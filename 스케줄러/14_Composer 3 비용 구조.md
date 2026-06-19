---
title: "Cloud Composer 3 비용 구조 (DCU 중심)"
status: draft
tags:
  - airflow
  - 스케줄러
  - composer3
  - cost
  - dcu
  - pricing
created: 2026-06-19
updated: 2026-06-19
---

# Cloud Composer 3 비용 구조 (DCU 중심)

> Cloud Composer 3 의 **청구 단위 4축** + DCU 의 정체 + Composer 2 와의 차이 + 절감 옵션의 제약. 비용 견적의 reference 노트.
>
> 실제 산정 사례는 [[7_1_실제 스펙 산정]], 일반론 비교는 [[7_Composer 비용]] 참조.

## 한 줄 요약

> Composer 3 는 compute 를 **DCU (Data Compute Unit)** 라는 추상 단위로 묶어 청구. vCPU 와 RAM 을 따로 받지 않고 둘을 합산한 단위로 한 번에. 청구는 **DCU + DB 스토리지 + 환경 스토리지 + 네트워크 egress** 4축.

## 1. 청구 4축 (전체 그림)

| 축                              | 무엇을 잡나                                                                                        | 단가 (참고)                                                                  | 0까지 내려가나?                                          |
| ------------------------------ | --------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------ | -------------------------------------------------- |
| **🌟 DCU (Data Compute Unit)** | Airflow workload 컴포넌트 (worker / scheduler / DAG processor / triggerer / web server) 의 compute | us-central1: $0.06/DCU-hour<br>asia-northeast3 (서울 추정): ~$0.072/DCU-hour | ❌ scheduler/triggerer/web/DAG processor 는 항상 floor |
| **Database storage**           | Airflow metadata DB (Cloud SQL) 디스크                                                           | GB-month (Cloud SQL 표준 단가)                                               | ❌ 최소 10 GiB                                        |
| **Environment storage**        | GCS bucket (DAGs, logs, plugins, data)                                                        | GCS Standard 표준 단가                                                       | ✅ 사용량 비례                                           |
| **Network egress**             | 외부 통신                                                                                         | 표준 GCP egress                                                            | ✅ 사용량 비례                                           |

→ **DCU 가 비용의 대부분**. 나머지 3축은 부수 청구.

## 2. DCU (Data Compute Unit) — Composer 3 의 핵심 단위

### 2.1 정의 (공식)

> *"Data Compute Unit (DCU) is an abstract metering unit that represents computational resources allocated by a Cloud Composer environment at a point in time."*

핵심 포인트:
- **추상 단위**: 어떤 자원이 정확히 1 DCU 인지 공식 정의 X — "compute capacity" 의 묶음
- **시간당 청구**: 실제 측정은 **milliDCU-hour** (1 DCU = 1000 milliDCU)
- **할당량 기준**: 환경에 **할당된 자원** 만큼 청구 (실제 사용량이 아님). worker autoscale 로 줄어드는 만큼만 절감.

### 2.2 환산식 (커뮤니티 리버스 엔지니어링)

공식 문서엔 vCPU / RAM → DCU 환산식이 명시 안 되어 있음. [Google Developer 포럼의 리버스 엔지니어링](https://discuss.google.dev/t/cloud-composer-3-pricing-explanation/158381) 결과:

> **1 DCU ≈ 1 vCPU-hour 또는 1 GB memory-hour**
>
> 환경의 시간당 DCU 소비량 ≈ **(vCPU 수) + (메모리 GB 수)**

### 2.3 검증 사례 (Composer 3 Small preset)

| 항목 | 값 |
|---|---|
| 컴포넌트 할당 | 3 vCPU + 9 GB RAM (scheduler / DAG processor / triggerer / web server / worker 합산) |
| 측정값 | **12 DCU/hour** |
| 환산 검증 | 3 + 9 = 12 ✅ |

→ `DCU/h = vCPU + GB_RAM` 공식이 실제 청구와 일치.

### 2.4 비용 감 잡기 — Small preset 기준

12 DCU/hour 환경의 floor:

| 리전 | DCU 단가 | 시간당 | 일당 | 월(730h) |
|---|---|---|---|---|
| us-central1 | $0.06 | $0.72 | $17.28 | ~$526 |
| asia-northeast3 (추정) | $0.072 | $0.864 | $20.74 | ~$631 |

→ **DAG 한 줄 안 돌려도 Small 환경이 월 $500~600 floor**. 메타DB / 스토리지 더하면 ~$700~800.

## 3. Composer 2 와 결정적 차이

### 3.1 청구 단위

| 측면 | Composer 2 | Composer 3 |
|---|---|---|
| Compute 청구 | **vCPU-hour 따로 + Memory GB-hour 따로** | **DCU-hour 로 묶음** |
| 단위 가시성 | 명확 (vCPU 1대 X 시간 = ...) | 추상 (DCU 가 뭔지 reverse engineering 필요) |
| Preset / 환경 크기 | small/medium/large 식 | small/medium/large + workload 개별 조정 |
| Spot / Preemptible | 일부 가능 (사용자 GKE 노드 풀) | ❌ 불가 (compute 가 DCU 추상화 안) |

### 3.2 변화의 의미

- **장점**: 사용자가 vCPU 와 RAM 비율을 신경 쓸 필요 없음. 환경 사양 조정만으로 자동 산정
- **단점**: 비용 산정 시 직관성 떨어짐. 환산식이 비공식 (DCU 가 뭔지 사용자에게 안 가르쳐줌)
- **부수효과**: Spot / Preemptible 적용 불가능해짐 (GCE/GKE 옵션과의 비용 격차 확대)

## 4. 환경 크기 vs 워크로드 사양

Composer 3 환경은 **두 영역**으로 사양 결정 — 둘이 별개로 청구된다:

```
[Composer 3 환경]
  │
  ├─ 워크로드 구성 (개별 조정 = DCU 청구)
  │   ├─ Scheduler        (vCPU / 메모리 / 스토리지 / count)
  │   ├─ DAG processor    (vCPU / 메모리 / 스토리지)
  │   ├─ Triggerer        (vCPU / 메모리)
  │   ├─ Web server       (vCPU / 메모리 / 스토리지)
  │   └─ Worker           (vCPU / 메모리 / 스토리지 / min~max autoscale)
  │
  └─ 환경 크기 (= 메타DB 사양만 결정)
      └─ Small / Medium / Large
         (Cloud SQL vCPU / RAM / 디스크)
```

### 4.1 환경 크기별 (참고)

| 환경 크기 | 메타DB | 적합 규모 | 메타DB 부분 월 비용 (참고) |
|---|---|---|---|
| Small | 1 vCPU / 3.75GB | DAG ~50 / 동시 task ~10 | ~$300~400/월 |
| Medium | 2 vCPU / 7.5GB | DAG ~200 / 동시 task ~50 | ~$600~800/월 |
| Large | 4 vCPU / 15GB | DAG ~1000 / 동시 task ~200 | ~$1,500+/월 |

### 4.2 변경 영향

- **워크로드 사양 변경**: 거의 무중단 (분 단위)
- **환경 크기 변경**: **1~2시간 다운타임** 발생. 신중히 결정

## 5. Floor 비용 메커니즘 — DAG 0개여도 발생하는 비용

### 5.1 항상 떠있는 컴포넌트

| 컴포넌트 | DCU 소비 | 0까지 내려가나? |
|---|---|---|
| Worker (Celery) | ✅ | ✅ autoscale 로 min 까지 |
| Scheduler | ✅ | ❌ 항상 1개 이상 |
| DAG processor | ✅ | ❌ 항상 1개 이상 |
| Triggerer | ✅ | ❌ 항상 1개 이상 |
| Web server | ✅ | ❌ 항상 1개 이상 |
| Cloud SQL (메타DB) | 별도 청구 | ❌ 24/7 |
| Memorystore (Redis) | DCU 안에 흡수 | ❌ 24/7 |

→ **DAG 가 없어도 위 5개 컴포넌트 + 메타DB + Redis 의 floor 비용** 이 매월 발생.

### 5.2 Floor 계산 워크 (예시)

만약 다음 사양으로 환경을 띄우면:

```
Scheduler:     2 vCPU + 8 GB   = 10 DCU/h
DAG processor: 2 vCPU + 8 GB   = 10 DCU/h
Triggerer:     2 vCPU + 4 GB   = 6  DCU/h
Web server:    1 vCPU + 4 GB   = 5  DCU/h
Worker (min):  2 vCPU + 8 GB   = 10 DCU/h
────────────────────────────────────────
합:                              41 DCU/h
```

월 floor: 41 × 730h × $0.072 = **~$2,156/월** (서울 추정)
+ Cloud SQL 메타DB (small ~$300, medium ~$600)
+ GCS 스토리지 + egress (~$10~30)

→ **DAG 가 0이어도 월 ~$2,500~3,000 (₩360~430만) 발생**.

## 6. 절감 옵션과 제약

### 6.1 가능한 절감

| 옵션 | 효과 | 적용 방법 |
|---|---|---|
| **Worker autoscale min ↓** | floor 자체 감소 | min=1 까지 가능. 단 cold start 영향 |
| **Worker 사양 다운사이즈** | DCU/h 직접 감소 | task 당 메모리 필요량 검증 후 |
| **Deferrable sensor → Triggerer** | Worker 점유 시간 ↓ → autoscale 더 잘 작동 | [[7_2_리소스 다이어트 포인트]] |
| **메타DB cleanup** | Database storage 청구 ↓ + 환경 크기 다운 가능 | [[5_Metadata DB 운영]] |
| **DAG 분리 (DAG processor)** | parsing 부하 분산 → DAG processor 사양 ↓ | DAG 수 ↑ 시 |
| **GCS lifecycle (로그 archive)** | Environment storage ↓ | Nearline / Coldline 전환 |
| **dev 환경 야간 종료** | dev 환경의 DCU 절감 | scheduler 로 환경 stop / start |

### 6.2 적용 불가 / 제한적

| 옵션 | 상태 | 이유 |
|---|---|---|
| **Spot / Preemptible** | ❌ 불가 | compute 가 DCU 추상화 안. 사용자가 노드 선택 불가 |
| **CUD (Committed Use Discount)** | △ 제한적 | 일부 SKU 만 적용 가능. 정확한 범위 공식 확인 필요 |
| **자체 GKE 노드 풀** | ❌ 불가 | tenant project 의 GKE 라 사용자 불가능 |
| **K8s autoscaler 직접 튜닝** | ❌ 불가 | Composer 가 추상화 |

→ Spot/CUD 가 불가능한 게 GCE/GKE 옵션 대비 **30~70% 비용 격차의 원천** ([[7_1_실제 스펙 산정]] 참조).

## 7. 견적 산출 워크플로

우리 환경을 Composer 3 견적으로 환산할 때:

### Step 1: 워크로드 합산 DCU/h 계산

```
DCU/h = Σ (각 컴포넌트의 vCPU + GB RAM)
      = Σ (worker × vCPU + worker × GB)
      + scheduler vCPU + scheduler GB
      + DAG processor vCPU + DAG processor GB
      + triggerer vCPU + triggerer GB
      + web server vCPU + web server GB
```

### Step 2: 월 DCU 비용

```
월 DCU 비용 = DCU/h × 730 × $0.072 (서울 추정 단가)
```

### Step 3: 나머지 3축 더하기

```
+ Database storage = (메타DB GB) × Cloud SQL 표준 단가
+ Environment storage = (DAG + logs + plugins GB) × GCS Standard 단가
+ Network egress = (월 egress GB) × egress 단가
```

### Step 4: 워커 autoscale 변동 반영

- Worker 가 min~max 사이에서 변동하면 평균 vCPU/RAM 으로 계산
- 보수적 견적은 worker max 기준
- 실제 청구는 worker autoscale 평균 기준

→ 우리 산정 사례는 [[7_1_실제 스펙 산정]] 참조.

## 8. 청구 모니터링 / 검증

운영 중 실제 청구액과 견적을 비교하려면:

| 확인 위치 | 무엇 |
|---|---|
| Cloud Console → Billing | 일/월별 청구액 트렌드 |
| Cloud Console → Billing → 보고서 | SKU 별 분해 (DCU / SQL / GCS / Network) |
| Composer Console → Monitoring | DCU 실측 / Worker count / Triggerer state |
| `gcloud composer environments describe` | 환경 현재 사양 |

→ 견적 대비 실청구액 변동 큰 항목 추적 → autoscale 평균 / sensor 동작 등 조정 단서.

## 9. 미확정 / 확인 필요

- [ ] **서울 리전 정확한 DCU-hour 단가** ([cloud.google.com/composer/pricing](https://cloud.google.com/composer/pricing) 직접 확인)
- [ ] **DCU 환산식 (`vCPU + GB_RAM`)** 이 모든 환경에서 정확히 맞는지 (커뮤니티 리버스, 공식 확인 X)
- [ ] **CUD 적용 가능한 SKU 범위** (Google 영업 / 문서 확인)
- [ ] **메타DB 환경 크기별 정확한 단가** (small/medium/large 의 Cloud SQL SKU)
- [ ] **워크로드 0 idle 시 milliDCU 단위 청구 동작** — 정확히 0 까지 가는지, floor 가 있는지

## 관련 문서

- [[7_Composer 비용]] — 일반론 (Composer 3 vs Self-managed 큰 그림)
- [[7_1_실제 스펙 산정]] — 우리 환경의 실제 견적 (옵션 A/B 매트릭스)
- [[1_개요#6. 비용]] — 사내 사용량 기반 비용 비교
- [[13_Composer 3 환경 업그레이드 정책]] — 업그레이드 시 DB 20GB 제한 관련

## 공식 출처

- [Managed Service for Apache Airflow pricing](https://cloud.google.com/composer/pricing) (공식 단가표)
- [Cloud Composer 3 pricing explanation — Google Developer 포럼](https://discuss.google.dev/t/cloud-composer-3-pricing-explanation/158381) (DCU 리버스 엔지니어링)
- [Cloud Composer 3: Truly "serverless"? (Medium)](https://medium.com/@shuvro_25220/cloud-composer-3-truly-serverless-5af001bc7930) (DCU 분석 사례)
