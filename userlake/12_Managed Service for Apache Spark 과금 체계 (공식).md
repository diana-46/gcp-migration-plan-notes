---
title: "Managed Service for Apache Spark (Dataproc) 과금 체계 — 공식 정리"
status: reference
created: 2026-06-29
대상: Dataproc / Managed Service for Apache Spark 의 GCP 공식 가격 체계
용도: 비용 시나리오 계산의 근거 reference
출처: https://cloud.google.com/products/managed-service-for-apache-spark/pricing
부모: [[1_userlake-worker 인프라 이관]]
---


# Managed Service for Apache Spark (Dataproc) 과금 체계 — 공식 정리

> **공식 출처**: [https://cloud.google.com/products/managed-service-for-apache-spark/pricing](https://cloud.google.com/products/managed-service-for-apache-spark/pricing) (2026-06-30 기준)
>
> **단가 기준**: **asia-northeast3 (Seoul)** — 실제 사용 region. VM / DCU / Shuffle storage 는 Seoul 가격 실측 (Iowa 대비 ~~+28~~30%). Cluster management fee ($0.010/vCPU/h) 는 region 무관 여부 미검증.
>
> **머신 시리즈 제약**: 사용자 환경에서 **C3 / N4 / C4** 만 선택 가능 (E2, N2 등 미지원)
>
> **사용처**: [[3_Spark Connect on Dataproc Serverless 비용 계산]] 의 시나리오 계산 근거

## 0. 큰 그림 — 두 가지 배포 모드


| 배포 모드          | 과금 모델                                                | 한 줄                         |
| -------------- | ---------------------------------------------------- | --------------------------- |
| **Serverless** | 사용한 자원만 (per-second), DCU 단위                         | scale-to-zero 가능, idle 비용 0 |
| **Clusters**   | 클러스터 uptime 동안 management fee + 별도 Compute Engine 비용 | always-on, VM CUD 적용 가능     |


> 둘 다 **Lightning Engine** add-on 사용 가능 (선택, 추가 비용).

추가로 **두 모드 모두** 다음 자원은 **별도 과금**:

- Google Cloud Storage (read/write)
- BigQuery (storage / query)
- Network egress

---

## 1. Serverless 과금 상세

### 1-1. DCU (Data Compute Unit) 단가

**Standard vs Premium 두 tier** (asia-northeast3 Seoul 실측, 730h/월 기준):


| Type               | Default ($/월/DCU)        | + BigQuery CUD 1년     | + BigQuery CUD 3년     |
| ------------------ | ------------------------ | --------------------- | --------------------- |
| **DCU (standard)** | **$56.19** ($0.076976/h) | $50.57 ($0.0692784/h) | $44.95 ($0.0615808/h) |
| **DCU (premium)**  | **$83.35** ($0.114181/h) | $75.02 ($0.1027629/h) | $66.68 ($0.0913448/h) |


> Iowa (us-central1) 대비 서울 **+28.3%** — VM 뿐 아니라 DCU 도 region-dependent.

### DCU 환산 룰 (공식 Spark properties 문서 기반)

> 출처: [Dataproc Spark Properties](https://docs.cloud.google.com/managed-spark/docs/concepts/spark-properties-serverless)

**DCU = Spark cores (vCPU) 합산** (메모리 영향 없음, 단 메모리 허용 범위 안에 있을 때):


| 속성                         | 유효한 값        | 비고      |
| -------------------------- | ------------ | ------- |
| `spark.driver.cores`       | **4, 8, 16** | 다른 값 불가 |
| `spark.executor.cores`     | **4, 8, 16** | 다른 값 불가 |
| `spark.executor.instances` | 2 ~ 2000     |         |


**메모리는 cores 의 함수** (memory + memoryOverhead 합):


| tier        | 코어당 메모리 허용 범위                 |
| ----------- | ----------------------------- |
| Standard    | **1024m ~ 7424m** (1~7.25 GB) |
| **Premium** | ~24576m (~24 GB)              |


> 예: `spark.executor.cores = 8` → standard 면 `memory + overhead` 가 8~58G 사이.

**비용은 cores 만의 함수**:

- 1 DCU = 1 vCPU
- 메모리는 허용 범위 내에서 자유롭게 잡아도 DCU/비용 변화 없음
- 단 cores 늘리면 DCU/비용 비례 증가

### 공식 예시 (page 2/3 검증)

```
12 DCUs for 24 hours
spark.driver.cores=4 + spark.executor.cores=4 × instances=2 = 12 cores
→ 12 DCU (1:1 매핑 확인)
```

→ **메모리 옵션 어떻게 잡든 cores 만의 함수**. 14G executor 든 28G executor 든 cores 8 이면 동일 DCU.

### Tier 자동 설정 룰


| 워크로드                    | dataproc.tier  | engine              | compute.tier |
| ----------------------- | -------------- | ------------------- | ------------ |
| Batch (Standard)        | standard       | default             | standard     |
| Batch (Premium)         | premium        | **lightningEngine** | premium      |
| **Interactive Session** | **premium 강제** | default (옵션)        | premium      |


→ **Interactive Session 은 Premium tier 자동** (Seoul $0.114181/h DCU 단가 적용)

→ Lightning Engine 은 Interactive 에서도 default OFF (선택 활성화)

#### ⚠ 중요: Interactive workloads = Premium 강제

> *"Interactive workloads are charged at premium."* (공식 인용)

→ Spark Connect 사용 (외부 client 에서 `sc://` 접속) = Interactive = **Seoul Premium $0.114181/h 적용**.

→ Batch 모드 (spark-submit 류) 만 Standard $0.076976/h 선택 가능.

#### 과금 단위

- 초 단위 (per-second)
- 1분 최소 청구
- 시간 단위 / 월 단위 토글 표시 가능

### 1-2. Shuffle Storage

**asia-northeast3 (Seoul) 실측**:


| Type         | $/월/GiB    | $/h/GiB      | 최소 청구 |
| ------------ | ---------- | ------------ | ----- |
| **Standard** | **$0.052** | $0.000071223 | 1분    |
| **Premium**  | **$0.13**  | $0.000178082 | 5분    |


> Iowa 대비 Seoul **+30%**.

> Premium shuffle storage 는 **Premium DCU 와만 사용 가능**.

#### ⚠ 핵심 — "셔플이 일어났는가" 와 무관

**할당된 디스크 공간 × 사용 시간** = 비용. 실제 셔플 read/write 데이터 사이즈 아님.

공식 페이지 3 예시 (계산식 검증):

```
"Each node consumes ... 400 GB shuffle storage"
"shuffleStorageGbSeconds: '72000'"
                = 400 GB × 3 VMs × 60 sec = 72,000 GB-seconds
```

→ 400 GB 가 잡 실행 60초 동안 **할당된 양** 그대로 청구.

#### 설정 속성 — `spark.dataproc.{driver,executor}.disk.size`


| 항목                                  | Default         | 최소      |
| ----------------------------------- | --------------- | ------- |
| `spark.dataproc.driver.disk.size`   | **코어당 100 GiB** | 250 GiB |
| `spark.dataproc.executor.disk.size` | **코어당 100 GiB** | 250 GiB |


→ **default 매우 큼**. cores 8 짜리 executor 1개 = **800 GiB shuffle 할당**.

#### 우리 케이스 시뮬레이션 (Premium tier, Interactive)

```
Default (코어당 100 GiB):
  Driver:    8 cores × 100 = 800 GiB
  Executor:  8 cores × 100 × 8 instances = 6,400 GiB
  Total:                                    7,200 GiB
  비용: 7,200 × $0.10 = $720/월

최소 (250 GiB):
  Driver:    250 GiB
  Executor:  250 × 8 = 2,000 GiB
  Total:                2,250 GiB
  비용: 2,250 × $0.10 = $225/월

→ 셔플 할당 줄이면 $495/월 절감
```

#### Cluster 모드는 별도 빌링 없음

> Serverless 의 shuffle storage 빌링은 **Serverless 만의 항목**.
>
> **Cluster (GKE / Dataproc Cluster) 는 Persistent Disk (PD) 가 셔플 + 임시 데이터 역할** — 별도 빌링 없음. 50 GiB PD 사이즈 그대로.

→ GKE 직접 / Dataproc Cluster 시나리오 ($964 /$1,490) 에는 shuffle 별도 비용 없음. PD $18/월 만.

#### 우리 워크로드의 실제 셔플

- **Gate stage**: union/intersect/except → 셔플 약간 발생 (hash 재분배)
- **Sync stage**: split + write → 적당
- **CSV write `coalesce(1)`**: 1 partition 으로 모음 → 마지막 단계 셔플
- 대용량 셔플 워크로드 아님 → **default 100 GiB/core 는 과다**

→ Serverless 갈 경우 250 GiB (최소) 로 줄이는 게 합리적.

### 1-3. Accelerator (GPU) — asia-northeast3 (Seoul) 실측


| Type             | $/월 (730h)    | $/h       |
| ---------------- | ------------- | --------- |
| NVIDIA L4        | **$630.02**   | $0.863044 |
| NVIDIA A100 40GB | **$2,716.97** | $3.721872 |
| NVIDIA A100 80GB | **$4,418.93** | $6.053328 |


→ 5분 최소 청구. 우리 워크로드 GPU 미사용.

### 1-4. Serverless 가격 계산 — 공식 문서 예시

**공식 문서 예시** (Iowa, 12 DCU × 24h × 25 GB shuffle):

```
Batch (Std):  12 × 24 × $0.060 + 25 × ($0.040/30) = $17.31 (1일)
Interactive:  12 × 24 × $0.089 + 25 × ($0.040/30) = $25.66 (1일)
```

→ Interactive 가 Standard 보다 **48% 비쌈** (비율은 region 무관).

> **우리 워크로드 기반 실제 비용 계산** (Baseline vs Downsize, Serverless 포함 전체 시나리오): → [[14_Spark Connect 다운사이즈 비용 &amp; 노드 구성 (Seoul 실측)]]

### 1-5. Workload 사용량 확인

`gcloud dataproc batches describe BATCH_ID` 로 `UsageMetrics` 확인 가능:

```yaml
runtimeInfo:
  approximateUsage:
    milliDcuSeconds: '720000'
    shuffleStorageGbSeconds: '72000'
    milliAcceleratorSeconds: '120000'
```

→ 1000 milli = 1 unit. 위 예시: 4 DCUs × 3 VMs × 60 sec × 1000 = 720,000 milliDcuSeconds.

---

## 2. Cluster 과금 상세

### 2-1. Management Fee

> *"$0.010 * # of vCPUs * hourly duration"* (공식 인용)


| 항목                 | $/월/vCPU (730h) | $/h/vCPU |
| ------------------ | --------------- | -------- |
| **Management fee** | **$7.30**       | $0.010   |


> 예: vCPU 72개 cluster = 72 × $7.30 = **$525.60/월** management fee

#### 적용 대상

- **모든 vCPU**: master + primary worker + secondary (Spot) worker 합산
- 초 단위 청구, 1분 최소

#### CUD / SUD 적용 안 됨

> Management fee 는 **CUD / SUD 할인 적용되지 않음** (Compute Engine VM 만 적용).

### 2-2. Underlying Compute Engine VM (별도 청구)

> *"Clusters pricing is in addition to the Compute Engine per-instance price for each virtual machine"*

VM 비용은 별도 (Dataproc 가 아닌 Compute Engine 가격):

- 1분 최소 청구
- 초 단위 billing
- **SUD (Sustained Use Discount)** 적용 (한 달 상주 시 자동 ~30%)
- **CUD (Committed Use Discount)** 적용 (1년 / 3년 약정 시 추가)

### 2-3. Persistent Disk (별도)

- Standard PD: ~$0.04/GB/월
- 클러스터의 master / worker 노드 부트 디스크

### 2-4. Lightning Engine Add-on (선택)

새로 도입된 가속 엔진 (vectorized execution, 최대 4.9x 빠름):


| 기간             | $/월/vCPU (730h) | $/h/vCPU |
| -------------- | --------------- | -------- |
| ~2026/5/31     | $0 (무료)         | $0.0000  |
| **2026/6/1 ~** | **$1.825**      | $0.0025  |


→ 기본 OFF. 사용 시 명시적 설정 필요.

### 2-5. Cluster 가격 계산 — 공식 문서 예시

**공식 문서 예시** (Persistent cluster, us-central1):

- 1 master + 4 worker = 5 노드, 총 20 vCPUs
- 730시간 (= 1개월)

```
Management fee = 20 vCPU × $7.30 / 월        = $146 / 월
+ Compute Engine VM 비용 (별도 청구, SUD 적용)
+ Persistent Disk (별도)
+ (선택) Lightning Engine = 20 vCPU × $1.825 = $36.50 / 월
```

> **우리 워크로드 기반 실제 비용 계산** (Baseline vs Downsize, Cluster/GKE 시나리오): → [[14_Spark Connect 다운사이즈 비용 &amp; 노드 구성 (Seoul 실측)]]

### 2-6. Clusters 추가 과금 시나리오


| 시나리오                      | 과금                                    |
| ------------------------- | ------------------------------------- |
| **Scaling / autoscaling** | VM 추가 시 active 동안 charge              |
| **Cluster 가 error state** | VM 활성 유지 → charge 계속 (cluster 삭제 시까지) |


→ Error state 방치 시 누적 비용. cleanup 필수.

---

## 3. Clusters on GKE — 특수 경우

> *"The Managed clusters on GKE pricing formula, $0.010 * # of vCPUs * hourly duration, is the same as clusters on Compute Engine pricing formula"*


| 항목             | 단가                               |
| -------------- | -------------------------------- |
| Management fee | **$0.010 / vCPU / h** (GCE 와 동일) |


→ **Dataproc on GKE 도 Dataproc management fee 부과**.

### ⚠ 헷갈리기 쉬운 구분


| 옵션                        | Dataproc fee    | 설명                                        |
| ------------------------- | --------------- | ----------------------------------------- |
| **Dataproc on GCE**       | ✅ $0.010/vCPU/h | Dataproc 이 GCE cluster 관리                 |
| **Dataproc on GKE**       | ✅ $0.010/vCPU/h | Dataproc 이 user 의 GKE 위에 spark cluster 띄움 |
| **GKE 직접 (Dataproc 안 씀)** | ❌ **$0**        | user 가 GKE 에 spark StatefulSet 직접 운영      |


→ "GKE = 저렴" 의 진짜 의미는 **마지막 옵션** (Dataproc 안 쓰는 GKE 직접 운영).

### Clusters on GKE 의 vCPU 계산

- Dataproc 가 생성한 node pool 의 VM 인스턴스의 vCPU 합산
- 1분 최소 청구
- Node pool 은 cluster 삭제 후에도 유지됨 (공유 가능). **node pool 직접 삭제 / scale-down 안 하면 charge 지속**.

---

## 4. 할인 옵션

### 4-1. Sustained Use Discount (SUD) — VM 만

- **자동 적용**
- 한 달 중 사용 비율 따라 0~30% 할인
- Compute Engine VM 에만 적용
- Dataproc management fee, DCU 비용에는 **적용 안 됨**

### 4-2. Committed Use Discount (CUD) — 두 가지 종류

⚠ **CUD 는 종류가 2개고, 각각 적용 대상이 다름**. 헷갈리기 쉬움.

#### A. Compute Engine CUD (resource-based)


| 약정  | 할인      |
| --- | ------- |
| 1년  | ~25-30% |
| 3년  | ~50-55% |


- **적용 대상**: Compute Engine VM (vCPU + memory 약정량)
- **우리 케이스**: GKE / Dataproc Cluster 의 **underlying VM 에 적용** ← 우리가 살 약정

#### B. BigQuery CUD (slot-based)


| 약정  | 할인 (DCU 기준)                                                            |
| --- | ---------------------------------------------------------------------- |
| 1년  | Seoul Std $0.076976 →$0.0692784 (~10%) / Premium $0.114181 →$0.1027629 |
| 3년  | Seoul Std $0.076976 →$0.0615808 (~20%) / Premium $0.114181 →$0.0913448 |


- **적용 대상**: BigQuery slot reservation + **Dataproc Serverless DCU**
- **우리 케이스**: Cluster 가니까 DCU 안 씀 → **무관**
- 다만 athlon 의 BigQuery 데이터 마이그레이션 ([[4_BigQuery 이관 (Presto 쿼리 엔진 전환)]]) 에서는 BigQuery 팀이 별도 약정 결정

> **CUD 두 종류는 별개 약정**. Compute Engine CUD 산다고 BigQuery 자동 할인되지 않고, 그 반대도 마찬가지.

#### 우리 케이스에 의미 있는 것

```
우리 시나리오 (Cluster 또는 GKE 직접):
  Compute Engine VM
    └─ Compute Engine CUD 1년/3년 ← 우리가 살 약정
        └─ ~25-55% 할인

  Dataproc management fee (Cluster 시)
    └─ CUD 적용 안 됨 (flat rate)

  BigQuery (별도 이관 후, BigQuery 팀 책임)
    └─ BigQuery CUD ← 별도 의사결정, 우리 Spark Connect 비용과 무관
```

→ **우리 Spark Connect 이관 시 살 약정 = Compute Engine CUD**.

→ BigQuery CUD 는 BigQuery 이관 시 별도 검토 (Spark Connect 비용 결정과 무관).

#### 정리: Dataproc management fee 는 CUD 적용 안 됨

→ 어떤 CUD 도 management fee 에는 영향 없음.

### 4-3. Spot VM

- Compute Engine VM 의 Spot 단가 사용 가능 (~~60~~91% 할인)
- Dataproc cluster 에서 secondary worker 로 활용 가능
- 회수 (preemption) 가능 — Spark fault tolerance 로 흡수

---

## 5. CUD / SUD 적용 매트릭스


| 비용 항목                       | SUD    | **Compute Engine CUD** | **BigQuery CUD**    | Spot      |
| --------------------------- | ------ | ---------------------- | ------------------- | --------- |
| **Compute Engine VM**       | ✅ ~30% | ✅ 1년 ~25% / 3년 ~55%    | ❌                   | ✅ ~60-91% |
| **Persistent Disk**         | ❌      | ❌                      | ❌                   | ❌         |
| **Dataproc Management Fee** | ❌      | ❌                      | ❌                   | ❌         |
| **DCU (Serverless)**        | ❌      | ❌                      | ✅ 1년 ~10% / 3년 ~20% | ❌         |
| **Shuffle Storage**         | ❌      | ❌                      | ❌                   | ❌         |
| **Lightning Engine fee**    | ❌      | ❌                      | ❌                   | ❌         |


### 핵심 룰


| 자원                                              | 살 CUD                     |
| ----------------------------------------------- | ------------------------- |
| **VM**                                          | Compute Engine CUD        |
| **DCU (Serverless)**                            | BigQuery CUD              |
| **management fee / shuffle / Lightning Engine** | 살 수 있는 CUD 없음 (flat rate) |


→ 두 CUD 는 **별도 약정**. 한 쪽이 다른 쪽에 영향 없음.

→ 우리 케이스 (Cluster / GKE) = **Compute Engine CUD 만 의미 있음**. BigQuery CUD 무관.

---

## 6. 별도 과금 (모든 모드 공통)

다음은 Dataproc 가격과 **별도 청구**:


| 자원                       | 비고                         |
| ------------------------ | -------------------------- |
| **Google Cloud Storage** | read/write, storage        |
| **BigQuery**             | query, storage             |
| **Network egress**       | inter-region / internet    |
| **Cloud Monitoring**     | metrics ingest, 무료 tier 있음 |
| **Bigtable**             | (사용 시)                     |


→ userlake-worker 의 GCS 사용 + BigQuery 쿼리도 위 항목으로 청구.

---

## 7. 한 줄 요약 — 우리 케이스에 적용

> Serverless **Interactive = Seoul Premium $0.114181/h** ← 기본 가정 (Iowa $0.06/Standard) 으로 잡으면 크게 과소 추정.
> **DCU = cores 만의 함수** (메모리 허용 범위 내라면 무관). 1 DCU = 1 vCPU.
> **DCU / Shuffle 도 region-dependent** — Seoul 은 Iowa 대비 +28~30%.
> **Shuffle = 할당된 공간 × 시간** (실제 셔플 양 무관). default 코어당 100 GiB 는 매우 큼.
> Cluster management fee **$0.010/vCPU/h** ← 어떤 CUD 도 적용 안 됨, VM 사이즈 줄이는 게 핵심 (region 무관 여부 미검증).
>
> **Cluster 는 shuffle 별도 빌링 없음** — PD 가 대체.
>
> **Dataproc on GKE 도 management fee 부과**. "GKE 저렴" = Dataproc 안 쓰는 직접 운영.
>
> **우리가 살 CUD = Compute Engine CUD** (VM 약정). BigQuery CUD 는 BigQuery 팀의 별도 의사결정으로 무관.

→ 상세 시나리오 계산: [[3_Spark Connect on Dataproc Serverless 비용 계산]]

---

## 8. 우리 케이스 핵심 단가 (참조 표) — asia-northeast3 (Seoul), 월 단위 (730h)

### Dataproc 매니지드 fee — 월 단위


| 항목                            | Default                             | + BigQuery CUD 1년 | + BigQuery CUD 3년 |
| ----------------------------- | ----------------------------------- | ----------------- | ----------------- |
| DCU standard                  | $56.19/월/DCU                        | $50.57            | $44.95            |
| **DCU premium (Interactive)** | **$83.35/월/DCU**                    | $75.02            | $66.68            |
| **Cluster management fee**    | **$7.30/월/vCPU** ⚠ region 무관 여부 미검증 | (할인 없음)           | (할인 없음)           |
| Lightning Engine              | $1.825/월/vCPU ⚠ region 무관 여부 미검증    | (할인 없음)           | (할인 없음)           |
| Standard shuffle storage      | $0.052/월/GiB                        |                   |                   |
| Premium shuffle storage       | $0.13/월/GiB                         |                   |                   |


> DCU 의 CUD 는 **BigQuery CUD** (BigQuery 팀이 사는 약정). 우리 Cluster/GKE 시나리오와 무관.

### VM 단가 (Compute Engine, 별도 청구) — asia-northeast3 (Seoul) 실측, 월 단위 (730h)

> 사용자 환경 제약: **C3 / N4 / C4** 만 선택 가능. e2/n2 등 예시 대상 아님.
>
> Seoul 공식 pricing 페이지 (2026-07 기준) 실측 반영.

#### N4 Standard (범용, 4:1 memory ratio)


| 인스턴스               | vCPU / RAM    | Default     | Flex CUD 1Y | Flex CUD 3Y | Res CUD 1Y | Res CUD 3Y  |
| ------------------ | ------------- | ----------- | ----------- | ----------- | ---------- | ----------- |
| n4-standard-2      | 2c / 8G       | $85.03      | $61.22      | $45.92      | $53.36     | $38.26      |
| n4-standard-4      | 4c / 16G      | $170.06     | $122.44     | $91.83      | $106.72    | $76.53      |
| **n4-standard-8**  | **8c / 32G**  | **$340.11** | $244.88     | $183.66     | $213.44    | **$153.05** |
| **n4-standard-16** | **16c / 64G** | **$680.23** | $489.76     | $367.32     | $426.87    | **$306.11** |
| n4-standard-32     | 32c / 128G    | $1,360.45   | $979.52     | $734.64     | $853.75    | $612.22     |


#### N4 High-memory (8:1 memory ratio) — memory-heavy 워크로드용


| 인스턴스              | vCPU / RAM     | Default     | Flex CUD 1Y | Flex CUD 3Y | Res CUD 1Y | Res CUD 3Y  |
| ----------------- | -------------- | ----------- | ----------- | ----------- | ---------- | ----------- |
| n4-highmem-2      | 2c / 16G       | $111.58     | $80.34      | $60.25      | $70.29     | $50.21      |
| n4-highmem-4      | 4c / 32G       | $223.15     | $160.67     | $120.50     | $140.58    | $100.42     |
| **n4-highmem-8**  | **8c / 64G**   | **$446.31** | $321.34     | $241.01     | $281.16    | **$200.84** |
| **n4-highmem-16** | **16c / 128G** | **$892.62** | $642.68     | $482.01     | $562.33    | **$401.69** |
| n4-highmem-32     | 32c / 256G     | $1,785.24   | $1,285.37   | $964.03     | $1,124.66  | $803.37     |


> **highmem = standard 대비 ~+31% 가격** 이지만 **memory 는 2배**. memory-bound 워크로드에서는 훨씬 유리.

### Persistent Disk


| Type                | 월 단가       |
| ------------------- | ---------- |
| Standard PD         | $0.04/GB/월 |
| SSD PD              | $0.17/GB/월 |
| 50GB boot disk × 노드 | $2/월/노드    |


### Accelerator (GPU)


| Type             | $/월 (730h) |
| ---------------- | ---------- |
| NVIDIA L4        | $490.59    |
| NVIDIA A100 40GB | $2,570.10  |
| NVIDIA A100 80GB | $3,441.10  |


> **asia-northeast3 (Seoul) 실측 가격** (2026-07 GCP pricing 페이지 기준).
>
> Iowa (us-central1) 대비 서울 VM **~+28%** (당초 추정 +15% 보다 큼).
>
> Dataproc management fee / DCU 단가 / Lightning Engine 은 **region 무관 동일**.
>
> 한 달 = 730시간 (GCP 표준).
>
> **SUD 는 N4 pricing 표에 명시 없음** — CUD 만 표시됨. 실제 청구 시 SUD 자동 적용 여부는 별도 확인 필요.

---

## 9. 참고 / 다음

- **공식 가격 페이지**: [https://cloud.google.com/products/managed-service-for-apache-spark/pricing](https://cloud.google.com/products/managed-service-for-apache-spark/pricing)
- **공식 Spark properties (DCU / 메모리 룰)**: [https://docs.cloud.google.com/managed-spark/docs/concepts/spark-properties-serverless](https://docs.cloud.google.com/managed-spark/docs/concepts/spark-properties-serverless)
- VM 단가: [https://cloud.google.com/compute/all-pricing](https://cloud.google.com/compute/all-pricing)
- Pricing Calculator: [https://cloud.google.com/products/calculator](https://cloud.google.com/products/calculator)
- 우리 시나리오 계산: [[3_Spark Connect on Dataproc Serverless 비용 계산]]
- 사용량 데이터: [[11_사용량 분석 (한달 데이터 기반)]]

