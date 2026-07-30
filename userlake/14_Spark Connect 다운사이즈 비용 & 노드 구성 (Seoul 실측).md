---
title: "Spark Connect 다운사이즈 비용 & 노드 구성 (Seoul 실측)"
status: decision
created: 2026-07-02
대상: 사내 Spark Connect StatefulSet (userlake-worker driver)
용도: GCP 이관 시 노드 스펙 확정 + 비용 비교 (현재 vs 다운사이즈)
부모: [[1_userlake-worker 인프라 이관]]
---

# Spark Connect 다운사이즈 비용 & 노드 구성 (Seoul 실측)

> **결론**: 다운사이즈 확정 (executor 3개 × **6 cores** / 35G, Spark request 26 vCPU). Master 는 **n4-standard-16** 로 driver 여유 확보. 배포 모드는 팀 결정 필요 (§ 10 결정 매트릭스 참고).
> - **GKE 직접**: **\$917/월** (Res CUD 3Y) — 최저 비용, 사내 패턴 재활용
> - **Dataproc on GKE**: **\$1,263/월** (Res CUD 3Y) — 매니지드 도구 자동
> - Dataproc on GCE: **\$1,045/월** — but master/worker 통일 강제 (사내 K8s 재활용 안 됨)
>
> **관련 문서**:
> - Spark 설정 다운사이즈 결정: [[13_Spark Connect 다운사이즈 결정 (실측 기반)]]
> - 배포 모드 선택 (Cluster vs GKE vs Serverless): [[3_Spark Connect on Dataproc Serverless 비용 계산]]
> - 단가 근거: [[12_Managed Service for Apache Spark 과금 체계 (공식)]]
> - 사용량 근거: [[11_사용량 분석 (한달 데이터 기반)]]

---

## 0. TL;DR

**현재 스펙**: 72 vCPU / 126 GB (executor 8개 × 8c × 14G + driver)
**다운사이즈 안**: **Spark request 26 vCPU / 131 GB pod** (executor 3개 × **6c** × 39G + driver 8c × 14G)

**노드 구성** (배포 모드별):
- **GKE 직접** (권장): **`n4-standard-16` (Master, 16c/64G)** + `n4-highmem-8 × 3` (Worker) — driver 여유 + K8s 스케줄 안전
- Dataproc on GKE: + control pool (n4-std-2) 추가 = 5 노드
- Dataproc on GCE: `n4-highmem-8 × 4` (master/worker 통일 강제)

**핵심 결정 3가지** (§ 13 근거 기반):
1. **executor 3개** (2개 대신) — pod memory 분산, node 여유 확보 (팀장님 지적)
2. **executor.memory 35G** (10G→35G) — data 집중 2.67배 대응
3. **executor.cores 6** (8→6) — peak 7 tasks vs 18 slots (여유 61%), K8s allocatable 스케줄 안전

**비용 비교 (asia-northeast3 Seoul 실측, 730h/월, CUD 3Y)**:

| 배포 | 다운사이즈 노드 구성 | Default | Res CUD 3Y |
|---|---|---|---|
| **GKE 직접** ⭐ | std-16 + hm-8 × 3 | \$2,027 | **\$917** |
| Dataproc on GKE | (+control pool) std-2 + std-16 + hm-8 × 3 | \$2,650 | \$1,263 |
| Dataproc on GCE | n4-hm-8 × 4 (통일 강제) | \$2,027 | \$1,045 |
| Serverless (참고, 부적합) | 26 DCU | \$2,208~\$2,545 | (BQ CUD 불확실) |
| **현재 (Baseline, 참고)** | n4-std-8 × 9 | \$3,605 (GCE) / \$3,079 (GKE) | \$1,921 (GCE) / \$1,395 (GKE) |

**연간 절감 (GKE 직접, Res CUD 3Y)**:
- 현재: \$1,395 × 12 = **\$16,740/년**
- 다운사이즈: \$917 × 12 = **\$11,004/년**
- **절감: \$5,736/년 (-34%)**

**GKE 직접 vs Dataproc on GKE 차액**: \$1,263 - \$917 = **\$346/월** (\$4,152/년)
- Dataproc management fee: \$306/월 (42 vCPU including control)
- Master 는 양쪽 std-16 동일 (K8s 스케줄 위해)
- 매니지드 도구 자동 (Spark UI/History/JMX) 값 = \$4,152/년

---

## 1. 전제 & 근거

### 계산 전제

| 항목 | 값 | 출처 |
|---|---|---|
| Region | **asia-northeast3 (Seoul)** | 실제 사용 region |
| 시간 | 730h/월 (24/7 상시) | Uptime 409일 실측 (§ 11) |
| Machine series | **C3 / N4 / C4 만 가능** | 사용자 환경 제약 |
| 다운사이즈 스펙 | Spark request 26 vCPU / 131 GB pod (executor 3개 × 6c) | [[13_Spark Connect 다운사이즈 결정 (실측 기반)]] |
| VM 단가 | Seoul N4 실측 | `~/Desktop/n4_price.png`, `n4_highmem.png` |
| DCU 단가 | Seoul 실측 | `~/Desktop/dcu_seoul_*.png` |
| Cluster fee | $0.010/vCPU/h (region 미검증) | [[12_Managed Service for Apache Spark 과금 체계 (공식)]] |

### 다운사이즈 대상 Spark 설정 (§ 13 참고)

```
현재                  → 다운사이즈
driver.cores 8       → 유지 (Full GC 다발 대응)
driver.memory 10G    → 유지 (향후 16G 상향 여지, Master std-16 로 여유 확보)
driver.memoryOverhead 4G → 유지
executor.instances 8 → 3 (static, 3대 분산으로 memory 여유 확보)
executor.cores 8     → 6 ⭐ (K8s allocatable 스케줄 안전, peak 여유 61%)
executor.memory 10G  → 35G  (data 집중 2.67배 대응)
executor.memoryOverhead 4G → 유지 (DP-2689 근거)
spark.sql.adaptive.enabled → true (신규)

총: 72 vCPU / 126 GB → Spark request 26 vCPU / 131 GB pod (-64% vCPU)
```

---

## 2. 노드 요구사항

### Pod Memory 계산 (팀장님 지적 반영)

**K8s Pod memory = `executor.memory + memoryOverhead`** (Spark on K8s 규칙):

| Pod | vCPU 요청 | Memory 요구 | 계산 |
|---|---|---|---|
| Driver | 8 | **14 GB** | 10G + 4G overhead |
| Executor × 3 | **6 each** | **39 GB each** | 35G + 4G overhead |

### 노드 요구사항

**⚠ K8s allocatable 고려** (GKE reservation + DaemonSet):
- 8 vCPU 노드: allocatable ~7.4 vCPU
- 16 vCPU 노드: allocatable ~15 vCPU
- Pod request 는 allocatable 이내여야 스케줄 가능

**Master 노드**:
- Driver pod: **8 vCPU** / 14 GB 요청
- 8 vCPU 노드: allocatable ~7.4 < **8 요구** → ❌ 스케줄 실패
- **16 vCPU 노드 필요**: allocatable ~15 > 8 → ✅
- **→ n4-standard-16** (16c/64G) 선택

**Worker 노드 (executor 호스팅)**:
- Executor pod: **6 vCPU** / 39 GB 요청
- 8 vCPU 노드: allocatable ~7.4 > 6 → ✅ 스케줄 OK
- Memory 64G 노드: allocatable ~57G > 39G → ✅
- **→ n4-highmem-8** (8c/64G) 선택
- **개수**: executor 3개 × 1 executor/node = **3 노드 필요**

---

## 3. 노드 후보 비교 (Seoul 실측)

### N4 시리즈 후보

**Master 노드 후보** (driver 8 vCPU / 14G 호스팅):

| 인스턴스 | vCPU / RAM | Default | Res CUD 3Y | K8s allocatable | Driver 8c 스케줄? |
|---|---|---|---|---|---|
| n4-standard-8 | 8c / 32G | \$340.11 | \$153.05 | ~7.4 vCPU | ❌ **부족** (7.4 < 8) |
| **n4-standard-16** ✅ | **16c / 64G** | **\$680.23** | **\$306.11** | ~15 vCPU | ✅ 여유 7 vCPU / memory 44G 여유 (향후 driver.memory 상향 가능) |

**Worker 노드 후보** (executor 6 vCPU / 39G 호스팅):

| 인스턴스 | vCPU / RAM | Default | Res CUD 3Y | K8s allocatable | Executor 6c 스케줄? |
|---|---|---|---|---|---|
| n4-standard-8 | 8c / 32G | \$340.11 | \$153.05 | ~7.4/27G | Memory 부족 (27G < 39G) ❌ |
| n4-standard-16 | 16c / 64G | \$680.23 | \$306.11 | ~15/58G | ✅ but overkill (executor cores 6 대비 15 여유) |
| **n4-highmem-8** ✅ | **8c / 64G** | **\$446.31** | **\$200.84** | ~7.4/57G | ✅ 여유 1.4c/18G |
| n4-highmem-16 | 16c / 128G | \$892.62 | \$401.69 | ~15/120G | ✅ but overkill (비용 2배) |

### K8s 시스템 overhead 여유 검증

**Master n4-standard-16 (64G/16c)**:

```
Total:          64G / 16 vCPU
K8s kube reserved: ~5.9G / ~100m
DaemonSet:      ~1G / ~500m
= Allocatable:  ~58G / ~15 vCPU
Driver pod:     14G / 8 vCPU
= 여유:         ~44G / ~7 vCPU  (매우 큼)
```

→ Driver.memory 향후 상향 여지 (10G → 16G) 확보. Full GC 대응.

**Worker n4-highmem-8 (64G/8c)**:

```
Total:          64G / 8 vCPU
K8s kube reserved: ~5.9G / ~100m
DaemonSet:      ~1G / ~500m
= Allocatable:  ~57G / ~7.4 vCPU
Executor pod:   39G / 6 vCPU
= 여유:         ~18G / ~1.4 vCPU  (~28%)
```

→ 여유 28% 확보. Peak activeTasks 7 도 18 slots (3 × 6) 로 처리 가능 (여유 61%).

### 왜 이 조합인가

**Master n4-standard-16 필수 이유**:
- Driver pod 8 vCPU 요청 → 8 vCPU 노드는 K8s allocatable (~7.4) 로 스케줄 실패
- 16 vCPU 노드로 스케줄 여유 확보
- 부가 이득: Driver Full GC 다발 대응 여지 (memory 향후 상향)

**Worker n4-highmem-8 최적 이유**:
- Executor cores 축소 (8→6) 로 pod request 6 vCPU → 8 vCPU 노드 fits
- Memory 64G 로 executor 39G pod + K8s overhead 여유
- highmem 8:1 ratio 로 memory-bound 워크로드 최적
- 3대 분산으로 SPOF 회피

---

## 4. 최종 노드 구성

**⚠ 배포 모드별 제약 상이**:

### 4-1. GKE 직접 (권장) — **Node pool 별 다른 spec** ✅

```
Node pool 1 (Master): n4-standard-16  (16c / 64G) × 1  → driver pod (8c/14G)
Node pool 2 (Worker): n4-highmem-8    (8c / 64G) × 3   → executor pod 각 1개 (6c/39G)
─────────────────────────────────────────────────────────────
Total: 4 노드, 40 vCPU 실제 하드웨어 / 224 GB
       Spark request: 26 vCPU / 131G
```

**여유**:
- Master: cores 7 여유, memory 44G 여유
- Worker: cores 1.4 여유, memory 18G 여유
- Peak 7 tasks vs 18 slots (여유 61%)

### 4-2. Dataproc on GKE — **3 pool 구조**

```
Node pool 1 (control):  n4-standard-2  (2c / 8G)  × 1  → Dataproc 관제
Node pool 2 (driver):   n4-standard-16 (16c / 64G) × 1 → driver pod
Node pool 3 (executor): n4-highmem-8   (8c / 64G) × 3  → executor pod × 3
─────────────────────────────────────────────────────────────
Total: 5 노드, 42 vCPU 실제 하드웨어 / 232 GB
```

### 4-3. Dataproc on GCE — **동일 spec 강제** (GCP Console UI 제약)

```
Master 1대 + Worker 3대: n4-highmem-8 (8c / 64G) × 4 (통일)
─────────────────────────────────────────────────────────────
Total: 4 노드, 32 vCPU / 256 GB
```

→ Console UI 에서 master/worker machine type 다르게 지정 불가.
→ Master 도 highmem 강제로 memory 낭비 (~30G 여분).

---

## 5. 비용 계산 상세 (Cluster on GCE, Seoul)

> ⚠ **Cluster on GCE 제약**: Master 와 Worker 는 **동일 machine type 강제** (GCP Console UI 제약).
> 다운사이즈 시 원래 필요치 (Master n4-std-8 + Worker hm-8) 대신 **전부 n4-hm-8 × 3** 사용.

### 5-1. 현재 스펙 (Baseline, 72 vCPU)

**노드 구성**: `n4-standard-8 × 9` (Master 1 + Worker 8, 각 executor 1개)

Baseline 은 master/worker 모두 n4-standard-8 이라 동일 spec 제약 문제 없음.

```
Management fee (72 vCPU × $7.30/월):        $525.60/월

Compute Engine VM:
  Default:    9 × $340.11             = $3,061.00/월
  Res CUD 1Y: 9 × $213.44             = $1,920.96/월
  Res CUD 3Y: 9 × $153.05             = $1,377.45/월

Persistent Disk (50GB × 9):
  9 × $2                              = $18.00/월

Lightning Engine (선택, 미사용 가정):
  72 × $1.825                         = $131.40/월

─────────────────────────────────────────
Total Default:      $525.60 + $3,061.00 + $18 = ~$3,605/월
Total Res CUD 1Y:   $525.60 + $1,920.96 + $18 = ~$2,465/월
Total Res CUD 3Y:   $525.60 + $1,377.45 + $18 = ~$1,921/월
```

### 5-2. 다운사이즈 (32 vCPU) — Cluster 동일 spec 강제

**노드 구성**: `n4-highmem-8 × 4` (Master + Worker 통일 4 노드, Console UI 제약)

```
Management fee (32 vCPU × $7.30/월):         $233.60/월

Compute Engine VM (n4-highmem-8 × 4, Seoul):
  Default:    4 × $446.31                = $1,785.24/월
  Res CUD 1Y: 4 × $281.16                = $1,124.64/월
  Res CUD 3Y: 4 × $200.84                = $803.36/월

Persistent Disk (50GB × 4):
  4 × $2                                = $8.00/월

Lightning Engine (선택, 미사용 가정):
  32 × $1.825                          = $58.40/월

─────────────────────────────────────────
Total Default:      $233.60 + $1,785.24 + $8 = ~$2,027/월
Total Res CUD 1Y:   $233.60 + $1,124.64 + $8 = ~$1,366/월
Total Res CUD 3Y:   $233.60 + $803.36 + $8   = ~$1,045/월
```

**참고**: Master 노드 n4-highmem-8 (64G) 이지만 driver pod 은 14G 만 사용 → **~30G memory 낭비**. Console UI 제약으로 불가피.

### 5-3. Cluster on GCE 요약 비교

| 시나리오 | Default | Res CUD 1Y | Res CUD 3Y |
|---|---|---|---|
| 현재 스펙 (72 vCPU, n4-std-8 × 9) | \$3,605 | \$2,465 | \$1,921 |
| **다운사이즈 (n4-hm-8 × 4, 통일 강제)** | **\$2,027** | **\$1,366** | **\$1,045** |
| **절감** | **-44%** | **-45%** | **-46%** |

> **주의**: Dataproc on GCE 는 executor.cores 6 이어도 노드는 8 vCPU 통일이라 노드 총 vCPU 32 → Dataproc fee \$234 그대로. Spark 관점 executor cores 축소 이득 미미.

---

## 6. GKE 직접 (Dataproc fee 없음) 비교

**차이**: GKE 는 Cluster management fee ($0.010/vCPU/h) 부과 안 됨. VM 가격은 동일.

### 6-1. 현재 스펙 GKE

```
GKE fee: $0 (zonal 1 무료)
VM Default:    $3,061.00
VM Res CUD 3Y: $1,377.45
PD:            $18

Total Default:    ~$3,079/월
Total Res CUD 3Y: ~$1,395/월
```

### 6-2. 다운사이즈 GKE ✅ (권장 후보)

**노드 구성**: `n4-standard-16 × 1` (Master) + `n4-highmem-8 × 3` (Worker)

```
GKE fee: \$0 (zonal 1 무료)

VM Default:
  Master  n4-standard-16:     \$680.23
  Worker  n4-highmem-8 × 3:   \$1,338.93
  합계:                       \$2,019.16/월

VM Res CUD 3Y:
  Master  n4-standard-16:     \$306.11
  Worker  n4-highmem-8 × 3:   \$602.52
  합계:                       \$908.63/월

PD (50GB × 4):
  4 × \$2                     = \$8.00/월

─────────────────────────────────────────
Total Default:    ~\$2,027/월
Total Res CUD 3Y: ~\$917/월
```

### 6-3. Dataproc on GKE 비용 (Res CUD 3Y)

**노드 구성**: 3 pool (control + driver + executor)

```
Control  n4-standard-2  × 1:  \$38.26/월
Driver   n4-standard-16 × 1:  \$306.11/월
Executor n4-highmem-8   × 3:  \$602.52/월
VM 합계:                       \$946.89/월

PD (50GB × 5):                 \$10.00/월
Dataproc fee (42 vCPU × \$7.30/월): \$306.60/월

─────────────────────────────────────────
Total Default:    (~\$1,764 + fee \$306.60 + PD = ~\$2,650)
Total Res CUD 3Y: ~\$1,263/월
```

### 6-4. 3-way 비용 요약 (다운사이즈 후)

| 배포 | 노드 구성 | Default | Res CUD 3Y |
|---|---|---|---|
| **GKE 직접** ⭐ | std-16 + hm-8 × 3 | \$2,027 | **\$917** |
| Dataproc on GKE | std-2 + std-16 + hm-8 × 3 | \$2,650 | \$1,263 |
| Dataproc on GCE | n4-hm-8 × 4 (통일) | \$2,027 | \$1,045 |

**차액 분석 (Res CUD 3Y)**:

| 비교 | 차액/월 | 차액/년 | 원인 |
|---|---|---|---|
| GKE 직접 vs Dataproc on GKE | +\$346 | +\$4,152 | Dataproc fee \$306 + control pool \$38 |
| GKE 직접 vs Dataproc on GCE | +\$128 | +\$1,536 | Dataproc fee \$234 - master upgrade offset |
| Dataproc on GCE vs Dataproc on GKE | +\$218 | +\$2,616 | Master 통일 낭비 vs Dataproc on GKE 의 control pool 추가 |

---

## 7. Serverless 비교 (참고 — 부적합)

**Interactive session 강제 Premium 조건** ($0.114181/h/DCU):

### 7-1. 현재 스펙 (72 DCU)

```
Compute (Premium, 24/7):
  72 × 730 × $0.114181                    = $6,001.75/월

Shuffle (default 7,200 GiB, Standard):
  7,200 × $0.052                          = $374.40/월

Total 최소 (min shuffle):                  ~$6,119/월
Total 최대 (default shuffle Premium):      ~$6,938/월
```

### 7-2. 다운사이즈 (26 DCU, executor 3 × 6 cores)

```
DCU = driver.cores 8 + executor.cores 6 × 3 = 26 DCU

Compute (Premium, 24/7):
  26 × 730 × \$0.114181                   = \$2,167.62/월

Shuffle:
  minimum 1,000 GiB × \$0.052 (Standard)  = \$52/월    (최소)
  default 2,600 GiB × \$0.13 (Premium)    = \$338/월   (최대)

Total 최소: ~\$2,220/월
Total 최대: ~\$2,505/월
```

### 7-3. Serverless vs GKE 직접 비교

| 시나리오 | Serverless (Prem 강제) | GKE 직접 (CUD 3Y) | GKE 대비 |
|---|---|---|---|
| Baseline 최대 | ~\$6,938 | \$1,395 | 5.0배 |
| Baseline 최소 | ~\$6,119 | \$1,395 | 4.4배 |
| Downsize 최대 | ~\$2,505 | \$917 | 2.7배 |
| Downsize 최소 | ~\$2,220 | \$917 | 2.4배 |

→ **Serverless 는 모든 시나리오에서 Cluster/GKE 대비 3배 이상 비쌈**.
→ 이유: Interactive session 강제 Premium + DCU 에 회사 활용 가능한 할인 없음.

---

## 8. 전체 시나리오 매트릭스

| 배포 | 노드 구성 | Node vCPU / Spark DCU | Default | Res CUD 3Y |
|---|---|---|---|---|
| 현재 (Baseline, 참고) | n4-std-8 × 9 | 72 vCPU | \$3,079 (GKE) / \$3,605 (GCE) | \$1,395 (GKE) / \$1,921 (GCE) |
| **GKE 직접 — 다운사이즈** ⭐ | **std-16 + hm-8 × 3** (mixed) | 40 vCPU / 26 DCU | **\$2,027** | **\$917** |
| Dataproc on GKE — 다운사이즈 | std-2 + std-16 + hm-8 × 3 | 42 vCPU / 26 DCU | \$2,650 | \$1,263 |
| Dataproc on GCE — 다운사이즈 | n4-hm-8 × 4 (통일 강제) | 32 vCPU / 26 DCU | \$2,027 | \$1,045 |
| Serverless (참고 — 부적합) | (자동) | 26 DCU (Premium) | \$2,220~\$2,505 | (BQ CUD 불확실) |

**옵션별 매력**:
- **비용 우선**: **GKE 직접** + Res CUD 3Y = **\$917/월**
- **매니지드 도구 우선**: **Dataproc on GKE** + Res CUD 3Y = **\$1,263/월** (매니지드 값 \$346/월 = \$4,152/년)
- **단순 매니지드 (K8s 없이)**: Dataproc on GCE + Res CUD 3Y = \$1,045/월

→ **팀 논의 필요**: 상세 결정 매트릭스는 § 10 참고

---

## 9. 연간 절감 & CUD 결정

### 배포 모드별 연간 절감 비교

**GKE 직접** (권장):

| 시나리오 | 월 | 연 |
|---|---|---|
| 현재 스펙 (Default) | \$3,079 | \$36,948 |
| 현재 스펙 (Res CUD 3Y) | \$1,395 | \$16,740 |
| 다운사이즈 (Default) | \$2,027 | \$24,324 |
| **다운사이즈 (Res CUD 3Y)** | **\$917** | **\$11,004** |

**Dataproc on GKE** (매니지드 대안):

| 시나리오 | 월 | 연 |
|---|---|---|
| 현재 스펙 (Default) | ~\$3,300 | ~\$39,600 |
| 다운사이즈 (Default) | \$2,650 | \$31,800 |
| **다운사이즈 (Res CUD 3Y)** | **\$1,263** | **\$15,156** |

**Dataproc on GCE** (통일 강제):

| 시나리오 | 월 | 연 |
|---|---|---|
| 현재 스펙 (Default) | \$3,605 | \$43,260 |
| 현재 스펙 (Res CUD 3Y) | \$1,921 | \$23,053 |
| 다운사이즈 (Default, hm-8 × 4) | \$2,027 | \$24,324 |
| **다운사이즈 (Res CUD 3Y)** | **\$1,045** | **\$12,540** |

**다운사이즈 효과 (CUD 3Y 기준)**:
- GKE 직접: 현재 \$16,740 → \$11,004 = **연 \$5,736 절감 (-34%)**
- Dataproc on GKE: 현재 \$39,600 → \$15,156 = **연 \$24,444 절감 (-62%)**
- Dataproc on GCE: 현재 \$23,053 → \$12,540 = **연 \$10,513 절감 (-46%)**

**모드 간 차이 (다운사이즈 후 CUD 3Y)**:
- GKE 직접 vs Dataproc on GKE: **+\$4,152/년** (Dataproc 이 더 비쌈)
- GKE 직접 vs Dataproc on GCE: **+\$1,536/년** (Dataproc GCE 가 더 비쌈)

### CUD 1Y vs 3Y 리스크 (GKE 직접 다운사이즈 기준)

| 항목 | Res CUD 1Y | Res CUD 3Y |
|---|---|---|
| 할인율 (VM 기준) | -37% (\$2,019 → \$1,272) | -55% (\$2,019 → \$909) |
| 월 비용 (GKE, VM + PD) | ~\$1,280 | \$917 |
| 3년 총 비용 (GKE) | \$46,080 | \$33,012 |
| 절감 (Default \$2,027/월 대비 3년) | \$26,892 | \$39,960 |
| 리스크 | 낮음 (1년 후 재검토 가능) | 중 (3년 락인) |

**추천**:
- **Phase 1~2**: Default 로 시작 (PoC 안정성 확인)
- **Phase 2 후 1주 안정 확인**: **Res CUD 3Y 약정**
- **3년 락인 리스크**: 워크로드 급변동 시 손해. 그러나 실측 (409일 상주, 워크로드 안정) 감안 시 감수 가능

### Spot pool 추가 (Phase 3 선택)

**Secondary executor 로 spot VM 활용**:
- 예: Worker `n4-highmem-8` 3대 중 1~2대는 spot
- Spot 할인 ~60~91% 가능
- **위험**: Preemption (2분 이내 강제 종료) → executor evict → task 재실행
- Spark fault tolerance 로 대응 가능하지만 latency 증가

**예상 비용** (GKE + Res CUD 3Y + Spot 1대):
- Master std-16 (CUD 3Y): \$306.11
- Regular hm-8 × 2 (CUD 3Y): \$401.68
- Spot hm-8 × 1 (~70% off): ~\$60
- PD: \$8
- **Total: ~\$776/월** (GKE 다운사이즈 CUD 3Y \$917 대비 -15%)

**Phase 3 검토 사항**: 실측 후 preemption 빈도 & 영향 파악 후 결정.

---

## 10. 배포 모드 결정 매트릭스 (3-way: GKE 직접 vs Dataproc on GKE vs Dataproc on GCE)

> **의사결정 필요**: 다운사이즈 확정 후 **어떤 배포 모드로 갈지** 팀 결정 필요.
> 시니어 의견: "K8s 로 운영 중이니 GKE 로" → **GKE 직접 or Dataproc on GKE** 중심 검토.
> Serverless 는 Interactive 강제 Premium 으로 2.4~2.7배 비쌈 → 부적합 (§ 7 참고).

### 10-1. 한 눈에 보는 비교

| 기준 | Dataproc on GCE | Dataproc on GKE | **GKE 직접** ⭐ |
|---|---|---|---|
| **월 비용 (Res CUD 3Y)** | \$1,045 | \$1,263 | **\$917** |
| **연 비용 (Res CUD 3Y)** | \$12,540 | \$15,156 | **\$11,004** |
| **차액 vs GKE 직접 (연)** | +\$1,536 | +\$4,152 | (baseline) |
| **노드 구성** | n4-hm-8 × 4 (통일 강제) | 3 pool (std-2 + std-16 + hm-8 × 3) | **std-16 + hm-8 × 3** |
| **총 노드 수** | 4 | 5 | 4 |
| **인프라 관리자** | Dataproc | Dataproc + 사용자 (GKE) | 사용자 |
| **Spark 배포 방식** | init action → Spark cluster | SparkApplication CRD | StatefulSet (사내 그대로) |
| **사내 spark-k8s-build 이미지** | ❌ Dataproc runtime 대체 | ⚠ Dataproc runtime 기반 재빌드 | ✅ 그대로 재활용 |
| **사내 kustomize manifest** | ❌ init action 재작성 | ❌ SparkApplication CRD 재작성 | ✅ 그대로 재활용 |
| **Spark UI / History / JMX** | ✅ 자동 | ✅ 자동 | ⚠ 직접 셋업 (~2~3일) |
| **Cloud Logging / 기본 Monitoring** | ✅ 자동 | ✅ 자동 (GKE 기본) | ✅ 자동 (GKE 기본) |
| **Autoscaling** | Dataproc policy | Dataproc + K8s (pool 별) | HPA + Cluster Autoscaler 직접 |
| **노드 fail 자동 복구** | ✅ Dataproc | ✅ Dataproc + K8s | K8s 기본 (재스케줄) |
| **Spark 업그레이드** | runtime version 변경 | spark-engine-version 변경 | 이미지 재빌드 → rolling update |
| **사이즈 변경** | cluster recreate | pool 업데이트 | StatefulSet patch |
| **Spot pool 활용** | Secondary workers | Node pool 별 | Node pool 별 |
| **UI 지원** | ✅ Console UI 완전 | ⚠ **gcloud CLI 만** | ✅ GKE Console |
| **GCP 홍보 정도** | ✅ 표준 | ⚠ 최소 (deprecated 아님) | 일반 K8s 패턴 |

### 10-2. 어떤 관점에서 이득?

**GKE 직접이 유리한 이유** (권장):

- 🎯 **사내 패턴 완벽 재활용** — `dp-gitops/athlon/spark-connect/` manifest 그대로
- 🎯 **사내 이미지 그대로** — `spark-k8s-build` fork 재활용 (GAR push 만)
- 🎯 **Node pool 유연성** — Master (std-16) + Worker (hm-8) mixed
- 🎯 **사내 K8s 경험 활용** — kustomize / kubectl 익숙
- 🎯 **세밀한 최적화 가능** — Spot pool, HPA, affinity/taint
- 💰 **연 \$4,152 절감 vs Dataproc on GKE** / **\$1,536 절감 vs Dataproc on GCE**

**Dataproc on GKE 가 유리한 이유** (K8s + 매니지드):

- 🎯 **Spark 특화 매니지드 자동** — Spark UI / History / JMX export 자동
- 🎯 **Pool 별 machine type + autoscaling** — K8s 유연성 유지
- 🎯 **사용자 GKE cluster 재활용** — K8s 인프라 활용
- ⚠ **단점**: gcloud CLI 만 지원 (UI 최소), SparkApplication CRD 학습, 이미지 재빌드 필요
- 💰 **연 \$4,152 지불** = Spark 매니지드 도구 값

**Dataproc on GCE 가 유리한 이유** (완전 매니지드):

- 🎯 **완전 매니지드** — K8s 신경 안 씀, Dataproc 이 다 관리
- 🎯 **Console UI 완전 지원** — 관리 편함
- 🎯 **표준 Dataproc 패턴** — 공식 가이드 풍부
- 🎯 **Spark UI / History / JMX 자동**
- ⚠ **단점**: Master/Worker 통일 강제 (master 도 hm-8 낭비), 사내 K8s 재활용 안 됨

> 참고: **Cloud Logging / 기본 Cloud Monitoring 은 GKE 도 자동 제공**. 진짜 차이는 **Spark 특화** 도구 (UI, History, JMX 메트릭) 및 인프라 관리 부담.

### 10-3. 우리 워크로드 특성 매핑

| 특성 | 유리한 모드 |
|---|---|
| 24/7 상시 가동 (uptime 409일) | 3가지 모두 동일 |
| Interactive session (Spark Connect) | 3가지 모두 지원 |
| 사이즈 변경 빈도 낮음 | 3가지 모두 동일 |
| **사내 이미 GKE + Spark Connect 운영 중** | **GKE 직접** ⭐ (완전 재활용) |
| **K8s 익숙한 팀 + 시니어 지지** | **GKE 직접** ⭐ (부담 낮음) |
| Spark UI / History / JMX 매니지드 자동 원함 | **Dataproc on GKE** or **on GCE** |
| K8s 신경 안 쓰고 완전 매니지드 | Dataproc on GCE |
| K8s 통제 + Spark 매니지드 | Dataproc on GKE (CLI 부담) |

### 10-4. Phase 별 실행 계획 (배포 모드별)

**GKE 직접 선택 시** ⭐ (권장):

| Phase | 활동 | 월 비용 |
|---|---|---|
| Phase 1 (사내 PoC) | 사내 GKE 에서 다운사이즈 검증 (executor 3 × 6c/35G) | (사내 자원) |
| Phase 2 (GCP 이관 초기) | GKE cluster 생성 + node pool 2개 (std-16 + hm-8) + StatefulSet 이관 + Spark UI/History/JMX 셋업 | \$2,027 (Default) |
| Phase 2 + 1주 (안정) ⭐ | Res CUD 3Y 약정 | **\$917** |
| Phase 3 (선택) | Spot pool 추가 | ~\$776 |

**Dataproc on GKE 선택 시**:

| Phase | 활동 | 월 비용 |
|---|---|---|
| Phase 1 (사내 PoC) | 사내 GKE 에서 다운사이즈 검증 | (사내 자원) |
| Phase 2 (GCP 이관 초기) | GKE cluster + Dataproc on GKE (gcloud CLI) + 3 pool 셋업 + SparkApplication CRD | \$2,650 (Default) |
| Phase 2 + 1주 (안정) ⭐ | Res CUD 3Y 약정 | **\$1,263** |
| Phase 3 (선택) | Spot pool 추가 (Dataproc 관리) | ~\$1,000 |

**Dataproc on GCE 선택 시**:

| Phase | 활동 | 월 비용 |
|---|---|---|
| Phase 1 (사내 PoC) | 사내 GKE 에서 다운사이즈 검증 | (사내 자원) |
| Phase 2 (GCP 이관 초기) | Dataproc cluster 생성 (Console UI) + init action (spark-connect server) + 이미지 검증 | \$2,027 (Default) |
| Phase 2 + 1주 (안정) ⭐ | Res CUD 3Y 약정 | **\$1,045** |
| Phase 3 (선택) | Secondary workers (spot) 추가 | ~\$800 |

### 10-5. 결정 체크리스트 (팀 논의용)

**팀장님 / 팀 논의 시 다음 항목 체크**:

- [ ] **비용 sensitivity**: 연 \$1,536 (vs Dataproc on GCE) or \$4,152 (vs Dataproc on GKE) 차이 유의미?
- [ ] **매니지드 Spark 도구 필요성**: Spark UI / History Server / JMX 메트릭 자동 셋업 원함?
- [ ] **사내 K8s 재활용도**: 사내 spark-connect manifest / 이미지 그대로 쓸지, 재작성 감수?
- [ ] **팀 K8s 익숙도**: 팀이 kustomize / K8s 매니페스트 관리에 익숙?
- [ ] **UI vs CLI**: Console UI 편의 vs gcloud CLI 감수?
- [ ] **초기 셋업 부담 감내**: GKE 직접 시 Spark UI / History / JMX exporter 2~3일 셋업 감수 가능?
- [ ] **시니어 의견**: "K8s 운영 중이니 GKE" — GKE 직접 지지

### 10-6. 공통 확정 사항

**어떤 모드를 선택하든 동일**:

1. **다운사이즈**: executor 3개 × 6 cores × 35G, Spark request 26 vCPU (§ 13)
2. **PoC**: 사내 GKE 에서 먼저 다운사이즈 검증 (Phase 1)
3. **CUD**: 안정 확인 후 Res CUD 3Y 약정
4. **모니터링**: cAdvisor 접근 확보 (사내 monitoring team 요청)
5. **Native memory 실측**: `/proc/1/status` VmRSS 로 overhead 검증

### 10-7. 최종 권장

**GKE 직접 (Master n4-std-16 + Worker n4-hm-8 × 3)** 이 가장 균형 잡힌 선택:

- ✅ 사내 spark-k8s-build 이미지 / kustomize manifest 완전 재활용
- ✅ 시니어 의견 반영 ("K8s 로 운영 중이니 GKE")
- ✅ 최저 비용 (\$917/월 = 연 \$5,736 절감 vs 현재)
- ✅ Node pool 유연성 (mixed spec)
- ✅ K8s allocatable 스케줄 안전 (executor cores 6)
- ✅ Driver Full GC 대응 여지 (Master std-16 memory 여유)
- ⚠ trade-off: Spark UI / History / JMX 셋업 2~3일

**Dataproc on GKE / Dataproc on GCE** 는 매니지드 도구 값 판단 시 대안.

---

## 11. 참고

### 데이터 소스

- **Seoul N4 pricing 스크린샷**:
  - `~/Desktop/n4_price.png` (standard)
  - `~/Desktop/n4_highmem.png` (highmem, Seoul)
- **Seoul DCU pricing 스크린샷**:
  - `~/Desktop/dcu_seoul_hourly.png`
  - `~/Desktop/dcu_seoul_monthly.png`

### 관련 문서

- **다운사이즈 결정**: [[13_Spark Connect 다운사이즈 결정 (실측 기반)]] — Spark 설정 근거
- **배포 모드**: [[3_Spark Connect on Dataproc Serverless 비용 계산]] — Cluster vs GKE vs Serverless 선택
- **단가 근거**: [[12_Managed Service for Apache Spark 과금 체계 (공식)]] — 순수 rate reference
- **사용량 근거**: [[11_사용량 분석 (한달 데이터 기반)]] — 워크로드 분석

### 검증 미완 사항

- [ ] Cluster management fee ($0.010/vCPU/h) 의 Seoul region 무관 여부 (Seoul pricing 페이지 캡쳐 필요)
- [ ] Lightning Engine ($1.825/vCPU/월) 의 Seoul region 무관 여부
- [ ] Spot VM 실 preemption 빈도 (Phase 3 검토용)
