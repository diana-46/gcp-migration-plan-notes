---
title: "Spark Connect 다운사이즈 결정 (실측 기반)"
status: decision
created: 2026-07-02
대상: 사내 Spark Connect StatefulSet (userlake-worker driver)
용도: GCP 이관 시 사이즈 결정 근거 + 사내 사전 PoC 근거
부모: [[1_userlake-worker 인프라 이관]]
---
# Spark Connect 다운사이즈 결정 (실측 기반)

> **결론**: `72 vCPU / 126 GB (executor 8개 × 8 cores) → 26 vCPU / 224 GB (executor 3개 × 6 cores, highmem × 3)` 다운사이즈.
>
> **근거**: CPU 이용률 0.26%, Active tasks 평균 0, Spill 0. Stage 실행 시간 평균 **7.25초** (78% <10s), executor 활용도 **0.97%**. Executor 축소 (8→3) 로 memory 집중 (2.67배) → 3대 highmem 분산. Cores 축소 (8→6) 로 K8s 스케줄 여유 확보. Peak activeTasks 7 vs 18 slots (여유 61%).
>
> **부가 발견**: Queue wait 평균 2시간 vs 실행 7초 = 1000:1. 병목은 executor 가 아닌 상위 스케줄링 (다운사이즈 안전 강화).
>
> **비용 영향** (GKE 직접, Res CUD 3Y): 월 **\$1,395 → \$917** (**-34%**). 상세: [[14_Spark Connect 다운사이즈 비용 &amp; 노드 구성 (Seoul 실측)]]

---

## 0. TL;DR


| 항목                 | 현재                          | 다운사이즈 안                    |
| ------------------ | --------------------------- | -------------------------- |
| Driver             | 8 cores / 10G + 4G overhead | **유지** (Major GC 다발)       |
| Executor instances | **8 (static)**              | **3 (static)** — 3대 분산으로 memory 여유 확보 |
| Executor cores     | 8                           | **6** — peak 7 tasks vs 18 slots (여유 61%), K8s 스케줄 안전 |
| Executor memory    | **10G** (peak 9.95G, 99.5%) | **35G** (data 집중 2.67배 대응) |
| Executor overhead  | 4G                          | **유지** (DP-2689 OOM 조사 근거, 미측정 native 존재) |
| Dynamic Allocation | 없음                          | **비활성화 유지** (실효성 낮음, 아래 § 지표 4 참고) |
| Adaptive Query     | 없음                          | **enabled**                |
| **Total Spark request** | **72 vCPU / 126 GB**        | **26 vCPU / 131 GB (pod)**   |


노드 구성:

- **GKE 직접**: Master `n4-standard-16` (16c/64G) + Worker `n4-highmem-8 × 3` (8c/64G each) — driver 여유 + K8s 스케줄 안전
- Dataproc on GKE (대안): + control pool (n4-std-2) 추가
- Cluster on GCE (대안): `n4-highmem-8 × 4` (통일 강제)

---

## 1. 실측 오버스펙 증거

### 데이터 소스


| 소스                                            | 지표                                  | 기간      |
| --------------------------------------------- | ----------------------------------- | ------- |
| Spark REST API (executor JSON)                | totalDuration, memory peak, task 통계 | 409일 누적 |
| Grafana (JVMCPU.jvmCpuTime)                   | 실제 CPU 사용률                          | 30일 시계열 |
| Grafana (ExecutorMetrics.JVMHeapMemory)       | 실제 JVM Heap 사용률                     | 30일     |
| Grafana (ExecutorMetrics.DirectPoolMemory)    | Native 메모리 (overhead 영역)            | 30일     |
| Grafana (executor.threadpool.activeTasks)     | 실행 중 task 수                         | 30일     |
| Grafana (shuffleTotalBytesRead, BytesSpilled) | Shuffle / Memory pressure           | 30일     |
| kubectl (pod startTime)                       | Uptime                              | 실시간     |


### Pod Uptime — 409일 (재시작 없이 상주)

```bash
$ kubectl get pod spark-connect-driver-0 -n athlon-prod -o jsonpath='{.status.startTime}'
2025-05-19T05:40:48Z
```

→ StatefulSet 이 매우 오래 상주. Interactive session 상시 가동 확인.

### 확보 자원 (현재 Spark 설정)

```
Driver:
  cores 8, memory 10G + overhead 4G = 14G

Executor × 8 instances (fixed):
  cores 8, memory 10G + overhead 4G = 14G

Total confirmed capacity: 72 vCPU / 126 GB
Dynamic Allocation: 없음 (static)
```

---

### 지표 1: CPU 이용률 — **0.26% (task) / 평균 1.25% (실제 JVM)**

#### (a) Task 실행 시간 관점 — Executor JSON `totalDuration`

```
core-hours 사용 = totalDuration (ms) / 1000 / 3600
core-hours 가능 = uptime × cores (executor 1개)
이용률 = 사용 / 가능
```


| Executor | totalDuration (ms) | core-hours 사용 | 이용률 (409일) |
| -------- | ------------------ | ------------- | ---------- |
| 1        | 734,944,157        | 204.15        | **0.26%**  |
| 2        | 731,749,821        | 203.26        | 0.26%      |
| 3        | 724,569,456        | 201.27        | 0.26%      |
| 4        | 731,287,159        | 203.13        | 0.26%      |
| 5        | 695,535,702        | 193.20        | 0.25%      |
| 6        | 700,282,729        | 194.52        | 0.25%      |
| 7        | 733,806,466        | 203.84        | 0.26%      |
| 8        | 731,157,287        | 203.10        | 0.26%      |
| **평균**   |                    |               | **0.26%**  |


Capacity (executor 1개): 409일 × 24h × 8 cores = **78,528 core-hours**

#### (b) 실제 JVM CPU 관점 — Grafana `JVMCPU.jvmCpuTime`

```promql
rate({__name__=~"userlake_spark_connect_prod\\..*\\.JVMCPU\\.jvmCpuTime"}[5m]) / 1e9
```

30일 실측 (executor 별):

- **평균: ~0.1 cores** 사용
- **Peak: 2.75 cores** (occasional spike, 아마 :20 배치)
- Baseline 대부분 0.05 이하

**8 cores per executor 대비**:

- 평균 CPU 이용률 **~1.25%** (0.1 / 8)
- Peak CPU 이용률 **~34%** (2.75 / 8)

#### CPU 종합

```
확보:      8 executor × 8 cores = 64 cores
평균 사용:  ~0.8 cores (8 × 0.1)
Peak 사용: ~22 cores (8 × 2.75)

Over-spec 배수 (평균): 64 / 0.8 = 80배
Over-spec 배수 (peak): 64 / 22 = 3배
```

→ **CPU 는 평상시 80배 over-spec, peak 시에도 3배 여유**.

---

### 지표 2: Memory 사용률 — **평균 67%, Peak 99.5%**

#### JVM Heap 실측 (Grafana `ExecutorMetrics.JVMHeapMemory`)

```
{__name__=~"userlake_spark_connect_prod\\..*\\.ExecutorMetrics\\.JVMHeapMemory"}
```

30일 실측 (8 executor 통합 평균 / avg_over_time + max_over_time):

- **평균: 6.7 GB** (executor.memory 10G 중)
- **Peak: 9.95 GB** (100% 근접)
- Executor 별 편차: 5.25 GB ~ 9.73 GB (idle 시간 포함 통합 평균)

**executor.memory 10G 대비**:

- **평균 memory 이용률: 67%** (여유 33%)
- **Peak: 99.5%** (안전 마진 0.5%) ⚠

#### Spill 검증 — 0 (30일 전체)

```promql
sum by (__name__) (
  rate({__name__=~"userlake_spark_connect_prod\\..*\\..*BytesSpilled\\.count"}[5m])
)
```

**결과**: 30일 전체 **BytesSpilled = 0**.

의미:

- Shuffle 시 memory 부족으로 disk 로 넘긴 적 **한 번도 없음**
- 즉 executor.memory 10G × 8 executor 조합에서 **memory 압박 없음**

#### ⚠ Memory 관점의 **executor 축소 위험**

**중요**: 현재 8 executor × 10G = 80G 로 데이터 분산. 평균 67% / peak 99.5% 로 안정 운영 중이지만 **peak 시 안전 마진 0.5% 밖에 없음**. **executor 를 3 으로 줄이면 데이터 집중이 2.67배**:

```
현재:  데이터 X (GB) → 8 executor 로 분산 = executor 당 X/8
축소:  데이터 X (GB) → 3 executor 로 분산 = executor 당 X/2.67  (2.67배 집중)

Peak 시 executor 당 memory 필요치:
  현재 peak 9.95 GB × 2.67 = ~26.5 GB  (executor 3개일 때 추정)
  안전 마진 30% 포함:         ~35 GB
```

→ **executor 축소 시 memory 를 반드시 증가**해야 함. 10G 유지는 OOM 위험.

→ 다운사이즈 안: **executor.memory 10G → 35G** (안전 마진 포함).

**왜 executor 2개 (50G) 가 아니라 3개 (35G)?**: 3대로 분산하면 pod 당 memory 낮아져 node memory pressure 여유 훨씬 큼. Pod = 39G on hm-8 (64G) → 여유 21G (~35%) vs 2 executor 안은 6G (~10%). 팀장님 지적 반영.

---

### 지표 3: Overhead (Native Memory) — **측정 가능 부분만 5%, 실제 필요치는 미상**

#### JVM 관점에서 측정 가능한 native (30일 평균)

| 항목 | Executor 평균 | Driver | 설명 |
|---|---|---|---|
| DirectPoolMemory | ~200 MB | ~165 MB | Netty direct buffer, Java NIO direct memory |
| JVMOffHeapMemory | ~195 MB | ~400 MB | Metaspace + Code Cache + Compressed Class Space |
| **측정된 native 합** | **~395 MB** | **~565 MB** | JVM 이 인식하는 것만 |
| **memoryOverhead 할당** | **4,000 MB** | **4,000 MB** | 설정값 |
| **미측정 native (추정)** | **~3,600 MB (90%)** | ~3,400 MB | 어디에 있는지 불명 |

#### 미측정 native 후보 (JVM 이 안 보는 영역)

- **glibc malloc arena** — `MALLOC_ARENA_MAX` 미설정 시 core 수 × ~64 MB. 8-core 면 최대 512 MB, fragmentation 로 더 큼
- **JNI native code** — Kerberos, Snappy, Zstd, native Hadoop libs
- **Netty native buffer** (DirectPool 밖 pool)
- **Thread stack memory**
- **Off-heap execution memory** (Spark 설정에 따라)

#### K8s Pod Memory 관점 (팀장님 지적 반영)

**Pod memory = executor.memory + memoryOverhead** (+ off-heap size, 있으면):

```
현재 pod 실제 할당:
  executor.memory 10G + memoryOverhead 4G = 14G per executor pod
  driver.memory   10G + driver.memoryOverhead 4G = 14G per driver pod

statefulset.yaml 실측 (git):
  requests.memory: 14G
  limits.memory:   14G
```

→ **overhead 튜닝 = pod 이 K8s 에 요청하는 memory 증가** = 노드 사이즈 산정에 반영 필요.

#### Overhead 4G 도입 경위 (Git 추적)

**Commit history** (2025-05-19):

| 시각 | Commit | 변경 |
|---|---|---|
| 14:16 | `81df609a` | overhead **1G → 3G** (driver + executor) |
| 14:36 (20분 후) | `3d76d2e4` | **3G → 4G** ← 3G 도 부족했음 |
| PR #74 병합 | `ea6ae87d` | **DP-2689 "OOM 원인 조사"** — 위 변경 병합 + Driver pod resources **12G → 14G** |

→ **PR 이름이 "OOM 원인 조사"** = 실제로 OOM 발생 후 조사. 1G → 3G → 4G 단계적 증가는 실증적 근거.
→ 원 개발자가 **statefulset pod memory 도 함께 12G → 14G 로 올림** = pod memory = memory + overhead 인식을 정확히 반영한 결정.

#### 정확한 native 사용량 측정 불가 이유

**cAdvisor 접근 불가**:

```promql
container_memory_working_set_bytes{namespace="athlon-prod", pod=~"spark-connect-.*"}
→ no data
```

사내 Prometheus 가 **`athlon-prod` namespace 를 scrape 대상 제외**. Application namespace 는 cAdvisor 메트릭 수집 안 함 → pod-level actual memory 확인 불가.

**논리적 상한 추론**:

- Pod memory limit = 14G (statefulset 확인)
- 409일 무 OOMKilled (재시작 없이 상주 확인)
- Peak heap = 9.95G (실측)
- → **Peak native ≤ 14 - 9.95 = 최대 4G** (실제 사용 가능 범위)
- **1G 로는 확실히 부족** (과거 OOM 근거)
- → **실제 필요치 = 1G ~ 4G 사이 어딘가** (정확히 불명)

#### Overhead 판정 — **4G 유지** (안전 우선, 근거 기반)

**유지 근거**:

1. **과거 OOM 이력** (DP-2689) — 1G 로는 확실히 부족
2. **3G → 4G 로 20분 안에 재조정** — 3G 도 완충 부족했음을 시사
3. **측정된 native (395 MB) 는 JVM 내부만** — glibc, JNI 등 관측 불가 영역 있음
4. **cAdvisor 미접근** — 정확한 pod memory footprint 확인 불가
5. **409일 안정** — 4G 조합에서 재발 없음 (over-provisioning 이든 딱 맞든 안전)

**축소 시 확인 필수**:

- cAdvisor 접근 확보 (사내 monitoring team 요청)
- OR JVM Native Memory Tracking (`-XX:NativeMemoryTracking=summary`) 활성화 + `jcmd VM.native_memory` 실행
- OR `/proc/<pid>/status` 의 VmRSS 로 pod 실 memory 확인
- OR PoC 환경에서 2G / 3G 로 낮춰 1주 관찰 (OOMKilled 이벤트 감시)

---

### 지표 4: Active Tasks — **평균 0, peak 6-7**

30일 실측:

- **평균: 0** (대부분 시간)
- **Peak: 6~7 tasks** (rare spike, 아마 :20 배치)
- Executor 개별 max 도 6~7 (8 다 채우는 경우 거의 없음)

**executor slots = 8 cores × 8 executors = 64 slots 대비**:

- 평균 active: **거의 0** (0/64)
- Peak active: 6~7 tasks per executor → 전체 시스템 8~10 tasks 정도

→ **8 executor 중 대부분 idle**. 필요한 executor 는 2~3개 수준.
→ **executor 3개 × 6 cores = 18 slots** 로 peak 처리 여유 61%.

#### ⚠ "평균 0" 의 근본 이유 — Stage 실행 시간 실측 (DB `userlake_cohort_run_stage.result.executionSeconds`)

30일 GATE + SYNC COMPLETED stage 27,641 개 실측:

| 구간 | 개수 | 비율 |
|---|---|---|
| **1-5s** | 15,984 | **57.83%** ⭐ |
| **5-10s** | 5,623 | 20.34% |
| 10-30s | 4,836 | 17.50% |
| 30-60s | 1,040 | 3.76% |
| 1-5m | 158 | 0.57% |

**집계**:

- 평균 실행 시간: **7.25초**
- 최대: **129초 (2분)** — 이보다 오래 걸린 stage 없음
- 30일 총 실행 시간: **55.67 시간**

**activeTasks 평균 0 인 이유**:

- 실행 시간 **7초**, Grafana scrape 간격 **15초**
- 실행 중 캐치 확률: 7/15 = 47%
- **BUT** 활성 시간 자체가 매우 짧음 (30일 720시간 중 55.67시간 = 7.7%)
- 결합 확률: 7.7% × 47% = **3.6% 만 non-zero 표본**
- **96.4% scrape 는 idle → 평균값 → 0 근접**

#### 🚨 Queue Wait 병목 발견 (다운사이즈와 별개 문제)

`updated_dt - created_dt` (총 lifetime) vs `executionSeconds` (실 실행) 차이:

| 지표 | 값 |
|---|---|
| **평균 큐 대기** | **7,280초 = ~2시간** |
| **최대 큐 대기** | **248,401초 = ~69시간 (~3일!)** |
| 평균 실행 | 7.25초 |
| **큐 대기 : 실행 비율** | **1000 : 1** |

**Executor 활용도 (진짜 사용)**:

```
55.67h 실행 / (720h × 8 executor) = executor 당 0.97% 활용
→ Executor 99% idle 확증
```

**병목은 executor 가 아니라 상위 스케줄링**:

- RabbitMQ consumer, DAG dependency, worker polling 등 후보
- **Executor 늘려도 큐 대기 안 줄어듦** (executor 는 이미 놀고 있음)
- **Executor 줄여도 큐 대기 안 늘어남** (병목 위치가 다름)
- → **다운사이즈 결정 강화**: executor 축소 안전

**GCP 이관 시 개선 여지**: Pub/Sub 병렬 consumer, Cloud Composer scheduling 등으로 상위 병목 완화 가능성.

---

### 지표 5: Shuffle 부담 — **매우 낮음**

30일 실측:

- Shuffle Read: **평균 &lt;500 KB/s** / **Peak ~6 MB/s** per executor
- Shuffle Write Time: **평균 &lt;5%** / **Peak ~20%**
- 누적 shuffleTotalBytesRead: 408 GB (409일, 일평균 1 GB/executor)
- shuffleWriteTime: 3.56 시간 (409일 중 0.036%)

→ **Shuffle 이 병목이 아님**. shuffle disk 축소 여유 큼.

---

### 지표 6: GC / Failed Tasks — **매우 건강**


| 지표                      | 값                | 판정      |
| ----------------------- | ---------------- | ------- |
| GC ratio (Executor)     | 0.4%             | ✅ 매우 건강 |
| Failed tasks (Executor) | 0.008%           | ✅ 매우 낮음 |
| Failed / Total          | 400 / 5,000,000+ | ✅ 무시 가능 |


**단 Driver 는 다름**:

- Major GC Count: **19,579회** (Full GC 다발)
- Major GC Time: 4.9 시간
- Driver JVMHeapMemory Peak: 9.46 GB / 10 GB (거의 다 씀)

→ **Driver 는 오히려 memory 부족 신호**. 축소 절대 불가, 유지 (or 증가 검토).

---

### 종합: Over-spec 매트릭스


| 자원                    | 확보량             | 평균 사용      | Peak 사용  | 이용률                 | Over-spec 배수 | 조치                        |
| --------------------- | --------------- | ---------- | -------- | ------------------- | ------------ | ------------------------- |
| **CPU (task 관점)**     | 64 cores        | 0.16 cores | ?        | 0.26%               | **385배**     | ✅ 축소                      |
| **CPU (실제 JVM)**      | 64 cores        | 0.8 cores  | 22 cores | 평균 1.25% / peak 34% | **80배 (평균)** | ✅ 축소                      |
| **Memory (executor)** | 80 GB           | 53.6 GB    | 79.6 GB  | 67% / peak 99.5%    | 1.5배         | ⚠ **executor 축소 시 증가 필수 (peak 안전 마진 0.5%)** |
| **Overhead (측정 가능)** | 36 GB           | 3.16 GB (JVM 내부) | ?  | ~9%                 | (측정 한계)      | 4G 유지 (미측정 native 있음)     |
| **Active tasks**      | 64 slots        | ~0         | ~10      | 0% (평균)             | ∞            | ✅ 축소                      |
| **Stage 실행 시간 (DB 실측)** | -             | 7.25초/stage | 129초    | 78% <10s / 96% <30s | -            | ✅ 짧은 실행 확증               |
| **Executor 활용도**    | 5,760 exec-h    | 55.67h     | -        | **0.97%**            | 100배         | ✅ 축소                      |
| **Queue Wait**        | -               | 2시간/stage | 69시간    | 큐 대기 : 실행 = 1000:1 | -            | (다운사이즈 무관, 상위 병목)  |
| **Shuffle bytes**     | (300~800 GB PD) | 500 KB/s   | 6 MB/s   | 매우 낮음               | 큼            | ✅ 축소                      |


---

## 2. 다운사이즈 대상 스펙

### Spark 설정 변경표


| Spark 설정                             | 현재             | 다운사이즈      | 근거                                       |
| ------------------------------------ | -------------- | ---------- | ---------------------------------------- |
| driver.cores                         | 8              | **8 유지**   | Major GC 다발 (19,579회), heap peak 9.46G   |
| driver.memory                        | 10G            | **10G 유지** | Peak 100% 근접, 여유 없음                      |
| driver.memoryOverhead                | 4G             | **4G 유지**  | DP-2689 OOM 조사 근거 (§ 지표 3)               |
| **executor.instances**               | **8 (static)** | **3**      | ⭐ CPU 0.26%, Active tasks 평균 0. 3대 분산으로 memory 여유 확보 |
| **executor.cores**                   | 8              | **6**      | ⭐ Peak 7 tasks vs 18 slots (여유 61%), K8s allocatable 스케줄 안전 |
| **executor.memory**                  | **10G**        | **35G**    | ⭐ executor 3대로 data 집중 2.67배 (peak 9.95G × 2.67 = 26.5G + 안전 마진 30%) |
| executor.memoryOverhead              | 4G             | **4G 유지**  | DP-2689 OOM 조사 근거 (§ 지표 3)               |
| **spark.dynamicAllocation.enabled**  | 없음             | **false 유지** | Peak 7 tasks = 18 slots 로 이미 충분. Cluster 모드에서 노드 상시 유지 → 실효성 낮음 |
| **spark.sql.adaptive.enabled**       | 없음             | **true**   | Shuffle 최적화                              |


**결과 사이즈**:

- Driver: 8 cores / 14 GB
- Executor × 3: 6 cores / 39 GB each (memory 35G + overhead 4G)
- **Total: 26 vCPU / 131 GB pod memory** (실제 요청)

---

## 3. 노드 선택 & 비용

**노드 후보 비교 · K8s 시스템 overhead 고려 · 대안 · 상세 비용 breakdown**:
→ [[14_Spark Connect 다운사이즈 비용 &amp; 노드 구성 (Seoul 실측)]]

**결론 요약** (배포 모드별 상이):

| 배포 | 노드 구성 | 이유 |
|---|---|---|
| **GKE 직접** ⭐ | `n4-standard-16` (Master) + `n4-highmem-8 × 3` (Worker) | Node pool 별 다른 spec 가능 → driver 여유 + K8s 스케줄 안전 |
| Cluster on GCE | `n4-highmem-8 × 4` (통일 강제) | Console UI 로 master/worker 동일 spec 강제 |

---

## 4. 위험 &amp; 완화


| 위험                                    | 심각도 | 완화                                                        |
| ------------------------------------- | --- | --------------------------------------------------------- |
| **Executor 3개로 data 집중 2.67배 → OOM**  | 중   | executor.memory 10G → 35G 로 증가 (peak 26.5G 추정 + 여유 30%)  |
| **Peak 시 burst 처리 부족**                | 낮음  | Peak activeTasks 7 vs 18 slots (3 executor × 6 cores) — 여유 61% |
| **Overhead 부족으로 native OOM**          | 낮음  | 4G 유지 (DP-2689 근거, JVM 밖 native 존재 가능성)                   |
| **Driver GC 과부하 심화**                  | 중   | Driver memory 10G 유지 (peak 9.46G, 여유 6%). Master n4-standard-16 (64G) 라 향후 12~16G 증설 여지 |
| **Shuffle disk 부족** (Serverless 갈 경우) | 낮음  | Cluster/GKE 는 PD 로 해결. Serverless 는 250 GiB min 설정        |
| **1% 거대 코호트 (10M+ 행) 처리 실패**          | 중   | PoC 필수. Peak 시 활발 관찰. 필요 시 executor.memory 상향 or 노드 autoscaling 검토 |
| **K8s 노드 memory pressure** (팀장님 지적)   | 낮음  | 3 executor 분산으로 pod = 39G. n4-highmem-8 (64G) 에 여유 18G (~28%) — 안전 확보 |
| **K8s CPU allocatable 스케줄 실패**        | 낮음  | Executor cores 6 (< 7.4 allocatable), Master std-16 (allocatable 15) — 스케줄 안전 |
| **Native memory 정확한 필요치 불명**          | 중   | cAdvisor 접근 불가로 미측정. Overhead 4G 유지 (DP-2689 근거). 향후 NMT 또는 `/proc/$pid/status` 로 실측 필요 |


---

## 5. 비용 영향

**상세 비용 breakdown · 시나리오 매트릭스 · 연간 절감 · CUD 결정**:
→ [[14_Spark Connect 다운사이즈 비용 &amp; 노드 구성 (Seoul 실측)]]

**요약** (Res CUD 3Y 기준, 다운사이즈 후):

| 배포 | 현재 | 다운사이즈 (3 exec, cores 6) | 연간 절감 |
|---|---|---|---|
| GKE 직접 | \$1,395/월 | **\$917/월** | \$5,736/년 (-34%) |
| Dataproc on GKE | (동일) | **\$1,263/월** | \$1,584/년 (-9%) |
| Cluster on GCE | \$1,921/월 | \$1,045/월 | \$10,513/년 (-46%) |

→ **배포 모드 선택은 팀 논의 필요** (매니지드 도구 vs 비용). 결정 매트릭스: [[14_Spark Connect 다운사이즈 비용 &amp; 노드 구성 (Seoul 실측)#10 배포 모드 결정 매트릭스]]

**배포 모드 선택** (Serverless vs Cluster vs GKE): [[3_Spark Connect on Dataproc Serverless 비용 계산]]

---

## 6. 실행 계획

### Phase 1 — 사내 GKE 에서 PoC (1~2주)

1. **사내 `spark-defaults.conf` 변경**:
  ```
   spark.executor.instances 3                (8 → 3, static 유지)
   spark.executor.cores 6                    (8 → 6)  ⭐
   spark.executor.memory 35G                 (10G → 35G)  ⭐
   spark.sql.adaptive.enabled true           (신규)
  ```
2. **사내 GKE 노드 사이즈 조정**:
  - Executor 를 담을 노드가 **memory ≥ 39G (pod 요구)** 여야 함
  - 사내 노드 스펙 확인 후 필요 시 노드풀 조정 or executor pod resource request 강화
3. **Native memory 실측** ⭐ (팀장님 지적 반영):
  - `kubectl top pod` 로 executor pod 실제 memory footprint 확인
  - `kubectl exec` → `/proc/1/status` VmRSS 로 OS 수준 실측
  - 또는 JVM Native Memory Tracking 활성화 (`-XX:NativeMemoryTracking=summary`) + `jcmd VM.native_memory`
  - **목표**: overhead 4G 가 실제로 얼마나 필요한지 확인 (향후 축소 여지 판단)
4. **검증 시나리오**:
  - Peak 시간대 (:20 배치) 정상 처리?
  - **1% 거대 잡 (10M+ 행) OOM 없이 처리?** ⭐ (executor 축소 시 최대 위험)
  - Spark UI 로 GC / memory / spill 재확인
  - Executor 3개 × 6 cores = 18 slots 로 activeTasks peak 처리 확인
  - K8s node memory pressure 이벤트 확인 (n4-highmem-8 대상)
5. **성공 기준**:
  - Failed task 비율 &lt;0.1% 유지
  - Driver GC time 30일 누적 &lt;10h (현재 4.9h 대비)
  - Spill 여전히 0
  - Peak memory 사용률 &lt;80% (executor.memory 35G 기준)
  - Pod OOMKilled 이벤트 0회
  - Node memory pressure eviction 0회

### Phase 2 — GCP 이관

5. **사내 spark-k8s-build 이미지 GCP 호환 검증** ([[2_Spark Connect → Dataproc Serverless 검토]] § 4-3)
6. **GKE 에 spark-connect StatefulSet 배포** — 사내 manifest + 다운사이즈 설정
7. **노드 구성**: `n4-standard-8` + `n4-highmem-8 × 2` (또는 자원 pool 로 통합)
8. **1주 운영** → 실제 청구서 확인
9. **CUD 3년 약정** (안정 확인 후) → 월 $736 확정

### Phase 3 — 추가 최적화 (선택)

10. **Spot pool 추가** — Secondary executor 로 spot VM 활용 (~60% 절감 가능하지만 evict 위험)
11. **더 aggressive 다운사이즈**:
    - executor.instances 2 시도 (peak activeTasks 7 이지만 16 slots 로 처리 가능하면)
    - memory 30G 시도 (peak 확인 후)
12. **노드 autoscaling** — Burst 상시 노드 확장 필요 시 Dataproc autoscaling policy 도입 검토 (지금은 필요 없음, peak 이 이미 커버됨)
13. **Driver 증설 검토** — Major GC 19,579회 원인 파악 후 memory 12~16G 검토
14. **Dynamic Allocation 재검토** — 만약 워크로드 특성이 크게 바뀌면 (peak activeTasks 가 18 slots 초과) 활성화 검토

---

## 7. 참고

- 실측 데이터 상세: [[11_사용량 분석 (한달 데이터 기반)]]
- 비용 계산 상세: [[12_Managed Service for Apache Spark 과금 체계 (공식)#2-5 Cluster 가격 계산 예시]]
- 배포 옵션 (Serverless vs Cluster vs GKE): [[3_Spark Connect on Dataproc Serverless 비용 계산]]
- Spark Connect 이관 (이미지, 카탈로그): [[2_Spark Connect → Dataproc Serverless 검토]]
- 사내 인프라: `dp-gitops/athlon/spark-connect/`
- 사내 빌드: `spark-k8s-build` (spark-connect-khp-3.4.2-hadoop2.10.2 branch)

