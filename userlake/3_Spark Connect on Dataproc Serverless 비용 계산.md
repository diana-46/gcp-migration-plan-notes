---
title: "Spark Connect on Dataproc Serverless 비용 계산"
status: revised
created: 2026-06-28
revised: 2026-06-29
대상: userlake-worker 가 사용하는 Spark Connect 의 Dataproc Serverless 이관 비용
용도: 시나리오별 견적 / Pricing Calculator 입력값 / PoC 비용 전략
부모: [[2_Spark Connect → Dataproc Serverless 검토]]
---

# Spark Connect on Dataproc Serverless 비용 계산

> **2026-06-29 갱신**: 한달 사용량 데이터 ([[11_사용량 분석 (한달 데이터 기반)]]) 기반으로 가정 3개 폐기 후 재계산.

## 0. 결론

> **PoC 추천 셋팅** (사용량 데이터 기반): Usage time **720h/월 (24h)**, Interactive **ON**, **다운사이즈** (Driver 4c + Executor 2~8 Dynamic Allocation), Memory per vCPU 2 GiB
> **예상 견적**: 월 **~$900~1,300**
> **시간대별 호출 거의 균일** → idle timeout 절약 효과 없음 → **24h 상주가 답**
> **Gate/Sync 평균 4~10초 < cold start 30~60초** → **Batch 모드 절대 불가능**, Interactive Session 만
> **75% 코호트가 10만 행 이하** → 현재 DCU 126 의 4~6배 과다, 다운사이즈가 진짜 답

### ⚠ 이전 추정 폐기 (2026-06-28)

| 항목 | 이전 (가정) | 데이터 기반 (현재) |
|---|---|---|
| Usage time | 240h/월 (8h × 30일) | **720h/월 (24h)** |
| DCU | 126 (현재 사이즈 유지) | **21~63 (다운사이즈)** |
| 월 비용 | $1,500~1,800 | **~$900~1,300** |
| idle timeout 가설 | "절약 효과 60~90%" | "거의 없음" — 야간도 시간당 ~900 호출 |

---

## 1. 가격 구조

- **DCU-시간** 단위 (DCU ≈ 0.6 vCPU + 4.5 GB 메모리)
- **최소 자원 강제**: Driver 최소 4 DCU + Executor 최소 4 DCU × 2 = **잡당 최소 12 DCU**
- 1분 최소 청구
- 셔플 스토리지 별도

DCU 환산식: `max(vCPU / 0.6, memory_GB / 4.5)` — 둘 중 큰 쪽이 binding

---

## 2. Dataproc Serverless 운영 모델 — **Interactive Session 만 가능, 24h 상주 필수**

userlake-worker 는 `SparkSession.builder().remote(sc://...)` 로 외부 서버에 붙음 → **Interactive Session 모델** 과 정확히 매핑.

| 모드 | 동작 | userlake-worker 적합도 |
|---|---|---|
| Batch | 잡 submit 마다 ephemeral 컴퓨트 | ❌ **불가능** — cold start (30~60초) > Gate/Sync 평균 실행시간 (4~10초). [[11_사용량 분석 (한달 데이터 기반)]] § 3 |
| **Interactive Session** | 세션 한 번 띄워두고 (Spark Connect 서버) 클라이언트가 붙음 | ✅ **유일한 옵션** |

### ⚠ 24h 상주가 사실상 필수 — 데이터 기반 결론

이전에 "업무 시간 집중, idle timeout 활용" 으로 추정했지만 **실제 데이터가 폐기**.

[[11_사용량 분석 (한달 데이터 기반)]] § 1 의 시간대별 분포:

| 시간대 (UTC / KST) | GATE+SYNC | 시사점 |
|---|---|---|
| 07 / 16 (퇴근시간) | 1,837 | peak |
| 15 / 00 (자정 배치) | 1,804 | peak |
| 14 / 23 | 980 | lowest |
| 03 / 12 ~ 23 / 08 | ~900~1,500 | 새벽도 시간당 900+ |

- peak/lowest 비율 = **1.87× (2배 미만)**
- 자정 (KST 00시) 도 peak (배치 스케줄 추정)
- 새벽 04~06 시 (KST 13~15시) 도 시간당 1,300+ 호출

→ **idle timeout 으로 야간 절약 효과 거의 없음**
→ **24h 상주 (월 720h) 가 사실상 답**

### Cold Start 보면 Batch 모드 완전히 불가능

| Stage | 평균 실행 시간 | 비고 |
|---|---|---|
| GATE | **4.3초** | < cold start 30~60초 |
| SYNC | **9.8초** | < cold start |
| TARGET | 71초 | cold start 흡수 가능하지만 Spark 안 씀 |

→ Gate/Sync 가 cold start 보다 짧음 → batch 면 stage 실행보다 cold start 가 더 오래 걸림. **무의미**.

→ Interactive Session 만 가능. **24h 상주 가정으로 비용 견적**.

---

## 3. 실제 사이즈 vs 권장 다운사이즈

### 3-1. 현재 사이즈 (dp-gitops spark-defaults.conf)

| 컴포넌트 | 사이즈 | DCU |
|---|---|---|
| Driver | 8 vCPU / 14 GB | ~14 |
| Executor × 8 | 8 vCPU / 14 GB 각 | ~112 |
| **합계** | | **126** |

### 3-2. 데이터 기반 권장 다운사이즈

[[11_사용량 분석 (한달 데이터 기반)]] § 4 의 코호트 사이즈 분포:

| 사이즈 | TARGET 비율 | GATE 비율 | SYNC 비율 |
|---|---|---|---|
| < 10k | 35% | 29% | **69%** |
| 10k ~ 100k | 40% | 47% | 23% |
| 100k ~ 1M | 16% | 19% | 6% |
| 1M ~ 10M | 7% | 4% | 2% |
| ≥ 10M | **1%** | 0.6% | 0.4% |

→ **75% 가 10만 행 이하**, **1% 만 10M 행 초과**.
→ 현재 사이즈는 1% 거대 잡 위한 over-provisioning.

### 3-3. 권장 사이즈 (다운사이즈)

| 컴포넌트 | 사이즈 | DCU |
|---|---|---|
| Driver | 4 vCPU / 8 GB | ~7 |
| Executor min | 2 × (4 vCPU / 8 GB) | ~14 |
| Executor max (Dynamic) | 8 × (4 vCPU / 8 GB) | ~56 |
| **base (min)** | | **~21 DCU** |
| **peak (max)** | | **~63 DCU** |
| **평균 (burst 가중)** | | **~25~30 DCU** |

> 75% 잡 (10만 이하) 는 base 21 DCU 로 처리. 1% 거대 잡 + burst 시 Dynamic Allocation 으로 확장.
> Burst 일 (Q7 의 6/8~6/13 같은 5,500 stage/일) 도 max 8 executor 로 충분 검증 필요.

---

## 4. 비용 시나리오 — 데이터 기반 재계산

### 4-1. 운영 모델별 ($0.06/DCU-시간, 서울 리전 추정)

| 시나리오 | 사이즈 | DCU-시간/월 | **월 비용** |
|---|---|---|---|
| ❌ 현재 사이즈 24h | 126 | 90,720 | **~$5,443/월** (과다) |
| ❌ 현재 사이즈 8h | 126 | 30,240 | ~$1,815/월 (실제론 8h 불가능, 데이터로 폐기) |
| ✅ **다운사이즈 + 24h + Dynamic (avg 25 DCU)** | 25 | 18,000 | **~$1,080/월** ← 추천 |
| ✅ 다운사이즈 + 24h + 보수 (avg 21 DCU) | 21 | 15,120 | **~$907/월** |
| ⚠ 최대 다운사이즈 (12 DCU) | 12 | 8,640 | ~$518/월 (burst 위험) |

### 4-2. 데이터 기반 추천 시나리오

| 항목 | 값 | 근거 |
|---|---|---|
| Usage time | **720h/월 (24h × 30일)** | Q1 시간대 균일, Q2 주말도 56% 호출 |
| DCU base | **~21** (Driver 7 + Executor min 14) | Q4 의 75% 잡이 10만 행 이하 |
| DCU peak (Dynamic) | ~63 (max 8 executor) | Q4 의 1% 거대 잡 + Q7 의 burst |
| **평균 DCU** | **~25** | 보수 가중 (10% peak 시간 가정) |
| **월 비용** | **~$1,080** | 25 × 720 × $0.06 |
| 변동폭 | **$900 ~ $1,300** | DCU 21~30 사이

---

## 5. Cloud Pricing Calculator 입력값 — 데이터 기반 갱신

[Cloud Pricing Calculator](https://cloud.google.com/products/calculator) → Dataproc Serverless 항목.

### 5-1. 추천 셋팅 (데이터 기반)

| 항목 | 값 | 근거 |
|---|---|---|
| **Usage time** | **720h/월** (24h × 30일) | Q1 시간대 균일, Q2 주말도 호출 |
| **Interactive** | **ON** 필수 | Q3 평균 4~10초 < cold start 30~60초, batch 모드 불가 |
| **Number of vCPU** | **16** (Driver 4 + Executor 3 × 4) | Q4 의 75% 가 10만 행 이하, 다운사이즈 가능 |
| **Memory per vCPU** | **2 GiB** | 16GB / 8 vCPU 비율 |
| **Shuffle Storage per vCPU** | **100 GiB** | Dataproc Serverless 기본값 |
| **Current cluster utilization** | **70%** | Q1 분포 평균 (peak/avg = 1.5x 정도) |

→ **예상 견적: 월 ~$900~1,300**

### 5-2. 비교: 시나리오별 입력값

| 시나리오 | Usage | vCPU | Mem/vCPU | 월 비용 |
|---|---|---|---|---|
| ❌ 현재 사이즈 24h | 720h | **72** | 1.75 | ~$5,443 |
| ❌ 이전 추정 (8h 가정) | 240h | 72 | 1.75 | ~$1,800 |
| ✅ **데이터 기반 추천** | **720h** | **16** | **2** | **~$1,080** |
| ⚠ 최소 다운사이즈 | 720h | 8 (Driver 4 + Exec 1×4) | 2 | ~$518 |
| ✅ 보수 (큰 코호트 대비) | 720h | 24 (Driver 4 + Exec 5×4) | 2 | ~$1,512 |

> Dynamic Allocation max=8 면 burst 시 더 늘어남. peak 가 짧으면 평균 25 DCU 정도.

---

## 6. ~~사이즈 다운 시뮬레이션~~ → 추천 (§5-1 로 통합)

이전 문서에서 "다운사이즈는 옵션" 으로 다뤘는데, 데이터 분석 결과 **다운사이즈가 기본 권장**으로 바뀜.

§5-1 의 추천 셋팅이 곧 다운사이즈 셋팅임.

### 백업: 현재 사이즈 유지 시 (참고용)

| 항목 | 값 | 비용 |
|---|---|---|
| 현재 사이즈 + 24h | 72 vCPU / 1.75 GiB | ~$5,443/월 |

→ 권장 안 함. 75% 잡이 10만 행 이하인데 over-provisioning.

---

## 7. PoC 비용 전략 (2 phase)

**Phase 1 (검증)**: 다운사이즈 시작 + Dynamic Allocation
- 셋팅: Driver 4c/8G, Executor min 2 / max 8 × (4c/8G), Dynamic Allocation **ON**
- 예상 비용: **월 ~$900~1,300**
- 측정: Spark UI 로 active executor / utilization / OOM 발생 여부
- 검증 대상: 1% 의 10M+ 거대 코호트가 max 8 executor 로 처리되는지

**Phase 2 (최적화)**:
- Phase 1 데이터 보고:
  - executor max 8 이 충분하면 → 유지
  - 부족하면 → Driver 사이즈 또는 max executor 증가
  - over-provisioning 이면 → min/max 더 줄임
- 목표: **월 ~$700~1,100**

**중지 (Stop) 조건**: Phase 1 에서 OOM / job 실패율 > 1% / SLA 위반 → 단계적 사이즈 증가

---

## 8. 비용 정확도 향상에 필요한 데이터

### ✅ 확인된 데이터 (2026-06-29)

[[11_사용량 분석 (한달 데이터 기반)]] 로 확인된 것:

1. ✅ **시간대별 호출 분포** — 거의 균일, 24h 상주 필요
2. ✅ **Stage 평균 실행 시간** — Gate 4초, Sync 10초, Target 71초
3. ✅ **코호트 사이즈 분포** — 75% 가 10만 이하, 1% 가 10M 이상
4. ✅ **재시도 / 실패 빈도** — FAILED 0.05%, max attempt 5
5. ✅ **일평균 / burst 트래픽** — 일 1,500 stage 평소, burst 5,500 stage

### ⚠ 아직 미확인

1. **`spark.dynamicAllocation.enabled` 여부** — 사내 `spark-defaults.conf` 에 있는지 (executor 8개가 고정인지 max 인지)
2. **현재 GKE spark-connect StatefulSet 의 평균 utilization** — CPU / 메모리 / 디스크. 다운사이즈 검증
3. **Shuffle 디스크 사이즈** — StatefulSet 의 `volumeMounts` 에 PV 또는 emptyDir 사이즈
4. **STOPPED 8~10% 의 정체** — timeout 인지 사용자 cancel 인지 ([[11_사용량 분석 (한달 데이터 기반)]] § 6)
5. **Burst 시간대 분포** — 5,500 stage/일 이 24h 균일인지, 특정 시간대 집중인지

→ 위 5가지 확인되면 **±5% 정확도** 견적 가능. 현재 ±15~20% 수준.

---

## 9. 다음 액션

1. ✅ 사용량 데이터 수집 완료 → [[11_사용량 분석 (한달 데이터 기반)]]
2. § 5-1 추천 셋팅으로 Pricing Calculator 견적 1차 확인 (Usage 720h, vCPU 16, Mem/vCPU 2)
3. § 8 의 미확인 데이터 5개 추가 확인 (특히 dynamic allocation 여부 + 현재 utilization)
4. PoC Phase 1: 다운사이즈 (DCU 21 base) + Dynamic Allocation (max 8) 으로 1~2주 운영
5. Spark UI / 청구서로 검증 → Phase 2 최적화 (월 ~$700~1,100 목표)

---

## 10. 참고

- **데이터 근거**: [[11_사용량 분석 (한달 데이터 기반)]] (이 문서의 거의 모든 결론이 여기서 나옴)
- 상위 문서: [[2_Spark Connect → Dataproc Serverless 검토]] § 3
- 상위의 상위: [[1_userlake-worker 인프라 이관]] § 2-2-1
- 코드 위치: `userlake-worker/src/main/kotlin/com/kakaopage/athlon/stage/sparkconnect/`
- 인프라: `dp-gitops/athlon/spark-connect/`