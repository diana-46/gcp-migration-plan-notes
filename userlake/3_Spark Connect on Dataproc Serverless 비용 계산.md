---
title: "Spark Connect 배포 모드 비교 — Serverless / Dataproc Cluster / GKE 직접"
status: revised
created: 2026-06-28
revised: 2026-07-02
대상: userlake-worker 의 Spark Connect 운영 모드 결정
용도: 3축 비교 (개념 / 운영 / 마이그레이션) + 최종 추천
부모: [[1_userlake-worker 인프라 이관]]
---

# Spark Connect 배포 모드 비교 — Serverless / Dataproc Cluster / GKE 직접

> **결론**: **GKE 직접** 이 우리 케이스에 가장 부합 (비용 최우선 + 사내 K8s 패턴 재활용).
> Cluster on GCE / Dataproc on GKE 는 매니지드 우선이면 대안 (연 \$1,536~\$4,152 추가).
> Serverless 는 Interactive 강제 Premium 으로 3~4배 비쌈 → 부적합.

> **관련 문서**:
> - 상세 비용 계산 (Baseline vs Downsize): [[14_Spark Connect 다운사이즈 비용 &amp; 노드 구성 (Seoul 실측)]]
> - 다운사이즈 Spark 설정: [[13_Spark Connect 다운사이즈 결정 (실측 기반)]]
> - 단가 근거: [[12_Managed Service for Apache Spark 과금 체계 (공식)]]
> - 사용량 근거: [[11_사용량 분석 (한달 데이터 기반)]]

---

## 0. 결론

**GKE 직접 + 다운사이즈 (executor 3개 × 6 cores × 35G) = 최적** (Res CUD 3Y 기준 **\$917/월** = 현재 대비 -34%).

**핵심 인사이트 3가지**:

1. **GKE 와 GCE 의 underlying VM 가격 100% 동일** — 차이는 매니지드 fee
2. **Dataproc Cluster fee 는 VM 사이즈에 비례** (32 vCPU 다운사이즈 시 \$234/월)
3. **Serverless 는 Interactive session 강제 Premium** → 24/7 상시 워크로드에 부적합 (3~4배 비쌈)

---

## 1. 3축 배포 모드 — 개념 비교

### 1-1. Dataproc Serverless

**모델**: GCP 가 관리하는 ephemeral 컴퓨트 위에 Spark 잡 실행

**과금**: DCU × 시간 (Interactive session = **Premium tier 강제**)

**장점**:
- 인프라 관리 부담 0 (session 만 만들면 끝)
- 사이즈 변경: properties 한 줄
- Spark 업그레이드: runtime version 한 줄

**단점**:
- **비용 가장 비쌈** (Cluster 대비 ~3배, § 14 참고)
- **VM CUD 약정 불가** (BQ CUD 는 회사 차원 결정 필요)
- **Interactive 강제 Premium** (\$0.114181/h/DCU vs Standard \$0.076976/h)
- **24/7 상시** 워크로드에는 idle timeout 효과 미미

### 1-2. Dataproc Cluster (on GCE)

**모델**: GCP 가 관리하는 always-on VM cluster + Spark Connect server (init action 등으로)

**과금**: VM (SUD/CUD 적용) + **Dataproc management fee** ($0.010/vCPU/h) + PD

**장점**:
- 매니지드 도구 자동: Spark UI, History Server, autoscaler
- 노드 fail 자동 복구
- VM 패치 GCP 관리

**단점**:
- **Dataproc management fee** (vCPU 비례) — CUD 적용 안 됨
- 사이즈 변경 시 cluster recreate 부담
- 사내 K8s 패턴과 다름 (init action 으로 Spark Connect server 띄워야)
- ⚠ **Master 와 Worker 동일 machine type 강제** (GCP Console UI 제약) — 다운사이즈 시 master 도 highmem 강제 → memory 낭비 & 추가 비용 (~$48/월 CUD 3Y)
- ⚠ 다운사이즈 (executor 3개) 시 총 32 vCPU → Dataproc fee \$234/월 (32 × \$7.30)

### 1-3. GKE 직접 운영

**모델**: 사용자 GKE cluster + Spark Connect StatefulSet 직접 배포 (**사내 현재 패턴 그대로**)

**과금**: VM (SUD/CUD) + GKE fee (zonal 1 무료) + PD — **Dataproc fee 없음**

**장점**:
- **비용 가장 저렴** — Dataproc fee 없음
- **사내 패턴 그대로** — `dp-gitops/athlon/spark-connect/` StatefulSet 그대로 이전
- **사내 이미지 재활용** — `spark-k8s-build` 사내 fork
- **Node pool 별 다른 machine type 가능** ⭐ — Master 는 std-16 (driver 여유), Worker 는 highmem-8 로 최적 배치
- Spot pool 활용 시 추가 절감 가능

**단점**:
- Spark Connect server 직접 운영 (Spark 특화 매니지드 도구 없음)
- Spark UI / History Server 직접 셋업 (K8s ingress or 별도 배포)
- Spark JMX 메트릭 export 직접 설정 (Prometheus + JMX exporter)
- Autoscaling 직접 설정 (HPA + Cluster Autoscaler)
- GCP "권장 패턴" 이 아님 (공식 가이드 외)

> Cloud Logging / 기본 Cloud Monitoring (pod/node) 는 GKE 도 **자동 제공**. 진짜 차이는 Spark 특화 도구.

---

## 2. Underlying 자원 비교 — VM 가격은 모두 동일

```
어떤 모드를 골라도 같은 GCE VM 이 underlying:
                    │
        ┌───────────┼────────────┐
        ↓           ↓            ↓
      GKE 위    Dataproc 위    Serverless
        │           │            │
   +GKE fee    +Dataproc fee    +DCU 비용 (Premium 강제)
   (zonal 무료) ($0.010/vCPU/h)  (VM CUD 적용 불가)
```

→ GKE vs Cluster on GCE **VM 가격 100% 동일**. 차이는 **Dataproc fee** 만.
→ Serverless 는 완전 다른 과금 (DCU) → 우리 워크로드에서 훨씬 비쌈.

**상세 시나리오 비용**: [[14_Spark Connect 다운사이즈 비용 &amp; 노드 구성 (Seoul 실측)]] § 8

---

## 3. 운영 측면 비교

| 측면 | Serverless | Dataproc Cluster | **GKE 직접** |
|---|---|---|---|
| 초기 셋업 부담 | 최저 | 중 (cluster + init action) | 중 (StatefulSet manifest) |
| 운영 자동화 | 최상 | 상 | 중 |
| **사내 패턴 일치** | 낮음 | 중 | **✅ 최상** |
| Spark Connect server 운영 | 자동 (session API) | init action | **사내 StatefulSet 그대로** |
| 이미지 관리 | Dataproc runtime | Dataproc runtime | **사내 `spark-k8s-build` fork** |
| Spark 업그레이드 | runtime version 1줄 | cluster recreate | 이미지 빌드 + rolling update |
| 사이즈 변경 | properties 1줄 | cluster recreate | StatefulSet patch |
| Spark UI / History | 자동 | 자동 | 직접 셋업 (or GCP Spark History Server 별도) |
| 노드 fail 복구 | 자동 | 자동 (Dataproc) | K8s 자동 (StatefulSet) |
| Autoscaling | 자동 (DCU) | Dataproc autoscaler | HPA + Cluster Autoscaler 직접 |
| Cloud Logging (container 로그) | 자동 | 자동 | 자동 (GKE 기본) |
| Cloud Monitoring (기본 pod/node) | 자동 | 자동 | 자동 (GKE 기본) |
| Cloud Monitoring (Spark JMX 메트릭) | 자동 | 자동 | 직접 셋업 (Prometheus + JMX exporter) |
| GCP 권장도 | 매니지드 표준 | Spark 워크로드 표준 | 일반 K8s 패턴 |

**핵심**: 사내가 **이미 GKE 에서 spark-connect 운영 중** — 새 패턴 학습 부담 없음.

---

## 4. 마이그레이션 작업량 비교

| 작업 | Serverless | Dataproc Cluster | **GKE 직접** |
|---|---|---|---|
| GKE/Cluster 인프라 프로비저닝 | 0 (session API) | Dataproc cluster 생성 | GKE cluster (사내 패턴) |
| Spark Connect server 구성 | 0 | init action 작성 | **사내 manifest 그대로** ✅ |
| 이미지 빌드 | Dataproc runtime | Dataproc runtime | **사내 fork** ([[2_Spark Connect → Dataproc Serverless 검토]] § 4-3) |
| ConfigMap (hadoop/spark-defaults) | properties 직접 | ConfigMap | **사내 ConfigMap 그대로** ✅ |
| 인증 (Workload Identity) | 자동 | 설정 필요 | 설정 필요 ([[7_Kerberos 제거 (인증 흐름 재설계)]]) |
| GCS 연동 | 자동 | 자동 | gcs-connector jar 추가 |
| BigQuery connector | 자동 | 자동 | jar 추가 |
| Cloud Logging / 기본 Monitoring | 자동 | 자동 | 자동 (GKE 기본) |
| Spark JMX 메트릭 export | 자동 | 자동 | 직접 셋업 (Prometheus + JMX exporter) |
| **전체 작업량** | 0.5~1주 | 1~2주 | **1~2주** (단 사내 패턴 활용) |

→ GKE 직접 = Cluster 와 작업량 유사. **단 사내 yaml / 이미지 그대로 활용 가능** → 새 학습 부담 낮음.

---

## 5. 위험 / 고려 사항

### 5-1. GKE 직접의 잠재 리스크

| 리스크 | 영향 | 완화 |
|---|---|---|
| Spark Connect 직접 운영 부담 | 중 | 사내 패턴 그대로 → 부담 작음 |
| Spark UI / History 자동 안 됨 | 중 | GCP Spark History Server 별도 배포 또는 Spark UI ingress 노출 |
| 사내 이미지 GCP 호환 | 중 | [[2_Spark Connect → Dataproc Serverless 검토]] § 4-3 참고 |
| GCP 권장 패턴이 아님 | 낮음 | 표준 K8s 패턴이라 문제 없음 |
| Spot 사용 시 회수 | 중 | Spark fault tolerance + task retry |

### 5-2. Dataproc Cluster 의 잠재 리스크

| 리스크 | 영향 |
|---|---|
| init action 으로 Spark Connect server — 사내 패턴과 다름 | 중 |
| Dataproc fee 가 vCPU 비례 → CUD 적용 안 됨 | 중 |
| Cluster recreate 시 다운타임 | 중 |

### 5-3. Serverless 의 잠재 리스크

| 리스크 | 영향 |
|---|---|
| Interactive session 강제 Premium ($0.114181/h) | 큼 (비용 3배) |
| VM CUD 적용 불가 (BQ CUD 는 회사 차원) | 큼 |
| 우리 워크로드 (24h 상주 필수) 에 부적합 | 큼 |

---

## 6. 최종 추천

### 6-1. 우리 케이스에 가장 부합

| 평가 기준 | 우리 케이스 | 적합 옵션 |
|---|---|---|
| 사내가 이미 K8s + Spark Connect 운영 중 | ✅ | **GKE 직접** |
| 24h 상주 필수 (정기 스케줄) | ✅ | GKE / Cluster (Serverless ❌) |
| 변경 빈도 낮음 (Spark 설정 stable) | ✅ | GKE / Cluster |
| 비용 민감 | ✅ | **GKE 직접** |
| GCP 매니지드 가치 큼? | △ | Cluster (하지만 fee 부담) |

→ **GKE 직접 운영 + 다운사이즈 + Res CUD 3Y 권장** ($561/월)

### 6-2. Cluster vs GKE 결정 기준

**GKE 직접 우선**:
- 사내 K8s 패턴 재활용 가능 (매뉴얼 운영에 익숙)
- 최저 비용 (연 \$1,536 절감 vs Dataproc on GCE, \$4,152 절감 vs Dataproc on GKE — CUD 3Y 기준)
- **Node pool 별 다른 machine type 가능** (Master std-16, Worker hm-8) — K8s 스케줄 안전 + driver 여유
- 사내 이미지 / manifest 그대로 활용

**Cluster on GCE 선택 근거** (있다면):
- 매니지드 Spark UI / History Server 원함
- Dataproc autoscaler 로 노드 자동 확장 필요
- init action 으로 spark-connect 서버 셋업 부담 감수
- ⚠ Master/Worker 동일 spec 강제 — Master 도 hm-8 되어 memory 낭비

→ **팀 결정 필요**: 비용 절감 (연 \$1,536~\$4,152) vs 매니지드 도구 자동 제공 tradeoff. 상세 결정 매트릭스: [[14_Spark Connect 다운사이즈 비용 &amp; 노드 구성 (Seoul 실측)#10 배포 모드 결정 매트릭스]]

**Serverless 는 왜 부적합**:
- Interactive session 강제 Premium → 24/7 상시 워크로드에서 \$2,040~\$6,938/월 (다운사이즈~현재)
- 같은 스펙 Cluster/GKE 대비 3배 이상
- 상세 계산: [[14_Spark Connect 다운사이즈 비용 &amp; 노드 구성 (Seoul 실측)]] § 7

### 6-3. Phase 별 실행

| Phase | 배포 | 활동 |
|---|---|---|
| **Phase 1** (사내 PoC) | 사내 GKE | 다운사이즈 스펙 검증 ([[13_Spark Connect 다운사이즈 결정 (실측 기반)]] § 6) |
| **Phase 2** (GCP 이관) | GKE 직접 | StatefulSet 배포 → 1주 안정 확인 → Default 요금 (\$2,027) |
| **Phase 2+1주** | 동일 | Res CUD 3Y 약정 → 월 **\$917** 확정 |
| **Phase 3** (선택) | 동일 | Spot pool → 월 ~\$776 / Driver 증설 검토 |

**비용 상세**: [[14_Spark Connect 다운사이즈 비용 &amp; 노드 구성 (Seoul 실측)]] § 9

---

## 7. 참고

- **비용 계산 상세**: [[14_Spark Connect 다운사이즈 비용 &amp; 노드 구성 (Seoul 실측)]]
- **다운사이즈 Spark 설정**: [[13_Spark Connect 다운사이즈 결정 (실측 기반)]]
- **단가 근거**: [[12_Managed Service for Apache Spark 과금 체계 (공식)]]
- **사용량 근거**: [[11_사용량 분석 (한달 데이터 기반)]]
- **Spark Connect 이관** (이미지, 카탈로그 등): [[2_Spark Connect → Dataproc Serverless 검토]]
- 사내 인프라: `dp-gitops/athlon/spark-connect/`
- 사내 빌드: `spark-k8s-build` (spark-connect-khp-3.4.2-hadoop2.10.2 branch)
