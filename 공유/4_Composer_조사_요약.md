# 4. Composer 조사 요약

> 두 결정을 뒷받침하는 조사 결과. 각 주제는 이미 별도 노트 (`[[스케줄러/*]]`) 에서 상세 다룸.
> 여기서는 매니저 / 플랫폼팀 리뷰용 요약.

## 결정 트리 한눈에

```
Composer vs Self-managed
  └─ Composer 3 채택 → 관리형 인프라 이득 우선

Airflow 2 vs 3
  └─ Airflow 3 채택 (Composer 3 번들)

Executor
  └─ CeleryKubernetesExecutor (Composer 3 강제) — 경량 Celery + 중량 K8s 병행

DAG 배포
  └─ GitDagBundle or GCS rsync (팀 저장소별 자율)

메타데이터 DB
  └─ Composer 관리 Cloud SQL, 20GB 상한 유지 필요

Provider 패키지
  └─ Artifact Registry Python repo + SemVer + 도메인별 lock
```

## 주요 결정과 근거

### 1. Composer 3 vs Self-managed GKE

- **결정**: Composer 3
- **근거**: 관리형 인프라 이득 (GKE / Cloud SQL / Memorystore / WI / 백업), Airflow 3 지원, 서울 리전
- **비용**: Composer 3 이 self-managed 대비 20-35% 비쌈, 그러나 운영 자원 (0.5+ FTE) 절감이 상쇄
- 상세: [[스케줄러/2_Cloud Composer vs Self-managed 비교]]

### 2. Airflow 2 → 3

- **결정**: Airflow 3 (Composer 3 번들 = 3.1.7+composer)
- **주요 변화**:
  - Task SDK 격리 (task → API-based DB 접근)
  - Asset / AssetWatcher (Dataset 진화형)
  - DAG Bundles
  - Removed: SubDAG, SLA, Smart Sensor
- **호환성**: 기존 DAG 80% passthrough, 15% 리팩토링, 5% 인프라 재구성
- 상세: [[스케줄러/6_Airflow 2 vs 3 비교]]

### 3. Executor

- **결정**: CeleryKubernetesExecutor (Composer 3 강제)
- **패턴**: Celery worker 로 경량 태스크, K8s pod 으로 중량 태스크
- **PoC 결과**: Celery + Triggerer 조합으로 대부분 커버, dbt 무거운 task 는 K8s executor 로 분리
- 상세: [[스케줄러/3_Executor 종류 및 비교]], [[스케줄러/4_Queue 라우팅과 Pod 스펙 설정]]

### 4. 비용 구조 (DCU)

- **모델**: 1 DCU ≈ 1 vCPU-시간 or 1 GB-RAM-시간 (커뮤니티 리버스, 비공식)
- **Floor cost**: 12 DCU/h Small preset ≈ $526/월 (us-central1), ~$631/월 (asia-northeast3 추정)
- **절감 가능**: Worker autoscale, sizedown, 센서 deferrable 전환, DB cleanup, DAG 파싱 최적화, log archival, dev env 셧다운
- **절감 불가**: Spot nodes, CUD (제한적 SKU)
- 상세: [[스케줄러/14_Composer 3 비용 구조]], [[스케줄러/7_0_Composer 비용 한눈에]]

### 5. 실측 스펙 산정 결과

30일 실측 기반 세 시나리오:

| 시나리오 | 월 비용 (GCE) | 월 비용 (Composer) | 월 비용 (GKE) |
|---|---|---|---|
| A. Concurrency=8 그대로 | ~$13.1k | ~$5.0k | ~$3.8k |
| B. Concurrency=4 + deferrable 센서 | ~$4.8k | ~$3.0-3.5k | ~$2.4k |

- **매니저 다운사이징 즉시 안전** (18% CPU, 9% 메모리 사용)
- **워커 메모리 다이어트는 센서 deferrable 전환 후 안전**
- 상세: [[스케줄러/7_1_실제 스펙 산정]], [[스케줄러/7_2_리소스 다이어트 포인트]]

### 6. 권한·인증

- **3-layer 모델**:
  - Layer 1 (UI): GCP IAM → Google SSO (IAP)
  - Layer 2 (Airflow Actions): FAB RBAC (Admin/Op/User/Viewer)
  - Layer 3 (GCP Resources): Workload Identity → 환경 SA → BQ/GCS/Pub/Sub
- **PoC 결과**: 3-layer 정상 동작 확인 (`test-airflow3` 환경)
- 상세: [[스케줄러/8_Composer 권한 및 인증]]

### 7. Airflow Asset

- **Dataset (2.4) → Asset (3.0)** 진화: URI + metadata + AssetAlias + AssetWatcher + `@asset` decorator
- **AssetWatcher**: FileTrigger, PubSubMessageTrigger 등 이벤트 기반 트리거
- **표준 URI**: `bigquery://`, `gs://`, `s3://`, `mysql://`, `postgres://`, `pubsub://`, `kafka://`
- **활용**: ExternalTaskSensor 대체 (cross-DAG dependency 이벤트 기반)
- 상세: [[스케줄러/9_Airflow Asset과 Dataset]]

### 8. DAG 배포 전략

- **선택지**:
  - Jenkins + GCS sync (기존 관성)
  - **GitDagBundle** (Airflow 3 native, PR/version-lock/refresh_interval 설정)
- **우리 선택**: 팀별 저장소 + GCS rsync (`gsutil rsync -c -d`)
  - GitDagBundle 은 미래 옵션 (팀별 refresh_interval 관리 여지)
- 상세: [[스케줄러/11_DAG Bundles와 배포 전략]]

### 9. 메타데이터 DB

- **관리**: Composer 3 이 Cloud SQL PostgreSQL 자동 관리
- **핵심**: **20GB 이하 유지 필수** (그 이상이면 Composer 업그레이드 차단)
- **Retention**: 90일 표준 (`airflow db cleanup`)
- 상세: [[스케줄러/5_Metadata DB 운영]], [[스케줄러/13_Composer 3 환경 업그레이드 정책]]

### 10. Provider 패키지 (Custom Operator)

- **결정**: Artifact Registry Python repo + SemVer + 도메인별 lock
- **패키지**: `apache-airflow-providers-kakaoent-dataplatform`
- **DE 사용**: `pip install` 후 `import` — 오픈소스 provider 와 동일
- **PoC**: 완료 (dp-airflow-provider 저장소)
- 상세: [[스케줄러/7_3_공통 Custom Operator 제공 방안]], [[스케줄러/7_4_DAG + dbt + Operator 3축 배포 통합]]

## PoC 통과 현황

| # | PoC 항목 | 상태 |
|---|---|---|
| 01 | Airflow 3 호환성 스캔 | ✅ |
| 02 | DAG 배포 (Bundle / GCS rsync) | ✅ |
| 03 | Custom operator PyPI (AR) | ✅ |
| 04 | Worker pool / queue | ✅ |
| 08 | Deferrable sensor 검증 | ✅ |
| 09 | Snapshot / rollback | 🧪 진행 |
| 10 | Provider 패키지 표준 형식 | 🧪 진행 |
| Loupe integration | ✅ (dp-airflow-provider) |
| bizberry 4 mart 이관 | ✅ (2026-07) |

## Migration Roadmap

- **Phase 0** (4-6주): 인프라 준비, 지원 체계, OKR 확보, template 배포
- **Phase 1** (4-8주): 얼리어답터 팀 (story-team) 이관, dual-run 검증
- **Phase 2** (2-4주): 추가 팀 확산
- **Phase 3**: Neptune / Actions UI 정리

상세: [[8_실행계획과_안전장치]], [[애슬론/6_마이그레이션 플랜]]

## 관련 문서

- [[스케줄러/0_결론]] — 최종 결론
- [[스케줄러/1_개요]] — 전체 방향
- [[스케줄러/README]] — 스케줄러 폴더 인덱스
