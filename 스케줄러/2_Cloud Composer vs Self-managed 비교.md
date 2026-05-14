---
title: "Cloud Composer vs Self-managed 비교"
status: draft
tags:
  - confluence
  - airflow
  - 스케줄러
created: 2026-05-11
updated: 2026-05-14
source: https://kakaoent.atlassian.net/wiki/spaces/DP/pages/5068260232/Airflow+Cloud+Composer+2+vs+Self-managed
---

# Cloud Composer vs Self-managed 비교

> GCP 이관 시 managed Airflow(Composer)를 쓸지, GKE에 직접 띄울지 결정하기 위한 비교.
>
> **2026년 시점 기본 가정은 Composer 3 (= Airflow 3)**. Composer 2는 EOL 흐름이므로 신규 환경 검토 대상 아님. 버전별 차이는 [[6_Airflow 2 vs 3 비교]] 참고.

## 같은 것

- Airflow 코어 (UI, DAG 작성 방식, Operator/Sensor/Hook)
- Connection / Variable / Pool 개념
- Plugin 시스템 (Airflow 3 기준으로 일부 변경)
- TaskFlow API (`@task`)

→ 표준적인 DAG 코드를 그대로 옮겨도 동작 (단, Airflow 2 → 3 마이그레이션은 별도 작업 — [[6_Airflow 2 vs 3 비교]]).

## 다른 것: 주변 인프라/운영

| 항목 | Cloud Composer 3 | Self-managed (GKE) |
|---|---|---|
| Executor 선택 | CeleryKubernetes 기본 (사실상 고정) | 자유 |
| Python 패키지 | PyPI 가능, 시스템 라이브러리 제약 | 자유 (Dockerfile 직접) |
| DAG 배포 | GCS bucket sync + DAG Bundles 지원 | git-sync sidecar / PV / 이미지 포함 / DAG Bundles |
| 메타스토어 DB | Cloud SQL 자동 (PostgreSQL) | 직접 운영 (Cloud SQL / AlloyDB / self-hosted) |
| airflow.cfg | 일부 lock | 완전 자유 |
| 모니터링/로깅 | Cloud Logging/Monitoring 자동 | 직접 구성 |
| 네트워크/보안 | Private IP, IAP, Workload Identity 자동 | 직접 구성 |
| 업그레이드 | GCP가 관리 | 직접 |
| 백업/복구 | Snapshot 자동 | 직접 구성 |
| Worker queue 분리 | ❌ 어려움 (단일 worker deployment) | ✅ 자유 |
| 비용 구조 | 컴포넌트별 vCPU·mem·시간 단위 | 노드+DB 비용 (실제 사용량) |
| 권한 모델 | IAM → Airflow Role **자동 매핑** (Composer 3 강점) | 직접 RBAC + IDP 연동 |
| Multi-tenancy | 환경 분리로 처리 (비용 ↑) | 단일 클러스터 내 자유 |

> Composer 2 → 3에서의 변경은 [[6_Airflow 2 vs 3 비교]] 참고. 핵심만 추리면: 과금이 "환경 패키지"에서 "컴포넌트 단위"로, 권한 매핑 자동화 강화, DAG Bundles 지원, Asset/AssetWatcher 기반 trigger.

## Composer가 자동화한 것

- GKE 클러스터 생성/관리
- Airflow 설치/설정
- Cloud SQL 메타스토어
- Webserver / Scheduler / Worker / Triggerer 배포
- Network 설정 (Private IP / IAP)
- Auto-scaling
- Cloud Logging / Monitoring 통합
- Secret Manager backend 연동
- Workload Identity 설정 (환경 SA)
- DAG 폴더 GCS sync + DAG Bundles
- 백업 / 복구 (Snapshot)
- 보안 패치 / 업그레이드
- IAP 인증 + (Composer 3) IAM ↔ Airflow Role 자동 매핑

## Composer가 막아둔 것

- 시스템 패키지(`apt-get`) 자유 설치 어려움
- Dockerfile 완전 제어 불가 (커스텀 이미지에 제약)
- Executor 자유 선택 불가 (CeleryKubernetes 사실상 고정)
- airflow.cfg 100% 자유 X (일부 옵션 lock)
- DB 직접 INSERT/UPDATE 제한 (SELECT는 가능)
- 네트워크 100% 자유 X
- 비표준 storage 사용 어려움
- **Celery worker queue별 분리 어려움** (단일 worker deployment)
- Multi-tenancy를 단일 환경에서 처리 어려움 → 환경 분리 → 비용 ↑

## Composer가 CeleryKubernetesExecutor를 쓰는 이유

빠른 task (대부분):
- Celery 워커가 즉시 실행
- Pod 생성 오버헤드 X

무거운 task (선택):
- `queue='kubernetes'` 지정 시 Pod로 분리 실행

→ "best of both worlds" 패턴
→ Redis (Memorystore) 필요

상세 — [[3_Executor 종류 및 비교]], [[4_Queue 라우팅과 Pod 스펙 설정]]

## 결정 기준

| 상황 | 추천 |
|---|---|
| 운영 인력 부족 / 빠른 마이그레이션 필요 | **Cloud Composer 3** |
| GCP 다른 서비스 깊이 활용 (Workload Identity, IAP, Secret Manager 자동) | Cloud Composer 3 |
| 권한 / IDP 관리 부담 최소화 | Cloud Composer 3 (특히 3은 IAM↔Role 자동 매핑) |
| 패키지 / Executor / 네트워크 완전 자유 | Self-managed |
| **Worker queue 분리** 필요 | Self-managed |
| Multi-tenancy (한 환경에 여러 팀) 필요 | Self-managed |
| 비용 절감 1순위 + 운영 인력 충분 | Self-managed (단, 인건비 고려 필수 — [[7_Composer 비용]]) |
| 사내 IDP 깊은 커스터마이즈 필요 | Self-managed |

> 비용 / 권한 측면 상세 — [[7_Composer 비용]], [[8_Composer 권한 및 인증]].
> 버전 결정 — [[6_Airflow 2 vs 3 비교]].

## 관련 문서

- [[1_개요]]
- [[3_Executor 종류 및 비교]]
- [[4_Queue 라우팅과 Pod 스펙 설정]]
- [[5_Metadata DB 운영]]
- [[6_Airflow 2 vs 3 비교]]
- [[7_Composer 비용]]
- [[8_Composer 권한 및 인증]]
