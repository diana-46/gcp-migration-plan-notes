---
title: "Cloud Composer vs Self-managed 비교"
status: draft
tags:
  - confluence
  - airflow
  - 스케줄러
created: 2026-05-11
updated: 2026-05-15
source: https://kakaoent.atlassian.net/wiki/spaces/DP/pages/5068260232/Airflow+Cloud+Composer+2+vs+Self-managed
---

# Cloud Composer vs Self-managed 비교

> GCP 이관 시 managed Airflow(Composer)를 쓸지, GKE에 직접 띄울지 결정하기 위한 비교.
>
> 기준: **Composer 3 (= Airflow 3)**.

## 같은 것

- Airflow 코어 (UI, DAG 작성 방식, Operator/Sensor/Hook)
- Connection / Variable / Pool 개념
- Plugin 시스템
- TaskFlow API (`@task`)

→ 표준적인 DAG 코드를 그대로 옮겨도 동작.

## 다른 것: 주변 인프라/운영

| 항목 | Cloud Composer | Self-managed (GKE) |
|---|---|---|
| Executor 선택 | `CeleryExecutor,KubernetesExecutor` 멀티 구성 (Celery default, 변경 어려움) | 자유 |
| Python 패키지 | PyPI 가능, 시스템 라이브러리 제약 | 자유 (Dockerfile 직접) |
| DAG 배포 | GCS bucket sync + DAG Bundles | git-sync sidecar / PV / 이미지 포함 / DAG Bundles |
| 메타스토어 DB | Cloud SQL 자동 (PostgreSQL 고정, 선택 불가) | 직접 운영 (Cloud SQL / AlloyDB / self-hosted) |
| airflow.cfg | 일부 lock | 완전 자유 |
| 모니터링/로깅 | Cloud Logging/Monitoring 자동 | 직접 구성 |
| 네트워크/보안 | Private IP, IAP, Workload Identity 자동 | 직접 구성 |
| 업그레이드 | GCP가 관리 | 직접 |
| 백업/복구 | Snapshot 자동 | 직접 구성 |
| Worker queue 분리 | ❌ 어려움 (단일 worker deployment) | ✅ 자유 |
| 비용 구조 | 컴포넌트별 vCPU·mem·시간 단위 | 노드+DB 비용 (실제 사용량) |
| 권한 모델 | IAM → Airflow Role 자동 매핑 | 직접 RBAC + IDP 연동 |
| Multi-tenancy | 환경 분리로 처리 (비용 ↑) | 단일 클러스터 내 자유 |

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
- IAP 인증 + IAM ↔ Airflow Role 자동 매핑

## Composer가 막아둔 것

- 시스템 패키지(`apt-get`) 자유 설치 어려움
- Dockerfile 완전 제어 불가 (커스텀 이미지에 제약)
- Executor 자유 선택 불가 (`CeleryExecutor,KubernetesExecutor` 멀티 구성 사실상 고정)
- airflow.cfg 100% 자유 X (일부 옵션 lock)
- DB 직접 INSERT/UPDATE 제한 (SELECT는 가능)
- 네트워크 100% 자유 X
- 비표준 storage 사용 어려움
- **Celery worker queue별 분리 어려움** (단일 worker deployment)
- Multi-tenancy를 단일 환경에서 처리 어려움 → 환경 분리 → 비용 ↑

## Composer가 Celery + Kubernetes 멀티 executor를 쓰는 이유

설정: `executor = CeleryExecutor,KubernetesExecutor` (앞이 default)

빠른 task (대부분):
- Celery 워커가 즉시 실행
- Pod 생성 오버헤드 X

무거운 task (선택):
- task에 `executor="KubernetesExecutor"` 지정 시 Pod로 분리 실행
- (옛 호환) `queue='kubernetes'` 도 동작

→ "best of both worlds" 패턴
→ Redis (Memorystore) 필요

상세 — [[3_Executor 종류 및 비교]], [[4_Queue 라우팅과 Pod 스펙 설정]]

## 결정 기준

| 상황 | 추천 |
|---|---|
| 운영 인력 부족 / 빠른 마이그레이션 필요 | **Cloud Composer** |
| GCP 다른 서비스 깊이 활용 (Workload Identity, IAP, Secret Manager 자동) | Cloud Composer |
| 권한 / IDP 관리 부담 최소화 | Cloud Composer (IAM↔Airflow Role 자동 매핑) |
| 패키지 / Executor / 네트워크 완전 자유 | Self-managed |
| **Worker queue 분리** 필요 | Self-managed |
| Multi-tenancy (한 환경에 여러 팀) 필요 | Self-managed |
| 비용 절감 1순위 + 운영 인력 충분 | Self-managed (단, 인건비 고려 필수 — [[7_Composer 비용]]) |
| 사내 IDP 깊은 커스터마이즈 필요 | Self-managed |

> 비용 / 권한 측면 상세 — [[7_Composer 비용]], [[8_Composer 권한 및 인증]].

---

## 현 사내 Airflow 셋업 → Composer 3 호환성

> 분석 대상: `~/WebstormProjects/data-platform-settings/playbooks/roles/airflow2` (Ansible role)

### 현재 셋업 요약

| 영역              | 현재                                                                                              |
| --------------- | ----------------------------------------------------------------------------------------------- |
| 설치              | pyenv venv + pip + Apache 공식 constraints. 사내 proxy (`proxy-ay.onkakao.net:3128`) 통과             |
| 메타 DB           | 사내 MySQL 직결                                                                                     |
| Celery broker   | 사내 RabbitMQ (AMQP)                                                                              |
| 인증              | LDAP (`iam-ldap.kakaocorp.com`) via `webserver_config.py`                                       |
| DAG 배포          | 사내 `github.kakaocorp.com` SSH + Vault 에서 키 fetch + git-sync                                     |
| Worker queue 5종 | `hadoop:6` / `cloud:6` / `http:5` / `sensor:40` / `doopey:2 (×3 variant)` — systemd 로 worker 별도 |
| 프로세스 관리         | systemd (`airflow-scheduler` / `webserver` / `worker-*` / `kerberos`)                           |
| 시스템 패키지         | JDK 11, Hadoop client, Kerberos, cyrus-sasl 등 yum 설치                                            |
| 사내 환경변수         | `HADOOP_HOME` / `HIVE_HOME` / `SPARK_HOME` / `JAVA_HOME` 사내 경로                                  |
| 모니터링            | statsd → 사내 `airflow_exporter:8125`                                                             |
| Cleanup         | `clean_db_logs.sh`, `clean_logs.sh` 시스템 crontab                                                 |

### A. 자연 해소 (Composer 든 Self-managed 든 어차피 사라짐)

| 항목                                    | 이유                                                               |
| ------------------------------------- | ---------------------------------------------------------------- |
| **Kerberos** (Hadoop/Hive 인증용)        | Hive 폐기 → 자연 사라짐                                                 |
| **Hadoop / Hive / Spark 경로 / JDK 11** | BQ 이관 → 모두 불필요                                                   |
| **`hadoop`, `doopey` worker queue**   | Hive 영역 폐기                                                       |
| **시스템 패키지 직접 설치 (yum)**               | dbt-bigquery 면 시스템 의존 거의 없음. JDK/Hadoop client 가 빠지면 PyPI 만으로 충분 |
| **MySQL 메타 DB**                       | Cloud SQL PostgreSQL 로 표준 이전                                     |
| **logrotate / 시스템 crontab cleanup**   | Cloud Logging 자동 + `airflow db clean` DAG 패턴                     |
| **systemd 프로세스 관리**                   | 양쪽 다 k8s 위에서 동작                                                  |

→ Hive/하둡 시대 부산물이 대거 정리됨. **이게 dbt+BQ 이관의 부수 효과**.

### B. ⚠️ 검증 필요 (회의 안건)

| # | 항목 | Composer 3 | Self-managed |
|---|---|---|---|
| 1 | **사내 LDAP 인증** | ❌ → **IAP + Google IAM 강제**. Google Workspace 로 통과 가능한지 확인 필요 | ✅ Okta/OIDC 가능 ([[8_Composer 권한 및 인증]]) |
| 2 | **사내 git (`github.kakaocorp.com`) SSH + Vault** | DAG 배포 흐름 통째로 변경. GCS sync 또는 DAG Bundles + Secret Manager | git-sync sidecar 그대로 가능 (사내 git 접근 패턴 유지) |
| 3 | **사내망 ↔ GCP VPC 연결** | Cloud Interconnect / VPN / Private Service Connect 필요 (양쪽 동일) | 동일 |
| 4 | **사내 PyPI / wheel 직접 설치** | Artifact Registry private repo 통과. 사내 PyPI mirror 필요 | 자유 |
| 5 | **Worker queue 5종 → 패턴 전환** | `hadoop`/`doopey` 폐기, `cloud`/`http` 는 Celery, **`sensor:40` 은 deferrable Sensor 로 전환** ⭐ | queue 별 worker 그대로 유지 가능 |
| 6 | **Custom plugin / operator 배포** | GCS `plugins/` sync 또는 PyPI 패키지화 | 기존 Ansible 패턴 유지 가능 |
| 7 | **사내 proxy 환경변수** (`HTTP_PROXY`/`NO_PROXY`) | Composer 환경변수 또는 사내망 ↔ VPC 직접 통신 | sysconfig 그대로 |
| 8 | **사내 statsd 모니터링 통합** | Cloud Monitoring 으로 대체 (사내 dashboard 통합 필요) | statsd 그대로 |

### C. 🟢 자연 통과 (Composer 가 알아서)

| 항목 | Composer |
|---|---|
| 메타 DB 운영 / 백업 / 패치 | 자동 (Cloud SQL) |
| 로그 수집 / 보관 | Cloud Logging 자동 |
| Worker / Scheduler 프로세스 관리 | k8s 자동 |
| DAG-level 동시성 (parallelism, parsing_processes) | 환경변수로 통과 |
| timezone, default_view 등 일반 cfg 옵션 | 통과 |
| Celery broker (RabbitMQ) | Memorystore Redis 자동 |
| statsd | Cloud Monitoring 자동 |

### D. 결론에 영향 주는 핵심 미지수 (회의 전 확인 1순위)

> 이 두 가지 답에 따라 Composer vs Self-managed 결정의 80% 가 나옴:

1. **사내 LDAP 인증을 Google Workspace 로 대체 가능한가?**
   - Yes → Composer (IAP+IAM) 매끄러움
   - No → Self-managed + Okta/OIDC

2. **사내망 ↔ GCP VPC 연결 (Cloud Interconnect/VPN) 가능한가?**
   - Yes → 사내 시스템 (Slack/Loupe/Nabi 등) 통신 OK
   - No → 외부 노출된 endpoint 만 사용 가능 (제약)

### E. 마이그레이션 작업량 추정 (Composer 채택 시)

| 작업 | 추정 |
|---|---|
| `hadoop`/`doopey` 관련 DAG/operator 폐기 | 2~4주 (dbt 변환과 동시) |
| `sensor` 워커 → deferrable Sensor 전환 (concurrency 40 → triggerer) | 1~2주 |
| Custom plugin/operator 인벤토리 + PyPI 패키지화 | 2~3주 |
| LDAP → IAP 권한 모델 재설계 | 1~2주 |
| 사내 git → GCS sync (or DAG Bundles) 흐름 | 1주 |
| 사내망 VPC 연결 설계 + 보안 검증 | 인프라 팀 협의 (가변) |
| **총 추정** (인프라 협의 제외) | **6~12주** |

→ Self-managed 면 인증·git·worker queue 부분이 거의 그대로라 마이그레이션 작업량 **~50% 적음**. 단, 운영 부담은 영구.

## 관련 문서

- [[1_개요]]
- [[3_Executor 종류 및 비교]]
- [[4_Queue 라우팅과 Pod 스펙 설정]]
- [[5_Metadata DB 운영]]
- [[7_Composer 비용]]
- [[8_Composer 권한 및 인증]]
