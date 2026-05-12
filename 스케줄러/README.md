# 스케줄러 — 컨텍스트

> 이 폴더는 **GCP 이관 시 Airflow를 어떻게 운영할지** 결정하기 위한 리서치 자료를 모은다.
> 결론 노트는 [[1_개요]].

---

## 풀고자 하는 문제 / 의사결정

GCP로 데이터플랫폼을 이관할 때, Airflow 스케줄러를:

1. **Cloud Composer 2** (managed) 로 띄울 것인가
2. **Self-managed Airflow on GKE** 로 직접 띄울 것인가

부수적으로 따라오는 결정:
- Executor 선택 (CeleryKubernetes / Kubernetes 단독 등)
- Worker queue 분리 전략
- Metadata DB 운영 방식

---

## 용어 / 약어

| 용어 | 의미 |
|---|---|
| **Executor** | Airflow에서 task가 실제로 어디서 어떻게 실행되는지 결정하는 컴포넌트 |
| **Celery worker** | Redis 큐에서 task 받아 상시 떠있는 워커 프로세스에서 실행 |
| **K8s Pod (task=pod)** | task마다 새 Pod를 띄워서 실행 (격리 ↑, 오버헤드 10~30초) |
| **Queue** | task가 어느 워커 그룹으로 갈지 라우팅하는 이름. `queue='kubernetes'`면 Pod로 |
| **Pool** | Airflow의 동시 실행 슬롯 제한 메커니즘 (UI Admin → Pools) |
| **Composer** | GCP의 managed Airflow 서비스 (Cloud Composer 2) |
| **Self-managed** | GKE 위에 직접 Airflow를 설치해 운영하는 방식 |
| **Memorystore** | GCP의 managed Redis (Composer가 Celery용으로 자동 프로비저닝) |
| **AlloyDB** | GCP의 PostgreSQL 호환 고성능 DB (Metadata DB 옵션 중 하나) |
| **PgBouncer** | PostgreSQL 커넥션 풀러 (대규모 Airflow에서 거의 필수) |
| **Workload Identity** | GKE Pod에 GCP IAM SA를 매핑하는 방식. Composer는 자동 구성 |

---

## 외부 자료

### Confluence

- **DP space — 스케줄러 폴더**: https://kakaoent.atlassian.net/wiki/spaces/DP/folder/5067145573
  - 이 폴더의 페이지들은 [[2_Cloud Composer 2 vs Self-managed 비교]], [[3_Executor 종류 및 비교]], [[4_Queue 라우팅과 Pod 스펙 설정]], [[5_Metadata DB 운영]]로 import 완료

### 관련 코드 레포 (로컬 경로)

- **airflow-dags**: `~/PycharmProjects/airflow-dags`
  - 현재 운영 중인 DAG. 이관 대상.
- **athlon**: `~/IdeaProjects/athlon`
  - Workflow/Action UI 백엔드. `neptune` 모듈에 `task` 정의 있음.
- **athlon-ui**: `~/WebstormProjects/athlon-ui`
  - Workflow/Action UI 프론트엔드.

### 공식 문서

- [Cloud Composer 2 공식 문서](https://cloud.google.com/composer/docs)
- [Airflow Executors](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/executor/index.html)
- [Airflow on Kubernetes (Helm chart)](https://airflow.apache.org/docs/helm-chart/stable/index.html)

---

## Stack / 환경

| 항목                      | 현재               | 이관 후 (검토 중)                                           |
| ----------------------- | ---------------- | ----------------------------------------------------- |
| Airflow 버전              | 2.x              | 최신 버전                                                 |
| 실행 환경                   | on-prem k8s (추정) | GCP (Composer 또는 GKE)                                 |
| Metadata DB             | (현재 운영 DB)       | Cloud SQL PostgreSQL                                  |
| Message Queue (Celery용) | (현재 Redis)       | Memorystore (Composer 자동) 또는 자체 Redis                 |
| DAG 배포                  | (현재 방식)          | GCS bucket sync (Composer) 또는 git-sync (Self-managed) |

---

## 로컬 규칙

이 폴더에서만 추가로 지키는 규칙:

- **비교 노트는 항상 동일한 평가 축으로**: 운영 부담 / 자유도 / Queue 분리 / 비용 / 업그레이드 / GCP 통합 / 마이그레이션 속도. ([[2_Cloud Composer 2 vs Self-managed 비교]] 의 결정 기준 표 참고)
- **PoC 항목은 [[1_개요]] 의 체크리스트에 누적**해서 적는다. 각 자료 노트에 흩어놓지 않는다.
- **비용 추정은 USD/월 단위로 통일**. KRW 환산은 부가 정보.

---

## Anti-context (검토 안 함)

- **AWS MWAA**: GCP 확정이라 검토 대상 아님.
- **Airflow 1.x**: 현 운영 버전이 2.x, 이관 후에도 유지.
- **자체 스케줄러 개발**: 검토 대상 아님 (Airflow 유지).

---

## 후속 계획 (이 폴더 외부와 연결될 가능성)

- 결정 후 실제 이관 작업 폴더가 생기면 별도 카테고리로 분리 (예: `airflow-migration/`).
- `athlon` / `athlon-ui` 리팩토링은 별도 폴더 ([[애슬론]] 같은 이름)로 분리. 이 폴더는 **인프라 결정만** 다룬다.
