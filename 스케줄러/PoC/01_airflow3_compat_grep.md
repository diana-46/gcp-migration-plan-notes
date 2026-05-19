---
title: "01. airflow-dags Airflow 3 호환성 인벤토리"
status: in-progress
tags:
  - poc
  - airflow3
  - compatibility
created: 2026-05-15
updated: 2026-05-15
---

# 01. airflow-dags Airflow 3 호환성 인벤토리

> 대상: `~/PycharmProjects/airflow-dags` (현재 Airflow **2.3.2**)
> 목표: Composer 3 (Airflow **3.1.7**) 이관 시 무엇이 깨지는지 인벤토리.

## 📊 코드 베이스 개요

| 항목 | 값 |
|---|---|
| 총 `.py` 파일 | ~19,691개 (ETL SQL 컨테이너 / 자동 생성 / 라이브러리 포함) |
| 도메인 폴더 | `ads_revenue`, `berriz`, `crm`, `kakaopage`, `kakaowebtoon`, `melon`, `etl`, `ingestion_watcher`, `global_usa`, ... (DAG 가 도메인별 분산) |
| `operators/` 자체 operator | **14개** 클래스 |
| 현재 Airflow 버전 | 2.3.2 (꽤 옛날 — Airflow 3 까지 큰 점프) |

## ✅ 그대로 / 거의 그대로 (전체적으로 깨끗)

| 패턴 | 사용 여부 | Airflow 3 |
|---|---|---|
| `SubDagOperator` | ❌ **없음** | ✅ 영향 없음 |
| `SmartSensor` | ❌ **없음** | ✅ 영향 없음 |
| `sla=` / `sla_miss_callback` | 거의 없음 (1개 미만) | ✅ 영향 미미 |
| `schedule_interval=` | 5개 파일 (`athlon/airflow.py`, `melon/...`, `ingestion_watcher/...`, tests 2개) | 🟡 `schedule=` 으로 자동 변환 가능 |

→ **Airflow 3 의 hard breaking change 는 거의 없음**. 다행.

## 🟡 손봐야 할 패턴

### B-1. `apply_defaults` deprecated decorator (4곳)

Airflow 2.5+ 부터 deprecated. Airflow 3 에서 제거. 4개 자체 operator 에서 사용:

- `operators/hive_to_slack_operator.py` (Hive → 폐기 영역이라 함께)
- `operators/snapshot_history_operator.py`
- `operators/slack_stat_operator.py` ← 유지 대상
- `operators/athlon_query_sensor.py` ← 유지 대상

→ `@apply_defaults` 데코레이터 제거 + `__init__` 에 `**kwargs` 처리. 각 파일 ~1시간씩.

### B-2. Provider 별도 install 강제

Airflow 3 는 provider 패키지 명시 install 필요. 현재 import 인벤토리:

| Provider | 사용 위치 | 운명 |
|---|---|---|
| `airflow.providers.google.cloud.*` (BQ / GCS) | 많음 ⭐ | ✅ 유지·확대 |
| `airflow.providers.apache.kafka.*` | 다수 | ✅ 유지 (Kafka 계속 사용) |
| `airflow.providers.slack.*` | 다수 | ✅ 유지 |
| `airflow.providers.http.*` | 일부 | ✅ 유지 |
| `airflow.providers.amazon.aws.*` (S3) | 일부 | 🔴 폐기 (S3 안 씀) |
| `airflow.providers.apache.hive.*` | 일부 | 🔴 폐기 (Hive → BQ) |
| `airflow.providers.apache.hdfs.*` | 일부 | 🔴 폐기 |
| `airflow.providers.presto.*` | 일부 | 🔴 폐기 (Presto → BQ) |

→ `requirements.txt` 갱신 작업: ~1시간. PyPI 또는 Artifact Registry 로.

### B-3. `requirements.txt` Airflow 2.3.2 → 3.1.7 점프

```
현재: apache-airflow[mysql,ldap,celery,async,crypto,rabbitmq]==2.3.2
변경: apache-airflow==3.1.7 (Composer 3 기본)
+ apache-airflow-providers-* (별도 install)
```

→ 약 **3년치 마이너 업데이트** 한꺼번에. 일부 라이브러리 (`SQLAlchemy 1.4`, `flask-bcrypt 1.0`) 도 같이 갱신.

## 🔴 폐기 영역 (BQ 이관과 동시에 정리)

| 영역 | 위치 | 운명 |
|---|---|---|
| **Hive 관련 operator** (`HiveOperator`, `HiveServer2ToSlackOperator`, `HiveToGcsSyncOperator`) | 4개 위치 + `operators/hive_*.py` 2개 | 🔴 폐기 |
| **HDFS / S3 ToHdfsSync** | 14개 위치 + `operators/s3_to_hdfs_sync_operator.py` | 🔴 폐기 (S3 안 씀) |
| **Kerberos / keytab** | 2개 위치 | 🔴 폐기 (Hive 인증용) |
| **Presto provider import** | 일부 | 🔴 BQ 로 대체 |

→ **dbt + BQ 이관 작업의 부수 효과로 자동 정리**. 별도 Airflow 3 마이그레이션 작업 아님.

## ⚠️ Task SDK 위반 가능성 (중요)

Airflow 3 의 큰 변화: task 실행 시점에 `from airflow.models import ...` 막힘. **DAG parse 시점은 OK**.

### C-1. `from airflow.models import` 사용 — **17개 파일**

각각 사용 위치가 task 안인지 / 모듈 top-level (parse 시점) 인지 확인 필요. 일부 위치만 점검:

```python
# athlon/airflow.py:13~14 — 모듈 top-level (parse 시점 OK)
from airflow.models import BaseOperator
from airflow.models import Variable
```

→ 대부분 `BaseOperator` / `Variable` import 일 가능성 → top-level 이면 OK. 그래도 17개 파일 한 번씩 점검 필요. **~3~4시간 작업**.

### C-2. SQLAlchemy 직접 사용 — `athlon/airflow.py` ⭐ **가장 큰 issue**

```python
# athlon/airflow.py:585~590
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
db_uri = Variable.get("athlon_db")
engine = create_engine(db_uri, echo=True)
Session = sessionmaker(bind=engine)
session = Session()
workflow = _get_workflow(session, name)
```

**위치**: `airflow_dag(name)` 함수 안 — **DAG parse 시점에 호출됨** (각 DAG 파일이 이 함수로 동적 DAG 생성).

**Airflow 3 호환성**:
- ✅ parse 시점이라 Task SDK 격리에 안 걸림
- ⚠️ **하지만 Composer worker 가 사내 MySQL (`athlon_db`) 에 닿아야 함** — 인프라 의존
- ⚠️ DAG parse 마다 사내 DB 호출 → 네트워크 + 성능 이슈

→ **Composer ↔ 사내망 연결** 또는 **athlon DB → Cloud SQL 이전** 이 PoC 필수 항목.

### C-3. `Variable.get` 사용 — 16개 위치

대부분 위치는 함수 안 / task 안에서 사용. Airflow 3 에서도 deprecated 아니라 가능. 다만 task 안에서 자주 호출하면 부하 ↑ — Connection 캐싱 권장 (사용성 문제, 호환성 X).

## 📋 자체 Operator 14개 인벤토리

| Operator | 분류 | 운명 |
|---|---|---|
| `HiveServer2ToSlackOperator` | Hive | 🔴 폐기 |
| `HiveToGcsSyncOperator` | Hive→GCS (이관 도구) | 🔴 이관 완료 후 폐기 |
| `S3ToHdfsSyncOperator` | S3/HDFS | 🔴 폐기 |
| `SnapshotHistoryOperator` | Hive partition snapshot | 🔴 폐기 (BQ 는 다른 방식) |
| `AthlonQuerySensor` | 사내 athlon DB query | 🟡 유지 + deferrable 화 검토 |
| `BigqueryQuerySensor` | BQ query | 🟡 유지 + deferrable 화 |
| `NabiSignalProduceOperator` | 사내 Nabi 시그널 | 🟡 유지 (`ProduceToTopicOperator` 상속) |
| `SignalProduceOperator` | 사내 시그널 | 🟡 유지 |
| `LoupeSignalProduceOperator` | 사내 Loupe 시그널 | 🟡 유지 |
| `LoupeSignalHttpOperator` | Loupe HTTP | 🟡 유지 (`SimpleHttpOperator` 상속) |
| `LoupeKafkaBatchOperator` | Loupe Kafka | 🟡 유지 (`BashOperator` 상속) |
| `ConsumeFromTopicWithContextOperator` | Kafka consume + context | 🟡 유지 |
| `ThirdPartyPullingOperator` | 외부 API extract | 🟡 유지 |
| `SlackStatOperator` | Slack 통계 알림 | 🟡 유지 |
| `dco_series_thumbnail_collect_operator.py` | 알 수 없음 (별도 확인) | 🟡 확인 필요 |

→ **14개 중 4개 폐기 / 10개 마이그레이션 대상**. 마이그레이션 = PyPI 패키지화 + `apply_defaults` 제거 + Airflow 3 base class 확인.

## 🌐 사내 의존성

| 항목 | 발견 |
|---|---|
| 사내 도메인 하드코딩 (`kakaocorp.com` 등) | 1개 파일 (의외로 적음) |
| 사내 IP 직결 (`10.x` / `192.168.x`) | ❌ 없음 ✅ |
| 사내 DB 연결 | `athlon/airflow.py` 의 `athlon_db` Variable 만 |
| Vault 직결 | 코드 베이스에서 안 보임 (Ansible 측만) |

→ **코드 레벨 사내 의존성은 의외로 적음**. 대부분 환경 변수 / Variable / Ansible 환경 의존.

## 🎯 작업량 추정

| 작업 | 추정 |
|---|---|
| `apply_defaults` 제거 (4 operator) | 4시간 |
| `schedule_interval` → `schedule` (5곳) | 1시간 (자동 변환 도구) |
| Provider import 정리 + requirements.txt 갱신 | 2~3시간 |
| `from airflow.models import` 17개 파일 점검 | 3~4시간 |
| Hive/S3/HDFS 영역 폐기 (operator + DAG) | 1~2주 (BQ 이관과 동시) |
| 자체 operator 10개 PyPI 패키지화 + Airflow 3 호환 검증 | 1~2주 |
| `athlon/airflow.py` SQLAlchemy / 네트워크 검증 | 인프라 의존 (가변) |
| `requirements.txt` 의 모든 의존성 Airflow 3 호환 버전으로 | 1~2일 |
| 전체 테스트 + 디버깅 | 1주 |
| **총 (인프라 협의 제외)** | **3~5주** |

→ [[../2_Cloud Composer vs Self-managed 비교]] §E 에서 추정한 6~12주 와 align.

## 🚨 진짜 큰 미지수 / 회의 안건

1. **사내 MySQL (`athlon_db`) ↔ Composer 연결**
   - DAG parse 마다 호출 — 네트워크 latency / 부하 영향
   - 사내망 ↔ GCP VPC 연결 필수 (Cloud Interconnect / VPN)
   - 또는 athlon DB 자체를 Cloud SQL 로 이관

2. **자체 operator 10개 PyPI 패키지화**
   - 단일 wheel 로 묶을지 / 도메인별 분리할지
   - Artifact Registry 운영 / 버전 관리 절차
   - 사내 Kafka broker / Slack webhook 등 endpoint 접근 가능성

3. **DAG factory 패턴 (`airflow_dag(name)`)**
   - 모든 DAG 가 athlon DB 에서 정의 읽어서 동적 생성
   - 이 패턴 자체를 유지할지, **정적 생성** (athlon 측에서 Python 파일 빌드) 으로 갈지

## ✅ Step 1 결론

| 분류 | 비율 (대략) |
|---|---|
| 🟢 그대로 (표준 패턴) | ~70% |
| 🟡 손봐야 (apply_defaults / provider / schedule_interval 등) | ~15% |
| 🔴 폐기 (Hive / S3 / HDFS / Kerberos) | ~10% |
| ⚪ 인프라 협의 필요 (사내 DB / 네트워크) | ~5% |

→ **Airflow 3 자체로의 코드 호환성은 양호**. 진짜 issue 는 **사내 인프라 연결** 과 **자체 operator 패키징**.

## 다음 step

- [ ] Step 2: Simple DAG Composer 3 실행 (이미 환경 있음)
- [ ] Step 3: 자체 wrapper PyPI 검증 — `SlackStatOperator` 같은 단순한 거 1개 picked
- [ ] Step 5: DAG Bundles
- [ ] Step 7: 실제 athlon DAG (의존성 최소화한 simple 한 것)

## 의문 / TODO

- `dco_series_thumbnail_collect_operator.py` 가 무엇인지 확인
- `Variable.get` 16개 위치 중 task 안 / 함수 호출마다인 곳 있는지 (성능 영향)
- DAG factory (`airflow_dag(name)`) 패턴을 Composer 환경에서 어떻게 — 유지 vs 정적 생성
- 사내 Vault / 인증서 사용처 (Ansible 외 코드에 있는지)

## 관련

- [[README]] — PoC 전체 인벤토리
- [[../2_Cloud Composer vs Self-managed 비교]] §"현 사내 Airflow 셋업 → Composer 3 호환성"
- [[../6_Airflow 2 vs 3 비교]] — Airflow 3 의 호환성 변화
