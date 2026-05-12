---
title: "Metadata DB 운영"
status: draft
tags:
  - confluence
  - airflow
  - 스케줄러
created: 2026-05-11
updated: 2026-05-11
source: https://kakaoent.atlassian.net/wiki/spaces/DP/pages/5068816679/Airflow+Metadata+DB
---

# Metadata DB 운영

> Airflow 자체가 사용하는 메타스토어 DB의 운영/접근 가이드.

## DB에 저장되는 것

- DAG 정의 (parsed)
- DAG runs (실행 이력)
- Task instances (각 task 실행 상태)
- Pool, Variable, Connection
- User/Role/Permission (RBAC)
- XCom (task 간 데이터 전달)
- Audit log

**PostgreSQL이 표준**. (MySQL 가능하지만 권장 X, SQLite는 dev용)

## Composer vs Self-managed

| 항목 | Cloud Composer 2 | Self-managed |
| --- | --- | --- |
| DB 종류 | Cloud SQL PostgreSQL 자동 | Cloud SQL / AlloyDB / Self-hosted |
| 운영 | GCP가 백업/패치/HA | 우리가 결정 |
| 사이즈 변경 | 환경 패키지 단위 | 자유 |
| 직접 접근 | 가능 (`gcloud composer ... db shell`) | 자유 |
| 비용 | 환경 패키지에 포함 | $100~$400/월 (Cloud SQL 기준) |
| PgBouncer | 자동 포함 | 직접 구성 |

## Self-managed DB 옵션

| 옵션 | 장점 | 단점 |
| --- | --- | --- |
| **Cloud SQL PostgreSQL** (추천) | HA, 자동 백업, 패치 | 비용 ($100~$400/월) |
| **AlloyDB** | 더 빠른 성능 | 비쌈, Airflow 호환성 추가 검증 필요 |
| **Self-hosted on GKE** | 저렴 (노드 비용만) | HA/백업/패치 직접. **권장 X** (critical 시스템) |

## SELECT는 권장, Write는 X

"DB 직접 접근 권장 X"는 **INSERT/UPDATE/DELETE/DDL**을 말함. **SELECT는 적극 권장**.

✅ SELECT (디버깅, 통계, count) — 권장
✅ Read-only 유저로 분석 — 최고
❌ INSERT/UPDATE/DELETE — Airflow 내부 상태 깨질 위험
❌ Schema 변경 (ALTER TABLE) — 절대 X
❌ 트랜잭션 길게 잡기 — Scheduler lock 충돌

## 자주 쓰는 쿼리

### 실패한 task 빠르게 찾기

sqlSELECT dag\_id, task\_id, execution\_date, state, duration
FROM task\_instance
WHERE state = 'failed'
AND execution\_date > NOW() - INTERVAL '7 days'
ORDER BY execution\_date DESC LIMIT 100;

### DAG별 실패율

sqlSELECT
dag\_id,
COUNT(\*) FILTER (WHERE state = 'failed') AS failed,
COUNT(\*) FILTER (WHERE state = 'success') AS success,
ROUND(100.0 \* COUNT(\*) FILTER (WHERE state = 'failed') / COUNT(\*), 2) AS failure\_rate
FROM task\_instance
WHERE execution\_date > NOW() - INTERVAL '30 days'
GROUP BY dag\_id
ORDER BY failure\_rate DESC;

### task별 평균 실행 시간

sqlSELECT dag\_id, task\_id, COUNT(\*), AVG(duration), MAX(duration)
FROM task\_instance
WHERE state = 'success' AND execution\_date > NOW() - INTERVAL '7 days'
GROUP BY dag\_id, task\_id
ORDER BY AVG(duration) DESC LIMIT 50;

### 현재 돌고 있는 task

sqlSELECT dag\_id, task\_id, execution\_date, start\_date, hostname, state
FROM task\_instance
WHERE state IN ('running', 'queued')
ORDER BY start\_date;

## Composer에서 DB 접근

bash# PostgreSQL shell 직접
gcloud composer environments run my-env \
--location=us-central1 \
db shell
# 또는 Cloud SQL Auth Proxy + DBeaver/DataGrip 등 GUI 클라이언트

## Read-only 유저 권장

sqlCREATE USER airflow\_readonly WITH PASSWORD 'xxx';
GRANT CONNECT ON DATABASE airflow TO airflow\_readonly;
GRANT USAGE ON SCHEMA public TO airflow\_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO airflow\_readonly;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT SELECT ON TABLES TO airflow\_readonly;

→ 디버깅/분석은 이 유저로. 실수로 write 못 함.

## 정기 Cleanup 필수

3~6개월마다 안 하면 DB 사이즈 폭증, query 느려짐.

bash# Self-managed
airflow db clean --clean-before-timestamp '2025-01-01' \
--tables dag\_run task\_instance xcom job
# Composer
gcloud composer environments run my-env --location=us-central1 \
airflow -- db clean --clean-before-timestamp '2025-01-01' \
--tables dag\_run task\_instance xcom job

## 사이즈 선택 기준

| 규모 | DB 사양 | 비고 |
| --- | --- | --- |
| Small (~50 DAG, ~10k task/day) | 2vCPU, 7.5GB |  |
| Medium (~200 DAG, ~100k task/day) | 4vCPU, 15GB |  |
| Large (~500+ DAG, ~1M task/day) | 8vCPU+, 30GB+ | PgBouncer 필수, Cleanup 자동화 |

## 운영 모니터링

체크할 메트릭:
- DB CPU 사용률
- Connection 수 (max 대비)
- Active queries
- DB 사이즈 증가율
- Replication lag (HA)
- Disk IOPS

## 관련 문서

- [[3_Executor 종류 및 비교]]
- [[2_Cloud Composer 2 vs Self-managed 비교]]
- [[4_Queue 라우팅과 Pod 스펙 설정]]
