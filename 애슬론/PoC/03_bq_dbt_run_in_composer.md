---
title: "03. dbt-bigquery + 실제 dbt run on Composer"
status: in-progress
tags:
  - poc
  - dbt
  - cosmos
  - composer3
  - bigquery
created: 2026-05-21
updated: 2026-05-21 (Phase 3 — 3-5 까지 완료)
---

# 03. dbt-bigquery + 실제 dbt run on Composer

> **검증 질문**: [[02_dbt_render_in_composer]] 에서 확인한 Composer + Cosmos 인프라 패턴을 **BQ adapter + 실제 dbt run** 으로 확장 가능한가?
>
> **상태**: 🚧 진행 중. 로컬 dbt-bigquery 셋업 단계.
>
> ### 🎯 본 PoC 가 답해야 할 것
>
> 1. 로컬 dbt-bigquery 가 우리 BQ 환경에서 동작 (auth / dataset / location)
> 2. Cosmos `DbtTaskGroup` + `LoadMode.DBT_MANIFEST` 패턴이 **mock 이 아니라 실제 dbt run** 까지 통과
> 3. **사내 musicdata 팀의 운영 패턴** (레포 분리 / DbtConfigFactory / target 분기 / manifest CI) 이 BQ adapter 로 그대로 이식 가능
> 4. Trino → BQ 매핑 시 dbt 모델/macro 차원에서 무엇이 바뀌어야 하는가 (incremental, partition, schema 분기)

---

## 환경

| 항목 | 값 |
|---|---|
| GCP project | `dev-dp-project-354904` |
| Dataset (개인 dev) | `dbt_test` |
| Location | `asia-northeast3` (서울) — dataset 실제 location 기준으로 profiles.yml 정정 완료 |
| Composer 환경 | `test-airflow3` (composer-3-airflow-3.1.7-build.9) — [[02_dbt_render_in_composer]] 와 공유 |
| dbt 버전 | 1.9.x |
| Adapter | **dbt-bigquery** |
| 인증 | 로컬: OAuth (`gcloud auth application-default login`) / 운영: Service Account 또는 Workload Identity |
| 로컬 dbt 프로젝트 | `~/PycharmProjects/dbt-test/dbt_test/` (dbt repo) |
| 로컬 Airflow DAGs | `~/PycharmProjects/dbt-test-airflow-dags/dags/` (DAG repo, **사내 컨벤션 따라 분리**) |

### profiles.yml (현재 — dev + composer 2 target)

프로젝트 안 (`~/PycharmProjects/dbt-test/dbt_test/profiles.yml`) 에 둠. 운영 컨벤션 (`--profiles-dir .`).

```yaml
dbt_test:
  target: dev
  outputs:
    dev:                                    # 로컬 (OAuth via gcloud)
      type: bigquery
      method: oauth
      project: dev-dp-project-354904
      dataset: dbt_test
      location: asia-northeast3
      threads: 4
      job_execution_timeout_seconds: 300
      job_retries: 1
      priority: interactive

    composer:                               # Composer worker (ADC via attached SA)
      type: bigquery
      method: oauth                         # 같은 method, ADC 가 SA credential 자동 잡음
      project: dev-dp-project-354904
      dataset: dbt_test
      location: asia-northeast3
      threads: 4
      job_execution_timeout_seconds: 300
      job_retries: 1
      priority: interactive
```

**핵심**: 두 target 모두 `method: oauth`. 환경의 ADC (Application Default Credentials) 가 다른 자격증명을 자동으로 잡아줌:
- 로컬 → 본인 gcloud OAuth 토큰
- Composer → attached SA (`dev-dp-airflow@...`)

### Composer 환경

| 항목 | 값 |
|---|---|
| Composer 환경 | `test-airflow3` (composer-3-airflow-3.1.7-build.9) |
| Region | `asia-northeast3` |
| Attached SA | `dev-dp-airflow@dev-dp-project-354904.iam.gserviceaccount.com` |
| SA 권한 | **BigQuery 관리자**, BigQuery 읽기 세션 사용자, 스토리지 객체 사용자, Composer 작업자, 보안 비밀 관리자 뷰어 등 |
| Composer DAGs bucket | `gs://asia-northeast3-test-airflow3-xxxxx-bucket/dags/` |
| PyPI 패키지 | `astronomer-cosmos`, `dbt-core==1.9.3`, `dbt-bigquery==<로컬과_동일>`, `kakao-airflow-poc==0.1.0`, `keyrings.google-artifactregistry-auth` |

---

## 사내 reference 패턴 (그대로 차용)

[[02_dbt_render_in_composer]] 와 `~/PycharmProjects/{music-airflow-dags, mlb-dbt}` 분석을 통해 검증된 패턴들. **BQ 이관 후에도 동일 컨벤션으로 도입.**

### 1. 레포 분리

```
airflow-dags repo (1개, 통합)        ← Airflow DAG
  └── <service>/<dag>.py
dbt repos (서비스별 N개)             ← dbt project
  └── <service>-dbt/
        ├── models/
        ├── dbt_project.yml
        ├── profiles.yml
        └── packages.yml
```

→ 사내 musicdata 팀이 `music-airflow-dags` + `mlb-dbt` + (`melon-dbt` 등) 으로 운영 중. dbt 변경이 Airflow 배포 안 흔듦. CI 분리. 권한 분리.

### 2. Cosmos `DbtTaskGroup` + `LoadMode.DBT_MANIFEST`

[[02_dbt_render_in_composer]] 에서 mock 으로 검증. 본 PoC 에서 BQ 실제 실행으로 확장.

핵심 결정:
- `DbtDag` 아님 → `DbtTaskGroup` (다른 task 와 합칠 수 있음, 예: Kafka exporter)
- `LoadMode.DBT_MANIFEST` → 런타임 `dbt ls` 없음. DAG parse 빠름. **CI 에서 manifest 사전 생성 필수.**

### 3. `DbtConfigFactory` 추상화

사내 `utils/DbtConfigFactory.get_dbt_configs(...)` 패턴. ProfileConfig / ProjectConfig 생성을 한 곳에서 책임. 매 DAG 에서 반복하지 않음.

→ BQ 버전으로 새로 구현 필요 (인증 방식이 LDAP env → SA/WI 로 바뀌므로).

### 4. `target.name` 기반 schema 분기

```yaml
+schema: "{{ env_var('MLB_BRONZE_SCHEMA') ~ ('_dev' if target.name == 'dev' else '_production') }}"
```

→ BQ 에서도 동일 적용 가능. `mlb_bronze_dev` / `mlb_bronze_production` dataset suffix 패턴.

### 5. 운영 부속

| 항목 | 사내 (Trino) | BQ 이관 후 |
|---|---|---|
| 알림 콜백 | `failure_alert(SenderConfig.watch_center(...))` | 동일 (WatchCenter 유지) 또는 Slack |
| dbt warning → fail | `fail_on_dbt_test_warnings` | 동일 (`on_warning_callback`) |
| 동시성 제어 | Airflow Pool (`dbt_small`, 3 slots) | 동일 (Composer Pool 동작 확인됨 — [[../../스케줄러/PoC/04_worker_pool_queue]]) |
| dbt 코드 sync | S3 sync (`s3cmd`) | **GCS sync** (Composer native `/home/airflow/gcs/dags/`) |
| 환경별 분리 | `target.name` (dev/production) | 동일 + 옵션으로 GCP project 분리 검토 |

---

## AS-IS → TO-BE 매핑 (Presto/HDFS → BigQuery/GCS)

### 그대로 가져갈 것 ✅ (운영 패턴)

| 패턴 | 비고 |
|---|---|
| 레포 분리 (Airflow / dbt 서비스별) | |
| `DbtTaskGroup` + `LoadMode.DBT_MANIFEST` | |
| `DbtConfigFactory` 추상화 | BQ 버전으로 재구현 |
| Pool 기반 동시성 제어 | |
| `fail_on_dbt_test_warnings` 콜백 | |
| WatchCenter / 사내 알림 콜백 | |
| `target.name` 기반 schema 분기 | |
| CI 에서 manifest 사전 컴파일 | |
| dev / production 환경 분리 | |

### 바꿀 것 🔄 (1:1 매핑)

| 구분 | 사내 (Trino/HDFS) | BQ 이관 후 |
|---|---|---|
| **어댑터** | `dbt-trino` (사내 fork `custom-dbt-trino`) | `dbt-bigquery` (공식) |
| **인증** | LDAP (`DBT_USER`/`DBT_PASSWORD` env) | Composer SA + Workload Identity |
| **스토리지** | HDFS Parquet | BigQuery native table (필요 시 GCS external) |
| **코드 sync** | S3 (`s3cmd get`) | GCS sync (Composer 자동 마운트) |
| **세션 설정** | `X-Presto-Session` HTTP headers | BQ job config / dbt profile |
| **카탈로그/DB** | `hadoop_kent` / `hadoop_kentdev` | GCP `project_id` |
| **시크릿** | `.env` + sendbag | Composer Secret Manager backend |

### 재설계할 것 ⚠️ (단순 매핑 X — 검증 항목)

#### 1. Incremental 전략

```yaml
# AS-IS (Trino)
+incremental_strategy: "delete+insert"
+format: parquet

# TO-BE (BigQuery)
+incremental_strategy: "insert_overwrite"  # partition 기반 atomic replace
+partition_by:
    field: partition_date
    data_type: date
    granularity: day
+cluster_by: ['user_id', 'event_type']     # 선택, 자주 필터링하는 컬럼
```

> `insert_overwrite` 는 Trino `delete+insert` 와 의미상 유사. 파티션 단위 atomic replace. PK 기반이 필요하면 `merge`.

#### 2. 파티셔닝 모델

| AS-IS (Hive) | TO-BE (BigQuery) |
|---|---|
| 디렉토리 기반 (`partition_date=20250520/`) | 컬럼 기반 (`PARTITION BY DATE(partition_date)`) |
| string 파티션 키 (`'20250520'`) | DATE/TIMESTAMP 컬럼 강제 |

→ **기존 SQL 의 `partition_date` 가 string 으로 들어오는 경우 캐스팅 추가 필요.** dbt incremental + partition_by 가 string 받지 않음.

#### 3. Schema 네이밍

| 옵션 | 형태 | 평가 |
|---|---|---|
| A. dataset suffix (사내 동일) | `mlb_bronze_dev` / `mlb_bronze_production` | ✅ PoC 시작점. 기존 컨벤션 유지 |
| B. GCP project 자체 분리 | `mlb-data-dev` 프로젝트 vs `mlb-data-prod` 프로젝트 | 운영 단계 검토. 격리 강함, IAM 분리 깔끔 |
| C. Cosmos custom schema | model 별 + Cosmos 설정 조합 | 복잡도 ↑ |

→ **PoC 는 옵션 A**. 운영 가서 옵션 B 검토.

#### 4. 사내 macros (`music-dbt-utils`) 호환성

- Trino SQL 기반 함수 (e.g., `date_format`, `from_unixtime` 등) → BQ 호환 함수로 재작성 필요
- PoC 에서 macro 단위 호환 매트릭스 작성 (어떤 게 1:1 되고 어떤 게 재작성인지)

#### 5. Source 정의 패턴

| 옵션 | 설명 | 사용처 |
|---|---|---|
| (a) BQ native table | 데이터를 BQ 로 적재 (Dataflow / DTS / Fivetran) | 본 운영 |
| (b) GCS external table | 데이터는 GCS, BQ 에서 SQL 로 read | 이관 초기 hybrid 단계 |

→ 데이터 이관 트랙과 별개. dbt source 선언만 결정 (`sources.yml`).

---

## PoC 단계

### Phase 1. 로컬 dbt-bigquery 단일 모델 ⭐ 1주

목표: BQ 환경 + 인증 + 단일 모델 빌드 흐름 검증.

- [x] `pip install dbt-bigquery`
- [x] `dbt init` → 프로젝트 생성
- [x] BQ console 에서 `dbt_test` dataset 수동 생성
- [x] `gcloud auth application-default login`
- [x] `dbt debug` → `All checks passed!`
- [x] **dataset location 일치 확인** → ⚠️ mismatch 발견. dataset = `asia-northeast3`, profiles.yml = `US` → profiles.yml 정정
- [x] `dbt debug` 재확인 (location 변경 후) → All checks passed
- [x] `dbt run` (기본 예제 2개 모델) → 성공
- [x] BQ console 에서 결과 테이블/뷰 확인 → `my_first_dbt_model` (table), `my_second_dbt_model` (view) 생성됨
- [x] `dbt test` 실행 → 의도된 `not_null` 실패 (starter 모델이 일부러 NULL 포함) 외 정상
- [x] `dbt docs generate && dbt docs serve` → lineage graph 확인 ✅

### Phase 2. 사내 모델 1개 BQ 포팅 1~2주

목표: Trino SQL → BQ SQL 변환 작업량 + 함정 파악.

- [ ] `mlb-dbt` 에서 적절한 모델 1개 선정 (silver 또는 mart, 너무 단순/복잡하지 않게)
- [ ] BQ 호환 SQL 로 변환 (sqlglot 활용 가능)
- [ ] `incremental` + `partition_by` 패턴 적용
- [ ] 사내 macro 호환성 점검 (`music-dbt-utils` 사용한다면)
- [ ] 결과 검증: 기존 Trino 결과와 row count / 핵심 지표 비교
- [ ] **변환 시간 측정** → 전체 모델 추정 근거

> ⚠️ 데이터 자체가 BQ 에 없으면 검증 불가. 어떻게 raw data 를 BQ 로 가져올지 사전 결정 필요 (별도 트랙).

### Phase 3. Cosmos + Composer 실제 dbt run 1주

목표: [[02_dbt_render_in_composer]] 의 렌더링 검증을 **실제 실행** 으로 확장.

**Phase 3 를 7개 sub-step 으로 쪼개서 진행** (운영자 시각 검증):

- [x] **3-1**. Composer 환경 확인 + `dbt-bigquery` PyPI 추가 (`dbt-trino` 제거, 의존성 충돌 해소)
- [x] **3-2**. Composer SA (`dev-dp-airflow`) BQ 권한 확인 → BigQuery 관리자 권한 있음
- [x] **3-3**. dbt 프로젝트 정리 (`profiles.yml` 에 composer target 추가) + `dbt parse` 로 manifest 생성
- [x] **3-4**. dbt 프로젝트 + manifest 를 GCS DAGs bucket 의 `dbt_projects/dbt_test/` 로 업로드
- [x] **3-5**. Cosmos DAG 2종 작성 (별도 레포 `dbt-test-airflow-dags/dags/`):
    - `poc_bq_dbtdag.py` — **옵션 3** (DbtDag, 단순)
    - `poc_bq_dbt_run.py` — **옵션 4** (DbtTaskGroup + BashOperator 부속 task, 사내 컨벤션)
- [ ] **3-6**. 두 DAG GCS 업로드 + Airflow UI 확인 + trigger → 실제 BQ 에 테이블 생성 검증
- [ ] **3-7**. 모델별 task 재실행 / 실패 시나리오 (test 실패 시 downstream SKIP) 검증

### Phase 4. 운영 패턴 이식 1~2주

- [ ] `DbtConfigFactory` BQ 버전 구현 (`utils/dbt/bq_config.py`)
- [ ] `target.name` 기반 schema 분기 동작 (dev/prod)
- [ ] `fail_on_dbt_test_warnings` 콜백 이식
- [ ] 알림 통합 (WatchCenter 또는 Slack)
- [ ] CI/CD 파이프라인 (PR → dbt compile/test → manifest 빌드 → GCS push)
- [ ] Pool 설정 (`dbt_small` 같은 컨벤션 BQ 버전)

### Phase 5. 비용·성능 측정 (선택, 운영 결정용)

- [ ] BQ on-demand vs slot 예약 비용 비교
- [ ] Incremental `insert_overwrite` vs `merge` 비용 비교 (동일 모델 대상)
- [ ] Partition + Cluster 효과 측정 (스캔량)

---

## 진행 상태

| Phase | 상태 | 노트 |
|---|---|---|
| Phase 1. 로컬 dbt-bigquery 단일 모델 | ✅ **완료** | starter 모델 2개 (table+view) BQ 빌드 / test / docs lineage 확인 |
| Phase 2. 사내 모델 1개 BQ 포팅 | ⏸️ 보류 | Phase 3 우선 (시연 가치). 데이터 이관 트랙과 함께 후속 |
| Phase 3. Cosmos + Composer 실제 dbt run | 🚧 **진행 중** | 3-1~3-5 완료. 3-6 (trigger) 다음. 산출물: GCS 업로드된 dbt 프로젝트 + manifest, DAG 2종 (옵션 3 + 4) |
| Phase 4. 운영 패턴 이식 | ⬜ 대기 | |
| Phase 5. 비용·성능 측정 | ⬜ 대기 | 운영 단계 |

### 산출물 (현재 시점)

| 위치 | 산출물 | 비고 |
|---|---|---|
| 로컬 `~/PycharmProjects/dbt-test/dbt_test/` | dbt 프로젝트 (dev + composer 2 target) | 학습용 starter 모델 2개 |
| 로컬 `~/PycharmProjects/dbt-test-airflow-dags/dags/` | `poc_bq_dbtdag.py` (옵션 3), `poc_bq_dbt_run.py` (옵션 4) | Airflow DAG 별도 레포 (사내 컨벤션) |
| GCS `gs://.../dags/dbt_projects/dbt_test/` | dbt 프로젝트 + `target/manifest.json` | Composer worker 가 `/home/airflow/gcs/dags/...` 로 자동 마운트 |
| GCS `gs://.../dags/` | 두 DAG `.py` 파일 | (업로드 예정 — Step 3-6) |
| BQ `dev-dp-project-354904.dbt_test` | `my_first_dbt_model` (table), `my_second_dbt_model` (view) | 로컬 `dbt run` 결과 (Phase 1) |

---

## 발견 / 깨달음 (진행하며 채움)

### 1. `dbt init` 실행 위치

- `dbt debug` 는 **`dbt_project.yml` 가 있는 디렉토리에서만** 동작. 한 단계 상위에서 돌리면 `project path not found` 에러.
- 함정: `dbt init` 이 만든 폴더 안으로 cd 해야 함. PycharmProjects 루트에서 돌리지 말 것.

### 2. `impersonate_service_account` 에러 메시지 함정

- 실제 원인이 단순 토큰 만료여도 에러 메시지에 `impersonate_service_account` 언급 → 무관한 검토로 빠질 수 있음.
- profiles.yml 에 `impersonate_service_account` 없으면 그냥 `gcloud auth application-default login` 재실행이 답.

### 3. Dataset location mismatch

- `dbt init` 의 인터랙티브 프롬프트에서 `[1] US [2] EU [3] other` 묻는 단계가 있음 → 무심코 `US` 선택하기 쉬움.
- 실제 dataset 은 BQ 콘솔에서 `asia-northeast3` (서울) 로 만들어둠 → mismatch.
- `dbt debug` 는 통과 (auth + project 만 확인). **`dbt run` 단계에서 `404 Not found: Dataset ... was not found in location US`** 에러로 드러남.
- **확인 명령**: `bq show --format=prettyjson <project>:<dataset> | grep -i location` (기본 출력엔 안 보임)
- → **PoC 체크리스트에 "dataset location ↔ profiles.yml 일치" 항목 필수.** dbt init 첫 단계에서부터 강제하는 게 좋음.
### 4. dbt test = 데이터에 대한 가설 검증

- 코드 unit test 가 아니라 **이미 빌드된 테이블의 데이터를 검증**하는 SQL.
- `schema.yml` 의 `data_tests: [unique, not_null]` 선언 → dbt 가 검증 SQL 자동 생성 → BQ 에서 실행 → 위반 행 1개 이상이면 FAIL.
- 4가지 빌트인: `unique`, `not_null`, `accepted_values`, `relationships(FK)`. 그 외 `dbt-utils` 및 singular test (커스텀 SQL).
- 운영 의미: **나쁜 데이터의 downstream 전파 차단**. test 실패 시 의존 모델 SKIP (severity=error 기본).
- 사내 `fail_on_dbt_test_warnings` 콜백 = warn 까지 fail 처리해서 더 엄격하게 차단.

### 5. ⚠️ dbt adapter 공존 시 의존성 충돌 (운영자 핵심 발견)

Composer 환경 `test-airflow3` 에 `dbt-bigquery` 추가 시 빌드 실패. 실측 사례:

```
dbt-core      1.9.3   → dbt-adapters: >=1.10.1, <2.0
dbt-trino     1.9.3   → dbt-adapters: >=1.15.1, <1.17    ← 천장 낮음
dbt-bigquery  1.11.1  → dbt-adapters: >=1.22.6, <2.0     ← 바닥 높음
```

→ dbt-trino 의 `<1.17` 과 dbt-bigquery 의 `>=1.22.6` 교집합 없음. `ResolutionImpossible`.

**원인**:
- dbt 어댑터마다 `dbt-adapters` 호환 범위가 다르게 발전.
- 신규 어댑터 (BQ) 는 최신 adapters 요구, legacy 어댑터 (Trino) 는 옛 범위에 고정.
- adapter 1개당 1개 환경 가정. **한 Composer 에 여러 어댑터 강제 시 충돌 잦음.**

**플랫폼 운영 결론**:
- **서비스/어댑터별 Composer 환경 분리** 가 권장. 한 환경에 한 어댑터.
- 또는 임시 공존 시 **각 어댑터의 adapter pin 매트릭스** 를 먼저 확인 후 호환 범위 안에서만 버전 선택.

**우리 PoC 결정**:
- 02번 PoC (`dbt-trino` 렌더링 검증) 끝남 → `dbt-trino` 제거 → `dbt-bigquery` 단독.
- 이관 후에는 `dbt-bigquery` 단일 어댑터 환경.

### 6. dbt 어댑터 버전 ≠ dbt-core 버전

- 동일 마이너 (`1.9.x`) 라인 안에서도 어댑터별 patch 번호 독립.
- `dbt-core==1.9.3` 명시해도 `dbt-bigquery==1.9.3` 은 PyPI 에 부재 가능.
- **확인 명령**: `pip index versions dbt-bigquery` 로 실제 PyPI 버전 확인.
- **운영 권장**: 로컬 dev 환경의 `pip show dbt-bigquery | grep Version` 결과를 Composer 에 정확히 명시.

### 7. manifest = dbt 프로젝트의 "설계도" 한 파일

- `dbt parse` 결과물. **DB 연결 없이** 컴파일된 SQL + 모델 의존성 + 메타데이터를 JSON 한 파일로 통합.
- Cosmos 가 `LoadMode.DBT_MANIFEST` 모드에서 매번 dbt 깨우지 않고 이 파일만 읽음 → DAG parse 1초 미만 + DB 연결 0.
- **운영 패턴**: CI 에서 dbt parse → manifest 를 dbt 프로젝트와 함께 GCS 에 업로드. (수동 PoC 단계에선 `gsutil cp` 로 흉내)
- manifest 가 있으면 사용자 코드에서 모델 목록 / 태그 / schema 등도 동적으로 활용 가능. 사내 `dbt_loupe_kafka_exporter` 가 manifest 의 태그 정보로 Kafka task 를 동적 wire 하는 게 그 예.

### 8. Cosmos 의 3-layer 구조 (운영자 멘탈 모델 ⭐)

| Layer | 누가 | 무엇 | 예시 |
|-------|------|------|------|
| **Layer 1: 패키지** | Composer 환경 PyPI | wheel/패키지 설치 | `astronomer-cosmos`, `dbt-bigquery`, `kakao-airflow-poc` |
| **Layer 2: 자동 생성** | Cosmos | dbt manifest 읽고 task 자동 생성 | `bz_user.run`, `bz_user.test` |
| **Layer 3: 명시적 코드** | 사용자 | DAG 안에 직접 작성 | `BashOperator`, custom operator, `dbt_loupe_kafka_exporter()` |

→ **Cosmos 는 Layer 2 만 책임.** Kafka publish, 알림, 검증 등은 모두 Layer 3 (사용자 코드).
→ **TaskGroup 의 진짜 가치는 Layer 2 + Layer 3 조합력**. `start >> dbt_tasks >> verify` 패턴.

### 9. DbtDag vs DbtTaskGroup 선택 기준

| 상황 | 추천 |
|------|------|
| dbt 만 돌리고 끝 | DbtDag (단순) |
| 뒤에 뭐가 붙음 (sensor, alert, publish, validation 등) | **DbtTaskGroup** |
| 여러 dbt 프로젝트 통합 | DbtTaskGroup (멀티) |
| 모델 수 많아서 시각적 wrapping 필요 | DbtTaskGroup |
| 팀 컨벤션 통일 (사내) | DbtTaskGroup |

→ 본 PoC 는 학습 가치 위해 **둘 다 작성**. 시연/운영 학습은 DbtTaskGroup 쪽.

### 10. Composer 의 GCS 마운트 = 무인증 GCP CLI 사용

- Composer worker 는 DAGs bucket 의 `dags/` 경로를 `/home/airflow/gcs/dags/` 로 자동 마운트. 코드 sync 인프라 따로 안 만들어도 됨.
- Worker 에 attached SA 의 credential 이 ADC 로 자동 노출 → BashOperator 안에서 `bq`, `gsutil`, `gcloud` 명령을 **인증 코드 0줄** 로 호출 가능.
- 예: `verify_results` task 에서 `bq query ...` 직접 호출 → SA 자격증명 자동 적용.
- → 운영자 입장에서 **Composer + Cosmos 는 GCP 네이티브 통합이 가장 강한 조합**. K8sPodOperator 보다 진입장벽 낮음.

### 11. (예정) Trino → BQ SQL 호환성 매트릭스
### 12. (예정) 사내 macros 호환성

---

## 안 검증할 것 (out of scope)

| 항목 | 이유 |
|---|---|
| 데이터 자체의 HDFS → GCS/BQ 이관 | 별도 트랙 (3~6개월 규모). dbt PoC 의 변수 아님 |
| BQ Slot 예약 vs on-demand 의사결정 | 본 PoC 범위 외. Phase 5 에서 선택적으로 측정만 |
| DataHub lineage | [[애슬론/PoC/README]] Step 4 별도 PoC |
| Asset-Centric prototype | Step 5 별도 PoC |
| Userlake / extract 의 BQ 전환 | [[../1_개요]] 의 별도 결정 (dbt 영역 X) |

---

## 관련 노트

- [[02_dbt_render_in_composer]] — 선행 PoC (Trino dbt 로 Cosmos 렌더링 검증)
- [[README]] — 애슬론 PoC 의 전체 흐름 (Step 2~3 위치)
- [[../1_개요]] — Asset-Centric 의사결정
- [[../2_Git 동기화·dbt 전환 계획]] — 사내 dbt 전환 계획
- [[../3_dbt 능력 경계와 영역 분담]] — dbt vs non-dbt 분담
- [[../../스케줄러/PoC/02_dag_deployment]] — Composer DAG 배포 흐름
- [[../../스케줄러/PoC/03_custom_operator_pypi]] — 사내 wheel install 패턴 (BQ 운영 시 사내 macro 모듈 재사용 시 활용 가능)
- [[../../스케줄러/PoC/04_worker_pool_queue]] — Pool 동작 검증

## 외부 참고

- [dbt-bigquery adapter 공식](https://docs.getdbt.com/docs/core/connect-data-platform/bigquery-setup)
- [astronomer-cosmos — BigQuery profile mapping](https://astronomer.github.io/astronomer-cosmos/profiles/GoogleCloudServiceAccountFileProfileMapping.html)
- [dbt incremental on BigQuery](https://docs.getdbt.com/docs/build/incremental-models)
- [BigQuery partitioned tables 공식](https://cloud.google.com/bigquery/docs/partitioned-tables)
