# 0. dbt 기본 개념

> 응용 패턴 (PoC, 1~7 노트) 보기 전에 잡아야 할 기본기. 한 번 읽고 reference 로.
> 관련: 이후 노트들 [[1_materialization]] ~ [[6_배포와 환경 분리]]

## 1. dbt 가 뭐 / 안 뭐

### dbt 는 무엇인가

> **SQL 변환 (Transform) 만 담당하는 도구.** ELT 의 **T**.

핵심 메커니즘:
1. 사용자가 `.sql` 파일에 **SELECT 문**만 적음
2. dbt 가 그 SELECT 를 `CREATE TABLE AS SELECT` / `INSERT` / `MERGE` 등 적절한 DDL 로 감쌈
3. 결과 테이블을 BQ (또는 다른 DW) 에 만듦

→ 사용자는 "이 데이터를 어떻게 변환할지" SELECT 만 쓰면, dbt 가 **물리화 / 의존성 / 테스트 / 문서화**를 자동 처리.

### dbt 는 무엇이 아닌가

| 오해 | 실제 |
|---|---|
| 데이터 이동 도구 (ETL의 E, L) | ❌ raw → DW 적재는 별도 (Airflow, Fivetran 등) |
| 스케줄러 | ❌ Airflow / Composer / dbt-cloud 같은 외부 스케줄러 필요 |
| 데이터베이스 | ❌ BQ / Snowflake / Postgres 등 위에서 동작 |
| 실시간 처리 | ❌ 배치만 |
| 외부 시스템 연동 | ❌ Kafka, API 호출 등은 Airflow 영역 |

→ **"SQL 변환 + 메타데이터 관리"** 가 dbt 책임의 전부.

---

## 2. 핵심 객체 6가지

| 객체           | 위치                                        | 무엇                              |
| ------------ | ----------------------------------------- | ------------------------------- |
| **model**    | `models/*.sql`                            | SQL 변환 (가장 핵심). 결과 = BQ 테이블/뷰   |
| **source**   | `models/*.yml`                            | 외부 raw 테이블 선언 (dbt 가 만들지 않은 입력) |
| **seed**     | `seeds/*.csv`                             | CSV → BQ 테이블 (작은 lookup 데이터용)   |
| **snapshot** | `snapshots/*.sql`                         | SCD Type-2 (변경 이력 추적)           |
| **test**     | `tests/*.sql` + schema.yml 의 `data_tests` | 데이터 품질 검증 SQL                   |
| **macro**    | `macros/*.sql`                            | 재사용 가능한 Jinja 함수                |
|              |                                           |                                 |

---

## 3. 프로젝트 구조 (디렉토리)

```
my_dbt_project/
├── dbt_project.yml         ← 프로젝트 설정 (필수)
├── profiles.yml            ← 연결 설정 (BQ project, dataset 등)
├── packages.yml            ← 외부 패키지 (dbt-utils 등)
│
├── models/                 ← SQL 변환의 핵심
│   ├── staging/            ← raw 정제 layer
│   ├── marts/              ← 비즈니스 layer
│   └── schema.yml          ← 모델 메타 / 테스트 정의
│
├── sources/                ← raw 데이터 source 선언
├── seeds/                  ← CSV
├── snapshots/              ← SCD2
├── tests/                  ← singular tests
├── macros/                 ← 재사용 Jinja
├── analyses/               ← ad-hoc 분석 SQL
│
└── target/                 ← dbt 가 생성 (compile / manifest 등)
    ├── manifest.json       ← 전체 의존성 그래프 (Cosmos 가 읽음)
    ├── compiled/           ← Jinja 렌더된 SQL
    └── run/                ← 실행된 SQL
```

---

## 4. 핵심 설정 파일 3개

### 4-1. `dbt_project.yml` — 프로젝트 설정

```yaml
name: 'my_project'
version: '1.0'
profile: 'my_project'        # profiles.yml 의 어느 profile 쓸지

model-paths: ["models"]
seed-paths: ["seeds"]
# ...

# 폴더별 default 설정
models:
  my_project:
    staging:
      +materialized: view       # staging 은 다 view
    marts:
      +materialized: table      # marts 는 다 table
```

### 4-2. `profiles.yml` — 연결 설정 (DW credentials)

```yaml
my_project:
  target: dev                  # default — --target 옵션 없을 때 사용
  outputs:
    dev:
      type: bigquery
      method: oauth
      project: dev-project
      dataset: dbt_test
      location: asia-northeast3
    prod:
      type: bigquery
      method: oauth
      project: prod-project
      dataset: dbt_prod
      location: asia-northeast3
```

#### `target` 의 의미

- `target: dev` 는 **"한 환경만 본다" 가 아니라 default 일 뿐**. outputs 에 정의된 둘 다 사용 가능.
- 명령 단에서 override: `dbt run --target prod` → prod outputs 사용
- 평소엔 `--target` 생략 → `target: dev` 따라감

#### 환경별로 어떻게 운영하나

| 환경 | 호출 방법 |
|---|---|
| **로컬 개발** | `dbt run` (default dev) — 실수로 prod 안 만지게 안전 |
| **CI** | `dbt run --target prod` (CI 가 명시) |
| **Composer (Cosmos)** | env var `DBT_TARGET` 기반. Composer 환경마다 다른 값. `common/dbt_presets.py` 가 `os.environ.get("DBT_TARGET", "dev")` 로 읽어서 `ProfileConfig(target_name=...)` 에 적용 |

→ **같은 profiles.yml, 같은 모델 SQL** 로 dev/prod 둘 다 운영. 환경 차이는 outputs 의 project / dataset 만.

#### 자주 하는 실수

- `target: prod` 로 두고 로컬에서 `dbt run` → 로컬이 실수로 prod 만짐. **default 는 dev 권장**.
- outputs 에 prod 만 정의 → 로컬 dev 환경 없음.

#### Jinja 에서 target 정보 접근

모델 SQL 안에서 현재 어느 target 인지 분기 가능:

```sql
{% if target.name == 'prod' %}
  SELECT * FROM {{ source('raw', 'orders') }}
{% else %}
  SELECT * FROM {{ source('raw', 'orders_sample') }}  -- dev 는 샘플만
{% endif %}
```

→ 환경별 모델 동작 customization 가능 (§ 4-2-1 참조).

### 4-3. `schema.yml` — 모델 메타 + 테스트

```yaml
version: 2

models:
  - name: orders                  # 모델 (.sql 파일명과 같아야)
    description: "주문 팩트 테이블"
    config:
      materialized: incremental
      contract:
        enforced: true
    columns:
      - name: order_id
        description: "주문 PK"
        data_type: int64
        constraints:
          - type: not_null
        data_tests:
          - unique
          - not_null
      - name: amount
        data_type: numeric
```

→ 컬럼 정의 + 테스트 + contract 강제 + 문서화 다 한 곳.

---

## 5. 실행 사이클

```
[1. dbt parse]
  파일 읽기 → manifest.json 생성
  (BQ 접속 안 함, 빠름)

[2. dbt compile]
  Jinja 렌더 → target/compiled/*.sql
  (BQ introspection 약간)

[3. dbt run]
  compiled SQL 을 BQ 에 실행
  CREATE / INSERT / MERGE 등
  → BQ 에 테이블 생성/갱신

[4. dbt test]
  schema.yml + tests/ 의 SQL 실행
  → 데이터 품질 검증

[5. dbt docs generate]
  manifest.json + BQ INFORMATION_SCHEMA
  → docs site
```

자주 쓰는 합본:
- `dbt build` = `seed + run + test + snapshot` 한 번에
- `dbt run --select tag:daily` 같은 selector 로 부분 실행

---

## 6. Jinja 기초 (dbt 의 SQL 안에서)

dbt 의 `.sql` 파일은 **Jinja 템플릿 + SQL**. dbt 가 Jinja 렌더 후 BQ 에 보냄.

### 6-1. 자주 쓰는 6가지

```sql
-- 1. config(): 모델 설정
{{ config(
    materialized='incremental',
    partition_by={'field': 'create_date', 'data_type': 'date'}
) }}

-- 2. ref(): 다른 모델 참조 (의존성 자동 추론)
SELECT * FROM {{ ref('stg_orders') }}

-- 3. source(): 외부 raw 데이터 참조
SELECT * FROM {{ source('raw', 'orders') }}

-- 4. var(): 외부 변수 (DAG / CLI 에서 주입)
WHERE create_date = '{{ var("run_date", "2026-06-15") }}'

-- 5. is_incremental(): incremental 모델의 두 번째 실행부터 true
{% if is_incremental() %}
WHERE event_ts > (SELECT MAX(event_ts) FROM {{ this }})
{% endif %}

-- 6. this: 현재 모델 자신의 BQ relation
SELECT * FROM {{ this }}    -- = `project.dataset.모델명`
```

### 6-2. Jinja 의 핵심 시점

- **dbt parse 시**: `config()`, `ref()`, `source()` 호출 → manifest 등록
- **dbt compile 시**: 모든 Jinja 렌더 → 순수 SQL 됨
- **BQ 가 받는 건**: 렌더된 순수 SQL (Jinja 흔적 0)

---

## 7. 핵심 명령어 정리

| 명령 | BQ 영향 | 용도 |
|---|---|---|
| `dbt parse` | ❌ | manifest 생성 (오프라인) |
| `dbt compile` | ⚠️ introspection | 렌더된 SQL 만 보고 싶을 때 |
| `dbt list` (or `dbt ls`) | ❌ | selector 검증 |
| `dbt run` | ✅ 테이블 생성 | 모델 실행 (주력 명령) |
| `dbt test` | ❌ (SELECT 만) | 테스트 SQL 실행 |
| `dbt seed` | ✅ CSV → 테이블 | seed 적재 |
| `dbt snapshot` | ✅ snapshot 테이블 | SCD2 갱신 |
| `dbt build` | ✅ 전부 | `seed + run + test + snapshot` 한 번에 |
| `dbt docs generate` | ❌ | docs site 데이터 생성 |
| `dbt source freshness` | ❌ | source 의 최신성 검사 |

### Selector 패턴

```bash
dbt run --select my_model               # 한 모델
dbt run --select +my_model              # my_model 과 그 upstream 다
dbt run --select my_model+              # my_model 과 그 downstream 다
dbt run --select tag:daily              # daily tag 붙은 모델들
dbt run --select path:models/staging    # 폴더 단위
dbt run --select tag:daily,tag:critical # AND (둘 다)
dbt run --select tag:daily tag:critical # OR (둘 중 하나)
dbt run --exclude tag:experimental      # 제외
```

---

## 8. `manifest.json` — dbt 의 두뇌

`dbt parse` 가 만드는 파일. 프로젝트의 **모든 메타데이터 + 의존성 그래프**:

- 모든 모델 / source / test 의 정의
- 각 노드의 `depends_on` (의존성)
- 각 노드의 BQ relation 경로 (`project.dataset.model_name`)
- config (materialized, tags, contract 등)

**누가 읽나?**:
- `dbt run` — 실행 순서 결정
- `dbt test` — 테스트 SQL 컴파일
- **Cosmos** — Airflow task 자동 생성 (이게 우리 PoC 의 핵심)
- `dbt docs` — 사이트 렌더

→ manifest 가 깨지면 모든 게 깨짐. 그래서 PoC 의 dbt 버전 호환성 사고가 치명적이었음 ([[6_배포와 환경 분리]] § 9-1).

---

## 9. PoC 의 응용 → 기본 개념 매핑

PoC 에서 봤던 것들을 기본 개념으로 다시 anchoring:

| PoC 에서 본 것 | 기본 개념 |
|---|---|
| `case2_kp_stat_ticket_use_daily.sql` | model |
| `{{ config(materialized='incremental', ...) }}` | config() Jinja 함수 |
| `partition_by={'field': 'create_date'}` | dbt-bigquery 의 incremental 설정 |
| `{{ var("run_date") }}` | var() Jinja 함수 + DAG 에서 vars 주입 |
| `{{ ref('cross_group_a') }}` | ref() — 의존성 자동 추론 |
| `schema.yml` 에 `data_tests: [unique]` | schema.yml — 테스트 정의 |
| `contract: enforced: true` | schema.yml + contract — 스키마 강제 |
| `on_schema_change: fail` | incremental 의 schema drift 정책 |
| `pre_hook: [...]` | config 안의 pre_hook (run 직전 SQL) |
| `post_hook` 의 `EXPORT DATA` | config 안의 post_hook (run 직후 SQL) |
| `tag:case2` | selector 의 tag (config 의 `tags` 매칭) |
| `target/manifest.json` | manifest — Cosmos 가 읽음 |
| `dbt parse` | 오프라인 manifest 갱신 |
| `dbt build` 가 실수로 테이블 생성 | seed + run + test + snapshot 합본 |

→ 응용 패턴들이 다 위 기본 개념 위에 쌓여 있음.

---

## 10. 학습 자료

| 자료 | 무엇 |
|---|---|
| [dbt Docs](https://docs.getdbt.com/) | 공식 문서 (가장 정확) |
| [dbt Fundamentals](https://courses.getdbt.com/) | 무료 코스 (기본기) |
| `dbt --help` / `dbt run --help` | CLI 도움말 |
| 우리 PoC 노트 [[1_materialization]] ~ [[6_배포와 환경 분리]] | 우리 환경 specific 응용 |

---

## 11. 한 번 더 정리 (1분 요약)

- dbt = **SQL 변환 도구**. 사용자는 SELECT 만 쓰면 dbt 가 DDL 로 감쌈
- `.sql` 파일에 Jinja + SQL 작성 → `dbt run` → BQ 에 테이블
- 핵심 함수: `config()`, `ref()`, `source()`, `var()`, `is_incremental()`, `this`
- 의존성은 `ref()` 가 자동 추론 → `manifest.json` 에 그래프 저장
- 테스트는 `schema.yml` 의 `data_tests` 로 선언 → `dbt test` 가 SQL 로 컴파일해 실행
- 환경 분리는 `profiles.yml` 의 target 으로 (`dev` / `prod`)
- dbt 자체는 스케줄러 없음 → Composer + Cosmos 가 그 역할
