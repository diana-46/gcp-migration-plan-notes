# 4. Parameter 치환 (Jinja, macro, Airflow context)

> Neptune 의 `${execution_date}` 같은 string 치환과 dbt 의 `{{ var() }}` Jinja 가 어떻게 매핑되는지, Airflow context 변수가 어디서 어떻게 흘러서 dbt 모델까지 도달하는지.
> 관련: [[1_materialization]], [[2_schema 관리]], [[3_backfill]]

## 0. 다루는 범위

- Neptune 의 `${variable}` 치환 메커니즘 + `EtlInputType.macro`
- dbt 의 `var()`, vars, macros, Jinja 전반
- Airflow context 변수 (data_interval_start 등) 가 어떻게 dbt 까지 전달되는지
- **Two-stage Jinja rendering** — 가장 헷갈리는 부분
- 타임존 처리 패턴
- 마이그레이션 시 SQL 변환 비용

## 1. Neptune 의 parameter 치환

### 1-1. 사용자 SQL 의 placeholder

Neptune 의 ETL SQL 본문에 `${variable_name}` 형태로 자리표시자 작성:

```sql
SELECT ...
FROM source_table
WHERE create_date = '${next_execution_date}'
  AND create_hour = '${next_execution_date_hour}'
```

### 1-2. 매크로 정의 (`EtlInputType.macro`)

각 placeholder 의 실제 값은 `EtlInputType` 엔티티의 `macro` 필드에 정의됨:

```
EtlInputType:
  id: 1
  name: "next_execution_date"
  macro: "{{ data_interval_end.in_timezone(dag.timezone).strftime('%Y-%m-%d') }}"
```

→ `${next_execution_date}` 가 SQL 안에 있으면, **Airflow DAG 생성 시점**에 이 매크로의 결과로 치환된 SQL 이 DAG 파일에 박힘.

### 1-3. 자주 쓰던 매크로 패턴

PoC 의 case 들에서 확인된 것들:

| placeholder | 매크로 | 의미 |
|---|---|---|
| `${execution_date}` | `{{ data_interval_start.in_timezone(...).strftime('%Y-%m-%d') }}` | KST 일자 (시작) |
| `${next_execution_date}` | `{{ data_interval_end.in_timezone(...).strftime('%Y%m%d') }}` | 다음 일자 (YYYYMMDD) |
| `${next_execution_date_hour}` | `{{ data_interval_end.strftime('%Y-%m-%d %H:%i:%s') }}` | UTC 시각 |
| `${next_execution_date_hour_kst}` | `{{ data_interval_end.in_timezone('Asia/Seoul').strftime(...) }}` | KST 시각 |
| `${next_date_kst}` | `{{ data_interval_end.in_timezone('Asia/Seoul').strftime('%Y-%m-%d') }}` | KST 일자 (다음) |
| `${start_dt_utc}`, `${next_dt_utc}` | UTC 기준 datetime | 시작/끝 |

### 1-4. 치환 메커니즘

```
[ETL 등록 시점]
SQL with ${var} 들 + partition macros
        ↓
[Athlon API 가 EtlContext 만들 때]
EtlInputType.macro 가 Jinja 표현으로 변환됨
        ↓
[Airflow DAG 생성 시점]
Airflow Jinja renderer 가 매크로 평가
        ↓
[DAG 파일에 박히는 최종 SQL]
WHERE create_date = '20260618'   ← 이미 치환됨
```

→ Neptune 은 **string-level 치환**이고 **DAG 만들 때 한 번 일어남**. dag_run 마다 다시 평가되지 않음.

⚠️ 정확히는 SQL 안에 Jinja 가 박혀 있어서, 매 run 의 data_interval_start 가 다를 때마다 Airflow 가 다시 렌더링. (Neptune 이 string 치환이 아니라 Jinja 매크로를 SQL 안에 끼워넣는 방식임)

---

## 2. dbt 의 parameter 치환

### 2-1. `var()` — 모델에서 외부 변수 받기

```sql
{{ config(
    partitions=["DATE('" ~ var('run_date', '2026-06-15') ~ "')"]
) }}

SELECT
    DATE('{{ var("run_date", "2026-06-15") }}') AS create_date,
    ...
```

- 첫 인자: 변수 이름
- 두 번째 인자: default (없으면 에러)
- 값은 `dbt run --vars '{run_date: "..."}'` 로 전달, 또는 `dbt_project.yml` 의 `vars` 섹션에서 설정

### 2-2. `dbt_project.yml` 의 vars

프로젝트 전역 default:
```yaml
vars:
  default_country: 'KR'
  rolling_window_days: 7
```

### 2-3. macros — 재사용 SQL 조각

`macros/` 디렉토리의 `.sql` 파일:
```sql
{% macro safe_div(numerator, denominator) %}
  CASE WHEN {{ denominator }} = 0 THEN NULL
       ELSE {{ numerator }} / {{ denominator }}
  END
{% endmacro %}
```

모델에서 호출:
```sql
SELECT {{ safe_div('total', 'count') }} AS avg
```

### 2-4. 외부 패키지 매크로 (dbt_utils 등)

`packages.yml` + `dbt deps` 로 설치 후:
```sql
{{ dbt_utils.date_spine(
    datepart="day",
    start_date="'2026-01-01'",
    end_date="'2026-12-31'"
) }}
```

### 2-5. config 도 Jinja

```sql
{{ config(
    materialized='incremental',
    partition_by={'field': 'create_date', 'data_type': 'date', 'granularity': 'day'},
    pre_hook=["ALTER TABLE {{ this }} ADD COLUMN IF NOT EXISTS revenue INT64"]
) }}
```

→ `{{ this }}` 같은 dbt-specific 매크로가 config 안에서도 동작.

---

## 3. Airflow context → Cosmos → dbt 흐름 (핵심)

가장 헷갈리는 부분. **two-stage Jinja rendering** 으로 이해.

### 3-1. 두 개의 Jinja 엔진

| 단계 | 엔진 | 입력 | 출력 |
|---|---|---|---|
| Stage 1 | **Airflow Jinja** | DAG 의 vars 정의 (Jinja 표현 포함) | 렌더된 string |
| Stage 2 | **dbt Jinja** | 모델 SQL + 받은 vars | 최종 BQ SQL |

### 3-2. 흐름 예시 (case2 기준)

```
[Airflow context]
data_interval_start = 2026-06-10 00:00:00 UTC
dag_run.logical_date = 2026-06-10
        ↓
[Stage 1: DAG vars 의 Airflow Jinja]
RUN_DATE_KST = "{{ data_interval_start.in_timezone('Asia/Seoul').strftime('%Y-%m-%d') }}"
        ↓ 렌더
"2026-06-10"
        ↓
[Cosmos 가 operator_args["vars"] 에서 받음]
{"run_date": "2026-06-10"}
        ↓
[dbt CLI 호출]
dbt run --select case2 --vars '{"run_date": "2026-06-10"}'
        ↓
[Stage 2: 모델 SQL 의 dbt Jinja]
SELECT DATE('{{ var("run_date") }}') AS create_date
        ↓ 렌더
SELECT DATE('2026-06-10') AS create_date
        ↓
[BQ 에 보내지는 최종 SQL]
INSERT OVERWRITE PARTITION DATE('2026-06-10') ...
```

### 3-3. 왜 두 단계인가?

- Stage 1 (Airflow Jinja): `data_interval_start` 같은 **Airflow context** 에 접근
- Stage 2 (dbt Jinja): `ref()`, `source()`, `this`, `var()` 같은 **dbt-specific** 함수에 접근

→ Airflow Jinja 에선 `ref('model')` 못 쓰고, dbt Jinja 에선 `data_interval_start` 못 씀. 둘 사이의 다리가 vars 딕셔너리.

### 3-4. 흔한 함정

**함정 1: 어느 단계의 Jinja 인지 헷갈림**
```python
# ❌ Airflow DAG 코드 안에 dbt Jinja 쓰면 안 됨
vars = {"table": "{{ ref('my_model') }}"}   # Airflow 가 못 풀고 에러

# ✅ Airflow Jinja 만 사용
vars = {"run_date": "{{ data_interval_start.strftime('%Y-%m-%d') }}"}
```

**함정 2: 모델 SQL 에 data_interval_start 직접 못 씀**
```sql
-- ❌ 모델 안에선 dbt Jinja 만 동작
WHERE event_ts >= '{{ data_interval_start }}'   -- 빈 문자열로 평가됨

-- ✅ vars 통해서 전달
WHERE event_ts >= '{{ var("run_date") }}'
```

**함정 3: Jinja 이중 평가**
```python
# DAG 코드
vars = {"x": "{{ '{{ var(\"y\") }}' }}"}   # 이상한 짓
```
하지 마세요. 일반적인 패턴 안 됨.

---

## 4. 자주 쓰는 Airflow context 변수 (Airflow 3 기준)

DAG vars 안에 Jinja 로 박을 수 있는 것들:

| 변수 | 타입 | 의미 |
|---|---|---|
| `data_interval_start` | datetime (UTC) | dag_run 의 데이터 윈도우 시작 |
| `data_interval_end` | datetime (UTC) | 데이터 윈도우 끝 |
| `logical_date` | datetime (UTC) | dag_run 의 logical 시각 (= data_interval_start) |
| `ds` | str | logical_date 의 YYYY-MM-DD |
| `ds_nodash` | str | YYYYMMDD |
| `ts` | str | logical_date ISO 8601 |
| `dag_run.run_id` | str | dag_run 고유 ID |
| `dag_run.conf` | dict | manual trigger 시 conf |
| `params` | dict | DAG 의 params |

⚠️ Airflow 2 의 `execution_date` 는 deprecated. Airflow 3 에선 안 쓰는 게 정답.

### 자주 쓰는 macro 패턴

```python
# KST 일자
"{{ data_interval_start.in_timezone('Asia/Seoul').strftime('%Y-%m-%d') }}"

# KST 시간까지 (시간별 ETL 용)
"{{ data_interval_start.in_timezone('Asia/Seoul').strftime('%Y-%m-%d %H:00:00') }}"

# YYYYMMDD (Neptune 의 ${next_execution_date} 호환)
"{{ data_interval_end.in_timezone('Asia/Seoul').strftime('%Y%m%d') }}"

# UTC ISO (DATETIME 컬럼용)
"{{ data_interval_start.strftime('%Y-%m-%dT%H:00:00') }}"

# 어제 (rolling window 용)
"{{ (data_interval_start - macros.timedelta(days=1)).strftime('%Y-%m-%d') }}"
```

---

## 5. 타임존 처리 패턴

Neptune 의 `in_timezone(dag.timezone)` 패턴을 dbt + Composer 에 옮길 때.

### 5-1. data_interval_start 는 항상 UTC

Airflow 의 datetime context 변수는 모두 UTC. KST 가 필요하면 `.in_timezone('Asia/Seoul')` 명시.

### 5-2. 권장 패턴

```python
# DAG vars 정의
RUN_DATE_KST = "{{ data_interval_start.in_timezone('Asia/Seoul').strftime('%Y-%m-%d') }}"
RUN_DATETIME_UTC = "{{ data_interval_start.strftime('%Y-%m-%dT%H:00:00') }}"
```

→ **명시적 변환**. 묵시적 변환은 사고 원인.

### 5-3. dbt 모델 안에서는 string 으로 받기

```sql
SELECT DATE('{{ var("run_date") }}') AS create_date
```

→ Airflow 가 이미 KST 로 포맷한 문자열을 전달. 모델 안에서 다시 타임존 변환 안 함. **타임존 변환은 Airflow 단에서 한 번만**.

### 5-4. Neptune 호환성

Neptune 의 매크로:
```
{{ data_interval_start.in_timezone(dag.timezone).strftime('%Y-%m-%d') }}
```

는 dbt + Composer 의 DAG vars 와 **거의 동일 표현**. `dag.timezone` 만 `'Asia/Seoul'` 로 명시하면 됨.

---

## 6. dbt macros — 재사용 SQL 조각

### 6-1. 언제 macro 만드나

- 같은 SQL 패턴이 여러 모델에 반복
- dbt-bigquery 특정 함수의 wrapper
- 복잡한 conditional 로직

### 6-2. 예시 — KST 일자 파싱

```sql
-- macros/kst_date.sql
{% macro to_kst_date(timestamp_col) %}
  DATE({{ timestamp_col }}, 'Asia/Seoul')
{% endmacro %}
```

모델에서:
```sql
SELECT {{ to_kst_date('created_at') }} AS create_date FROM source
```

### 6-3. 매크로 vs Airflow vars

| 용도 | 매크로 | vars |
|---|---|---|
| SQL 로직 반복 제거 | ✅ | ❌ |
| 런타임 값 (날짜 등) 주입 | ❌ | ✅ |
| 다른 모델 참조 | `ref()` (built-in) | ❌ |
| 컴파일 시점 결정 | 매크로 | vars (run 시점) |

---

## 7. PoC 의 실제 예시 모음

case2 (daily incremental):
```python
# DAG
CASE2_VARS = {"run_date": "{{ data_interval_start.in_timezone('Asia/Seoul').strftime('%Y-%m-%d') }}"}

# 모델
DATE('{{ var("run_date", "2026-06-15") }}') AS create_date
partitions=["DATE('" ~ var('run_date', '2026-06-15') ~ "')"]
```

case4 (AVRO export, 복합 vars):
```python
RUN_DATE_KST = "{{ data_interval_start.in_timezone('Asia/Seoul').strftime('%Y-%m-%d') }}"
CASE4_VARS = {
    "run_date": RUN_DATE_KST,
    "avro_export_uri": f"gs://{BUCKET}/neptune_poc/case4/{RUN_DATE_KST}/*.avro",
}
```

case5 (hourly):
```python
CASE5_VARS = {
    "run_datetime_utc": "{{ data_interval_start.strftime('%Y-%m-%dT%H:00:00') }}",
}

# 모델
DATETIME('{{ var("run_datetime_utc", "2026-06-15T00:00:00") }}') AS snap_at
```

---

## 8. Neptune → dbt 마이그레이션 매핑

| Neptune 자산 | dbt 대응 | 변환 비용 |
|---|---|---|
| `${variable_name}` 치환 | `{{ var('variable_name') }}` | 단순 검색 치환 (`${X}` → `{{ var("X") }}`) |
| `EtlInputType.macro` (Jinja) | DAG 의 vars 매크로 | 매크로 정의를 DAG vars 로 옮김. helper 모듈 권장 |
| 매크로 평가 시점 (DAG 생성 시) | 매 dag_run task 실행 시 | dbt 가 매번 새로 렌더 → backfill 자동 친화 |
| ETL 마다 매크로 분리 | 모든 vars 가 한 DAG 의 dict | helper 로 case 별 그룹화 |
| Presto 함수 (date_format, date_parse 등) | BigQuery 함수 (FORMAT_DATE, PARSE_DATE 등) | **별도 SQL 방언 번역 작업** (parameter 치환과 별개) |

**예상 마이그레이션 흐름**:
1. ETL SQL 의 `${var}` → `{{ var('var') }}` (자동 sed)
2. EtlInputType.macro → DAG vars dict (helper 모듈에 등록)
3. Presto → BigQuery 방언 번역 (가장 큰 비중)
4. (필요 시) 반복 SQL 패턴은 dbt macros 로 추출

---

## 9. 미해결 / 추가 검토

- dbt 매크로의 `is_incremental()` 같은 컨텍스트 함수와 Airflow Jinja 의 매크로가 충돌할 가능성 (없을 듯 하지만 검증 필요)
- `dbt_project.yml` 의 vars 와 CLI `--vars` 우선순위 (CLI 가 우선)
- Cosmos 가 vars 를 Jinja 렌더할 때 escape 처리 (특수 문자, 한글 등)
- Composer 의 Airflow 3 의 `params` (DAG 파라미터) 가 vars 와 어떻게 상호작용하는지 (dag_run.conf override 등)