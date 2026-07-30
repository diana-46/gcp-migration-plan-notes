# 7. Lineage 와 관측성

> 두 결정 (dbt + 팀별 DAG) 이 자연스럽게 열어주는 lineage · 관측성 이득.
> 관련: [[스케줄러/9_Airflow Asset과 Dataset]], [[스케줄러/10_DataHub vs Knowledge Catalog 비교]], [[애슬론/4_Asset-Centric 아키텍처 안]]

## 지금 (파편화된 lineage)

- **Neptune SQL**: TEXT 컬럼에 갇힘 → model 간 자동 lineage X
- **Airflow (일부 task)**: datahub-plugin 이 표준 operator 만 부분 push
- **DataHub**: 수집 → ETL 연결 끊김. column-level lineage 없음. 문서 · owner · tag 별도 관리.

DE 질문 vs 현재 대응:

| 질문 | 지금 하는 것 |
|---|---|
| "이 컬럼 어디서 왔지" | 슬랙에서 물어봄 |
| "내 모델 지우면 뭐 깨져" | grep + 짐작 |
| "이 데이터 뭘 뜻해" | 담당자 사냥 |

## 이후 — dbt + Airflow 출처가 BQ 테이블 URN 공통 키로 stitching

### 1. dbt manifest → DataHub

- `dbt parse` 후 `manifest.json` 이 dbt 이관 CI 로 push
- **Model-level lineage** 자동 — `ref()` / `source()` 그래프 기반
- **Column metadata** (컬럼 이름 · 설명 · 데이터 타입 · 테스트) — `schema.yml` 이 선언, `manifest.json` 에 반영되어 DataHub 에 그대로 노출
- **Column-level lineage** (컬럼 → 컬럼 파생 매핑) — DataHub 가 아래 조합으로 도출:
  1. `manifest.json` (모델 간 그래프 + compiled SQL)
  2. `catalog.json` (dbt docs generate 로 BQ 스키마 introspection) — 실제 컬럼 목록
  3. **SQLGlot 등 SQL AST 파서** — SELECT 절에서 어떤 컬럼이 어떤 upstream 컬럼에서 나왔는지 추론
- **dbt-core 자체가 SELECT 자동 분석은 안 함** (dbt Cloud premium 은 별개 기능). DataHub 의 dbt integration 이 처리.
- 현재 CI 에서는 `dbt parse` 로 manifest 만 push, `dbt docs generate` (catalog) 는 시연 후 추가 예정.

### 2. Airflow DAG (Python) → DataHub

- `datahub-airflow-plugin` (OpenLineage) 이 task inlets / outlets 를 push
- 우리 케이스에선 **Cosmos 가 자동 emit** (dbt task 마다 outlet Asset URI 부착)
- Loupe / BQ / GCS operator 에 명시 outlets 추가 시 lineage 확장

## 이 통합 lineage 로 뭐가 가능해지나

DE 관점에서 강력한 3가지:

### 1. Impact analysis

- 지금: "이 컬럼 지우면 뭐 깨질까" → grep + 짐작
- 이후: DataHub UI 에서 downstream lineage 클릭 → 영향 모델 · 대시보드 · consumer 자동 표시
- **PR 리뷰 시 사전 확인 가능** (breaking change 예방)

### 2. 데이터 문서화

- 지금: 별도 위키에 수동 관리, 최신 상태 유지 어려움
- 이후: `schema.yml` 의 `description` → DataHub UI 에 자동 노출
- **문서 = 코드** (git PR 로 리뷰 → 배포 즉시 반영)

### 3. 이상 디버깅

- 지금: "값 이상해요" → 담당자 사냥
- 이후: DataHub lineage 따라 upstream 트래버스 → 어느 단계에서 값이 이상해졌는지 추적

## Airflow Asset — cross-DAG dependency 를 이벤트로

### Before (ExternalTaskSensor)

Upstream DAG 완료를 downstream DAG 이 sensor 로 폴링:
```python
# downstream_dag.py
wait = ExternalTaskSensor(
    task_id="wait_upstream",
    external_dag_id="upstream_dag",
    external_task_id="final_task",
    mode="reschedule",   # 그래도 slot 점유
)
```

문제:
- Worker slot 점유 (polling 동안)
- 지연 (polling interval)
- 코드 커플링 (external_dag_id 직접 참조)

### After (Airflow Asset)

Producer 가 outlet Asset 을 emit, Consumer 가 schedule 로 subscribe:

**Producer**:
```python
# 우리 케이스에선 cosmos 가 자동으로 outlets=[Asset(bq_uri)] 부착
```

**Consumer**:
```python
# downstream_dag.py
schedule=[Asset("bigquery/dev-dp-project-354904/datawarehouse_berriz/bizberry_community_contents_summary_integration")]
```

이득:
- **이벤트 기반** (polling 없음) → worker slot 절약
- **자원 절약** → GCP 비용 축소 축과 정합 (관련: [[3_결정B_팀별_DAG_저장소]])
- **cross-team / cross-repo 지원** — DAG 간 직접 참조 없이 Asset URI 만 공유

### 실증 (Story 팀 2026-07)

- Producer: `berriz_0101_bizberry_hourly_integration` (4 mart)
- Consumer: `berriz_bizberry_downstream_demo_integration` (`schedule=[Asset(summary), Asset(overview_trend)]`)
- Producer mart 완료 → Consumer 자동 트리거 확인

## Cosmos 의 Asset emit 매커니즘

- Cosmos 는 dbt 모델마다 `DatasetAlias` 로 outlet 자동 부착 (Airflow 3 native)
- URI 포맷 (Airflow 3): `bigquery/<project>/<dataset>/<table>` (slash 구분)
- Airflow 2 시절 dot 구분 (`bigquery/<project>.<dataset>.<table>`) 은 deprecated
- 관련 warning suppress: `AIRFLOW__COSMOS__USE_DATASET_AIRFLOW3_URI_STANDARD=1`

## DataHub 관리 정책

### 현재 (임시)

- CI 에서 DataHub emit 은 disable — GitHub-hosted runner 가 사내 DataHub GMS (`datahub-gms-gcp-dev.kakaodev.io`) 접근 불가
- **로컬에서 수동 emit** — 데이터플랫폼팀 담당 (관련: [[애슬론/8_배포 시 유의할 점]] § 4)

### 이후 (self-hosted runner 세팅 후)

- GH Actions 로 자동 emit
- 매 dbt parse 후 manifest → DataHub push
- Airflow task 도 datahub-airflow-plugin 자동 push

### 툴 선택 근거

DataHub vs Knowledge Catalog (Dataplex) 비교 결과 DataHub 채택:
- dbt manifest native integration
- Sidecar system lineage (MySQL CDC / Kafka) 지원
- Column-level lineage
- Acryl Cloud SaaS 옵션 (미래)

상세: [[스케줄러/10_DataHub vs Knowledge Catalog 비교]]

## End-to-end lineage 예시

한 번에 이어지는 그림:

```
[MySQL: kakaopage.user_action]
         │  (사내 extract operator, OpenLineage outlet)
         ▼
[BQ: raw.user_action_raw]           ← dbt source()
         │  (dbt ref)
         ▼
[BQ: stg.stg_user_action]            ← dbt model
         │  (dbt ref)
         ▼
[BQ: marts.daily_user_summary]       ← dbt model + column doc
         │  (Airflow Python task, OpenLineage outlets)
         ▼
[GCS: gs://reports/daily/...]
```

**"이 GCS 파일 어디서 왔지" → MySQL 원천까지 한 번에 추적 가능**.

## 관련 문서

- [[스케줄러/9_Airflow Asset과 Dataset]] — Asset 개념 상세
- [[스케줄러/10_DataHub vs Knowledge Catalog 비교]] — 툴 선정 근거
- [[애슬론/4_Asset-Centric 아키텍처 안]] — Asset 중심 아키텍처 설계
- [[애슬론/8_배포 시 유의할 점]] § 4-5 — DataHub CI 제약 실전
- [[dbt/2_schema 관리]] — schema.yml 을 통한 문서화 흐름
