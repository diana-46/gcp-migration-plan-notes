# 애슬론 — 컨텍스트

> 이 폴더는 **athlon 플랫폼이 GCP 이관 + dbt 도입 후 어떻게 재구현되어야 하는지** 조사하는 자리.
> ETL→dbt, Presto→BigQuery 전환은 이미 **확정**된 전제. 그 위에서 athlon이라는 플랫폼이 어떻게 바뀌어야 하나를 다룬다.
>
> 결론 노트는 [[1_개요]].
> 편입 노트 (2026-08-20, 구 `neptune/`·`memo/` 폴더에서 병합): [[9_이관대상 Operator 인벤토리]] · [[10_athlonQuerySensor 관련]]

---

## athlon 의 미션 (dbt 시대)

> **dbt가 흡수한 영역(SQL 변환) + Airflow operator로 남은 영역(orchestration·sensor·extract)을 5축으로 통합 관리하는 플랫폼.**

5축:

1. **UI** — dbt 모델 + actions_meta + ActionGroup 단일 편집
2. **실행 그래프** — ActionGroup 단위로 묶고 Airflow task로 변환
3. **백필** — ActionGroup 단위, dbt vars + non-dbt 파라미터 일관 주입
4. **Git Sync** — dbt project + actions_meta yml 한 레포 형상관리
5. **DataHub Lineage** — dbt manifest + Airflow + athlon-등록 edge 통합 그래프

→ dbt + Airflow 만으로는 절대 안 되는 영역이 있다. 특히 **수집(extract) → ETL(transform)** 연결 lineage가 dbt 시야 밖. athlon이 이 gap을 메우는 게 존재 이유. 디테일은 [[3_dbt 능력 경계와 영역 분담]].

---

## 풀고자 하는 문제 / 의사결정

상위에서 결정된 것 (재논의 X):

- **ETL**: athlon (neptune) → **dbt**
- **쿼리 엔진**: presto → **BigQuery**

이 결정 위에서 풀어야 할 문제:

1. **dbt 지원** — 어떤 형태로 athlon이 dbt 모델을 품을지
2. **UI 세팅 지원** — dbt 모델 / actions_meta / ActionGroup 단일 UI
3. **UI → DBT Sync + Git 관리** — UI 저장이 git commit + dbt parse로 이어지는 흐름
4. **DataHub 리니지 관리** — dbt manifest + Airflow + athlon-등록 edge 통합
5. **ActionGroup** — 이종 묶음을 의존성·백필·리니지 단위로 관리 (Neptune 리아키텍처에서 살린 개념)
6. **Custom Operator triage** — 기존 athlon 측 operator를 어디(dbt vs Airflow)로 보낼지

상세는 [[1_개요]]의 결정 포인트.

---

## 용어 / 약어

### athlon 내부

| 용어 | 의미 |
|---|---|
| **athlon** | 데이터플랫폼 메인 백엔드 (Kotlin / Spring Boot / GraphQL) |
| **athlon-ui** | 사용자 UI (React / TypeScript) |
| **neptune** | athlon 내부 ETL/Workflow 모듈. `task` 정의가 여기 있음 |
| **Etl** | Neptune의 ETL 정의 (SQL 변환 로직) |
| **EtlParameter** | ETL에 주입되는 동적 파라미터 (dbt `var()`에 해당) |
| **EtlPartition** | 타겟 테이블의 파티션 정의 (dbt incremental에 해당) |
| **EtlDependencies** | 상위 ETL/DAG 의존성 (dbt `ref()`에 해당) |
| **actions** | task의 실행 단위. DB row로 관리. Airflow가 read |
| **action_dependencies** | actions 간 의존성 |
| **workflows** | actions를 묶는 상위 개념 (DAG 단위) |
| **actions_meta** | action 템플릿 (현재 1:1, 향후 1:N 검토되었던 영역). dbt 도입 후 일부 잔존 |
| **ActionGroup** | (Neptune 리아키텍처 제안 개념) actions_meta 묶음. dbt+non-dbt 통합 단위 |
| **EtlMapping** | ETL ↔ Actions junction table |
| **Userlake** | athlon (userlake) 모듈. 조건 기반 쿼리 생성 |
| **extract** | athlon (extract) 모듈. 외부 → 내부 수집 |
| **PipelineBuilder** | athlon의 ETL → Actions 자동 생성 엔진 |

### dbt

| 용어 | 의미 |
|---|---|
| **dbt** | Data Build Tool. SQL 기반 ELT 변환 도구 |
| **dbt model** | SELECT 하나 = 모델 하나. `.sql` + `schema.yml` |
| **`ref('model')`** | 같은 dbt project 안 다른 모델 참조 → 의존성 자동 |
| **`source('schema', 'table')`** | 외부 데이터(raw) 입력 선언 |
| **macro** | 재사용 가능한 Jinja 매크로 (actions_meta의 dbt 대응) |
| **on-run-start / end hook** | dbt run 시작·끝에 실행되는 SQL |
| **dbt test** | unique / not_null / 관계 검증 |
| **dbt seed** | CSV import |
| **dbt snapshot** | SCD (Slowly Changing Dimension) |
| **incremental** | 증분 적재 모델 |
| **manifest** | dbt parse 결과물. 모든 모델 / 의존성 / 메타데이터 포함. DataHub 통합의 핵심 |
| **Python model** | dbt 1.3+. Dataproc Serverless 위 PySpark 실행 |
| **profiles.yml** | 환경별 (dev/prod) DB 연결 설정 |
| **target** | profiles.yml 안의 환경 변수 묶음 |
| **dbt-bigquery** | dbt의 BigQuery 어댑터 |

### 통합 / 운영

| 용어 | 의미 |
|---|---|
| **Cosmos** | `astronomer-cosmos`. dbt 모델을 Airflow task로 자동 분해하는 라이브러리 |
| **OpenLineage** | task lineage 표준. Airflow → DataHub 흐름의 기반 |
| **datahub-airflow-plugin** | OpenLineage 이벤트를 DataHub로 push |
| **datahub-dbt** | dbt manifest를 DataHub로 push |
| **BigQuery Remote Function** | Cloud Run/Function을 BQ SQL에서 호출. dbt에서 외부 API 부르는 우회로 |
| **`EXPORT DATA` / `LOAD DATA`** | BQ ↔ GCS SQL 명령. dbt에서 가능 |

### DataHub

| 용어 | 의미 |
|---|---|
| **Dataset** | 테이블 / 파일 / 외부 source. lineage 의 노드 |
| **DataJob** | Airflow task / dbt model 같은 실행 단위 |
| **DataFlow** | DataJob 묶음. Airflow DAG / ActionGroup 매핑 |
| **URN** | DataHub entity의 고유 식별자. `bigquery://project/dataset/table` 같은 형식 |
| **column-level lineage** | 컬럼 단위 lineage. dbt가 잘 지원 |

---

## 외부 자료

### 관련 코드 레포

| 레포 | 경로 | 용도 |
|---|---|---|
| **athlon** | `~/IdeaProjects/athlon` | 백엔드. neptune 모듈. |
| **athlon-ui** | `~/WebstormProjects/athlon-ui` | 프론트엔드. |
| **airflow-dags** | `~/PycharmProjects/airflow-dags` | 현재 Airflow DAG. ETL 호출 부분이 dbt로 이전 대상. |
| **neptune-sql** | `github.kakaocorp.com/kakaopage/neptune-sql` | 현재 수동 관리되는 SQL 레포. ~390개 파일. 신규 athlon-etl-definitions 레포로 대체 예정 |
| **athlon-etl-definitions** (예정) | (계획 중) | dbt project + actions_meta yml 통합 git 레포 |

### Confluence

- **자매 문서 (import 완료)**:
  - [[2_Git 동기화·dbt 전환 계획]] (`https://kakaoent.atlassian.net/wiki/spaces/DP/pages/5034707500/...`)
- **참고 자료 (import 안 함)**:
  - "[Neptune] 리아키텍처 플랜 — workflows/actions UI 관리 및 통합" (`https://kakaoent.atlassian.net/wiki/spaces/~711302581/pages/5031592304/...`) — Hive 가정 기반의 옛 플랜. **ActionGroup 개념만 살리고** 그 외는 dbt 시대에 맞게 재설계 필요.

### dbt / DataHub 공식 자료

- [dbt Documentation](https://docs.getdbt.com/)
- [dbt-bigquery adapter](https://docs.getdbt.com/docs/core/connect-data-platform/bigquery-setup)
- [astronomer-cosmos](https://astronomer.github.io/astronomer-cosmos/)
- [DataHub dbt integration](https://datahubproject.io/docs/generated/ingestion/sources/dbt)
- [DataHub Airflow plugin](https://datahubproject.io/docs/lineage/airflow)
- [OpenLineage](https://openlineage.io/)

---

## 관련 사람

- **Owner / 의사결정자**: 디아나 (diana.46@kakaoent.com)
- _(필요 시 백엔드/프론트 담당자 / Git 동기화 문서 작성자 등 추가)_

---

## Stack / 환경

| 항목 | 현재 (AS-IS) | 이관 후 (TO-BE) |
|---|---|---|
| ETL 정의 | athlon (neptune) / DB row | dbt project (SQL + schema.yml, git) + 잔존 actions_meta yml |
| ETL 실행 | Airflow → athlon PipelineBuilder | Airflow → dbt run (KubernetesPodOperator 또는 Cosmos) |
| 쿼리 엔진 | Presto + Hive | BigQuery |
| Userlake 쿼리 생성 대상 | Presto SQL | BigQuery SQL |
| Workflow 정의 | athlon DB (workflows / actions / action_dependencies) | athlon DB + dbt manifest 통합 |
| Extract | athlon (extract) 모듈 | athlon (extract) 유지 (GCP native 대안도 검토) |
| 형상관리 | DB 직접 수정 (일부 neptune-sql 수동 sync) | git PR 기반 (athlon-etl-definitions 신설 또는 dbt project 분리) |
| Lineage | 부분적 (DataHub + Airflow plugin 일부) | dbt manifest + Airflow OpenLineage + athlon-등록 edge 통합 |

---

## 로컬 규칙

이 폴더에서만 추가로 지키는 규칙:

- **"ETL→dbt", "Presto→BQ" 전환 자체는 재논의 대상이 아니다.** 이미 확정 (Anti-context).
- **dbt vs non-dbt 분담 기준**: [[3_dbt 능력 경계와 영역 분담]] 의 Custom Operator triage 매트릭스 사용. 개별 결정 시 매트릭스 어느 칸인지 명시.
- **모든 새 기능 제안**은 5축(UI / 실행 그래프 / 백필 / Git Sync / Lineage) 중 어디에 속하는지 태그.
- **기존 athlon 데이터 모델 변경**은 마이그레이션 영향 (manual actions ~3,952개 등)까지 같이 적는다.
- **단순 "dbt가 좋은가" 같은 일반론**은 여기 안 적음. [[../스케줄러/README]] 또는 별도 dbt 폴더 (필요 시) 영역.

---

## Anti-context (재논의 안 함)

- **athlon (neptune)을 그대로 유지** → ❌ 이미 dbt 전환 확정
- **Hive 기반 옛 Neptune 리아키텍처 플랜 (`HIVE_ETL` template type 통합 등)** → ❌ Hive 가정 폐기. ActionGroup 개념만 살림
- **Presto 유지 옵션** → ❌ BigQuery 확정
- **다른 ETL 도구 (Airbyte / Fivetran 등) 검토** → ❌ dbt 확정
- **Airflow 외 스케줄러** → [[../스케줄러/README]] 영역 (Airflow 유지 확정)
- **dbt Cloud 도입 자체** → 별도 검토 가능하지만 우선순위 X (dbt-core 가정)

---

## 스케줄러 폴더와의 분담

| 어디서? | 다루는 내용 |
|---|---|
| [[../스케줄러/README\|스케줄러/]] | Airflow 인프라(Composer vs Self-managed), **Airflow에서 dbt를 어떻게 실행** (Operator 선택 / Pod 스펙 / manifest 전달) |
| 여기 (애슬론/) | **dbt 모델 자체의 관리·생성·배포**가 athlon에서 어떻게 일어날지, ActionGroup, DataHub lineage 통합, Userlake/extract 변화 |

겹치는 영역은 한쪽에 적고 다른 쪽에서 wikilink로 참조.
