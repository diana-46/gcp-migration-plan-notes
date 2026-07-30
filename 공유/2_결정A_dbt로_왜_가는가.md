# 2. 결정 A — SQL: Neptune → dbt

> 왜 SQL 을 dbt 로 이관하는가. Neptune 관리의 구조적 한계 + dbt 로 얻는 것 + 실증.
> 관련: [[애슬론/5_Neptune SQL 변환의 dbt-BigQuery 대체 검토]], [[dbt/0_dbt 기본 개념]]

## Neptune SQL 관리의 한계 2가지

### 1. 코드가 DB 안에 있음

Neptune SQL 은 MySQL 의 TEXT 컬럼에 저장.

- **`git blame` 불가** — "3개월 전 이 WHERE 왜 이렇게 짰지" 답을 찾기 어려움
- **diff / PR 리뷰 없음** — 저장 전후 무엇이 바뀌었는지 UI 에서 확인 힘듦. 리뷰 = 슬랙 스크린샷.
- **롤백 자연스럽지 않음** — 실수로 WHERE 빼먹은 뒤 이전 상태로 되돌리기 번거로움
- **검색 제한** — "이 컬럼 쓰는 ETL 어디 있지" 를 grep 으로 찾을 방법 없음
- **테스트 히스토리 없음** — 이 SQL 이 언제부터 무슨 결과 냈는지 추적 안 됨

### 2. 로컬에서 미리 돌려보기 어려움

Neptune 반복 사이클:
```
UI 에 SQL 넣기 → 저장 → Airflow 트리거 → 대기 → 로그 확인 → 실패 → 다시
```
한 사이클 ~10분. 실험 많은 날엔 시간 소모 큼.

dbt:
```
$ dbt run --select my_model
Completed successfully in 8s.
```
로컬에서 10초. 컬럼 값 훑어보며 반복 개발 가능.

## dbt 로 얻는 것

| 관점 | Neptune | dbt |
|---|---|---|
| 저장 위치 | MySQL TEXT | `.sql` 파일 (git) |
| 히스토리 | 없음 | `git blame` / `git log` |
| 리뷰 | 슬랙 스크린샷 | GitHub PR + CODEOWNERS |
| 로컬 사이클 | ~10분 | ~10초 |
| 의존성 | UI 클릭 | `{{ ref('upstream') }}` — 자동 감지 |
| 파라미터 | EtlParameter | `{{ var('run_date') }}` |
| 파티션 | `ADD PARTITION` + HDFS | `partition_by` + `insert_overwrite` — BQ 처리 |
| 테스트 | 별도 구축 | `dbt test` (`unique`, `not_null`, custom) |
| 문서 | 별도 위키 | `schema.yml` — 자동 사이트 |
| Lineage | 파편 / 컬럼 X | dbt manifest → DataHub column-level |
| Impact analysis | grep + 짐작 | `dbt ls --select +model` / DataHub downstream |

**부수 효과**: dbt 는 세계 표준 툴. 익힌 스킬이 팀 밖·회사 밖·커리어에서도 활용됨.

## Neptune ↔ dbt semantic 매핑

이관 시 실제 마주치는 주요 대응:

| Neptune 패턴 | dbt 대응 | 상세 |
|---|---|---|
| PLAIN, no partition, 매번 재생성 | `materialized='table'` | [[dbt/1_materialization]] |
| PLAIN, daily partition, 재빌드 | `incremental` + `insert_overwrite` | 같은 노트 |
| YAML multi-temp + perm | `temp/` 계층 + mart | [[dbt/7_테이블 아웃풋]] |
| hourly-in-daily block replace | custom `insert_only` + pre_hook DELETE | [[dbt/8_insert_overwrite_매커니즘]] |
| hourly partition + replace | `insert_overwrite` + hourly TIMESTAMP partition | 같은 노트 |
| Presto SQL 방언 | BigQuery SQL 방언 (`FORMAT_DATE`, `PARSE_TIMESTAMP`, `DATETIME_ADD` 등) | 자동 변환 도구 준비 |
| Presto `ROW` / `ARRAY(ROW)` | BQ `STRUCT` / `ARRAY<STRUCT>` | 매핑 1:1 |
| `${var}` 치환 | `{{ var('...') }}` | [[dbt/4_parameter 치환]] |
| 명시적 upstream 선언 | `ref()` 자동 추론 | [[dbt/5_의존성 관리]] |
| 스키마 자동 sync | `on_schema_change` + `contract` | [[dbt/2_schema 관리]] |

## PoC 실증 — bizberry hourly 4 mart 이관 (2026-07)

Story 팀 얼리어답터 케이스. Neptune 원본 세만틱을 유지하며 이관:

| Mart | Neptune 유형 | dbt 전략 | Partition | 특징 |
|---|---|---|---|---|
| userpost (176) | PLAIN 단일 쿼리 | custom `insert_only` + pre_hook | daily | hour-block replace 시맨틱 |
| artistpost (175) | PLAIN 단일 쿼리 | `insert_overwrite` | hourly | dbt 기본 매커니즘 |
| overview_trend (165) | YAML 3-temp + nested `STRUCT` | custom `insert_only` + pre_hook | daily | Presto `ROW` / `ARRAY(ROW)` → BQ `STRUCT` / `ARRAY<STRUCT>` |
| contents_summary (181) | YAML 5-temp + UNION ALL | `insert_overwrite` | hourly | 5-way UNION, `contents_type` 롤업 |

**검증된 것**:
- dbt-bigquery 매크로 override 로 커스텀 `insert_only` 전략 정의 → Neptune block-replace 세만틱 재현
- `run-scoped alias` 로 백필 병렬 안전 확보 (Note 7 § 5-6 패턴)
- `hours_to_expiration=2` 로 temp 자동 소멸 (Neptune 의 "ETL 끝나면 사라지는 temp" 세만틱 1:1)
- Nested type (`STRUCT`, `ARRAY<STRUCT>`) BQ 표현 완비
- Cosmos + Airflow 3.1 (`airflow.sdk`) 로 dbt DAG 자동 렌더

**관련 노트**: [[애슬론/5_Neptune SQL 변환의 dbt-BigQuery 대체 검토]], [[애슬론/8_배포 시 유의할 점]], [[dbt/8_insert_overwrite_매커니즘]]

## 흔한 질문

**"내가 못 짜면 어쩌지"**
- CI 에서 DAG parse, dbt parse 자동 검증
- PR 리뷰 안전망 (지금 UI 저장은 리뷰 자체가 없음)
- 첫 3 모델은 플랫폼팀 pair 리뷰

**"로컬 세팅 부담"**
- `python3.11 -m venv` + `pip install -r requirements.txt` + `gcloud auth application-default login` 3 스텝
- README 에 정리됨

**"dbt 학습 곡선"**
- SELECT + Jinja 조합. SQL 은 그대로.
- LLM 도구 (Claude, Copilot) 로 첫 뼈대 생성 후 검수
- 5-page 챗봇용 gotcha 노트 [[dbt/0_dbt 기본 개념]] ~ [[dbt/8_insert_overwrite_매커니즘]]

## 관련 문서

- [[애슬론/5_Neptune SQL 변환의 dbt-BigQuery 대체 검토]] — 기술 검증 결과
- [[애슬론/8_배포 시 유의할 점]] — 실전 배포 함정
- [[dbt/0_dbt 기본 개념]] — dbt 기본기
- [[dbt/1_materialization]] — materialization 별 trade-off
- [[dbt/7_테이블 아웃풋]] — 저장 전략
- [[dbt/8_insert_overwrite_매커니즘]] — insert_overwrite 내부 동작
