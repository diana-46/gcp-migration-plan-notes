# 애슬론 — 컨텍스트

> 이 폴더는 **athlon 플랫폼이 GCP 이관 후 어떻게 재구현되어야 하는지** 조사하는 자리.
> ETL→dbt, Presto→BigQuery 전환은 이미 **확정**된 전제. 그 위에서 athlon이라는 플랫폼이 어떻게 바뀌어야 하나를 다룬다.
>
> 결론 노트는 [[1_개요]].

---

## 풀고자 하는 문제 / 의사결정

상위에서 결정된 것:
- **ETL**: athlon (neptune) → **dbt**
- **쿼리 엔진**: presto → **BigQuery**

이 결정 위에서, 우리 플랫폼(athlon / athlon-ui)에서 다음을 어떻게 구현할지 조사한다:

1. **dbt 모델 관리**: athlon이 dbt 코드를 어떻게 품을 것인가? (직접 호스팅? 외부 git repo로 분리? UI에서 편집?)
2. **워크플로 / 액션 모델 변화**: 기존 `actions` / `action_dependencies` / `workflows` / `actions_meta` 개념은 dbt 전환 후 어떻게 재정의되나?
3. **Userlake**: 쿼리 엔진이 BQ로 바뀌면 athlon (userlake) 쿼리 생성 로직은 어떻게 바뀌나?
4. **extract**: athlon (extract)는 유지인데, GCP 환경에서 대체 가능한 GCP native 서비스가 있는지?
5. **UI 변화**: athlon-ui에서 dbt 모델 편집 / 의존성 시각화 / git PR 연동 등은 어떻게 구성하나?
6. **마이그레이션 전략**: 기존 athlon ETL을 dbt로 옮기는 절차 (자동 변환 가능? 수동?)

---

## 용어 / 약어

| 용어 | 의미 |
|---|---|
| **athlon** | 데이터플랫폼 메인 백엔드. ETL / Extract / Userlake / Workflow 등 묶음 |
| **athlon-ui** | athlon의 사용자 UI (프론트엔드) |
| **neptune** | athlon 내부의 ETL/Workflow 모듈명. `task` 정의가 여기 있음 |
| **actions** | athlon에서 task의 실행 단위. DB row로 관리 |
| **action_dependencies** | actions 간의 의존성 (상위 의존성 기반 그래프) |
| **workflows** | actions를 묶는 상위 개념 (DAG 단위에 가까움) |
| **actions_meta** | action 템플릿. 현재 1:1 매핑, 향후 1:N 확장 검토되었던 영역 |
| **Userlake** | athlon (userlake) 모듈. 사용자가 정의한 조건으로 쿼리를 생성하는 부분 |
| **extract** | athlon (extract) 모듈. 외부 → 내부로 데이터 가져오기 |
| **dbt** | Data Build Tool. SQL 기반 ETL 변환 도구. ELT 패러다임 |
| **dbt model** | dbt에서 SELECT 하나 = 모델 하나. 의존성은 `ref()`로 표현 |

---

## 외부 자료

### 관련 코드 레포 (로컬 경로)

- **athlon**: `~/IdeaProjects/athlon`
  - 백엔드. neptune 모듈에 task / workflow 정의.
- **athlon-ui**: `~/WebstormProjects/athlon-ui`
  - 프론트엔드. workflow / action 관리 UI.
- **airflow-dags**: `~/PycharmProjects/airflow-dags`
  - 현재 Airflow DAG. ETL 호출 부분이 dbt로 옮겨갈 대상.

### Confluence

- _(필요 시 추가)_

### dbt 공식 자료

- [dbt Documentation](https://docs.getdbt.com/)
- [dbt-bigquery adapter](https://docs.getdbt.com/docs/core/connect-data-platform/bigquery-setup)

---

## 관련 사람

- **Owner / 의사결정자**: 디아나 (diana.46@kakaoent.com)
- _(필요 시 백엔드/프론트 담당자 추가)_

---

## Stack / 환경

| 항목 | 현재 (AS-IS) | 이관 후 (TO-BE) |
|---|---|---|
| ETL 정의 | athlon (neptune) / DB row | dbt project (SQL + yaml, git) |
| ETL 실행 | Airflow → athlon | Airflow → dbt run (Composer or Self-managed에서) |
| 쿼리 엔진 | Presto | BigQuery |
| Userlake 쿼리 생성 대상 | Presto SQL | BigQuery SQL |
| Workflow 정의 | athlon DB (`workflows`, `actions`, `action_dependencies`) | _(조사 대상)_ |
| 형상관리 | DB 직접 수정 | git PR 기반 (요구사항이었음) |

---

## 로컬 규칙

이 폴더에서만 추가로 지키는 규칙:

- **"ETL→dbt", "Presto→BQ" 전환 자체는 재논의 대상이 아니다.** 이미 확정. (Anti-context 참고)
- 비교/조사는 **athlon 플랫폼 관점**에서 수행. 단순 "dbt가 좋은가" 같은 일반론은 [[../스케줄러/README]] 또는 다른 폴더에서.
- 기존 athlon 데이터 모델(actions / workflows 등) 변경 제안은 **마이그레이션 영향**까지 같이 적는다.

---

## Anti-context (재논의/조사 안 함)

- **athlon (neptune)을 유지할까?** → ❌ 이미 dbt 전환 확정.
- **Presto 유지 옵션** → ❌ 이미 BigQuery 확정.
- **다른 ETL 도구 (Airbyte, Fivetran 등) 검토** → ❌ dbt 확정.
- **Airflow vs 다른 스케줄러** → [[../스케줄러/README]] 에서만 다룸 (Airflow 유지 확정).

---

## 스케줄러 폴더와의 분담

| 어디서? | 다루는 내용 |
|---|---|
| [[../스케줄러/README\|스케줄러/]] | Airflow 인프라(Composer vs Self-managed), Airflow에서 **dbt를 어떻게 실행**할지 (Operator 선택, 실행 환경, 격리) |
| 여기 (애슬론/) | dbt **모델 자체의 관리·생성·배포**가 athlon에서 어떻게 일어날지, athlon DB/UI/extract/userlake의 변화 |

겹치는 영역(예: dbt 실행 시점의 manifest 전달)은 한쪽에 적고 다른 쪽에서 wikilink로 참조한다.
