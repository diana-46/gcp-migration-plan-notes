# dbt — 컨텍스트

## 풀고자 하는 문제 / 의사결정

Neptune/Presto 기반 SQL ETL을 **dbt + BigQuery + Composer(Cosmos)** 스택으로 이관하기 위한 검토·PoC 기록과 팀 공통 규약 초안.
`0_`~`7_`은 2026-06 PoC에서 각 축을 "Neptune이 하던 것 → dbt로는 어떻게" 형태로 실측 검증한 기록, `8_`~`11_`은 그 결과로 굳어진 전략·규약·라이브러리 운영 모델.

## 확정 사항

- 주력 incremental 전략 = **`insert_overwrite`** (예외적으로 `merge`는 승인제)
- 스키마는 자동 sync 대신 **명시**: `contract: enforced` + `on_schema_change: fail`
- 파티션/타임존은 KST·UTC suffix 강제 (`_kst`/`_utc`)
- 환경 분리는 profiles.yml target + Composer env var
- 공용 매크로는 별도 레포([dp-dbt-utils](https://github.com/kakaoent/dp-dbt-utils)) + **git 태그 pin** (브랜치 금지)
- dbt 버전은 로컬/CI/Composer 모두 **1.9.x 고정** (1.11 manifest는 1.9가 못 읽어 task hang)

## 노트

- [[0_dbt 기본 개념]] — dbt 정의·핵심 객체·실행 사이클·manifest 역할 (reference)
- [[1_materialization]] — materialization별 trade-off와 Neptune 패턴 매핑. PoC는 효율 우선, 운영 후 가시성 확보
- [[2_schema 관리]] — Neptune 자동 sync vs dbt 명시 스키마 실측 비교 → 명시 패턴 채택
- [[3_backfill]] — `insert_overwrite`가 idempotency를 강제하므로 백필 DAG가 단순해짐 (실측)
- [[4_parameter 치환]] — `${execution_date}` → `{{ var() }}`. Airflow/dbt 이중 Jinja 함정. 실비용은 SQL 방언 번역
- [[5_의존성 관리]] — `upstreamEtlIds` → `ref()`/`source()`. DAG 간 wiring은 helper 또는 Asset 필요
- [[6_배포와 환경 분리]] — dbt 레포/DAG 레포 GCS·Composer 배포, manifest 재생성 필수, rollback
- [[7_테이블 아웃풋]] — BQ Native/GCS External/BigLake 저장 전략, ephemeral 디폴트 + lifecycle 설계
- [[8_insert_overwrite_매커니즘]] — 실제 SQL 시퀀스(tmp→MERGE ON FALSE), dynamic/static, 병렬 백필 안전성
- [[9_Presto-BigQuery 이관 규약]] — **팀 공통 이관 규약 v0.1 draft** (표준/금지/검증/이관 순서/Open Questions)
- [[10_incremental_strategy_비교]] — 전략별 비교표와 결정 매트릭스
- [[11_공용 라이브러리 (dp-dbt-utils)]] — 재사용 매크로 분리 rationale과 소비/기여 워크플로우

## 용어 / 약어

- **Neptune** — 기존 사내 SQL ETL 플랫폼 (`EtlInputType.macro`, BackfillService 등)
- **Cosmos** — Airflow에서 dbt 프로젝트를 DbtTaskGroup으로 펼쳐주는 라이브러리
- **insert_overwrite** — 파티션 단위 replace incremental 전략 (dynamic/static/copy_partitions)
- **manifest.json** — dbt 파싱 산출물. 깨지면 전부 깨짐. schema v12(1.9)/v13(1.11) 비호환 주의

## 외부 자료

- [dbt 공식 문서](https://docs.getdbt.com/) · [dp-dbt-utils](https://github.com/kakaoent/dp-dbt-utils)
- PoC 레포: `dbt-test` / `dbt-test-airflow-dags` · 파일럿: `berrizdata-dbt`, `storydata-dbt`, `mlb-dbt`, `musicdata-lab-dbt`
- 상위 검토 문서: [[../애슬론/5_Neptune SQL 변환의 dbt-BigQuery 대체 검토|애슬론/5_Neptune SQL 변환의 dbt-BigQuery 대체 검토]]
