---
title: "Asset scheduling 심화 (데이터플랫폼팀)"
status: draft
tags:
  - airflow
  - asset
  - data-platform
  - baseline
created: 2026-07-24
updated: 2026-07-24
---

# Asset scheduling 심화 (데이터플랫폼팀)

> Airflow 3 asset scheduling 의 **실전 동작 + 제약 + 3.2/3.3 개선 + 해결 패턴** 을 정리한 팀 내부 baseline. Story 팀 dogfooding (`storydata-airflow-dags`) 기반 실측 + 3.2/3.3 이론 정리. **월요일 (2026-07-27) MDL 조사자와의 aligning 자료로도 활용**.

기존 [[../스케줄러/9_Airflow Asset과 Dataset]] 문서가 개념/기능 소개를 다뤘다면, 이 디렉토리는 **"실제로 붙여보니 어떻게 동작하고, 뭐가 안 되고, 3.2/3.3 이 어떻게 바뀌는가"** 를 다룬다.

## 읽는 순서

교수법 순서 (**mechanics → gap → 개선 → 우회 → 조직 방침**):

| # | 문서 | 성격 | 대상 | 예상 시간 |
|---|---|---|---|---|
| 0 | [[0_결론]] | Executive summary | 모두 | 5분 |
| 1 | [[1_동작_원리]] | Asset event 3층 mechanics | 팀원 baseline | 15분 |
| 2 | [[2_문제_정의]] | 3.1 gap 3가지 | 팀원 baseline | 10분 |
| 3 | [[3_Airflow_3.1_3.2_비교]] | 3.1 vs 3.2/3.3 비교 + 로드맵 | 팀원 baseline | 15분 |
| 4 | [[4_해결_패턴]] | Bridge patterns (Stamp / Alias / Mapping) | 실무자 | 20분 |
| 5 | [[5_계층_분리_원칙]] | Cosmos vs Stamp 계층 분리 | 아키텍트 | 10분 |
| 6 | [[6_Asset_vs_Sensor_판단표]] | 언제 asset, 언제 sensor | 팀원 상시 참고 | 5분 |
| 7 | [[7_Provider_확장_제안]] | Provider 확장 로드맵 (팀 내부) | 팀 내부 | 15분 |
| 8 | [[8_시연_스토리라인]] | 시연 chapter 1~5 | 시연 준비 | 10분 |
| 9 | [[9_Past_DAG_재실행과_Downstream]] | 재실행 실측 (진행 중) | 실측 참여자 | 10분 |
| 10 | [[10_MDL_aligning_노트]] | 월요일 MDL 조사자 aligning | 개인 미팅 준비 | 15분 |

**팀 공유 baseline**: 1 ~ 4 (여기까지 읽으면 asset 이 어떻게 돌아가는지 다 이해)
**심화 참고**: 5 ~ 8
**실측 / 미팅**: 9, 10

## 이 디렉토리의 두 목적

### 목적 A — 팀 내부 baseline

> "Airflow asset 이 어떻게 돌아가고 있는가" 를 팀원이 baseline 으로 이해

- 신규 팀원 온보딩
- 소비자 팀 (Story / MDL) 지원 시 참조
- Provider 확장 결정 시 근거

### 목적 B — 월요일 MDL 조사자와의 aligning

> MDL 팀 dataset_test.md 조사에 대응할 우리 관점 정리 + 대화 아젠다

- 우리 팀 관점 (조직 표준 + 3.1 실측 + 3.2/3.3 이론) 공유
- 서로의 발견 aligning
- 조직 방향 3가지 옵션 놓고 감 잡기

## 배경 요약

**시작점**: 데이터플랫폼팀이 Story 팀 DAG (`storydata-airflow-dags`) 을 dogfooding 하며 asset scheduling 실전 검증.

**발견한 3가지 gap (3.1 기준)**:
1. **Payload 없음** — Cosmos 자동 outlet 은 URI 만 emit. Downstream 이 producer 시간/파티션 정보 못 앎
2. **조건부 emit 없음** — Producer 완료 시 무조건 downstream 파급
3. **Partition 매칭 없음** — AND 조건이 시간축 매칭 안 함

**3.2/3.3 개선 방향** (이론 정리):
- `partition_key` first-class → gap 1, 3 대부분 해결
- `RollupMapper` / `FanOutMapper` → cadence 변환 native
- Cosmos 호환 여부는 PoC 확인 필요

**우리 팀 bridge (3.1 상태에서)**:
- Stamp task 로 self-describing event + 조건부 emit
- AssetAlias 로 partition-specific 채널
- Consumer dynamic task mapping

## 월요일 aligning 논점 (MDL 조사자와)

1. **MDL 팀의 Dagster PoC 결과 청취** — 확신도와 남은 논점
2. **Airflow 3.2/3.3 이 MDL 요구 얼마나 커버** — 정직한 매트릭스 ([[3_Airflow_3.1_3.2_비교]] Part E)
3. **조직 방향 3가지 옵션 감 잡기**
   - Option A: 전체 Airflow (커스텀 개발로 gap 메꿈)
   - Option B: 전체 Dagster (조직 이동)
   - Option C: 하이브리드 (계층별 도구, MDL 문서에서도 제안)
4. **데이터플랫폼팀 PoC 스코프 확정** — Docker Airflow 3.2/3.3 검증 항목
5. **재미팅 시점 / 각자 후속 액션**

## 후속 액션 (미팅 후 확정 예정)

**결정 필요 없이 진행 가능**:
- Docker Airflow 3.2/3.3 PoC 착수 ([[3_Airflow_3.1_3.2_비교]] Part F 항목 검증)
- Story 팀 dogfooding 실측 계속 ([[9_Past_DAG_재실행과_Downstream]])
- Provider 로컬 프로토타입 (얇게 유지)

**미팅 결과에 따라**:
- 조직 방향 상위 결정 프로세스 (매니저 / 리드 미팅)
- MDL 팀 Dagster 이관 지원 여부
- Provider 확장 스코프 확정 ([[7_Provider_확장_제안]] 톤 조정)

## 관련 문서 (외부)

- [[../스케줄러/9_Airflow Asset과 Dataset]] — Asset/Dataset 개념 소개 (선행 자료)
- [[../공유/10_Sensor를_Triggerer와_Asset으로_대체하기]] — 초기 sensor 대체 논의
- [[../공유/11_Cosmos란]] — Cosmos 개념
- [[dataset_test]] — MDL 조사자의 Dagster PoC 리포트 (상대 자료)

## 코드 레퍼런스

- 레포: `~/PycharmProjects/storydata-airflow-dags`
- Producer: `dags/storydata/berriz_0101_bizberry_hourly_integration.py`
- Consumer demo (debug task 추가됨): `dags/storydata/berriz_bizberry_downstream_demo.py`
- 여기서 논의된 stamp task / alias 는 로컬 프로토타입 예정 (아직 코드 반영 전)
