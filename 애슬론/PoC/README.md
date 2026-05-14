# 애슬론 PoC

> Asset-Centric 방향성 ([[../4_Asset-Centric 아키텍처 안]]) 을 직접 만져보면서 체화하는 자리.
> 빠르게 손으로 확인 / 실험한 결과를 메모로 누적.

## 🎯 PoC 의 목적

1. **체감**: Airflow 3 Asset / dbt / DataHub 를 직접 만져보고 패러다임 익숙해지기
2. **검증**: 우리 GCP 환경에서 진짜 동작하는지 (Composer 3 + BQ + Memorystore 등)
3. **회의 ammunition**: 팀 논의 시 "이거 진짜 돼요" 라는 시연 자료

## 📋 PoC 항목 (단계별)

### Step 1. Airflow 3 Asset 맛보기

목표: `inlets / outlets / schedule=[Asset]` 직접 만져보기

- [ ] 로컬에 Airflow 3 docker-compose 띄우기
- [ ] 샘플 DAG A 만들기 — `outlets=[Asset("test://my_table")]`
- [ ] 샘플 DAG B 만들기 — `schedule=[Asset("test://my_table")]` → 자동 트리거 확인
- [ ] AssetWatcher 1개 동작시켜 보기 (file watcher 등)
- [ ] UI 의 "Datasets" 메뉴 둘러보기 — 기대와 실제 비교

기록: `01_airflow3_asset.md` (TBD)

### Step 2. dbt + BigQuery 맛보기

목표: dbt project 가 무엇을 자연스럽게 해주는지 체감

- [ ] BQ sandbox 프로젝트 생성 (개인 dev, 무료 quota 내)
- [ ] `dbt init` → 샘플 project
- [ ] sources → staging → marts 3 layer 모델 5~10개 작성
- [ ] `dbt run` / `dbt test` / `dbt docs generate` 실행
- [ ] `schema.yml` 에 test 선언 → 실패 시 동작
- [ ] manifest.json 구조 살펴보기

기록: `02_dbt_bigquery.md` (TBD)

### Step 3. dbt + Airflow 통합

목표: dbt run 을 Airflow task 로 어떻게 실행하나 패턴 비교

- [ ] Pattern A: 단순 `BashOperator + dbt run --select`
- [ ] Pattern B: `KubernetesPodOperator + dbt-runner image`
- [ ] Pattern C: Cosmos 도입 — 모델별 task 분해 확인
- [ ] 각 패턴의 Airflow Grid view 비교
- [ ] DataHub lineage 가 어떻게 들어오는지 확인

기록: `03_dbt_airflow_integration.md` (TBD)

### Step 4. DataHub 연동

목표: Asset URN ↔ DataHub Dataset 매핑 검증

- [ ] DataHub 로컬 구동 (Docker)
- [ ] `datahub-dbt` 로 manifest push
- [ ] `datahub-airflow-plugin` 으로 task lineage push
- [ ] BQ URN 으로 자동 stitching 되는지 확인
- [ ] AssetGroup → DataFlow 매핑 가능성

기록: `04_datahub_integration.md` (TBD)

### Step 5. Asset-Centric 미니 prototype

목표: 우리 디자인 ([[../4_Asset-Centric 아키텍처 안]]) 의 일부를 손으로 구현해보기

- [ ] Asset YAML 스키마 v1 작성
- [ ] 샘플 ETL 1개를 Asset YAML 로 손 변환
- [ ] 미니 compiler (Python script) — YAML → Airflow DAG
- [ ] Composer 3 sandbox 에서 실행
- [ ] **회의 시연용** 으로 정리

기록: `05_asset_centric_proto.md` (TBD)

### Step 6. 사고법 학습 (병행)

- [ ] Dagster docs "Software-Defined Assets" 챕터 읽기
- [ ] dbt 의 "model versioning", "exposures" 개념 학습
- [ ] 사례 reading (dbt + Airflow 통합한 회사들 후기)

기록: `06_asset_paradigm_notes.md` (TBD)

## 📝 기록 규칙

각 step 별 노트에 다음 포함:

```yaml
---
title: "..."
status: in-progress | done
created: YYYY-MM-DD
---

# 제목

## 목표
## 환경 / 사전 준비
## 진행 메모 (시간순)
## 발견 / 깨달음
## 안 풀린 의문
## 다음 step 으로 가져갈 것
```

## 🔄 진행 상태

| Step | 상태 | 비고 |
|---|---|---|
| 1. Airflow 3 Asset | ⬜ 대기 | |
| 2. dbt + BQ | ⬜ 대기 | |
| 3. dbt + Airflow 통합 | ⬜ 대기 | |
| 4. DataHub 연동 | ⬜ 대기 | |
| 5. Asset-Centric prototype | ⬜ 대기 | |
| 6. 사고법 학습 (병행) | ⬜ 대기 | |

## 🔗 관련 노트

- [[../1_개요]] — 의사결정 메인
- [[../4_Asset-Centric 아키텍처 안]] — 이 PoC가 검증할 디자인
- [[../3_dbt 능력 경계와 영역 분담]] — dbt vs non-dbt 분담 (실험 시 reference)

## 💡 PoC 진행 시 마음가짐

- **빨리 가지 말 것**. 패러다임 체화가 목적
- **실패도 기록**. "안 됐다" 도 의미 있음
- **궁금증 즉시 메모**. 안 풀린 의문이 다음 step 단서
- **회의 시연용 1개** (Step 5) 가 가장 큰 보상
