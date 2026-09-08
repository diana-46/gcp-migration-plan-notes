---
title: "Git 동기화·dbt 전환 계획"
status: draft
tags:
  - confluence
  - athlon
  - dbt
  - git-sync
created: 2026-04-14
updated: 2026-04-14
source: https://kakaoent.atlassian.net/wiki/spaces/DP/pages/5034707500/Athlon+ETL+Neptune+Git+dbt
---
야르가 전에 작성하신 내용  가져옴.

# Git 동기화·dbt 전환 계획

> Confluence import. 원본: `Athlon ETL(Neptune) Git 동기화 계획 — dbt 패러다임 적용` (2026-04-14, v1).
> 이 문서는 athlon-side에서 본 git 동기화 + dbt 전환의 단계적 로드맵. 우리 GCP 이관 결정과 큰 틀에서 일치.
> 우리 애슬론 폴더의 의사결정 흐름: [[1_개요]], [[3_dbt 능력 경계와 영역 분담]]

## Context

Athlon Neptune 모듈은 ETL 파이프라인을 관리하는 시스템으로, SQL 변환 코드와 메타데이터가 MySQL에 저장되어 있다. 현재 `GitClient.kt`가 Airflow DAG Python 파일만 git에 커밋하고 있어서, **ETL 변환 로직 자체는 버전 관리되지 않는** 상태다.

### 기존 neptune-sql 레포 현황

https://github.com/kakaoent/neptune-sql 에 ETL SQL 코드를 **수동으로** 관리 중:

- **디렉토리 구조**: `{서비스}/{데이터레이어}/{etl이름}.sql` (예: `kakaopage/stat/xxx.sql`, `berriz/dw/xxx.sql`)
- **서비스별 분류**: `kakaopage`, `berriz`, `kakaowebtoon`, `common`, `sensor`
- **레이어별 분류**: `stat`, `dw`, `mart`, `view`, `service`, `analysis` 등
- **SQL 헤더 주석 규약**: 파일 상단에 `Desc`, `ETL URL`, `Template tags` 등 메타데이터를 주석으로 기재
- **규모**: 약 390개 SQL 파일
- **PR 기반**: CODEOWNERS + PR 템플릿으로 코드 리뷰 진행 중
- **SQL 코딩 가이드**: `guide.md`에 SQL 작성 컨벤션 정의

**neptune-sql의 한계:**

1. Athlon DB와 수동 동기화 — SQL 변경 시 Athlon UI와 git 양쪽을 따로 업데이트해야 함
2. 메타데이터(파라미터, 파티션, 의존성, 옵션 등)는 SQL 헤더 주석에 참고용으로만 있고, 실제 Athlon 설정과 동기화되지 않음
3. 디렉토리 구조가 Athlon의 Workflow 기준이 아닌 서비스/레이어 기준 — ETL 간 의존 관계 파악이 어려움

**새 레포를 만드는 이유**: neptune-sql은 아카이브하고 새로운 자동 동기화 레포를 생성한다. 기존 수동 프로세스를 자동화로 대체하면서, 메타데이터까지 포함하는 완전한 ETL 정의를 관리한다.

### 새 레포의 목표

dbt처럼 ETL 정의를 git에서 관리하면:

- 변환 SQL 코드의 변경 이력 추적 (누가, 언제, 왜 바꿨는지)
- PR 기반 코드 리뷰로 품질 관리
- dev → staging → prod 환경 간 배포
- 장애 시 이전 버전으로 롤백
- ETL 정의의 재현 가능성(reproducibility) 확보
- **neptune-sql의 수동 동기화 문제 해결**

---

## 1. 동기화 대상 분류 및 근거

### Git에 포함하는 엔티티 (정의/코드)

| 엔티티 | dbt 대응 개념 | 포함 근거 |
|---|---|---|
| **Etl** | dbt model | Neptune의 핵심. SQL/Presto/R 변환 코드(`code`), 옵션, 슬롯 등이 실제 데이터 변환 로직이다. dbt에서 `.sql` 모델 파일에 해당. |
| **EtlParameter** | dbt vars/macros | ETL에 주입되는 동적 파라미터(날짜 포맷, Airflow 매크로 등). dbt의 `{{ var('...') }}`에 해당. 파라미터 변경이 ETL 동작을 바꾸므로 반드시 추적해야 한다. |
| **EtlPartition** | dbt incremental config | 타겟 테이블의 파티션 정의. dbt incremental 모델의 `partition_by` 설정에 해당. 잘못된 파티션 설정은 데이터 정합성 문제를 일으키므로 리뷰 대상. |
| **EtlDependencies** | dbt ref() | 상위 ETL/DAG 의존성. dbt의 `ref('upstream_model')` 그래프에 해당. 의존성 변경은 DAG 구조를 바꾸므로 반드시 리뷰 필요. |
| **Workflows** | dbt project/schedule | Airflow DAG의 논리적 정의(스케줄, SLA, 알림, kwargs). dbt Cloud의 Job 설정에 해당. 스케줄 변경이 파이프라인 실행 타이밍을 바꾸므로 추적 필요. |
| **ActionsMeta** | dbt macro | 재사용 가능한 액션 템플릿(GCS_UPLOAD, BIGQUERY_JOB 등). dbt 매크로처럼 여러 ETL에서 공유하는 정의. 템플릿 변경이 여러 파이프라인에 영향을 미치므로 리뷰 필수. |

### Git에서 제외하는 엔티티 (런타임/파생 데이터) 및 근거

| 엔티티 | 제외 근거 |
|---|---|
| **Actions** | ETL + PipelineBuilder에서 **자동 생성**되는 Airflow Task. ETL 정의가 있으면 언제든 재생성 가능하므로 별도 저장 불필요. dbt에서 compiled SQL이 git에 없는 것과 동일. |
| **ActionDependencies** | EtlDependencies + PipelineBuilder에서 **자동 생성**. Actions와 마찬가지로 파생 데이터. |
| **EtlMapping** | ETL↔Actions 매핑 테이블. `saveEtlActionsAndDependencies()`에서 자동 관리되는 junction table. |
| **Backfill** | 특정 시간 범위에 대한 **일회성 실행 요청**. dbt의 `dbt run --full-refresh` 명령어에 해당 — 정의가 아닌 오퍼레이션. |
| **CatalogDb** | 인프라 참조 데이터. 환경마다 다른 DB 인스턴스를 가리키므로 git보다는 환경 설정으로 관리. |

---

## 2. 동기화 방향: 단계적 전환 전략

### Phase 1: Athlon → Git (자동 Export)

```
[사용자] → [Athlon UI/GraphQL] → [MySQL 저장] → (Spring Event) → [GitSyncService] → [Git Repo]
```

**이 단계를 먼저 하는 이유:**

1. **기존 데이터 마이그레이션 문제 회피**: MySQL에 이미 수백 개의 ETL 정의가 있다. Git→Athlon으로 시작하면 기존 데이터를 모두 YAML로 변환한 뒤 Athlon을 비우고 Git에서 import해야 하는데, 이 빅뱅 마이그레이션은 위험하다. Export부터 시작하면 기존 데이터를 점진적으로 git에 쌓을 수 있다.
2. **파일 포맷 검증**: 실제 데이터로 YAML/SQL 직렬화를 테스트할 수 있다. 포맷에 문제가 있으면 Import 구현 전에 수정 가능.
3. **팀 적응 시간**: 기존 UI 워크플로우를 그대로 유지하면서 git 이력을 보는 것에 먼저 익숙해질 수 있다. "이번 ETL 변경이 잘못됐는데 git에서 이전 버전 확인해보자" 같은 경험을 쌓는다.
4. **위험도 제로**: Export는 기존 시스템에 전혀 영향을 주지 않는다. git push 실패해도 ETL 실행에 영향 없음.

**구현 핵심:**

- GraphQL mutation(createEtl/updateEtl/deleteEtl) 성공 후 Spring `ApplicationEvent` 발행
- `GitSyncService`가 이벤트를 받아 비동기로 직렬화 → git commit & push
- 최초 1회 bulk export로 기존 데이터 전체 git에 적재

### Phase 2: Git → Athlon (Import/Apply)

```
[Git Repo] → [Import API] → [기존 Service Layer 재사용] → [MySQL]
```

**이 단계가 필요한 이유:**

1. **환경 간 배포(Promotion)**: dev 환경에서 검증된 ETL을 staging/prod에 배포하려면, dev의 git export를 prod에 import하는 경로가 필요하다.
2. **재해 복구**: DB가 손상되었을 때 git에서 전체 ETL 정의를 복원할 수 있다.
3. **CI/CD 기반 배포**: Jenkins/GitHub Actions에서 `POST /api/gitsync/import/etl`을 호출하여 자동 배포 파이프라인 구축 가능.

**구현 핵심:**

- YAML/SQL 파일을 파싱하여 기존 `EtlService.createEtl()`/`updateEtl()` 호출 → 기존 유효성 검증 로직 재사용
- `ReferenceResolver`가 자연키(이름)를 DB ID로 변환 (예: `destDb: "dataplatform_prod"` → CatalogDb ID)
- dry-run 모드로 변경 사항 미리 확인
- diff 엔드포인트로 Git↔DB 차이 감지

### Phase 3: Git as Source of Truth (GitOps)

```
[사용자] → [Git PR] → [코드 리뷰 + CI 검증] → [머지] → [자동 Import] → [MySQL + Airflow]
```

**이 단계가 최종 목표인 이유:**

1. **dbt의 핵심 가치 실현**: dbt에서 모델 변경은 반드시 PR을 통과해야 한다. SQL 변환 로직 변경에 코드 리뷰를 강제하면, "실수로 WHERE 절 빼먹어서 전체 데이터 덮어쓴" 같은 사고를 예방할 수 있다.
2. **감사 추적(Audit Trail)**: 모든 변경이 PR에 기록되어 "누가, 왜, 언제" 변경했는지 명확하다.
3. **롤백 용이성**: git revert 한 번으로 이전 상태 복원.
4. **환경 일관성**: 모든 환경이 동일한 git repo에서 배포되므로, "dev에서는 되는데 prod에서 안 된다"는 문제 감소.

**구현 핵심:**

- Athlon UI의 ETL 편집 기능을 비활성화하거나, UI가 직접 git에 커밋하도록 변경
- GitHub webhook → Athlon Import API 자동 호출
- CI에서 YAML 유효성 검증 + dry-run 테스트

---

## 3. 파일 포맷 및 디렉토리 구조

### 디렉토리 구조 설계 근거

```
athlon-etl-definitions/
├── workflow/
│   └── {workflow_name}.yml           # Workflow(DAG) 정의
├── etl/
│   └── {workflow_name}/              # Workflow별로 ETL을 그룹핑
│       └── {uniqueTitle}/
│           ├── etl.yml               # ETL 메타데이터
│           └── code.sql              # 변환 SQL 코드 (별도 파일)
├── actions-meta/
│   └── {workflow_name}/
│       └── {uniqueTitle}.yml         # ActionsMeta 템플릿 정의
└── _references/
    └── etl_input_types.yml           # EtlInputType 등 참조 데이터 (읽기 전용 스냅샷)
```

**neptune-sql과의 구조 비교 및 전환 이유:**

| 관점 | neptune-sql (기존) | 새 레포 (제안) | 전환 이유 |
|---|---|---|---|
| 1차 분류 | 서비스별 (`kakaopage/`, `berriz/`) | Workflow별 (`data_neptune_daily/`) | Athlon에서 ETL은 Workflow에 속한다. 서비스 분류는 Athlon 데이터 모델에 없어 자동 매핑이 불가능. Workflow 기준이면 자동 export 시 디렉토리를 결정할 수 있다. |
| 2차 분류 | 데이터레이어별 (`stat/`, `dw/`) | ETL uniqueTitle별 디렉토리 | 데이터레이어 분류도 Athlon 데이터 모델에 없다. uniqueTitle이 ETL의 불변 식별자이므로 자동화에 적합. |
| 메타데이터 | SQL 헤더 주석 (비구조적) | 별도 `etl.yml` (구조적 YAML) | 주석은 파싱이 불안정하고 Athlon과 자동 동기화가 어렵다. YAML은 프로그래밍적으로 읽고 쓸 수 있어 import/export 자동화에 필수. |
| SQL 코드 | `.sql` 파일 1개 | `code.sql` + `etl.yml` 분리 | neptune-sql처럼 SQL은 별도 파일로 유지. IDE의 SQL 구문 강조/린팅/자동완성 활용 가능. dbt도 `.sql`과 `schema.yml`을 분리. |

**핵심 원칙: 파일에 DB ID 없음**

DB ID는 환경(dev/staging/prod)마다 다르다. 자연키(이름)로 참조해야 환경 간 이식성이 보장된다. dbt도 `ref('model_name')`이지 `ref(42)`가 아니다.

### YAML 스키마 예시 (요약)

`workflow/data_neptune_daily.yml`:

```yaml
version: 1
kind: Workflow
metadata:
  name: "data_neptune_daily"
spec:
  scheduleInterval: "0 6 * * *"
  startDate: "2024-01-01"
  slaTimedelta: 3600
  slackNotiChannel: "#data-noti"
```

`etl/data_neptune_daily/daily_user_agg/etl.yml`:

```yaml
version: 1
kind: Etl
metadata:
  uniqueTitle: "daily_user_aggregation"
spec:
  workflow: "data_neptune_daily"
  destDb: "dataplatform_prod"
  destTable: "daily_user_agg"
  codeType: PRESTO
  parameters:
    - name: "target_date"
      inputType: "execution_date"
  partitions:
    - key: "dt"
      value: "{{ ds_nodash }}"
  dependencies:
    - type: ETL
      ref: "upstream_user_log_etl"
```

`code.sql`:

```sql
SELECT
    dt,
    user_uid,
    COUNT(*) AS action_count
FROM dataplatform_prod.user_action_log
WHERE dt = '{{ target_date }}'
GROUP BY dt, user_uid
```

---

## 4. 구현 아키텍처 (요약)

### 신규 컴포넌트

```
api/src/main/kotlin/com/kakaopage/athlon/gitsync/
├── GitSyncClient.kt        # JGit 기반 git 조작
├── GitSyncService.kt       # export/import/diff 오케스트레이터
├── GitSyncController.kt    # REST API
├── ReferenceResolver.kt    # 자연키 → DB ID 변환
├── serializer/             # Etl/Workflow/ActionsMeta → YAML
└── event/                  # Spring ApplicationEvent 정의
```

### Export 흐름 (이벤트 기반)

```
EtlService.createEtl() / updateEtl() / deleteEtl()
  → DB 트랜잭션 커밋 성공
  → applicationEventPublisher.publishEvent(EtlChangedEvent(etl, changeType))
  → GitSyncService.@TransactionalEventListener(phase = AFTER_COMMIT)
  → EtlSerializer.serialize(etl) → YAML 파일 + SQL 파일 생성
  → GitSyncClient.commitAndPush(files, "Update ETL: ${etl.uniqueTitle}")
```

### REST API

```
POST /api/gitsync/export/etl              # 전체 ETL bulk export
POST /api/gitsync/export/etl/{uniqueTitle} # 단건 export
POST /api/gitsync/import/etl/{uniqueTitle} # 단건 import
POST /api/gitsync/import/etl?dryRun=true   # 전체 import (dry-run)
GET  /api/gitsync/diff/etl                 # Git↔DB 차이 조회
```

---

## 5. 단계별 구현 계획 (요약)

### Phase 1: Export 전용 (4~6주)

| 단계 | 작업 |
|---|---|
| 1-1 | `athlon-etl-definitions` git 저장소 생성 |
| 1-2 | Vault 기반 SSH 설정 |
| 1-3 | `GitSyncClient` (JGit) 구현 |
| 1-4~6 | EtlSerializer, WorkflowSerializer, ActionsMetaSerializer 구현 |
| 1-7 | `EtlService`, `WorkflowService`에 이벤트 발행 추가 |
| 1-8 | `GitSyncController` (export 엔드포인트만) |
| 1-9 | bulk export 실행 |

**Phase 1 완료 시 가치:** 모든 ETL 변경이 git에 기록됨. neptune-sql의 수동 동기화 프로세스 자동화 대체.

### Phase 2: Import + Diff + 정기 동기화 (4~6주)

3단계 동기화 체계:

| 종류 | 주기 | 동작 |
|---|---|---|
| **실시간 Export** | 매 mutation | ETL 변경 즉시 git에 커밋 |
| **경량 Diff** | 매 1시간 | 최근 1시간 내 변경된 ETL의 `modifyDt` 기준 비교 |
| **전체 Diff** | 매일 새벽 (03:00) | 모든 ETL을 DB↔git 전수 비교 |

**Phase 2 완료 시 가치:** dev → prod 배포 가능. PR에서 dry-run 결과 리뷰. 1시간 이내 drift 감지·복구.

### Phase 3: GitOps — Git as Source of Truth (4~6주)

- GitHub Webhook → Import 자동화
- Athlon UI ETL 편집을 비활성화 또는 git 커밋으로 변경
- Branch 전략 (`main` = prod, `develop` = dev)
- 롤백 자동화 (`git revert` → webhook → import)

---

## 6. 리스크 분석 (요약)

| 리스크 | 대응 |
|---|---|
| Git Push 실패 시 Export 유실 | `@TransactionalEventListener(AFTER_COMMIT)` + 재시도 큐 + Slack 알림 |
| 동시 Mutation에 의한 Git 충돌 | `@Synchronized` + fetch-rebase-push |
| YAML 포맷 변경 시 하위 호환성 | `version: 1` 필드 + 마이그레이션 스크립트 |
| DE 팀의 Actions 수동 편집 덮어쓰기 | Actions는 git 동기화 대상 아님. ETL 구조 변경 시에만 재생성 |
| 환경 간 참조 데이터 불일치 | 다음 섹션 참고 |

---

## 7. 환경별 적용 전략 (Dev / Prod 분리)

### 환경별 프로파일 + 브랜치 전략 (dbt profiles 패턴)

```
athlon-etl-definitions/
├── profiles/
│   ├── dev.yml                 # dev 환경 변수
│   └── prod.yml                # prod 환경 변수
└── etl/
    └── {workflow_name}/
        └── {uniqueTitle}/
            ├── etl.yml         # 공통 정의 (환경 변수 참조)
            └── code.sql        # SQL 코드 (환경 변수 참조)
```

`profiles/prod.yml`:

```yaml
environment: prod
variables:
  DB_NAME: "dataplatform_prod"
  SLACK_CHANNEL: "#data-alert"
  DEFAULT_SLOT: LARGE
```

`etl.yml` 에서 환경 변수 참조:

```yaml
spec:
  destDb: "${DB_NAME}"
  slackChannel: "${SLACK_CHANNEL}"
  slot: "${DEFAULT_SLOT}"
```

### 환경별 ETL 필터링

```yaml
metadata:
  uniqueTitle: "experimental_user_clustering"
  environments: [dev]   # dev에만 import됨
```

### 브랜치 전략

```
main (prod)     ←  PR 머지로만 변경
  │
  └── develop (dev)    ← 개발 브랜치
        │
        └── feature/xxx  ← ETL 변경 작업
```

---

## 8. dbt 마이그레이션 로드맵 (장기 계획)

### Git 동기화가 dbt 마이그레이션의 기반이 되는 이유

| Git 동기화 산출물 | dbt 마이그레이션에서의 역할 |
|---|---|
| SQL 파일 (`code.sql`) | dbt model `.sql` 파일로 1:1 변환 가능 |
| 메타데이터 (`etl.yml`) | dbt `schema.yml`의 model description, column description으로 변환 |
| 의존성 (dependencies) | dbt `ref()` 매크로로 변환 |
| 파라미터 (parameters) | dbt `var()` 또는 Jinja 매크로로 변환 |
| 파티션 (partitions) | dbt `incremental` 모델의 `partition_by` 설정으로 변환 |
| 환경 프로파일 (profiles/) | dbt `profiles.yml`의 target으로 직접 대응 |
| 브랜치 전략 | dbt Cloud의 environment (dev/staging/prod) 와 동일 개념 |

### 마이그레이션 단계

#### Stage 1: dbt 프로젝트 초기화 + 병행 운영

- `dbt init athlon-etl`
- 어댑터: `dbt-presto` (현재) → 이후 `dbt-bigquery` (GCP 이관 시)
- profiles.yml 자동 생성 (기존 profiles/ 활용)
- SQL 변환 스크립트로 모델 1:1 복사
- 기존 Airflow ETL + dbt run 병행 실행 결과 비교

#### Stage 2: ETL별 점진적 전환

- 간단한 ETL부터 (stat, view 유형)
- dbt test 추가
- 결과 비교 자동화
- Airflow DAG에서 dbt run으로 task 교체

#### Stage 3: 전면 전환

- 모든 ETL이 dbt로 전환
- Athlon Neptune의 ETL CRUD 기능을 dbt 프로젝트 관리 UI로 대체
- Athlon은 실행 모니터링/알림 허브로 역할 전환

### 변환이 복잡한 경우

| 케이스 | 대응 |
|---|---|
| R 코드 ETL | dbt는 SQL 전용. R ETL은 Airflow task로 유지, dbt DAG에서 `external_source`로 참조 |
| YAML codeFormat ETL (멀티스텝) | dbt의 pre-hook/post-hook 또는 여러 model로 분리 |
| ActionsMeta 커스텀 액션 (GCS_UPLOAD, BQ_JOB 등) | dbt on-run-end hook 또는 Airflow task로 유지 |
| Airflow 센서 의존성 | `ExternalTaskSensor`를 dbt-external-nodes 패키지로 대체 |

> ⚠️ **우리 의사결정 컨텍스트**: 이 원본 문서는 "dbt-presto" 가정과 "Airflow PipelineBuilder 점진 대체"를 그림. 우리 GCP 이관 시점에는 dbt-bigquery로 가야 하고, athlon의 역할도 "모니터링/알림 허브"가 아니라 "**dbt + 비-dbt 통합 관리 레이어**"로 재정의됨. 자세한 우리 측 의사결정은 [[1_개요]] 참고.

---

## 9. 핵심 파일 (구현 시 참조)

| 파일 | 용도 |
|---|---|
| `api/.../neptune/GitClient.kt` | JGit 패턴 원본 |
| `api/.../service/neptune/EtlService.kt` | ETL CRUD + 유효성 검증 |
| `api/.../service/neptune/WorkflowService.kt` | Workflow CRUD, Actions 자동 생성 |
| `api/.../service/neptune/BackfillService.kt` | 기존 GitClient 사용 예시 |
| `api/.../config/VaultNeptuneConfig.kt` | Vault 설정 패턴 원본 |
| `core/.../model/Etl.kt` | ETL 엔티티 |
| `core/.../model/Workflows.kt` | Workflow 엔티티 |
| `core/.../model/ActionsMeta.kt` | ActionsMeta 엔티티 |
| `api/.../resources/graphql/etl.graphqls` | GraphQL 스키마 |

---

## 관련 문서

- [[1_개요]] — 애슬론 폴더 결론 / 의사결정
- [[3_dbt 능력 경계와 영역 분담]] — dbt vs non-dbt 분담 디테일
