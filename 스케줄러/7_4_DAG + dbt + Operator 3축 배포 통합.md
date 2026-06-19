---
title: "DAG + dbt + Operator 3축 배포 통합 전략"
status: draft
tags:
  - airflow
  - 스케줄러
  - composer
  - dbt
  - cosmos
  - dag-bundles
  - artifact-registry
  - deployment
created: 2026-06-12
updated: 2026-06-12
---

# DAG + dbt + Operator 3축 배포 통합 전략

> Composer 3 환경에서 운영할 **3개 자산 (Custom Operator / dbt Project / Airflow DAG)** 의 배포 전략 통합 그림.
>
> 각 자산의 detail 은 별개 노트에 정리되어 있고 ([[7_3_공통 Custom Operator 제공 방안]] / [[애슬론/PoC/02_dbt_render_in_composer]] · [[애슬론/PoC/03_bq_dbt_run_in_composer]] / [[11_DAG Bundles와 배포 전략]]), 본 문서는 **세 가지를 한 장에 묶어** 통합 그림 + 의사결정 인덱스 역할.

## 결론 먼저

> 세 자산은 **같은 인프라 (Artifact Registry / GCS / Composer SA)** 를 공유하되, **각자의 lifecycle 과 배포 메커니즘을 분리**해서 운영한다.
>
> | Layer | 자산 | 배포 메커니즘 | 결정된 노트 |
> |---|---|---|---|
> | 1 | Custom Operator 패키지 | AR Python repo (`pip install`) + SemVer | [[7_3_공통 Custom Operator 제공 방안]] |
> | 2 | dbt Project | DAG repo 안 `dbt_projects/` 동봉 + CI 가 `manifest.json` 생성 → GCS | [[애슬론/PoC/02_dbt_render_in_composer]] · [[애슬론/PoC/03_bq_dbt_run_in_composer]] |
> | 3 | Airflow DAG | GitDagBundle + Pull off + Jenkins push 트리거 | [[11_DAG Bundles와 배포 전략]] |
>
> Cosmos 의 `DbtTaskGroup` 이 layer 2 와 3 의 접점, Provider Package 의 `import` 가 layer 1 과 3 의 접점.

## 1. 왜 통합 그림이 필요한가

- 세 자산의 PoC / 안이 **각자 잘 정리되어 있지만**, 그것들이 같은 Composer 환경에서 어떻게 공존하는지 한 장으로 보여주는 노트가 비어있음
- 의사결정자 (팀장 등) 가 "전체 배포 전략" 을 한 번에 보고 싶을 가능성
- CI/CD 파이프라인 설계 시 세 자산의 release 트리거 / 순서 / 의존이 한곳에 있어야 충돌 안 남
- 환경 분리 정책 (dev/stg/prod) 이 세 자산에 일관되게 적용되는지 검증

## 2. 3-Layer 구조 (도식)

```
┌─────────────────────────────────────────────────────────────────────┐
│                  Cloud Composer 3 Environment                       │
│                                                                      │
│  ┌────────────────────────────────────────────────────────────┐    │
│  │  Layer 1 — Custom Operator Package (7_3)                   │    │
│  │   pip install kakaoent-airflow-providers==X.Y.Z            │    │
│  │   Source: AR Python repo (asia-northeast3-python.pkg.dev)  │    │
│  │   Auth:   Composer SA → AR Reader                          │    │
│  └────────────────────────────────────────────────────────────┘    │
│                              ▲                                       │
│                              │ import                                │
│  ┌───────────────────────────┴─────────────────────────────────┐   │
│  │  Layer 3 — Airflow DAG (11_DAG Bundles)                     │   │
│  │   GitDagBundle (repo_url=git@.../<domain>-airflow-dags.git) │   │
│  │   refresh_interval = 86400 (Pull off)                       │   │
│  │   Trigger: Jenkins → `gcloud composer ... bundles refresh`  │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                              ▲                                       │
│                              │ Cosmos DbtTaskGroup 이 읽음          │
│  ┌───────────────────────────┴─────────────────────────────────┐   │
│  │  Layer 2 — dbt Project (PoC 02 / 03)                        │   │
│  │   Composer DAGs bucket 의 dbt_projects/<service>/           │   │
│  │   - dbt_project.yml, models/, macros/, profiles.yml         │   │
│  │   - target/manifest.json  ← CI 에서 dbt parse 로 생성       │   │
│  │   LoadMode.DBT_MANIFEST 로 Cosmos render                    │   │
│  └─────────────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────────┘

[Artifact Registry — 인프라 공유]
├── Python repo (airflow-providers)    : Layer 1 wheel
└── (선택) Container repo               : dbt runner image 가 필요해진다면
```

## 3. 자산별 Lifecycle / 변경 빈도 / 배포 메커니즘

| 자산                      | 변경 빈도     | release 단위                      | 배포 lag                                | 버전 표현                              | 환경 분리 방식                                         |
| ----------------------- | --------- | ------------------------------- | ------------------------------------- | ---------------------------------- | ------------------------------------------------ |
| **Custom Operator 패키지** | 낮음 (월)    | SemVer (`v1.0.0`)               | 수십 분 (release → install → env update) | `==1.2.3` (pyproject.toml)         | 환경별 lock 버전 다르게                                  |
| **dbt Project**         | 중 (주~일)   | git tag 또는 commit               | 분 (CI 에서 manifest 생성 → GCS sync)      | git ref + manifest checksum        | `profiles.yml` 의 target 분기 (dev/composer)        |
| **Airflow DAG**         | 높음 (일~시간) | git ref (`main` / `production`) | 즉시 ~ Jenkins 트리거                      | bundle commit hash (Airflow UI 표시) | GitDagBundle `tracking_ref` 분기 또는 GCP project 분리 |

→ 세 자산이 **lifecycle / 변경 속도가 모두 다름**. 한 release pipeline 에 묶으면 가장 느린 자산이 가장 빠른 자산의 속도를 끌어내림.

## 4. AR 인프라 공유 매트릭스

| AR repo type | 누가 publish | 누가 install | 비고 |
|---|---|---|---|
| **Python (`airflow-providers`)** | Operator 패키지 release pipeline (GitHub Actions, tag push 시) | Composer 환경 (`requirements.txt` + extra-index-url) | 7_3 §4.3 |
| **Container** (필요 시) | dbt runner image build pipeline | KubernetesPodOperator (dbt 의존성 격리 시) | 현재 미도입. Composer PyPI 옵션으로 `dbt-bigquery` install 검증 완료 ([[03_bq_dbt_run_in_composer]]) |
| **Container** (필요 시) | DAG bundle image build | Composer DAG bundle | 현재 GitDagBundle 직행 결정 |

→ 현재 결정은 **Python repo 만 사용**. dbt / DAG 는 GCS sync + GitDagBundle 로 충분.

## 5. 레포 구조 (musicdata 팀 패턴 차용)

[[애슬론/PoC/03_bq_dbt_run_in_composer]] 의 분석에서 확인된 사내 검증 패턴:

```
[Git Org]

kakaoent-airflow-providers/     ← Layer 1: Operator 패키지 (단일 repo)
  kakaoent_airflow/
    operators/ sensors/ callbacks/ ...
  pyproject.toml

<domain>-airflow-dags/          ← Layer 3: DAG repo (도메인별 N개, GitDagBundle)
  dags/
    <service>/<dag>.py
  dbt_projects/ (선택)          ← Layer 2 동봉 옵션 A
    <service>/
      dbt_project.yml
      models/
      target/manifest.json
  tests/
  README.md

<domain>-dbt/                   ← Layer 2: dbt repo (서비스별 N개, 분리 옵션 B)
  dbt_project.yml
  models/
  profiles.yml
  packages.yml
  .github/workflows/
    publish-manifest.yml        ← CI: dbt parse → manifest → GCS upload
```

### Layer 2 동봉 vs 분리 — 2가지 운영 패턴

| 패턴 | 구조 | 장점 | 단점 |
|---|---|---|---|
| **A. DAG repo 안에 동봉** (`dbt_projects/`) | dbt project 가 DAG repo 의 sub-folder | DAG ↔ dbt 변경이 atomic. 한 PR 에 둘 다. | dbt project 단독 lifecycle 운영 어려움 (dbt 만 변경해도 DAG repo PR 필요) |
| **B. dbt repo 분리** (musicdata 팀 패턴) | dbt project 가 별도 repo, CI 가 manifest 를 GCS 에 push | dbt / DAG 각자 변경 가능. dbt 팀 권한 분리 가능. | DAG 가 어떤 manifest 버전을 보고 있는지 추적 필요 |

→ musicdata 팀은 **B (분리)** 채택. 우리도 dbt 변경 빈도 / 권한 분리 필요성에 따라 결정.

## 6. CI/CD 통합 흐름

각 layer 의 release pipeline 이 **독립적으로 돌되**, 같은 인프라 (Composer / AR / GCS) 를 공유:

```
[Layer 1: Operator 패키지]
  PR → CI (ruff/pytest) → main merge
                              ↓
                          git tag v1.0.0
                              ↓
                       release.yml (GitHub Actions)
                              ↓
                       python -m build + twine upload → AR Python repo
                              ↓
                       각 Composer 환경의 requirements.txt PR (수동)
                              ↓
                       gcloud composer environments update --update-pypi-packages

[Layer 2: dbt Project]
  PR → CI (sqlfluff/dbt compile) → main merge
                              ↓
                       publish-manifest.yml (GitHub Actions)
                              ↓
                       dbt deps + dbt parse → target/manifest.json
                              ↓
                       gsutil cp → gs://<composer-bucket>/dags/dbt_projects/<service>/
                       (Composer 가 GCS sync 로 자동 인식)

[Layer 3: Airflow DAG]
  PR → CI (DAG import test / dag validation) → main merge
                              ↓
                       작업자 Jenkins job 실행 (수동 트리거)
                              ↓
                       gcloud composer ... bundles refresh -- <bundle_name>
                              ↓
                       Composer 가 git pull → DAG 반영
```

세 pipeline 이 서로의 release 를 알 필요 없음. **인프라 layer 에서 자연스럽게 합류**.

## 7. 환경별 (dev/stg/prod) 분리 정책

세 자산이 환경 분리에 일관된 정책을 따라야 함:

| 환경 | Operator 패키지 lock | dbt manifest | DAG bundle tracking_ref | refresh_interval |
|---|---|---|---|---|
| dev | `==X.Y.Z-dev` 또는 latest | `main` 의 manifest 자동 sync | `main` | 60s (빠른 iteration) |
| stg | `==X.Y.Z-rc` | `release` 브랜치 manifest | `release` | 300s |
| prod | `==X.Y.Z` (stable) | `production` tag manifest | `production` | 86400 (Pull off, [[11_DAG Bundles와 배포 전략]] §4 결정) |

환경 분리의 물리적 방법은 **GCP project 단위 Composer 환경 분리** ([[11_DAG Bundles와 배포 전략]] §6 결정 인용).

## 8. 각 Layer 의 의사결정 인덱스

세 노트에 흩어져 있는 결정 사항 한곳에 모음:

### Layer 1 — Operator 패키지 ([[7_3_공통 Custom Operator 제공 방안]])

- ✅ Internal Provider Package + GCP Artifact Registry (Python repo)
- ✅ flat layout, 패키지명 `kakaoent_airflow`
- ✅ 도메인 단위 operator 묶음 (14 → 6 파일)
- ✅ `get_provider_info.py` 로 Airflow Provider 표준 entry point
- ✅ `utils/dag_defaults.py` 의 함수형 helper (`get_kakaoent_default_args`)
- ✅ Slack 통합은 `callbacks/slack_notifier.py` 한곳에 응집
- ✅ SemVer + 도메인별 lock

### Layer 2 — dbt Project ([[애슬론/PoC/02_dbt_render_in_composer]], [[애슬론/PoC/03_bq_dbt_run_in_composer]])

- ✅ Cosmos + `LoadMode.DBT_MANIFEST` 패턴
- ✅ CI 에서 `dbt parse` → `target/manifest.json` 생성 → GCS 업로드
- ✅ Composer worker 의 ADC (attached SA) 가 BigQuery 자격증명 자동 해결
- ✅ `profiles.yml` 의 target 분기 (`dev` 로컬 OAuth / `composer` ADC)
- ✅ BQ 이관 시 adapter 만 교체 (`dbt-trino` → `dbt-bigquery`)
- ⚠️ Layer 2 동봉 vs 분리 — 미확정 (§5 참조)

### Layer 3 — Airflow DAG ([[11_DAG Bundles와 배포 전략]])

- ✅ Composer 이관 시 옵션 (2) GitDagBundle 채택
- ✅ Pull off + Push only 패턴 (`refresh_interval = 86400`)
- ✅ Jenkins 버튼 → `gcloud composer ... bundles refresh`
- ✅ 환경 분리는 GCP project 단위 Composer 환경
- ✅ Git deploy key/PAT → Secret Manager, 환경 SA 에 secretAccessor
- ✅ stale / drift 알람 (Cloud Monitoring)

## 9. 미확정 / 후속 결정

- [ ] **Layer 2 동봉 vs 분리** — DAG repo 안 `dbt_projects/` 동봉 (atomic) vs dbt repo 분리 (musicdata 팀 패턴, 권한 분리) — 도메인별로 다르게 가도 OK
- [ ] **dbt manifest 의 publish 주체** — Layer 2 의 CI 가 직접 GCS 에 올릴지, DAG repo 의 CI 가 dbt repo 를 fetch 해서 같이 올릴지
- [ ] **dbt-bigquery 의존성 위치** — Composer PyPI 옵션 (전역) vs KubernetesPodOperator image (격리)
- [ ] **Operator 패키지의 dbt-airflow 통합 helper** — Cosmos 의 `DbtTaskGroup` 보일러플레이트를 Layer 1 에 helper 로 뽑을지 (예: `kakaoent_airflow.dbt.cosmos_dbt_task_group(...)`)
- [ ] **세 자산의 release 추적 dashboard** — "지금 prod Composer 가 보고 있는 Operator 버전 / dbt manifest checksum / DAG bundle commit" 한눈에 보는 view
- [ ] **CI runner 분리** — Layer 1/2/3 가 사내 GitHub Actions runner 공유할지 분리할지

## 10. 관련 문서

- [[7_3_공통 Custom Operator 제공 방안]] — Layer 1 detail
- [[애슬론/PoC/02_dbt_render_in_composer]] — Layer 2 렌더링 검증 (Cosmos 패턴)
- [[애슬론/PoC/03_bq_dbt_run_in_composer]] — Layer 2 BQ 실제 실행 검증
- [[11_DAG Bundles와 배포 전략]] — Layer 3 detail (옵션 (2) + Pull off 결정)
- [[7_2_리소스 다이어트 포인트]] — sensor → deferrable 등 코드 레벨 다이어트 (orthogonal)
- [[2_Cloud Composer vs Self-managed 비교]]
- [[4_Queue 라우팅과 Pod 스펙 설정]] — Pod 이미지로 의존성 격리할 때
- [[13_Composer 3 환경 업그레이드 정책]]
