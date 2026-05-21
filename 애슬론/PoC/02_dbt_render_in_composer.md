---
title: "02. 사내 dbt 프로젝트 Composer 렌더링 검증"
status: done
tags:
  - poc
  - dbt
  - cosmos
  - composer3
  - render-only
created: 2026-05-21
updated: 2026-05-21
---

# 02. dbt 프로젝트 Composer 렌더링 검증 (Trino 사례)

> **검증 질문**: dbt 프로젝트가 Cloud Composer + Cosmos 에서 잘 로드되고 Airflow UI 에 어떻게 보이는가? (=인프라 패턴 검증)
>
> **결론**: ✅ **완벽 렌더링**. Cosmos 가 39개 모델 → 60+ task 로 자동 분해. layer 별 의존성도 그래프로 시각화.
>
> ### 🎯 본 PoC 의 진짜 가치
>
> **adapter-agnostic 패턴 검증**. Trino 든 BQ 든 Snowflake 든, **Cosmos + Composer + manifest** 조합은 동일하게 동작.
>
> → 사내 이관 방향은 **BQ 로 결정**. 이 PoC 가 검증한 Composer/Cosmos 인프라 패턴이 BQ adapter 로 그대로 적용됨.
>
> 후속 검증: **[[03_bq_dbt_run_in_composer]]** — 같은 패턴을 **dbt-bigquery + 실제 dbt run** 으로 확장.
>
> ### 우연히 mlb-dbt 를 검증 대상으로 쓴 이유
>
> 사내 팀 dbt 프로젝트 = mlb-dbt (Trino 기반) 가 손에 있어서 PoC 시작점으로 활용.
> 결과적으로 **adapter 종류 무관하게 패턴 동작** 확인 — 더 강한 일반화.

## 대상

| 항목 | 값 |
|---|---|
| dbt 프로젝트 | `~/PycharmProjects/mlb-dbt` (멜론 데이터 팀 운영) |
| Airflow DAG repo | `~/PycharmProjects/music-airflow-dags` (현재 Airflow 2.10.2) |
| dbt 버전 | 1.9.3 |
| Adapter | **dbt-trino** (사내 Hadoop Presto 대상 — BQ 가 아님) |
| 모델 갯수 | **39** |
| Layer 구조 | bronze / silver / meta / gold / api / dl (사실상 6 layer) |
| Composer 환경 | `test-airflow3` (composer-3-airflow-3.1.7-build.9) |

## ⚠️ 검증 대상의 특이성 — Trino adapter

이 dbt 프로젝트는 사내 팀의 현재 운영 stack:
- `profiles.yml` 의 `type: trino` (= dbt-trino adapter)
- `host: presto-adhoc.dev.melon.com` (사내 Hadoop Presto)
- LDAP 인증
- 사내 git 의 `music-dbt-utils` 패키지 의존
- 사내 fork `custom-dbt-trino` 사용

→ **사내 이관 방향은 BQ** 이므로 이 stack 자체는 폐기 대상. 하지만 Cosmos + Composer 패턴은 adapter 무관해서 BQ 에서도 동일하게 통함을 본 PoC 가 보여줌.

→ BQ 시나리오 검증은 [[03_bq_dbt_run_in_composer]] 에서.

## 셋업 — 2 Phase

### Phase 1. 로컬에서 manifest 생성 ✅

목표: `target/manifest.json` 확보. Cosmos 가 이걸 읽어서 task render.

```bash
cd ~/PycharmProjects/mlb-dbt
python3 -m venv .venv         # Python 3.12 권장 (3.11 도 dbt-core 1.9 호환)
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

# .env (DBT_GIT_ACCESS_TOKEN 만 진짜, 나머지 dummy)
set -a; source .env; set +a

# dbt-trino install (사내 fork → 실패 시 PyPI fallback)
pip install "dbt-trino @ git+https://${DBT_GIT_ACCESS_TOKEN}@github.kakaocorp.com/melondatadev/custom-dbt-trino.git@v1.9.3-1" \
  || pip install "dbt-trino==1.9.3"

# 사내 dbt 패키지 받기
dbt deps    # dbt-labs/dbt_utils + 사내 music-dbt-utils

# DB 연결 없이 parse — manifest 생성
dbt parse --target dev
ls -lh target/manifest.json    # → 2.1MB
```

**결과**: 통과. `dbt --version` 에 `trino: 1.9.3` plugin 등록됨.

### Phase 2. Composer 에 업로드 + Cosmos 로 렌더링 ✅

#### 2-1. mlb-dbt 프로젝트 + manifest 를 GCS 에 업로드

```bash
BUCKET=dev-airflow-test-bucket
TARGET=gs://$BUCKET/dags/dbt_projects/mlb-dbt

gsutil -m cp -r \
  models macros seeds snapshots tests dbt_packages \
  dbt_project.yml profiles.yml packages.yml package-lock.yml \
  $TARGET/

gsutil cp target/manifest.json $TARGET/target/manifest.json
```

> 제외: `.venv`, `.env`, `.git`, `logs/`, `target/` 의 부수 파일 (`partial_parse.msgpack`, `perf_info.json` 등)
> 포함: `dbt_packages/` (dbt deps 결과) + `target/manifest.json` (핵심)

#### 2-2. Composer 에 PyPI 패키지 등록

콘솔 → 환경 `test-airflow3` → PyPI 패키지 탭 → 추가:

| 패키지 | 버전 |
|---|---|
| `astronomer-cosmos` | `==1.8.0` |
| `dbt-core` | `==1.9.3` |
| `dbt-trino` | `==1.9.3` |

→ 환경 update 10~30분.

> 사내 fork `custom-dbt-trino` 는 PoC 렌더링엔 불필요 (mock profile 사용).
> 실제 실행 시도 시엔 [[../../스케줄러/PoC/03_custom_operator_pypi]] 의 AR 패턴으로 wheel push.

#### 2-3. 간소화된 DAG 작성 + 업로드

`~/PycharmProjects/composer-poc-pkg/dags/poc_mlb_dbt_render.py`:

```python
from datetime import datetime
from pathlib import Path

from airflow import DAG
from cosmos import (
    DbtTaskGroup,
    ProfileConfig,
    ProjectConfig,
    RenderConfig,
)
from cosmos.constants import LoadMode

DBT_PROJECT_PATH = "/home/airflow/gcs/dags/dbt_projects/mlb-dbt"

profile_config = ProfileConfig(
    profile_name="kakaoent_presto",
    target_name="dev",
    profiles_yml_filepath=Path(f"{DBT_PROJECT_PATH}/profiles.yml"),
)

with DAG(
    dag_id="poc_mlb_dbt_render",
    schedule=None,
    start_date=datetime(2026, 5, 1),
    catchup=False,
    tags=["poc", "dbt", "render-only"],
) as dag:
    dbt_tasks = DbtTaskGroup(
        group_id="mlb_dbt",
        project_config=ProjectConfig(
            dbt_project_path=DBT_PROJECT_PATH,
            manifest_path=f"{DBT_PROJECT_PATH}/target/manifest.json",
        ),
        profile_config=profile_config,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_MANIFEST,
            enable_mock_profile=True,     # 실제 Presto 접근 없이 렌더링만
        ),
        operator_args={"install_deps": False},
    )
```

**핵심 선택**:
- `LoadMode.DBT_MANIFEST` — 미리 생성한 manifest 만으로 render (DB 연결 ❌)
- `enable_mock_profile=True` — Trino profile 검증 우회
- 사내 utils/loupe/sender import 모두 제거 — 순수 Cosmos 만

```bash
gsutil cp ~/PycharmProjects/composer-poc-pkg/dags/poc_mlb_dbt_render.py \
  gs://dev-airflow-test-bucket/dags/
```

## 검증 결과 ✅

### Airflow UI 에서 보이는 것

```
poc_mlb_dbt_render
  └─ mlb_dbt (task group, 39 Tasks)
      ├─ bz_track_stream      → [run, test]   ← bronze layer
      ├─ bz_track_stream_source → [run, test]
      ├─ bz_user              → [run, test]
      ├─ sv_track_stream      → [run, test]   ← silver
      ├─ sv_track_listener    → [run, test]
      ├─ sv_track_like        → [run, test]
      ├─ sv_album_*           → [run, test]
      ├─ mt_track             → [run, test]   ← meta
      ├─ mt_album             → [run, test]
      ├─ mt_accode_track      → [run, test]
      ├─ gd_track_list        → [run, test]   ← gold
      ├─ gd_album_*           → [run, test]
      ├─ api_track_list       → [run, test]   ← api
      ├─ api_overview         → [run, test]
      ├─ dl_overview          → [run, test]   ← dl
      └─ dl_hottrack          → [run, test]
       ...
   의존성 화살표가 layer 간 정확히 시각화됨 (bz → sv → mt/gd → api/dl)
```

### 발현된 Cosmos 의 가치

| 항목 | 기존 사내 (Cosmos 사용 중) | 본 PoC (Composer 3 + Cosmos) |
|---|---|---|
| 모델별 task 분해 | ✅ | ✅ |
| `run` + `test` task 자동 분리 | ✅ | ✅ |
| layer 간 의존성 자동 시각화 | ✅ | ✅ |
| 모델별 재실행 / 모니터링 | ✅ | ✅ |
| Airflow 버전 | 2.10.2 | **3.1.7** |

→ Cosmos 가 Airflow 3 (Composer 3) 에서 **identical 한 task graph** 를 생성. 마이그레이션 시 패턴 변경 X.

## 회의 메시지

> **Q1: dbt 프로젝트가 Composer 3 + Cosmos 로 잘 로드되는가?**
> ✅ **확정** (adapter 무관). 39개 모델 → 60+ task (run + test) 로 자동 분해. layer 의존성 그래프 그대로 시각화. UI 캡쳐 확보.
>
> **Q2: BQ 이관 시 패턴은?**
> 본 PoC 가 검증한 **Cosmos + manifest + DBT_MANIFEST mode** 패턴이 그대로 적용됨. adapter 만 dbt-trino → dbt-bigquery 로 교체.
> 후속 검증: [[03_bq_dbt_run_in_composer]] 에서 토이 BQ dbt 프로젝트로 실제 dbt run 까지 확인.
>
> **Q3: 렌더링 마이그레이션 작업량은?**
> **거의 0** (manifest 만 GCS 에 올리고 DAG 1개 작성하면 끝)
>
> **Q4: 운영 영향은?**
> - 모델 변경 시 manifest 재생성 + GCS 재업로드 필요 (CI 에서 자동화 가능)
> - 또는 `LoadMode.DBT_LS` 로 매번 dbt 가 manifest 생성 (느림)
> - 현실적 운영: **CI 에서 dbt parse → manifest 를 dbt project 와 함께 push** 가 표준 패턴
>
> **Q5: BQ 이관 시 dbt 프로젝트 자체 변환 작업은?**
> 별개 작업 (Cosmos 인프라 PoC 와 무관):
> - profile 교체 (`type: trino` → `type: bigquery`)
> - dbt-bigquery adapter 로 변경
> - 모델 SQL 방언 변환 (sqlglot 80% 자동 + 수동 검토)
> - 사내 macros (music-dbt-utils) BQ 호환 재작성
> - 데이터 자체의 BQ 이관 (가장 큰 작업, 별개 프로젝트)
>
> 추정: 모델 SQL/macros 변환만 **4~5주**. 데이터 이관은 3~6개월 별도 트랙.

## 시연용 자료 (회의)

| # | 자료 | 메시지 |
|---|---|---|
| 1 | `poc_mlb_dbt_render` DAG 가 Airflow UI 에 보임 | "Composer 가 사내 dbt 프로젝트의 DAG 받음" |
| 2 | Graph view 전체 (task group 닫힌 상태) | "Cosmos 가 mlb-dbt 를 하나의 task group 으로 통합" |
| 3 | Graph view 펼친 화면 (39 task) ⭐ | "Cosmos 가 모델별로 자동 분해, run + test 까지" |
| 4 | 개별 모델 task 클릭 → Details | "각 모델이 독립 Airflow task — granular 운영 가능" |

## 발견 / 깨달음

### 1. `enable_mock_profile=True` 의 가치

사내 Presto 접근 없이도 렌더링만 검증 가능. Cosmos 의 운영 시나리오 분리:
- **CI 단계**: dbt parse → manifest 생성 (사내 git + dbt 환경 필요)
- **Composer 단계**: manifest 만 받아서 render (사내 Presto 만 있으면 실행 가능)
- **PoC 단계**: 둘 다 mock — 렌더링만 ⭐

→ **렌더링 ≠ 실행**. Composer 가 받아주는지의 질문은 렌더링 단계만으로 답 가능.

### 2. dbt-trino 가 Composer 에서도 install 됨

Composer 3 가 dbt + Cosmos 의 일반적 stack 다 받아줌. 사내 fork (`custom-dbt-trino`) 가 필요한 경우엔 [[../../스케줄러/PoC/03_custom_operator_pypi]] 의 AR 패턴 적용 가능.

### 3. Cosmos 의 LoadMode 선택

| Mode | 사용 시점 | 비고 |
|---|---|---|
| `DBT_MANIFEST` | manifest 미리 준비된 경우 ⭐ | DB 연결 X, 빠름 |
| `DBT_LS` | 런타임에 dbt ls 실행 | DB 연결 필요 (보통) |
| `AUTOMATIC` | 알아서 결정 | 예측 어려움 |

→ 운영 시 `DBT_MANIFEST` + CI 에서 manifest 생성 패턴이 안정적.

## 안 검증한 것 (다음 PoC 영역)

> 사내 이관 방향이 BQ 로 확정되어, Trino specific 항목들은 검증 가치 낮음. 본 PoC 다음은 BQ 시나리오.

| 항목 | 검증 영역 | 노트 |
|---|---|---|
| **BQ adapter 로 같은 패턴 실제 실행** | [[03_bq_dbt_run_in_composer]] | ⭐ 다음 PoC (토이 BQ dbt) |
| Airflow 3 Asset 모드와의 조합 | Step 1 | Asset-Centric 패러다임 |
| DataHub/Loupe lineage 연동 | Step 4 | 별개 PoC |

→ Trino 관련 후속 (사내 Presto VPC, LDAP, 사내 utils wheel 화, Airflow 2→3 변환) 은 **검증 안 함**. BQ 이관 시 무효.

## 관련 노트

- [[README]] — 애슬론 PoC 의 전체 흐름 (Step 2 위치)
- [[03_bq_dbt_run_in_composer]] — ⭐ **후속 PoC** (BQ adapter + 실제 dbt run)
- [[../1_개요]] — Asset-Centric 의사결정
- [[../2_Git 동기화·dbt 전환 계획]] — 사내 dbt 전환 계획
- [[../3_dbt 능력 경계와 영역 분담]] — dbt vs non-dbt 분담
- [[../../스케줄러/PoC/04_worker_pool_queue]] — Composer 의 worker / queue / pool 검증

## 부록 — Phase 1 단계별 출력 (기록)

```
$ dbt --version
Core:
  - installed: 1.9.3
Plugins:
  - trino: 1.9.3

$ dbt deps
Installing dbt-labs/dbt_utils  → Installed from version 1.3.0
Installing music-dbt-utils      → Installed from revision a7aa155...

$ dbt parse --target dev
Registered adapter: trino=1.9.3
Performance info: target/perf_info.json

$ ls -lh target/manifest.json
-rw-r--r-- 1 diana.46 staff 2.1M  manifest.json
```
