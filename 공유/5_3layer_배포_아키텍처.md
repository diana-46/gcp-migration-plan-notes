# 5. 3-layer 배포 아키텍처

> Custom Operator / dbt / DAG 세 layer 의 독립 배포 + 정합성 확보 전략.
> 관련: [[스케줄러/7_4_DAG + dbt + Operator 3축 배포 통합]], [[스케줄러/15_관리 레포 인벤토리]]

## 왜 3-layer 인가

세 자산이 변경 주기 · 소유자 · 배포 방식이 다르기 때문:

| Layer | 자산 | 변경 주기 | 소유자 | 배포 |
|---|---|---|---|---|
| 1. Custom Operator | 사내 operator, hook, sensor | 월 1-2회 | 플랫폼팀 | Artifact Registry (Python) |
| 2. dbt Project | SQL 모델, schema.yml, 소스 | 주 1-3회 | DE (팀별) | CI → GCS `data/dbt/` |
| 3. DAG | DAG 파일, 스케줄, 의존성 | 일-시간 단위 | DE (팀별) | GCS `dags/{team}/` |

세 layer 를 하나의 저장소에 묶으면 변경 주기 mismatch 로 배포 마찰 발생. 별도 관리가 자연.

## Layer 1 — Custom Operator (`apache-airflow-providers-kakaoent-dataplatform`)

### 목적

기존 사내 14 operators + helper 를 Python 패키지로 재배포. DE 는 `pip install` + `import` 로 오픈소스 provider 처럼 사용.

### 저장소

- **레포**: `dp-airflow-provider` (플랫폼팀 소유)
- **배포**: GCP Artifact Registry (Python repo, `asia-northeast3-python.pkg.dev/dev-dp-project-354904/dev-dp-python-registry`)
- **버전**: SemVer, 도메인별 lock

### 구성 (플랫폼팀 관리)

```
apache-airflow-providers-kakaoent-dataplatform/
├── src/airflow/providers/kakaoent/dataplatform/
│   ├── operators/
│   │   ├── loupe_kafka_batch.py
│   │   ├── loupe_signal_http.py
│   │   └── ...
│   ├── sensors/
│   │   └── bigquery_query.py
│   └── ...
├── pyproject.toml
└── .github/workflows/publish.yml
```

### 소비 팀 사용

`requirements.txt`:
```
apache-airflow-providers-kakaoent-dataplatform==0.3.0
--extra-index-url https://asia-northeast3-python.pkg.dev/dev-dp-project-354904/dev-dp-python-registry/simple/
```

DAG 파일:
```python
from airflow.providers.kakaoent.dataplatform.operators.loupe_kafka_batch import LoupeKafkaBatchOperator
```

### 왜 dags_folder git sync 아닌가

| 비교 | dags_folder sync | pip 패키지 |
|---|---|---|
| 환경별 버전 lock | 불가 (한 코드 강제) | `==X.Y.Z` 각자 |
| SemVer breaking change 처리 | 팀 전체 동시 영향 | 도메인별 upgrade 자유 |
| 표준 Python 사용법 | 관례적 import | 오픈소스와 동일 |
| API 표면 | 모호 | `pyproject.toml` 명시 |

**포인트**: 팀별 Composer 운영 예정 → 각 Composer 가 자기 페이스로 `pip upgrade` 하도록 하려면 패키지 방식이 자연.

## Layer 2 — dbt Project

### 목적

SQL 변환 로직 (마트, 소스 정의, 테스트, 문서) 을 코드로.

### 저장소

- **팀별 dbt 저장소**: `storydata-dbt`, `kpayment-dbt`, ...
- **배포**: GitHub Actions 가 `dbt parse` → `manifest.json` 생성 → `gs://COMPOSER_BUCKET/data/dbt/` 로 sync
- **Composer 워커 마운트**: `/home/airflow/gcs/data/dbt/`

### 구성 (팀 소유)

```
storydata-dbt/
├── dbt_project.yml
├── profiles.yml
├── requirements.txt        # dbt-core, dbt-bigquery
├── models/
│   ├── sources.yml
│   ├── mart/               # 최종 산출물
│   └── temp/               # Neptune YAML temp 대응
├── macros/                 # Custom incremental strategy 등
└── .github/workflows/
    └── deploy.yml          # dbt parse + GCS sync (+ DataHub emit)
```

### 사용 예시 (Composer)

Cosmos 가 manifest 를 읽어 Airflow DAG task 로 렌더:
```python
DbtTaskGroup(
    project_config=ProjectConfig(
        dbt_project_path="/home/airflow/gcs/data/dbt",
        manifest_path=f"{DBT_PROJECT_PATH}/target/manifest.json",
    ),
    profile_config=ProfileConfig(
        profile_name="storydata",
        target_name="integration",
        profiles_yml_filepath=f"{DBT_PROJECT_PATH}/profiles.yml",
    ),
    render_config=RenderConfig(
        load_method=LoadMode.DBT_MANIFEST,
        select=["+bizberry_community_overview_trend"],
    ),
    ...
)
```

## Layer 3 — DAG

### 목적

Task 오케스트레이션. dbt DAG + Loupe / BQ / GCS operator + 스케줄.

### 저장소

- **팀별 DAG 저장소**: `storydata-airflow-dags`, `kpayment-airflow-dags`, ...
- **배포**: GitHub Actions → `gsutil rsync -r -c -d dags/{team}/ BUCKET/dags/{team}/`
- **Composer scheduler**: `dags/` 재귀 스캔, 서브디렉토리 자동 인식

### 구성 (팀 소유)

```
storydata-airflow-dags/
├── requirements.txt        # airflow + provider 패키지 pin
├── dags/
│   └── storydata/          # 팀 서브디렉토리
│       ├── berriz_0101_bizberry_hourly_integration.py
│       ├── berriz_bizberry_downstream_demo.py
│       └── ...
├── .github/workflows/
│   └── deploy.yml          # gsutil rsync -c -d
└── conftest.py             # pytest 격리
```

## 세 layer 의 독립성 vs 정합성

### 독립적으로 관리되는 것

- **Layer 1** 은 Layer 2/3 와 무관하게 릴리즈 (SemVer)
- **Layer 2** 는 Layer 3 와 무관하게 배포 (manifest 만 갱신)
- **Layer 3** 은 Layer 1/2 의 최신 버전을 참조만 함

### 정합성이 필요한 순간

Layer 2 (dbt) 신규 모델 + Layer 3 (DAG) 그 모델 참조 = 함께 배포:

**표준 순서** (관련: [[애슬론/8_배포 시 유의할 점]] § 1):
1. `storydata-dbt` 에서 새 모델 커밋 → PR merge
2. Actions 로 `manifest.json` 갱신 + GCS sync 완료 확인
3. `storydata-airflow-dags` 에서 DAG 커밋 (cosmos `select` 에 새 모델 포함) → PR merge

**역순은 위험**: Cosmos 가 `select=["new_model"]` 을 만난 순간 manifest 에 아직 없으면 "model not found" 에러 → DAG import error UI 배지.

### 통합 지점

- **Composer 환경** 하나가 세 layer 결합해서 실행
- **Cosmos** 가 Layer 2 (manifest) → Layer 3 (Airflow task) 렌더
- **Provider 패키지** (Layer 1) 는 `pip install` 로 Layer 3 코드에서 import

## 팀별 저장소 x 3-layer

**팀별 DAG + 팀별 dbt + 공용 provider** 라는 조합:

```
플랫폼팀 (공용):
    dp-airflow-provider           ──── GAR (모든 Composer 가 pip install)

Story 팀:
    storydata-dbt                 ──┐
    storydata-airflow-dags        ──┴── Composer: storydata-composer

Kpayment 팀:
    kpayment-dbt                  ──┐
    kpayment-airflow-dags         ──┴── Composer: kpayment-composer
```

- 팀 하나 늘어날 때 저장소 2 개 + Composer 인스턴스 1 개
- Provider 는 공용 (GAR) 유지, 각 Composer 가 자기 페이스로 `pip install`
- 팀 간 완전 격리 (저장소 · Composer 모두 팀 소유)

## 관련 문서

- [[스케줄러/7_3_공통 Custom Operator 제공 방안]] — Provider 패키지 상세
- [[스케줄러/7_4_DAG + dbt + Operator 3축 배포 통합]] — 3-layer 통합 배포
- [[스케줄러/15_관리 레포 인벤토리]] — 저장소 인벤토리 + athlon → git 전환
- [[스케줄러/11_DAG Bundles와 배포 전략]] — DAG 배포 세부
- [[애슬론/8_배포 시 유의할 점]] — 실전 배포 순서 함정
