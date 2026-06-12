---
title: "다중 Airflow 환경에 공통 Custom Operator 제공 방안"
status: draft
tags:
  - airflow
  - 스케줄러
  - operator
  - packaging
  - artifact-registry
  - composer
created: 2026-06-08
updated: 2026-06-08
---

# 다중 Airflow 환경에 공통 Custom Operator 제공 방안

> **도메인별로 분리된 여러 Airflow 환경** (모두 우리 팀 운영) 에 14개 custom operator 와 helper 코드를 **일관되게 제공**하기 위한 안.
>
> DAG 정의는 DE 가 직접 코딩하는 방향이므로 (athlon framework 의존성 축소 트랙은 별도), 본 문서는 **공통 운영 자산(operators / sensors / hooks / macros) 의 패키징·배포** 만 다룬다.

## 결론 먼저

> **Internal Provider Package 를 만들고 GCP Artifact Registry (Python repository) 로 배포**.
>
> - 각 Airflow 환경의 `requirements.txt` 에 `kakaoent-airflow-providers==X.Y.Z` 한 줄
> - SemVer + 도메인별 버전 lock 으로 도메인별 Airflow 버전 가변성 흡수
> - Artifact Registry 는 이미 사내 테스트 완료 → 인프라 추가 비용 0
> - 기존 자산은 포장이사 — 신규 repo 에서 raw Airflow 친화적 helper (`get_kakaoent_default_args` 등) 와 함께 정리해 시작

## 1. 배경 — 왜 지금 이걸 정해야 하나

### 1.1 운영할 Airflow 환경 — 도메인별 분리

| 항목         | 값                                          |
| ---------- | ------------------------------------------ |
| 환경 수       | 3개 (가정, 향후 확장 가능)                          |
| 구분 축       | **도메인별 분리** (예: 도메인 A / B / C — 서비스/사업 단위) |
| 운영 주체      | 모두 우리 팀 (외부 consumer 없음)                   |
| Airflow 버전 | **환경별로 같을 수도, 다를 수도 있음** — 도메인별 자율 결정 가능   |
| 환경 추가 가능성  | 있음 — 신규 도메인 / 분리된 워크로드 등                   |

도메인별 분리이므로 핵심 함의:

1. **각 환경에서 실제로 쓰는 operator subset 이 다를 수 있음** — 예: NabiSignal 류는 일부 도메인만, Hive 류는 다른 일부만. → 패키지를 단일하게 두되 **import 시점에 lazy 로드** + extras 로 옵션 의존성 분리 가능
2. **각 환경의 service account 가 다름** → Artifact Registry 인증 권한을 **도메인 환경별 SA 에 각각 부여** 필요
3. **운영 주체가 동일** → SemVer / 안정된 API 의 절대성은 낮음. 다만 환경별 deploy 사이클 / 우선순위가 달라질 수 있어 **환경별 버전 lock** 자체는 가치 있음
4. **환경별 Airflow 버전 가변성** → `apache-airflow` 를 **범위 의존**으로 두는 게 안전 (특정 버전 고정 X)

### 1.2 DAG 직접 코딩 방향 결정 (전제)

- DAG 정의를 athlon DB → DE 직접 코딩으로 전환
- 따라서 framework 레벨에서 자동 wrapping 되던 부분 (`on_failure_callback` injection, queue 지정, pool 매핑 등) 이 DAG 코드에 명시적으로 들어옴
- **operator 자체는 그대로 재사용 자산** → 공통 패키지로 분리할 명분이 더 명확해짐

## 2. 현황 진단

`~/PycharmProjects/airflow-dags` 의 현재 구조를 본 결과:

| 항목 | 현재 상태 | 다중 환경 관점의 위험 |
|---|---|---|
| 패키지화 | ❌ `setup.py` / `pyproject.toml` 없음. `operators/__init__.py` 의 단순 re-export 만 존재 | `pip install` 으로 다른 환경에 줄 방법 없음 — git 공유 외엔 수단 0 |
| 버전 관리 | ❌ git commit hash 가 사실상 버전 | "지금 환경에 무엇이 떠 있나" 추적 불가 |
| import path | `from operators import X` — `dags_folder` 가 sys.path 에 들어와야만 작동 | 다른 환경에서 동일 sys.path 보장 어려움 |
| `__init__.py` | `__all__` 이 class 객체 list (잘못된 사용법). 14개 중 3개 (`S3ToHdfsSyncOperator` 등) export 누락 | 표면 API 가 모호 |
| CI/CD | `.github/` 에 CODEOWNERS + PR 템플릿만. **workflow 0** | 빌드/배포 자동화 from scratch |
| 배포 방식 | repo 전체를 worker `dags_folder` 로 sync (추정) | 환경별 버전 lag 표현 불가, 모든 환경 동시 push 필요 |
| Airflow 버전 lock | `requirements.txt` 에 `apache-airflow==2.3.2` hard-coded | 다른 Airflow 버전을 쓰는 환경에서 즉시 깨질 위험 (환경 버전 동일 여부 확인 필요) |

**가장 큰 위험**: `requirements.txt` 의 `apache-airflow==2.3.2` 가 박혀있어, 다른 환경의 Airflow 버전이 다를 경우 import 단계부터 깨질 수 있음.

### 2.1 공통 코드 자산 목록

패키징 후보:

| 모듈 | 파일 수 | 외부 의존 |
|---|---|---|
| `operators/` | 14개 (BigqueryQuerySensor, AthlonQuerySensor, NabiSignalProduceOperator, ...) | airflow providers 다수 |
| `sender/` | `send_slack_message`, `config` 등 | Slack SDK / HTTP_PROXY |
| `macros/` | `user_defined_macros` (다수 DAG 에서 import) | - |
| `common_project/` | (확인 필요) | - |
| `utils/` | repr_utils 등 | - |

→ `operators/` 가 `sender/`, `macros/` 에 의존하므로 **함께 패키지화하거나 별도 패키지로 분리** 두 가지 결정 필요. 1차 안은 **단일 패키지에 sub-module 로 묶기**.

## 3. 옵션 비교

| 옵션 | 표준성 | 환경별 버전 분리 | 배포 lag | 구축 비용 | 평가 |
|---|---|---|---|---|---|
| A. Internal Python Package (pip) | 🟢 | 🟢 | 🟡 release→install (수십분) | 🟡 | 표준이지만 B 와 동시 고려 |
| **B. Provider Package + Artifact Registry** | 🟢🟢 | 🟢 | 🟡 | 🟡 (이미 AR 인프라 있음) | **✅ 추천** |
| C. Git Submodule | 🟡 | 🟡 (branch 관리) | 🟡 | 🟢 | 단기 우회 가능 |
| D. GCS sync (Composer plugins/) | 🟡 | 🔴 | 🟢🟢 즉시 | 🟢 | 운영 자산으로는 위험. hotfix 보조용 |
| E. Monorepo | 🟢 (소속 같으면) | 🔴 (atomic) | 🟢 | 🟡 | 도메인별 환경 분리 컨텍스트에 부적합 |

→ **B 가 거의 결정**. A 와 B 의 차이는 "Airflow 컨벤션 따르기"로, 추가 비용 1~2일 수준.

## 4. 추천안 — Artifact Registry 기반 Provider Package

### 4.1 신규 repo 구조

```
kakaoent-airflow-providers/        ← 신규 repo
├── .gitignore
├── README.md
├── pyproject.toml                  # 패키지 메타데이터 + 빌드 설정
│
├── kakaoent_airflow/               # 패키지 루트 (flat layout)
│   ├── __init__.py
│   ├── get_provider_info.py        # Airflow UI 노출용 provider info
│   │
│   ├── operators/                  # 도메인 단위 묶음
│   │   ├── __init__.py
│   │   ├── nabi_operator.py        # NabiSignalProduce...
│   │   ├── loupe_operator.py       # LoupeKafkaBatch / LoupeSignalProduce / LoupeSignalHttp
│   │   ├── sync_operator.py        # S3ToHdfsSync / HiveToGcsSync
│   │   ├── signal_operator.py      # SignalProduce / ProduceToTopic
│   │   ├── snapshot_operator.py    # SnapshotHistory
│   │   └── etl_operator.py         # ThirdPartyPulling / DcoSeriesThumbnailCollect / HiveServer2ToSlack / SlackStat
│   │
│   ├── sensors/                    # BaseSensorOperator 류 분리 (Airflow 컨벤션)
│   │   ├── __init__.py
│   │   ├── athlon_sensor.py        # AthlonQuerySensor
│   │   └── bigquery_sensor.py      # BigqueryQuerySensor
│   │
│   ├── callbacks/                  # Slack 통합 한곳에 응집
│   │   ├── __init__.py
│   │   ├── slack_notifier.py       # send_text_message + failure/sla callback factory + webhook operator builder
│   │   └── config.py               # SlackToken / HTTP_PROXY (기존 sender/config 이동)
│   │
│   ├── macros/
│   │   ├── __init__.py
│   │   └── custom_macros.py        # user_defined_macros
│   │
│   └── utils/                      # 함수형 helper
│       ├── __init__.py
│       ├── dag_defaults.py         # get_kakaoent_default_args, get_kakaoent_sla_miss_callback
│       └── time_utils.py           # _get_interval_diff 등 시간 계산 로직
│
└── tests/                          # src 디렉토리와 1:1 mirror
    ├── __init__.py
    ├── operators/
    ├── sensors/
    ├── callbacks/
    └── utils/
```

**디자인 원칙**:
- **flat layout** — 작은 패키지엔 충분. src/ 없이 패키지 루트가 repo root 한 단계 아래
- **도메인 단위 묶음** — 14개 operator → 6개 파일로 압축. 관련 operator 동시 보기 좋음
- **함수형 helper** (utils/dag_defaults.py) — 컨텍스트 매니저 magic 대신 `default_args=get_kakaoent_default_args(...)` 식. raw Airflow 코드와 자연스럽게 결합
- **Slack 통합 응집** — `callbacks/slack_notifier.py` 에 send + callback factory + webhook operator builder 모두 (기존 `sender/` 흡수)
- **`get_provider_info.py` 별도 파일** — Airflow Provider 표준 entry point 사용

### 4.2 `pyproject.toml` 골격

```toml
[build-system]
requires = ["setuptools>=68", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "kakaoent-airflow-providers"
version = "1.0.0"
description = "Kakao Entertainment internal Airflow custom operators / sensors / helpers"
requires-python = ">=3.10"
dependencies = [
    "apache-airflow>=2.10,<3",               # 범위로 lock (특정 버전 X)
    "apache-airflow-providers-google>=10.0",
    "apache-airflow-providers-apache-hive>=6.0",
    "apache-airflow-providers-apache-kafka>=1.0",
    "apache-airflow-providers-amazon>=8.0",
    "apache-airflow-providers-slack>=8.0",
    # ...
]

# Airflow provider 표준 — UI 에 자동 노출
[project.entry-points."apache_airflow_provider"]
provider_info = "kakaoent_airflow.get_provider_info:get_provider_info"

[tool.setuptools.packages.find]
include = ["kakaoent_airflow*"]
```

→ `apache-airflow` 를 **범위로 lock**. 도메인별 환경의 Airflow 버전 차이를 한도 안에서 흡수.
→ Provider 표준 entry point 채택 — Airflow UI 의 Providers 탭에 자동 노출 + connection type / hook 자동 discovery.

### 4.3 Artifact Registry 설정

```bash
# 1. Python repository 생성 (1회)
gcloud artifacts repositories create airflow-providers \
    --repository-format=python \
    --location=asia-northeast3 \
    --description="Internal Airflow custom operators"

# repository URL:
# https://asia-northeast3-python.pkg.dev/<project>/airflow-providers/
```

**Publish (release.yml 에서 자동)**:

```yaml
# .github/workflows/release.yml
on:
  push:
    tags: ['v*']

jobs:
  publish:
    runs-on: ubuntu-latest
    permissions:
      contents: read
      id-token: write          # WIF 인증용
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: '3.11' }
      - uses: google-github-actions/auth@v2
        with:
          workload_identity_provider: projects/.../providers/...
          service_account: airflow-providers-publisher@....iam
      - run: |
          pip install build twine keyrings.google-artifactregistry-auth
          python -m build
          twine upload \
            --repository-url https://asia-northeast3-python.pkg.dev/<project>/airflow-providers/ \
            dist/*
```

**Install (각 Airflow 환경)**:

```bash
# pip.conf 또는 requirements.txt 에 index-url 지정
pip install \
    --extra-index-url https://asia-northeast3-python.pkg.dev/<project>/airflow-providers/simple/ \
    kakaoent-airflow-providers==1.2.3
```

### 4.4 환경별 install 방식

| 환경 유형 | 방식 | 인증 |
|---|---|---|
| **각 도메인 Composer 환경** | `gcloud composer environments update --update-pypi-packages-from-file` 옵션 + `[tool.pip.global]` 에 `extra-index-url` 추가 | 각 도메인 환경의 service account 에 **Artifact Registry Reader** 권한 부여 → 자동 인증 |
| **GKE 기반 Airflow (있다면)** | worker Dockerfile 빌드 단계에서 `pip install` | build pod 의 service account 에 AR Reader + `keyrings.google-artifactregistry-auth` plugin |
| **로컬 개발** | `pip install` 시 `gcloud auth application-default login` 이후 자동 인증 | ADC + keyring plugin |

→ Artifact Registry 의 Python repo 는 **keyring plugin** 이 표준. pip 가 자동으로 GCP credentials 사용.
→ **도메인이 늘어날 때마다 해당 환경의 SA 에 AR Reader 권한 1개 추가**만 하면 끝.

### 4.5 SemVer + 도메인별 버전 lock 전략

도메인 환경은 Airflow 버전이 같을 수도, 다를 수도 있음 → **둘 다 자연스럽게 수용**하는 SemVer 정책.

**SemVer 정책**:
- **major bump** — Airflow major 비호환 변경 대응 (provider import path 변경, deprecated API 제거 등)
- **minor** — 신규 operator 추가, 호환 가능한 변경
- **patch** — 버그 fix

**도메인별 lock 의 의미**:
- 도메인 환경 owner 가 자기 페이스로 버전 bump 가능 (PR → install)
- 한 도메인이 새 버전에서 문제 발견 → 다른 도메인은 영향 0 (이전 lock 유지)
- 환경 Airflow 버전이 다른 시점이 오면 자연스럽게 major lock 으로 분기

### 4.6 도메인별 의존성 분리 (extras, 선택)

도메인 환경마다 실제로 쓰는 operator 집합이 다른 경우, `[project.optional-dependencies]` 로 운영 의존성 분리 가능:

```toml
[project.optional-dependencies]
hive = ["apache-airflow-providers-apache-hive>=6.0"]
kafka = ["apache-airflow-providers-apache-kafka>=1.0"]
gcp = ["apache-airflow-providers-google>=10.0"]

# 도메인 환경의 requirements.txt:
# kakaoent-airflow-providers[hive,gcp]==1.0.0
```

→ 의존성 footprint 감소 + worker image 크기 감소 + 보안 surface 감소. 처음엔 base 의존성에 다 두고, 운영 안정화 후 도입 검토.

### 4.7 CI/CD 파이프라인

```
[Developer]
  ↓ PR 생성
[CI (ci.yml)]
  - ruff / black
  - pytest
  - mypy (선택)
  ↓ PR merge
[main branch]
  ↓ 수동 release: git tag v1.2.3 && git push --tags
[Release (release.yml)]
  - python -m build
  - twine upload → Artifact Registry
  - GitHub Release 자동 생성 (changelog)
  ↓
[각 도메인 환경의 deploy]
  - requirements.txt 의 버전 bump PR
  - merge 시 Composer update / image rebuild
```

→ release 는 **수동 tag 만 사용**. 자동 release 는 초기엔 위험.

### 4.8 Helper API 카탈로그 (athlon framework 의 자동 wrapping 대체)

DAG 직접 코딩 방향으로 가면서, athlon framework 가 암묵적으로 처리해주던 boilerplate 는 **명시적 함수형 helper** 로 대체. magic 줄이고 가독성/디버깅 확보.

**A. Default args / SLA callback (`utils/dag_defaults.py`)**:

```python
def get_kakaoent_default_args(
    alert_channel: str,
    retries: int = 2,
    emails: list[str] | None = None,
    **overrides,
) -> dict:
    """default_args 표준 셋업. on_failure_callback + emails + retries 포함."""

def get_kakaoent_sla_miss_callback(channel: str) -> Callable:
    """SLA miss 시 slack 알림 callback factory."""
```

사용 예:

```python
from datetime import datetime
from airflow import DAG
from kakaoent_airflow.utils.dag_defaults import (
    get_kakaoent_default_args,
    get_kakaoent_sla_miss_callback,
)
from kakaoent_airflow.macros.custom_macros import user_defined_macros

with DAG(
    'berriz_0011_dw_hourly',
    schedule_interval='@hourly',
    start_date=datetime(2024, 1, 1, tzinfo=KST),
    default_args=get_kakaoent_default_args(alert_channel='#alert-berriz'),
    sla_miss_callback=get_kakaoent_sla_miss_callback('#alert-berriz'),
    user_defined_macros=user_defined_macros,
) as dag:
    ...
```

**B. Slack 통합 (`callbacks/slack_notifier.py`)** — 한 파일에 응집:

```python
# 메시지 송신
def send_text_message(message: str, channel: str, **kwargs) -> None: ...

# Callback factories
def slack_failure_callback(channel: str) -> Callable: ...
def slack_failure_callback_with_fields(channel: str, operator_class: str) -> Callable: ...
def dump_snapshot_failure_callback(channel: str, region: str, phase: str) -> Callable: ...

# Slack message operator builder
def slack_webhook_operator(task_id: str, dag, channel: str, **kwargs) -> SlackWebhookOperator: ...
```

**C. 시간 계산 (`utils/time_utils.py`)** — 순수 함수:

```python
def get_interval_as_timedelta(schedule_interval, run_datetime=None) -> timedelta: ...
def get_interval_diff(internal_interval, external_interval, run_datetime=None) -> timedelta: ...
    """주기가 다른 DAG 사이의 ExternalTaskSensor execution_delta 자동 계산."""
```

**D. Provider info (`get_provider_info.py`)** — Airflow UI 노출:

```python
def get_provider_info() -> dict:
    return {
        "package-name": "kakaoent-airflow-providers",
        "name": "Kakao Entertainment",
        "description": "Kakao Entertainment internal Airflow custom operators",
        "versions": ["1.0.0"],
    }
```

**디자인 원칙**:
- helper 는 **명시적 호출** (athlon 의 암묵적 wrapping 과 다름)
- helper 안 쓰고 **raw Airflow 그대로** 작성도 가능 (강제 안 됨)
- 함수형 (default_args dict 반환 등) — 컨텍스트 매니저 magic 회피
- 도메인 특화 boilerplate 가 반복 패턴으로 발견되면 그때 helper 추가 (early abstraction 회피)

## 5. 셋업 가이드 — 포장이사 컨텍스트

> 기존 athlon framework 기반 운영을 **점진적으로 마이그레이션 하지 않고** 신규 환경에서 처음부터 패키지 install 해서 시작. shim / dual-mode / 기존 코드 호환 불필요.

### 5.1 준비물

- [ ] 신규 GitHub repo `kakaoent-airflow-providers` 생성
- [ ] Artifact Registry Python repository 생성 (§4.3)
- [ ] WIF 또는 Service Account 셋업 (GitHub Actions 가 AR publish 권한 보유)
- [ ] 각 도메인 환경의 SA 에 **Artifact Registry Reader** 권한 부여
- [ ] 사내 GitHub Actions runner 또는 public runner 사용 정책 확인

### 5.2 작업 흐름

1. **Repo 골격 작성**
   - `pyproject.toml`, `README.md`, `.gitignore`
   - 디렉토리 구조 (§4.1) 생성, `__init__.py` 들 추가
   - `get_provider_info.py` 작성
2. **코드 이동 + 정비**
   - 기존 14개 operator → 6개 도메인 묶음 파일로 정리
   - `__init__.py` 의 `__all__` 정확히 (string list)
   - `sender/` → `callbacks/slack_notifier.py` + `callbacks/config.py`
   - `macros/user_defined_macros` → `macros/custom_macros.py`
   - athlon 의 callback factory / time util 들 → `utils/`, `callbacks/`
3. **Helper API 작성** (§4.8)
   - `utils/dag_defaults.py`, `utils/time_utils.py`
   - `callbacks/slack_notifier.py` (Slack 응집)
4. **CI 작성** (§4.7)
   - `ci.yml`: ruff / pytest
   - `release.yml`: tag push 시 AR publish
5. **첫 release `v1.0.0` 찍기**
   - `git tag v1.0.0 && git push --tags` → AR 에 publish 검증
6. **각 도메인 환경에 install**
   - `requirements.txt` 에 `kakaoent-airflow-providers==1.0.0` + `--extra-index-url` 설정
   - `gcloud composer environments update ...` 로 적용 (Composer)
7. **신규 DAG 작성 시작**
   - `from kakaoent_airflow.operators.nabi_operator import NabiSignalProduceOperator`
   - `default_args=get_kakaoent_default_args(...)` 등

### 5.3 도메인 환경 추가 시 (운영 사이클)

- 해당 환경의 SA 에 **Artifact Registry Reader** 권한 부여
- `requirements.txt` 에 패키지 추가
- 끝

### 5.4 Airflow major 업그레이드 발생 시 (운영 사이클)

- 호환 변경 사항 정리 → `v2.0.0` 등 major bump
- 업그레이드 도메인은 `==2.x` 로 lock 변경
- 다른 도메인은 `==1.x` 그대로 유지 → **도메인별 진도 자유**

## 6. 미확정 / 후속 결정

- [ ] **도메인 환경별 Airflow 버전 정책** — 인프라 차원에서 한 가지 버전 통일을 권고할지, 도메인 자율로 둘지
- [ ] **`common_project/` 의 내용 확인** — operator 와 의존 있으면 패키징 범위에 포함, 없으면 별개
- [ ] **Hook / Connection type 도 함께 패키징할지** — 현재 `BaseHook.get_connection` 방식 사용 중 ([[operators/athlon_query_sensor.py:30]])
- [ ] **GitHub Actions 인증 정책** — WIF (Workload Identity Federation) vs SA key — 사내 보안 정책 확인
- [ ] **도메인 묶음 작명 컨벤션** — 도메인 (nabi/loupe) vs 기능 (sync/signal) 혼용 여부 확정
- [ ] **operator 도메인 묶음 매핑 검토** — §4.1 의 14개 → 6개 매핑이 도메인 관점에서 합리적인지 DE 팀 리뷰

## 7. 관련 문서

- [[7_2_리소스 다이어트 포인트]] — sensor → deferrable 등 코드 레벨 다이어트 (별도 트랙)
- [[11_DAG Bundles와 배포 전략]] — DAG 자체의 배포 (본 문서는 operator 자산만 다룸)
- [[13_Composer 3 환경 업그레이드 정책]] — Composer 측 PyPI 패키지 업데이트 사이클
- [[2_Cloud Composer vs Self-managed 비교]]

## 참고

- [Artifact Registry Python repository](https://cloud.google.com/artifact-registry/docs/python)
- [keyrings.google-artifactregistry-auth](https://pypi.org/project/keyrings.google-artifactregistry-auth/)
- [Airflow Provider Packages](https://airflow.apache.org/docs/apache-airflow-providers/index.html)
- [Composer 3 PyPI packages](https://cloud.google.com/composer/docs/composer-3/install-python-dependencies)
