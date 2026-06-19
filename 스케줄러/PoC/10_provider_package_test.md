---
title: "10. apache-airflow-providers 규격 패키지 PoC"
status: in_progress
tags:
  - poc
  - custom-operator
  - provider-package
  - apache-airflow-providers
  - artifact-registry
  - composer3
created: 2026-06-12
updated: 2026-06-12
---

# 10. apache-airflow-providers 규격 패키지 PoC

> **검증 질문 1**: `apache-airflow-providers-kakaoent` 같은 **공식 provider 규격** 패키지를 만들어 사내 AR 에 push 하고 Composer 3 에 install 했을 때, 일반 Python 패키지([[03_custom_operator_pypi]]) 대비 **실질적 가치 (UI Providers 자동 등록 / Connection type 자동 발견 / extras / OpenLineage hook)** 가 정말 동작하는가?
>
> **검증 질문 2**: PoC 03 의 `kakao-airflow-poc` 와 동일한 operator 를 provider 규격으로 재포장했을 때, **DAG 코드 import path 변경 외에 추가 변경 사항이 무엇인가?**
>
> **답 (예상)**: ✅ provider 규격 따르면 UI 메뉴에 자동 등록 + Connection type 자동 발견. 단 `provider.yaml` 작성 + 디렉토리 재배치 1회 비용.

## 검증 의도

[[03_custom_operator_pypi]] 는 "사내 wheel 이 Composer 3 에 install 되는가?" 까지만 검증. 사내 패키지의 **장기 운영 규격** 으로 [provider package 규격](https://airflow.apache.org/docs/apache-airflow-providers/index.html) 을 따를지는 별도 검증 필요.

특히 다음 운영 가치가 진짜 동작하는지 실측:
- Admin → Providers 메뉴에 사내 패키지 자동 등록
- Connection 생성 UI 드롭다운에 사내 connection type 자동 노출
- `extras` 로 옵션 의존성 (예: hive / gcp) 분리 설치
- OpenLineage hook 자동 emit (선택)

→ 결과에 따라 [[../7_3_공통 Custom Operator 제공 방안]] / [[../7_4_DAG + dbt + Operator 3축 배포 통합]] 의 패키지 규격 결정 최종화.

## 환경

| 항목 | 값 |
|---|---|
| Composer | `test-airflow3` (asia-northeast3) |
| Airflow | composer-3-airflow-3.1.7-build.9 |
| 환경 버킷 | `dev-airflow-test-bucket` |
| AR repo | `dev-dp-python-registry` (PoC 03 재사용) |
| 테스트 패키지 위치 | `~/PycharmProjects/composer-poc-provider-pkg` (가칭) |

## 1. 테스트 패키지 설계

PoC 03 의 `HelloKakaoOperator` + 추가로 `AthlonConnection` (가상) 같은 connection type 까지 포함해 **provider 규격의 4가지 핵심 요소 검증**.

### 1.1 디렉토리 구조

```
composer-poc-provider-pkg/
├── pyproject.toml                              # name="apache-airflow-providers-kakaoent"
├── README.md
├── airflow/
│   └── providers/
│       └── kakaoent/
│           ├── __init__.py                     # get_provider_info() 함수
│           ├── provider.yaml                   # ⭐ 메타데이터
│           ├── hooks/
│           │   ├── __init__.py
│           │   └── athlon.py                   # AthlonHook (가상 Connection 활용)
│           ├── operators/
│           │   ├── __init__.py
│           │   └── hello_kakao.py              # HelloKakaoOperator (PoC 03 재사용)
│           └── sensors/
│               ├── __init__.py
│               └── time_marker.py              # 임시 deferrable sensor 1개
└── dags/
    └── poc_provider_pkg.py                     # 검증용 DAG
```

### 1.2 `pyproject.toml` 핵심

```toml
[project]
name = "apache-airflow-providers-kakaoent"
version = "0.1.0"
dependencies = [
    "apache-airflow>=3.0.0",
]

[project.optional-dependencies]
hive = ["thrift>=0.16.0", "pyhive>=0.7.0"]
gcp = ["google-cloud-bigquery>=3.0.0"]

[project.entry-points."apache_airflow_provider"]
provider_info = "airflow.providers.kakaoent:get_provider_info"

[tool.setuptools.packages.find]
include = ["airflow.providers.kakaoent*"]
namespaces = true
```

### 1.3 `provider.yaml` 핵심

```yaml
package-name: apache-airflow-providers-kakaoent
name: Kakaoent
description: |
    Custom operators / hooks / sensors for kakaoent data platform.

versions:
  - 0.1.0

dependencies:
  - apache-airflow>=3.0.0

additional-extras:
  - name: hive
    dependencies:
      - thrift>=0.16.0
      - pyhive>=0.7.0
  - name: gcp
    dependencies:
      - google-cloud-bigquery>=3.0.0

integrations:
  - integration-name: Kakaoent Athlon
    external-doc-url: https://wiki.kakaoent/athlon
    tags: [service]

hooks:
  - integration-name: Kakaoent Athlon
    python-modules:
      - airflow.providers.kakaoent.hooks.athlon

operators:
  - integration-name: Kakaoent Athlon
    python-modules:
      - airflow.providers.kakaoent.operators.hello_kakao

sensors:
  - integration-name: Kakaoent Athlon
    python-modules:
      - airflow.providers.kakaoent.sensors.time_marker

connection-types:
  - hook-class-name: airflow.providers.kakaoent.hooks.athlon.AthlonHook
    connection-type: athlon
```

### 1.4 `airflow/providers/kakaoent/__init__.py`

```python
__version__ = "0.1.0"


def get_provider_info():
    return {
        "package-name": "apache-airflow-providers-kakaoent",
        "name": "Kakaoent",
        "description": "Custom operators for kakaoent data platform.",
        "versions": [__version__],
    }
```

## 2. 실행 단계

### Step 1: 패키지 빌드

```bash
cd ~/PycharmProjects/composer-poc-provider-pkg
python -m build --wheel
```

### Step 2: AR push (PoC 03 인프라 재사용)

```bash
twine upload \
  --repository-url https://asia-northeast3-python.pkg.dev/dev-dp-project-354904/dev-dp-python-registry/ \
  dist/apache_airflow_providers_kakaoent-0.1.0-py3-none-any.whl
```

### Step 3: Composer 환경에 install

```bash
cat > /tmp/requirements.txt <<EOF
keyrings.google-artifactregistry-auth
apache-airflow-providers-kakaoent==0.1.0
EOF

gcloud composer environments update test-airflow3 \
  --location=asia-northeast3 \
  --update-pypi-packages-from-file=/tmp/requirements.txt
```

→ 10~30분 대기 (PoC 03 와 동일).

### Step 4: 검증용 DAG 배포

```python
# dags/poc_provider_pkg.py
from airflow import DAG
from airflow.providers.kakaoent.operators.hello_kakao import HelloKakaoOperator
import pendulum

with DAG(
    dag_id="poc_provider_pkg",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["poc", "provider"],
) as dag:
    HelloKakaoOperator(task_id="hello")
```

```bash
gsutil cp dags/poc_provider_pkg.py gs://dev-airflow-test-bucket/dags/
```

## 3. 관찰 포인트

| # | 관찰할 것 | 어디서 | 기대 결과 |
|---|---|---|---|
| 1 | DAG import 동작 | `airflow dags list-import-errors` | 에러 없음 |
| 2 | **Admin → Providers 메뉴 자동 등록** | Airflow UI | `Kakaoent 0.1.0` 항목 등장 |
| 3 | Provider 상세 페이지 | UI Providers 클릭 | operators / hooks / sensors 목록 표시 |
| 4 | **Connection 생성 UI 의 Connection Type 드롭다운** | UI Admin → Connections → Add | `Athlon` 타입 자동 등장 |
| 5 | DAG 실행 + log | UI DAG trigger | `안녕 kakao` 류 로그 정상 |
| 6 | `extras` 옵션 의존성 동작 | requirements 에 `apache-airflow-providers-kakaoent[hive]==0.1.0` 변경 후 재설치 | hive deps 추가 설치 확인 |
| 7 | DAG 코드 import path 표준 | DAG | `from airflow.providers.kakaoent.operators...` 동작 |

### 3.1 결정적 시각 검증

다음 두 화면 스크린샷이 핵심 결과물:

- **UI Admin → Providers** 화면에 **"Kakaoent 0.1.0"** 행 등장
- **UI Admin → Connections → Add** 의 Connection Type 드롭다운에 **"Athlon"** 옵션 등장

→ 둘 다 보이면 provider 규격의 핵심 가치 (자동 발견) **실증 완료**.

## 4. 비교 (PoC 03 vs 본 PoC)

| 항목 | PoC 03 (`kakao-airflow-poc`) | PoC 10 (`apache-airflow-providers-kakaoent`) |
|---|---|---|
| 패키지 명 | `kakao-airflow-poc` | `apache-airflow-providers-kakaoent` |
| 디렉토리 | `kakao_airflow_poc/operators.py` | `airflow/providers/kakaoent/operators/...` |
| `provider.yaml` | ❌ | ✅ |
| `entry_points` | ❌ | ✅ |
| DAG import | `from kakao_airflow_poc.operators import ...` | `from airflow.providers.kakaoent.operators... import ...` |
| Admin → Providers UI | ❌ | ✅ 기대 |
| Connection Type 자동 등록 | ❌ | ✅ 기대 |
| AR push 메커니즘 | 동일 | 동일 |
| pip.conf 셋업 | 동일 (재사용) | 동일 (재사용) |

## 5. 실측 결과 (테스트 중)

> _(채울 자리 — 테스트 완료 후 추가)_

- [ ] DAG import 정상
- [ ] Admin → Providers 메뉴에 등록 확인 (스크린샷)
- [ ] Connection Type 드롭다운 등장 확인 (스크린샷)
- [ ] DAG 실행 정상 동작
- [ ] `extras` (`[hive]`) 옵션 동작 확인
- [ ] PoC 03 패키지와 공존 가능 여부 (양쪽 같이 install)
- [ ] 패키지 빌드 시 함정 / 디버깅 노트

## 6. 잔여 검증 (회의 이후 진행 가능)

- [ ] **OpenLineage hook 자동 emit** — provider 가 lineage 표준 인터페이스 따를 때 DataHub 통합 자동 동작 여부
- [ ] **Connection extra fields 커스텀 UI** — `AthlonHook.get_connection_form_widgets()` 정의 시 Connection 생성 UI 에 커스텀 필드 노출되는지
- [ ] **Multi-version 공존** — `0.1.0` 과 `0.2.0` 을 동시에 install 시 충돌 여부
- [ ] **추가 메뉴 항목** — provider 가 Airflow UI 에 커스텀 메뉴 추가하는 메커니즘

## 7. 의사결정 영향

본 PoC 결과에 따라:

| 결과 | 영향 |
|---|---|
| ✅ 자동 등록 + Connection type 동작 | [[../7_3_공통 Custom Operator 제공 방안]] / [[../7_4_DAG + dbt + Operator 3축 배포 통합]] 에 **provider 규격으로 확정** |
| ⚠️ 일부만 동작 | 동작하는 가치만 가져오고 일반 패키지로 갈 가능성 검토 |
| ❌ 의외의 차단 | PoC 03 의 일반 패키지 방식 유지, 별도 등록 작업 추가 |

## 관련 문서

- [[03_custom_operator_pypi]] — 일반 패키지로 install 검증 (선행 PoC, 인프라 재사용)
- [[../7_3_공통 Custom Operator 제공 방안]] — 본 PoC 결과 반영 대상
- [[../7_4_DAG + dbt + Operator 3축 배포 통합]] — Layer 1 결정 영향
- [[../0_결론]] — 사내 wheel 운영 방향

## 참고

- [Apache Airflow Provider Packages 공식 문서](https://airflow.apache.org/docs/apache-airflow-providers/index.html)
- [Provider packages 작성 가이드](https://airflow.apache.org/docs/apache-airflow-providers/howto/create-custom-providers.html)
- [provider.yaml schema](https://airflow.apache.org/docs/apache-airflow-providers/airflow-provider-yaml-schema.html)
