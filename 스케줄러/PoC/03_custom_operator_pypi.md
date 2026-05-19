---
title: "03. 자체 wrapper Operator → Composer install 검증"
status: in-progress
tags:
  - poc
  - custom-operator
  - pypi
  - artifact-registry
  - composer3
created: 2026-05-19
updated: 2026-05-19
---

# 03. 자체 wrapper Operator → Composer install 검증

> **검증 질문**: 사내 custom operator(wheel) 를 Composer 3 에 어떻게 install 하나?
> **답**: **wheel 을 GCS 에 두고 `file://` 로 참조하는 방법은 불가**. → **Artifact Registry 강제**.

## 환경

| 항목 | 값 |
|---|---|
| Composer | `test-airflow3` (asia-northeast3) |
| Airflow | composer-3-airflow-3.1.7-build.9 |
| DAG bucket | `gs://dev-airflow-test-bucket/dags` |
| Composer 환경 버킷 | `dev-airflow-test-bucket` (DAG/plugins/data/logs 자동 마운트) |
| SA | `dev-dp-airflow@dev-dp-project-354904.iam.gserviceaccount.com` |
| 테스트 패키지 | `~/PycharmProjects/composer-poc-pkg` (`kakao-airflow-poc==0.1.0`) |

## 시도한 접근

### A. wheel 을 환경 버킷 `plugins/` 에 두고 `file://` 참조 — ❌ 불가

가설: Composer 가 자기 버킷을 `/home/airflow/gcs/` 로 마운트하므로 컨테이너 안에서는 로컬 파일. PEP 508 의 `name @ file:///...` 문법으로 install 가능할 거라고 예상.

**1. wheel 빌드 + 업로드 성공**

```bash
# 로컬: ~/PycharmProjects/composer-poc-pkg/
python -m build --wheel
# → dist/kakao_airflow_poc-0.1.0-py3-none-any.whl (1.8KB)

# GCS 업로드 (콘솔 드래그&드롭)
# → gs://dev-airflow-test-bucket/plugins/kakao_airflow_poc-0.1.0-py3-none-any.whl
```

**2. 콘솔 UI ("PyPI 패키지" 탭) → 거부**

```
패키지 이름:        kakao-airflow-poc
Extras 및 버전:     @ file:///home/airflow/gcs/plugins/kakao_airflow_poc-0.1.0-py3-none-any.whl
```

UI 에러:
> *PyPI 패키지의 extras 및 버전에는 extras(선택사항)와 versionspec(선택사항)을 차례로 입력해야 합니다. 예를 들어 '>=1.10.3'과 같이 입력합니다.*

→ "Extras 및 버전" 필드는 **PEP 508 versionspec 만** 받음. `@ <url>` URL specifier 거부.

**3. `gcloud` CLI 도 동일하게 거부**

```bash
cat > /tmp/requirements.txt <<EOF
kakao-airflow-poc @ file:///home/airflow/gcs/plugins/kakao_airflow_poc-0.1.0-py3-none-any.whl
EOF

gcloud composer environments update test-airflow3 \
  --location=asia-northeast3 \
  --update-pypi-packages-from-file=/tmp/requirements.txt
```

```
ERROR: (gcloud.composer.environments.update) INVALID_ARGUMENT: Found 1 problem:
    1) Error validating key kakao-airflow-poc @ file:///home/airflow/gcs/plugins/kakao_airflow_poc-0.1.0-py3-none-any.whl.
       PyPI dependency name is not formatted properly.
       It must follow the format of 'identifier' specified in PEP-508.
```

→ Composer 의 PyPI 패키지 API 가 **`{identifier: versionspec}` 맵만 허용**. URL specifier 진입 자체 차단.

### 결론 — Composer 에서 사내 wheel 직접 install 방법

| 방법 | 동작 여부 | 비고 |
|---|---|---|
| 공개 PyPI `--update-pypi-package=foo==1.0` | ✅ | 공개 패키지 한정 |
| `gs://<env-bucket>/plugins/foo.whl` + `file:///home/airflow/gcs/...` | **❌** | API 가 `@ <url>` 거부 |
| `gs://...` URI 직접 | ❌ | pip 스킴 아님 |
| **Artifact Registry private repo** | ✅ | **사내 wheel 의 정공법** |
| Custom container image (Composer base + Dockerfile) | ✅ | 시스템 의존성 필요할 때 |

→ self-hosted Airflow 에서 익숙한 "GCS/공유 디스크에 wheel 두고 `file://` 로 install" 패턴은 **Composer 에선 불가**. 운영적으로도 강제로 정식 경로(=AR)로 몰아넣음.

## 다음 단계 — Artifact Registry 검증 (B 단계)

기존 ansible 셋업의 `additional_packages` (사내 sendbag wheel) 패턴 → AR 로 1:1 매핑 검증.

### 준비

```bash
PROJECT=dev-dp-project-354904
LOC=asia-northeast3
REPO=airflow-poc-pypi
ENV=test-airflow3

# 1. Python repo 생성
gcloud artifacts repositories create $REPO \
  --repository-format=python \
  --location=$LOC \
  --project=$PROJECT

# 2. Composer SA 에 read 권한
gcloud artifacts repositories add-iam-policy-binding $REPO \
  --location=$LOC --project=$PROJECT \
  --member=serviceAccount:dev-dp-airflow@dev-dp-project-354904.iam.gserviceaccount.com \
  --role=roles/artifactregistry.reader
```

### wheel push

```bash
pip install --user twine keyrings.google-artifactregistry-auth
python -m twine upload \
  --repository-url https://$LOC-python.pkg.dev/$PROJECT/$REPO/ \
  dist/kakao_airflow_poc-0.1.0-py3-none-any.whl
```

### Composer 등록

```bash
cat > /tmp/requirements.txt <<'EOF'
keyrings.google-artifactregistry-auth==1.1.2
kakao-airflow-poc==0.1.0
EOF

gcloud composer environments update $ENV --location=$LOC \
  --update-env-variables=PIP_EXTRA_INDEX_URL=https://$LOC-python.pkg.dev/$PROJECT/$REPO/simple/ \
  --update-pypi-packages-from-file=/tmp/requirements.txt
```

### 검증

```bash
gcloud composer environments run $ENV --location=$LOC \
  python -- -c "import kakao_airflow_poc; print(kakao_airflow_poc.__version__)"
# → 0.1.0 출력되면 성공
```

DAG `poc_custom_pkg` trigger → log 에 `안녕 kakao from Composer (kakao-airflow-poc 0.1.0)` 확인.

## 사내 셋업 → Composer 매핑

> 참조: `~/WebstormProjects/data-platform-settings/playbooks/roles/airflow2`

| 기존 (ansible/sendbag) | Composer (AR) |
|---|---|
| `requirements-<ver>.txt` (provider pin) | `--update-pypi-packages-from-file` |
| `additional_packages` (sendbag wheel) | **AR repo 에 push + `--update-pypi-package`** |
| `get_url` → `/tmp/<file>.whl` → `pip install --no-deps --force-reinstall` | `twine upload` → `pip install` (정상 의존성 resolve) |
| 인증 없음 (사내망) | SA `roles/artifactregistry.reader` (자동) |
| `PRESTO_HOME` 등 sysconfig env | `--update-env-variables` |
| `--proxy http://proxy.onkakao.net:3128` | 불필요 (GCP 내부망) |

## 시간 측정 (회의 ammunition)

> 회의에서 "사내 패키지 1개 배포에 N분" 답을 위한 baseline.

| 단계 | 측정값 |
|---|---|
| wheel 빌드 (`python -m build`) | <1초 |
| AR repo 생성 | _(측정 예정)_ |
| `twine upload` | _(측정 예정)_ |
| Composer `--update-pypi-package` 반영까지 | _(측정 예정 — 일반적으로 10~30분)_ |
| 0.1.0 → 0.2.0 재배포 cycle | _(측정 예정)_ |
| DAG UI parsing 반영 | _(측정 예정)_ |

## 회의에서 답할 메시지

> **사내 custom operator 를 Composer 에 install 가능한가?**
> ✅ 가능. 단 경로는 **Artifact Registry 강제** (GCS 에 wheel 두고 직접 참조하는 self-hosted 식 trick 은 API 가 차단).
>
> **이행 작업?**
> 1. AR Python repo 1개 생성
> 2. 기존 `additional_packages` 인벤토리 (사내 sendbag 14개 등) → wheel 빌드 파이프라인 + AR push 로 옮김
> 3. Composer 환경에 `PIP_EXTRA_INDEX_URL` + `keyrings.google-artifactregistry-auth` + 패키지 목록 등록
>
> Self-hosted 였다면 ansible 그대로 유지 가능 — Composer 채택 시에만 발생하는 작업.

## 관련 노트

- [[README]] — 본 PoC 의 전체 흐름 (Step 3)
- [[../2_Cloud Composer vs Self-managed 비교]] — 호환성 표 B-4 "사내 PyPI / wheel 직접 설치"
- [[02_dag_deployment]] — DAG 배포는 GCS sync 가능. 본 노트는 코드 install 쪽
- [[../11_DAG Bundles와 배포 전략]] — DAG 배포 측면

## 부록 — 테스트 패키지 구조

`~/PycharmProjects/composer-poc-pkg/`

```
composer-poc-pkg/
├── pyproject.toml                                       # name="kakao-airflow-poc", version="0.1.0"
├── README.md
├── kakao_airflow_poc/
│   ├── __init__.py                                      # __version__ = "0.1.0"
│   └── operators.py                                     # HelloKakaoOperator
├── dags/
│   └── poc_custom_pkg.py                                # 검증용 DAG
└── dist/
    └── kakao_airflow_poc-0.1.0-py3-none-any.whl         # 빌드 산출물 (1.8KB)
```
