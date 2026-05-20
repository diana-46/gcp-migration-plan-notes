---
title: "03. 자체 wrapper Operator → Composer install 검증"
status: done
tags:
  - poc
  - custom-operator
  - pypi
  - artifact-registry
  - composer3
created: 2026-05-19
updated: 2026-05-20
---

# 03. 자체 wrapper Operator → Composer install 검증

> **검증 질문**: 사내 custom operator(wheel) 를 Composer 3 에 install 가능한가?
> **결론**: ✅ **가능. 단 Artifact Registry + pip.conf 조합 필수.** GCS 에 wheel 두고 `file://` 참조하는 self-hosted 식 trick 은 API 가 차단. 운영 시 함정 3개 존재.

## 환경

| 항목 | 값 |
|---|---|
| Composer | `test-airflow3` (asia-northeast3) |
| Airflow | composer-3-airflow-3.1.7-build.9 |
| 환경 버킷 | `dev-airflow-test-bucket` |
| Composer SA | `dev-dp-airflow@dev-dp-project-354904.iam.gserviceaccount.com` |
| 테스트 패키지 | `~/PycharmProjects/composer-poc-pkg` (`kakao-airflow-poc==0.1.0`) |
| AR repo | `dev-dp-python-registry` (Python 포맷, asia-northeast3) |

## 최종 성공 흐름

```
[로컬]
  python -m build --wheel
    ↓ kakao_airflow_poc-0.1.0-py3-none-any.whl

[Artifact Registry]
  twine upload → dev-dp-python-registry

[Composer 환경 설정]
  ① gs://<bucket>/config/pip/pip.conf 업로드 (extra-index-url 지정)
  ② PyPI 패키지: keyrings.google-artifactregistry-auth + kakao-airflow-poc==0.1.0
  ③ Composer SA 에 roles/artifactregistry.reader

[검증]
  DAG poc_custom_pkg trigger → HelloKakaoOperator 실행 → "안녕 kakao from Composer (kakao-airflow-poc 0.1.0)" 로그 ✅
```

## ⚠️ 함정 3개 (PoC 핵심 발견)

### 함정 1. wheel `file://` 참조 — Composer API 거부

가설: Composer 가 환경 버킷을 컨테이너의 `/home/airflow/gcs/` 로 마운트 → `file:///home/airflow/gcs/plugins/foo.whl` 로 install 가능할 거라 예상.

**실제**: Composer 의 PyPI 패키지 API 는 PEP 508 의 `identifier + versionspec` 만 받음. `@ <url>` URL specifier 진입 자체 차단.

**증거**:

콘솔 UI 에러:
> PyPI 패키지의 extras 및 버전에는 extras(선택사항)와 versionspec(선택사항)을 차례로 입력해야 합니다. 예를 들어 '>=1.10.3'과 같이 입력합니다.

`gcloud` CLI 에러:
```
ERROR: (gcloud.composer.environments.update) INVALID_ARGUMENT: Found 1 problem:
    1) Error validating key kakao-airflow-poc @ file:///home/airflow/gcs/plugins/...
       PyPI dependency name is not formatted properly.
       It must follow the format of 'identifier' specified in PEP-508.
```

→ self-hosted Airflow 에서 익숙한 "GCS/공유 디스크에 wheel 두고 `file://` install" 패턴은 **Composer 에선 불가**.

→ **Artifact Registry 강제**.

### 함정 2. `PIP_EXTRA_INDEX_URL` 환경 변수는 빌드 컨텍스트에 전달 안 됨

가설: Composer 환경 변수 탭에서 `PIP_EXTRA_INDEX_URL=https://...pkg.dev/.../simple/` 설정하면 pip install 시 AR 인덱스 자동 추가.

**실제**: 환경 변수는 **런타임 컨테이너** (scheduler/worker/DAG processor) 에만 주입됨. 환경 update 시 pip install 빌드 단계는 별도 컨테이너에서 돌아 env var 못 받음.

**증거** — 빌드 로그:
```
Installing Python3 Requirements
+ COMPOSER_PYTHON_VERSION=3
+ python3 -m pip install --retries 2 -r requirements.txt --no-cache-dir
No custom pip.conf file found.
...
ERROR: Could not find a version that satisfies the requirement kakao-airflow-poc==0.1.0 (from versions: none)
ERROR: No matching distribution found for kakao-airflow-poc==0.1.0
The command '/bin/sh -c bash installer.sh $COMPOSER_PYTHON_VERSION $FAIL_ON_CONFLICT' returned a non-zero code: 1
```

→ `(from versions: none)` 이 함정의 시그니처. 실제론 인증 실패이지만 401 이 아니라 "패키지 없음" 으로 위장돼 디버깅 헷갈리게 함.

→ `installer.sh` 의 `No custom pip.conf file found.` 메시지가 결정적 힌트. 빌드 단계 pip 가 **GCS 의 `config/pip/pip.conf` 파일을 찾는다**는 의미.

**해결** — pip.conf 를 환경 버킷에 업로드:

```ini
# pip.conf
[global]
extra-index-url = https://asia-northeast3-python.pkg.dev/dev-dp-project-354904/dev-dp-python-registry/simple/
```

```bash
gsutil cp pip.conf gs://dev-airflow-test-bucket/config/pip/pip.conf
```

> ⚠️ 콘솔 UI 에 pip.conf 설정 진입점 **없음**. GCS 직접 업로드만 가능.

### 함정 3. `keyrings.google-artifactregistry-auth` 사전 설치 안 됨

가설: Composer 3 base image 가 AR 인증용 keyring 어댑터를 사전 설치해뒀을 것.

**실제**: `composer-3-airflow-3.1.7-build.9` 에 **포함되지 않음**. requirements 에 명시 추가 필요.

→ requirements 에 같이 명시 시 정상 동작 (1-pass 설치). 닭/달걀 우려와 달리 `installer.sh` 가 잘 처리.

| 패키지 | 출처 | 역할 |
|---|---|---|
| `keyrings.google-artifactregistry-auth` | 공개 PyPI | AR 인증 어댑터 |
| `kakao-airflow-poc==0.1.0` | AR | 실제 사내 코드 |

## 셋업 절차 (운영 가이드)

> 신규 Composer 환경에서 AR 사내 wheel 사용 시 한 번만 셋업.

### 1. AR Python repo 생성 (이미 있으면 skip)

```bash
PROJECT=dev-dp-project-354904
LOC=asia-northeast3
REPO=dev-dp-python-registry

gcloud artifacts repositories create $REPO \
  --repository-format=python \
  --location=$LOC \
  --project=$PROJECT
```

### 2. Composer SA 에 reader 권한

```bash
gcloud artifacts repositories add-iam-policy-binding $REPO \
  --location=$LOC --project=$PROJECT \
  --member=serviceAccount:dev-dp-airflow@$PROJECT.iam.gserviceaccount.com \
  --role=roles/artifactregistry.reader
```

### 3. pip.conf 업로드 ⭐ 함정 회피의 핵심

```bash
cat > /tmp/pip.conf <<EOF
[global]
extra-index-url = https://$LOC-python.pkg.dev/$PROJECT/$REPO/simple/
EOF

gsutil cp /tmp/pip.conf gs://dev-airflow-test-bucket/config/pip/pip.conf
```

### 4. wheel push

```bash
# 로컬 venv
python3 -m venv ~/.venv/twine-poc
source ~/.venv/twine-poc/bin/activate
pip install --quiet twine keyrings.google-artifactregistry-auth

# build
cd ~/PycharmProjects/composer-poc-pkg
python -m build --wheel

# push (gcloud auth 활용 자동)
twine upload \
  --repository-url https://$LOC-python.pkg.dev/$PROJECT/$REPO/ \
  dist/kakao_airflow_poc-0.1.0-py3-none-any.whl
```

### 5. Composer PyPI 패키지 등록

콘솔 UI 또는 gcloud:

```bash
cat > /tmp/requirements.txt <<EOF
keyrings.google-artifactregistry-auth
kakao-airflow-poc==0.1.0
EOF

gcloud composer environments update test-airflow3 --location=$LOC \
  --update-pypi-packages-from-file=/tmp/requirements.txt
```

→ 10~30분 대기.

### 6. DAG 업로드 + 검증

```bash
gsutil cp ~/PycharmProjects/composer-poc-pkg/dags/poc_custom_pkg.py \
  gs://dev-airflow-test-bucket/dags/

# import error 확인
gcloud composer environments run test-airflow3 --location=$LOC \
  dags list-import-errors
```

Airflow UI 에서 `poc_custom_pkg` DAG trigger → task log:
```
안녕 kakao from Composer (kakao-airflow-poc 0.1.0)
```

## 비교 — 가능한 install 방법

| 방법 | 동작 | 비고 |
|---|---|---|
| 공개 PyPI `--update-pypi-package=foo==1.0` | ✅ | 공개 패키지 한정 |
| `gs://<env-bucket>/plugins/foo.whl` + `file://` | ❌ | API 가 `@ <url>` 거부 |
| `gs://` URI 직접 | ❌ | pip 스킴 아님 |
| HTTPS public/signed URL | △ | URL 만료 / 보안 노출 |
| **Artifact Registry + pip.conf** | ✅ | **사내 wheel 의 정공법** |
| Custom container image | ✅ | 시스템 의존성(`libsasl2-dev` 등) 필요할 때만 |

## 사내 셋업 → Composer 매핑

> 참조: `~/WebstormProjects/data-platform-settings/playbooks/roles/airflow2`

| 기존 (ansible/sendbag) | Composer (AR) |
|---|---|
| `requirements-<ver>.txt` (provider pin) | requirements 그대로 + AR 패키지 추가 |
| `additional_packages` (사내 sendbag wheel) | **AR repo 에 push + PyPI 패키지 등록** |
| `get_url` → `/tmp/<file>.whl` → `pip install --no-deps --force-reinstall` | `twine upload` → 정상 의존성 resolve |
| 인증 없음 (사내망) | SA `roles/artifactregistry.reader` (자동) |
| `--proxy http://proxy.onkakao.net:3128` | 불필요 (GCP 내부망) |
| (없음) | `pip.conf` 업로드 추가 작업 |
| (없음) | `keyrings.google-artifactregistry-auth` 명시 |

## 운영 시 추가 고려

- **CI/CD**: 사내 wheel 14개 (기존 `additional_packages` 인벤토리) → 빌드 파이프라인에서 `twine upload` 자동화 가능 (Jenkins/GH Actions)
- **버전 관리**: AR 이 자동 버전 메타데이터 관리. 기존 sendbag URL 1회용 방식 대비 큰 개선
- **환경 분리**: dev/stg/prod 별 별도 AR repo 또는 같은 repo 공유 — 정책 결정 필요
- **`pip.conf` 환경별 분리**: 환경마다 별도 버킷이라 자연스럽게 분리됨
- **업데이트 시간**: PyPI 패키지 변경마다 10~30분 환경 update. CI 에서 자주 호출 금지 — 큰 변경 모아서 배포

## 회의에서 답할 메시지

> **사내 custom operator 를 Composer 에 install 가능한가?**
> ✅ 가능. 단 셋업 경로는 **Artifact Registry + pip.conf** 강제 (self-hosted 식 GCS `file://` trick 차단).
>
> **이행 작업 인벤토리**:
> 1. AR Python repo 1개 생성 (한 번)
> 2. `pip.conf` GCS 업로드 — 콘솔 UI 진입점 없음, 직접 업로드 필요 ⚠️
> 3. Composer SA 에 `artifactregistry.reader` 부여 (한 번)
> 4. 기존 `additional_packages` 14개 → wheel 빌드 파이프라인 + AR push 로 옮김
> 5. PyPI 패키지 목록에 `keyrings.google-artifactregistry-auth` + 사내 패키지 등록
>
> **함정**:
> - 환경 변수 `PIP_EXTRA_INDEX_URL` 로 풀릴 거 같지만 빌드 컨텍스트엔 전달 안 됨 → 시간 낭비
> - 실패 메시지가 "패키지 없음" 으로 위장돼 인증 문제 추적 어려움
> - 콘솔 UI 가 `@ file://` / `@ git+https://` 등 PEP 508 URL specifier 거부 → CLI/Terraform 도 동일
>
> Self-hosted 였다면 ansible 그대로 유지 가능 — Composer 채택 시에만 발생하는 작업이지만, **AR 운영 자체는 사내 sendbag 보다 깔끔** (인증/버전관리/공유).

## 관련 노트

- [[README]] — 본 PoC 의 전체 흐름 (Step 3)
- [[../2_Cloud Composer vs Self-managed 비교]] — 호환성 표 B-4 "사내 PyPI / wheel 직접 설치"
- [[02_dag_deployment]] — DAG 배포 측면. 본 노트는 코드 install 쪽
- [[../11_DAG Bundles와 배포 전략]] — DAG 배포 전략 일반

## 부록 — 테스트 패키지 구조

`~/PycharmProjects/composer-poc-pkg/`

```
composer-poc-pkg/
├── pyproject.toml                                       # name="kakao-airflow-poc", version="0.1.0"
├── README.md
├── kakao_airflow_poc/
│   ├── __init__.py                                      # __version__ = "0.1.0"
│   └── operators.py                                     # HelloKakaoOperator (BaseOperator)
├── dags/
│   └── poc_custom_pkg.py                                # 검증용 DAG
└── dist/
    └── kakao_airflow_poc-0.1.0-py3-none-any.whl         # 빌드 산출물 (1.8KB)
```

## 부록 — 시도/실패 타임라인

| 시각 | 시도 | 결과 | 학습 |
|---|---|---|---|
| (D-1) | wheel build, GCS plugins/ 업로드 | wheel 파일은 GCS 에 존재 | — |
| (D-1) | 콘솔 UI 에서 `@ file:///home/airflow/gcs/plugins/...` 입력 | UI 거부 | 함정 1: UI 가 PEP 508 versionspec 만 받음 |
| D 13:34 | `gcloud --update-pypi-packages-from-file` with `@ file://` | INVALID_ARGUMENT | 함정 1: API 도 동일하게 URL specifier 거부 |
| D 13:39 | 환경 변수 `PIP_EXTRA_INDEX_URL` 설정 + `kakao-airflow-poc==0.1.0` | "(from versions: none)" 빌드 실패 | 함정 2: env var 가 빌드 컨텍스트에 안 전달 |
| D (잠시 뒤) | `keyrings.google-artifactregistry-auth` 추가 | 동일 실패 | env var 문제가 먼저 — keyring 무관 |
| D | 빌드 로그 정독 → `No custom pip.conf file found.` 발견 | 진짜 원인 잡힘 | 함정 2 가설 확정 |
| D | `gs://<bucket>/config/pip/pip.conf` 업로드 | 환경 update 성공 | 함정 회피 완료 |
| D 14:47 | DAG `poc_custom_pkg` trigger | ✅ Success, log: `안녕 kakao from Composer (kakao-airflow-poc 0.1.0)` | end-to-end 검증 완료 |
