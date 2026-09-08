---
title: "DAG Bundles와 배포 전략"
status: draft
tags:
  - airflow
  - 스케줄러
  - dag-bundles
  - deployment
created: 2026-05-15
updated: 2026-05-15
---

# DAG Bundles와 배포 전략

> Airflow 3에서 도입된 DAG Bundles 개념과, 여러 팀/여러 repo의 DAG을 Cloud Composer에서 어떻게 관리·배포할지 정리.
>
> 기준: **Airflow 3 / Composer 3**.

## 1. DAG Bundles 개념 복습

기존 Airflow 2의 "DAG 폴더 단 하나" 모델을 대체하는 방식.

| 항목 | Airflow 2 | Airflow 3 (DAG Bundles) |
|---|---|---|
| DAG 소스 | 단일 `dags_folder` | 여러 bundle 등록 가능 |
| Multi Git repo | sidecar 트릭으로 우회 | bundle로 native 지원 |
| 버전 고정 | ❌ run 중에 코드 바뀔 수 있음 | ✅ run이 bundle version에 lock |
| Refresh 주기 | 폴더 단위 일괄 | bundle별 따로 설정 |
| 출처 추적 | 약함 | UI에 bundle/version 표시 |

### Bundle 종류

- `LocalDagBundle` — 로컬 디렉터리
- `GitDagBundle` — Git 저장소 직접 clone/pull
- (외부) S3 / GCS / Azure Blob — provider package
- Composer는 내부적으로 **GCS bundle을 자동 등록** (`gs://composer-bucket/dags/`)

### 설정 예시

```ini
[dag_processor]
dag_bundle_config_list = [
    {
        "name": "ml_team",
        "classpath": "airflow.dag_processing.bundles.git.GitDagBundle",
        "kwargs": {
            "repo_url": "git@github.com:org/ml-dags.git",
            "tracking_ref": "main",
            "refresh_interval": 60
        }
    },
    {
        "name": "analytics",
        "classpath": "airflow.dag_processing.bundles.git.GitDagBundle",
        "kwargs": {
            "repo_url": "git@github.com:org/analytics-dags.git",
            "tracking_ref": "production",
            "refresh_interval": 300
        }
    }
]
```

## 2. 현재 방식 (self-managed Airflow + Jenkins)

### 구조

```
~/airflow/dags/             ← Airflow가 보는 dags_folder (host 파일시스템)
├── neptune_dags/
│   ├── .git/                  ← 각 디렉터리가 별도 git repo (nested git)
│   └── dag1.py
├── athlon-dags/
│   ├── .git/
│   └── dag2.py
└── api-dags/
    ├── .git/
    └── dag3.py
```

각 하위 디렉터리가 **별도 git repo**. 자체 발명한 nested-git 패턴.

### 배포 흐름

```
작업자: PR merge → Jenkins UI에서 해당 repo job 실행
   ↓
Jenkins:
   ssh airflow-server
   cd /opt/airflow/dags/<repo_name>
   git checkout master
   git pull
   ↓
Airflow scheduler가 다음 DAG parse cycle에 변경 인식
```

### 평가

- **현재 환경에선 동작 잘 됨**. host 파일시스템에 직접 git이 살아있으니까 `git pull` 가능
- 작업자 workflow가 단순 (job 선택 → 실행)
- 단점:
  - commit hash 추적이 Jenkins build log에만 있음 (Airflow UI엔 없음)
  - run 중에 코드 바뀔 수 있음 (version lock 없음)
  - 4번째 repo 추가 시 Jenkins job + airflow-server SSH 권한 셋업 반복
  - Airflow 서버가 단일 장애 지점

## 3. Composer로 이관 시 강제로 바뀌는 이유

**현재의 nested-git + Jenkins SSH pull 흐름은 Composer로 그대로 못 옮김.**

이유:
- Composer의 DAG 폴더 = **GCS 버킷** (`gs://composer-bucket/dags/`)
- GCS 객체 스토리지엔 `.git` 디렉터리 두고 `git pull` 못 함
- Composer 환경 SSH 접근도 일반적이지 않음 (worker pod에 들어가는 건 가능하지만 권장 안 됨)

→ Composer 이관 = **배포 메커니즘 강제 변경 시점**. 어차피 둘 중 하나 골라야 함:

### 옵션 (1) Jenkins 유지 + GCS sync로 역할 변경

```
Jenkins worker:
  git clone git@github.com:org/neptune-dags.git /tmp/neptune
  cd /tmp/neptune && git checkout master && git pull
  gsutil -m rsync -r -d /tmp/neptune gs://composer-bucket/dags/neptune_dags/
```

- 현재 Jenkins job을 거의 그대로 살림. SSH 대상이 airflow-server → Jenkins worker 자기 자신으로 바뀜
- 권한: Jenkins worker가 GCS write 가능한 GSA 사용
- DAG 코드 → GCS 업로드 → Composer GCS bundle이 자동 인식
- **운영 부담은 현재와 거의 동일. 작업자 workflow도 동일** (Jenkins UI 그대로)
- 마찰 가장 적음 → **이관 직후 추천**

### 옵션 (2) GitDagBundle로 대체

```python
# Composer airflow.cfg override
[dag_processor]
dag_bundle_config_list = [
    {
        "name": "neptune",
        "classpath": "airflow.dag_processing.bundles.git.GitDagBundle",
        "kwargs": {
            "repo_url": "git@github.com:org/neptune-dags.git",
            "tracking_ref": "master",
            "refresh_interval": 60
        }
    },
    {
        "name": "athlon",
        "classpath": "airflow.dag_processing.bundles.git.GitDagBundle",
        "kwargs": {"repo_url": "...athlon-dags.git", "tracking_ref": "master", "refresh_interval": 60}
    },
    {
        "name": "api",
        "classpath": "airflow.dag_processing.bundles.git.GitDagBundle",
        "kwargs": {"repo_url": "...api-dags.git", "tracking_ref": "master", "refresh_interval": 60}
    }
]
```

```
작업자: PR merge → 끝
Composer가 60초마다 git pull → DAG 자동 반영
Airflow UI에 bundle 이름 + commit hash 표시
```

- **Jenkins 배포 job 자체가 사라짐**. CI는 PR validation (lint / DAG import test) 만
- 환경 분기: `tracking_ref`를 dev/stg/prod 별로 다르게
- 롤백: `tracking_ref`를 이전 tag로 변경 → Composer env update
- 단점: 인증/네트워크 셋업 필요 (아래)

### 두 옵션 비교

| 항목               | 현재 (self-managed + Jenkins) | (1) Jenkins + GCS   | (2) GitDagBundle                                    |
| ---------------- | --------------------------- | ------------------- | --------------------------------------------------- |
| 배포 트리거           | Jenkins 수동                  | Jenkins (변경 적음)     | PR merge → 자동 pull                                  |
| Jenkins job      | 살아있음                        | 살아있음                | 없어도 됨 (검증만)                                         |
| 작업자 workflow     | repo 선택 → Jenkins 실행        | 동일                  | PR만 merge하면 끝                                       |
| commit hash 추적   | ❌ (Jenkins log만)            | ❌ (Jenkins log만)    | ✅ Airflow UI 자동 표시                                  |
| run version lock | ❌                           | ❌                   | ✅ 자동                                                |
| 환경 분기 (dev/prod) | Jenkins job 분기              | Jenkins job 분기      | `tracking_ref` 분기                                   |
| 롤백               | Jenkins로 이전 commit pull     | 동일                  | `tracking_ref` 변경 / tag revert                      |
| Git 인증 셋업        | 기존 그대로                      | 기존 그대로 (Jenkins)    | **Composer에서 새로** (deploy key/PAT → Secret Manager) |
| 네트워크 셋업          | 기존 그대로                      | Jenkins → GCS 통하면 됨 | **Composer → Git 통해야 함** (Private IP면 NAT/PSC 필요)   |
| 초기 마이그 비용        | —                           | **낮음**              | **중간**                                              |
| 장기 운영 비용         | 중간                          | 중간                  | **낮음**                                              |
| 신규 repo 추가 시     | 디렉터리/Jenkins job 셋업         | Jenkins job 셋업      | bundle config 한 줄 추가                                |

### 옵션 (2)의 현실적 이슈

| 이슈                              | 대응                                                                                      |
| ------------------------------- | --------------------------------------------------------------------------------------- |
| Git 인증                          | deploy key 또는 PAT를 Secret Manager에 저장 → env var로 노출 → bundle config 참조                  |
| Private IP Composer면 외부 Git 못 감 | Cloud NAT 또는 사내 Git이면 VPC peering / PSC                                                 |
| Pull 실패 시                       | 직전 buffer 유지. 모니터링 + 알람 필수                                                              |
| 큰 repo 초기 clone 느림              | 첫 셋업 시 몇 분 소요. 이후 incremental fetch                                                     |
| requirements.txt 변경             | 여전히 Composer env update (수 분~십수 분). 의존성 변경 잦으면 Pod 이미지로 빼기 ([[4_Queue 라우팅과 Pod 스펙 설정]]) |

## 4. 배포 시점 컨트롤 — Pull off + Push only 패턴

> "자동 sync는 끄고, 내가 원하는 때만 배포한다." 다이앤 환경 채택 패턴.

### 설정

`refresh_interval`을 매우 크게 잡아 자동 polling을 사실상 비활성화:

```python
{
    "name": "neptune",
    "classpath": "airflow.dag_processing.bundles.git.GitDagBundle",
    "kwargs": {
        "repo_url": "git@github.com:org/neptune-dags.git",
        "tracking_ref": "master",
        "refresh_interval": 86400      # 1일 = 사실상 자동 sync off
    }
}
```

→ 배포는 **모두 수동 트리거**.

### 트리거 방법

| 방법                                 | 명령 / 위치                                                                          | 적합                            |
| ---------------------------------- | -------------------------------------------------------------------------------- | ----------------------------- |
| Jenkins job                        | `gcloud composer environments run ENV --location=LOC bundles refresh -- neptune` | 기존 Jenkins UI 유지. 가장 자연스러운 이행 |
| Airflow UI                         | Admin → DAG Bundles → Refresh 버튼                                                 | 1인 운영 / 즉석 deploy             |
| GitHub Actions `workflow_dispatch` | 작업자가 GitHub UI에서 수동 실행                                                           | tag 기반 release                |

### Jenkins 흐름

```groovy
// Jenkinsfile
stage('Validate') {
    sh 'pytest tests/dag_validation/'
}
stage('Confirm') {
    input message: 'prod-composer에 neptune-dags 배포할까요?'
}
stage('Deploy') {
    sh '''
    gcloud composer environments run prod-composer \
      --location=asia-northeast3 \
      bundles refresh -- neptune
    '''
}
```

→ 작업자 입장에선 기존 Jenkins UI 그대로. 내부 동작만 "SSH+git pull" → "API call"로 변경.

### 장단점

| 장점                                 | 단점                              |
| ---------------------------------- | ------------------------------- |
| 배포 시점 완전 통제                        | 트리거 깜빡하면 안 올라감                  |
| 점검 / 피크 시간대 deploy 회피              | "자동 안전망" 없음                     |
| 감사 / change mgmt 친화                | 신규 인원 onboarding 비용             |
| 의도치 않은 deploy 차단 (main push 실수 보호) | emergency deploy 경로 잘 알려져 있어야 함 |

### 안전장치

1. **stale 알람** — bundle의 last_refresh가 24h 이상이면 Cloud Monitoring 알람
2. **drift 알람** — main HEAD commit vs prod bundle commit hash 차이가 N개 이상이면 알람
3. **하이브리드 fallback** — `refresh_interval`을 86400 정도로 두면 "트리거 깜빡해도 결국 다음 날엔 반영"
4. **pre-deploy validation** — refresh API 호출 전에 DAG import test 한 번 더

### 환경별 분리 (보통의 정착 형태)

```python
# dev composer    — 빠르게
{"tracking_ref": "main", "refresh_interval": 60}

# stg composer    — 약간 통제
{"tracking_ref": "release", "refresh_interval": 300}

# prod composer   — 수동 only
{"tracking_ref": "production", "refresh_interval": 86400}
```

## 5. 배포(Deploy) 공통 전략

### 5-1. 검증 단계 (CI / Jenkins PR check)

DAG가 망가지면 환경 전체 영향. **PR 단계에서 강제**:

```yaml
# 예시
- name: DAG import check
  run: |
    python -m pytest tests/dag_validation/
    airflow dags list-import-errors  # 에러 있으면 fail

- name: DAG structure test
  run: pytest tests/

- name: Ruff
  run: ruff check dags/
```

검증 대상:
- import error (가장 흔한 장애 원인)
- `default_args` 누락
- 순환 의존성
- 너무 무거운 top-level 코드 (DAG parsing 느려짐)

옵션 (1)이든 (2)든 이 검증은 동일하게 필요. Bundle로 가면 Jenkins 배포 job은 없어지지만 **검증 job은 살아남음**.

### 5-2. 롤백 전략

| 옵션 | 롤백 방식 |
|---|---|
| (1) Jenkins + GCS sync | 이전 commit으로 git revert → Jenkins 재실행 → GCS 재sync |
| (2) GitDagBundle | `tracking_ref`를 이전 tag로 변경 → Composer env update |

→ (2)가 GitOps적으로 더 깔끔. tag 기반 release 가능. (1)은 기존 Jenkins 흐름 그대로라 익숙함.

### 5-3. PyPI 패키지 / 의존성 배포

DAG 코드뿐 아니라 `requirements.txt` 같은 의존성도 deploy 대상:

- Composer: PyPI 패키지 추가는 환경 update (수 분~십수 분 소요)
- 잦은 의존성 변경은 운영 부담 → 가능하면 **Pod 이미지에 박아서 KubernetesExecutor로 분리** ([[4_Queue 라우팅과 Pod 스펙 설정]])
- 옵션 (1), (2) 모두 동일하게 해당

## 6. 다이앤 환경 결정

> 현재: self-managed Airflow + nested-git (neptune/athlon/api repo) + Jenkins SSH pull. → Composer 3 이관.

**옵션 (2) GitDagBundle 직행 + Pull off + Push only 패턴**.

- 각 repo (neptune / athlon / api)를 **GitDagBundle로 등록**
- `refresh_interval = 86400` (자동 sync 사실상 off)
- 배포는 **Jenkins 버튼 → `gcloud composer ... bundles refresh`** 트리거
- 작업자 workflow 거의 동일 (Jenkins UI에서 repo 선택 → 버튼)
- 내부 동작만 "SSH+git pull" → "API call"로 변경
- 환경 분리는 **GCP project 단위**로 처리 (dev/stg/prod 별도 Composer 환경)
- Jenkins는 PR validation job + deploy 트리거 job 둘 다 유지
- 공통 코드는 internal Python package화 (필요해지면)

### 셋업 체크리스트

- [ ] Git deploy key 또는 PAT → Secret Manager 등록
- [ ] 환경 SA에 `roles/secretmanager.secretAccessor` 부여
- [ ] Airflow Connection (`git_neptune`, `git_athlon`, `git_api`) 등록
- [ ] Composer 환경변수에 `AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST` 주입
- [ ] Private IP 환경이면 Cloud NAT / VPC peering 확인 (Git 호스트 도달성)
- [ ] Jenkins worker에 `gcloud` + Composer 환경 호출 권한
- [ ] stale / drift 알람 셋업 (Cloud Monitoring)
- [ ] runbook 정리 — "수동 deploy하는 법, rollback하는 법"

## 관련 문서

- [[1_개요]]
- [[2_Cloud Composer vs Self-managed 비교]]
- [[4_Queue 라우팅과 Pod 스펙 설정]]
- [[6_Airflow 2 vs 3 비교]]
