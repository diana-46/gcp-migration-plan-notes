---
title: "Composer 권한 및 인증"
status: draft
tags:
  - airflow
  - 스케줄러
  - composer
  - auth
  - security
created: 2026-05-14
updated: 2026-05-15
---

# Composer 권한 및 인증

> Cloud Composer 3 기준. 권한·인증을 어떻게 처리하는지, Self-managed로 갈 때는 무엇을 직접 구성해야 하는지. 사용자 로그인, task 권한, GCP 리소스 접근까지 3개 레이어로 정리.
> 본 문서의 Composer 3 부분은 공식 문서 [Airflow UI 액세스 제어][doc-rbac]와 [Managed Airflow IAM 액세스 제어][doc-iam] 기준.

## 큰 그림: 3개의 권한 레이어

```
[Airflow UI 접근]   → 사용자가 UI에 들어올 수 있는가?
        ↓ (Google IAP + IAM)
[Airflow 내부 RBAC] → 들어와서 무엇을 할 수 있는가? (DAG trigger / 변경 / 보기)
        ↓ (Airflow Role)
[Task → GCP 리소스]  → task가 BigQuery / GCS 등을 호출할 수 있는가?
        ↓ (Workload Identity → GCP SA)
```

| 레이어             | Composer 처리                                                      | Self-managed에서 해야할 일         |
| --------------- | ---------------------------------------------------------------- | ---------------------------- |
| 1. UI 접근        | IAP 자동 통합 (Google 계정)                                            | IAP / OAuth2 Proxy 직접 구성     |
| 2. Airflow RBAC | IAM이 1차 게이트, 들어온 뒤 Airflow Native RBAC가 세분화 (자동 등록 default `Op`) | RBAC 직접 관리 (DB 또는 LDAP/OIDC) |
| 3. Task → GCP   | Workload Identity 자동 (환경 SA)                                     | Workload Identity 직접 설정      |

> **중요**: Composer 3의 권한 모델은 "IAM이 막거나 / Airflow Native가 막거나" 가 아니라 **두 레이어가 동시에 적용되는 AND 게이트**. IAM이 통과시켜야 Airflow UI 자체에 닿고, 그 다음 Airflow Native RBAC가 UI 안에서 행동을 제한한다.

## 레이어 1: Airflow UI 접근 (IAM)

Composer 3의 Airflow UI / DAG UI 접근은 **2단계**로 제어됨 (공식 문서 표현 그대로):

> **1단계 — IAM 기반 액세스 제어**: 프로젝트에서 계정에 관리형 Airflow 환경을 볼 수 있는 역할이 없으면 Airflow UI 및 DAG UI를 사용할 수 없습니다. IAM은 Airflow UI 또는 DAG UI에서 세분화된 추가 권한 제어를 제공하지 않습니다.
>
> **2단계 — Apache Airflow 액세스 제어 모델**: 사용자 역할에 따라 Airflow UI 및 DAG UI의 가시성을 줄일 수 있습니다.

### IAM Role과 UI 접근

UI 접근에 가장 직접 관련되는 Role:

| IAM Role | UI 접근 가능? | 비고 |
|---|---|---|
| `roles/composer.admin` | ✅ | 환경 전체 제어 |
| `roles/composer.editor` | ✅ | DAG / 환경 / operation 관리 |
| `roles/composer.user` | ✅ | 환경 list/get, **DAG UI에서 DAG 보기·트리거** |
| `roles/composer.viewer` | ✅ | 읽기 전용 |
| `roles/composer.environmentAndStorageObjectAdmin` | ✅ | + 모든 버킷 객체 제어 (DAG 업로드 가능) |
| `roles/composer.environmentAndStorageObjectUser` | ✅ | + 버킷 객체 읽기 |
| `roles/composer.environmentAndStorageObjectViewer` | ✅ | 읽기 전용 + 버킷 객체 조회 |
| `roles/composer.worker` | ❌ | 서비스 계정 전용 |

핵심 권한 `composer.environments.get` 이 있으면 Airflow UI에 들어갈 수 있다.

### 권한 회수 시 12시간 캐시 (함정)

> "사용자에게 Airflow UI 액세스 권한을 부여하는 IAM 역할을 취소하면 이전에 할당된 권한이 웹브라우저에서 **최대 12시간 동안 캐시**될 수 있습니다."

즉 IAM에서 권한 빼도 즉시 차단되지 않을 수 있음. 보안 사고 시 IAM 회수만으로 안 됨.

### Airflow UI 액세스 제어가 막지 못하는 것 (공식 명시)

- gcloud CLI를 통해 실행되는 Airflow CLI 명령어
- DAG 및 작업 코드
- 예: Airflow 역할과 사용자 할당을 변경하는 DAG를 배포할 수 있음

### Self-managed

직접 만들어야 함. 옵션:

| 옵션                        | 장점                                                             | 단점                                                             |
| ------------------------- | -------------------------------------------------------------- | -------------------------------------------------------------- |
| **Okta (OIDC/SAML)** ⭐    | 사내 SSO 통합. 한 번 로그인하면 모든 시스템 공유. 그룹 클레임 → Airflow Role 자동 매핑 가능 | Okta 앱 등록 + Airflow `webserver_config.py` OAuth provider 설정 필요 |
| **OAuth2 Proxy + Google** | 간단한 OAuth 흐름                                                   | RBAC와 별도 매핑 필요                                                 |
| **Basic Auth (개발용만)**     | 제일 간단                                                          | 운영 절대 비권장                                                      |

> 사내 IDP가 이미 있으면 (Okta 등) 그걸 쓰는 게 정답. 모든 사내 시스템이 같은 SSO 쓰면 입사/퇴사 시 권한 회수가 IDP 한 곳에서 끝남. Airflow 측은 OIDC provider만 등록하면 됨.

#### Okta + Airflow 연동 개요

Airflow는 Flask-AppBuilder(FAB) auth manager를 통해 OAuth provider 지원. `webserver_config.py` 에 Okta를 OAuth provider로 등록:

```python
# webserver_config.py (예시 골격)
from flask_appbuilder.security.manager import AUTH_OAUTH

AUTH_TYPE = AUTH_OAUTH
AUTH_USER_REGISTRATION = True
AUTH_USER_REGISTRATION_ROLE = "Op"  # 신규 사용자 default Role

OAUTH_PROVIDERS = [
    {
        "name": "okta",
        "icon": "fa-circle-o",
        "token_key": "access_token",
        "remote_app": {
            "client_id": "<OKTA_CLIENT_ID>",
            "client_secret": "<OKTA_CLIENT_SECRET>",
            "api_base_url": "https://<your-org>.okta.com/oauth2/v1/",
            "client_kwargs": {"scope": "openid profile email groups"},
            "access_token_url": "https://<your-org>.okta.com/oauth2/v1/token",
            "authorize_url": "https://<your-org>.okta.com/oauth2/v1/authorize",
            "server_metadata_url": "https://<your-org>.okta.com/.well-known/openid-configuration",
        },
    }
]

# Okta 그룹 클레임 → Airflow Role 매핑
AUTH_ROLES_MAPPING = {
    "airflow-admins": ["Admin"],
    "airflow-ops": ["Op"],
    "airflow-viewers": ["Viewer"],
}
AUTH_ROLES_SYNC_AT_LOGIN = True  # 로그인 시마다 그룹 → Role 재동기화
```

핵심:
- **`AUTH_ROLES_MAPPING`**: Okta 그룹 → Airflow Role. 그룹에서 빼면 Airflow Role도 빠짐
- **`AUTH_ROLES_SYNC_AT_LOGIN = True`**: 매 로그인 시 그룹 재확인 → 권한 변경이 빠르게 반영
- **`scope: openid profile email groups`**: Okta가 그룹 정보를 토큰에 실어 보내도록
- Okta 측에서는 "OIDC Web Application" 으로 앱 만들고 Redirect URI를 `https://<airflow-domain>/oauth-authorized/okta` 로 설정

이렇게 하면 **Composer 3의 IAM 기반 그룹 권한과 거의 동등한 운영 모델**을 Self-managed에서도 구현 가능. 차이점은 Composer가 GCP IAM에 위임한 걸 Self-managed에서는 Okta에 위임한다는 것.

## 레이어 2: Airflow 내부 RBAC

IAM 통과 후, Airflow 내부에서 어떤 행동을 할 수 있는가. Airflow 자체의 권한 모델.

### Airflow 기본 Role (5종)

| Role | 권한 |
|---|---|
| **Admin** | 모든 권한 (Variable/Connection/User 관리 포함) |
| **Op** | DAG 트리거, Pause, Variable/Connection 보기·수정 |
| **User** | DAG 트리거, Pause, 자기 task 로그 보기 |
| **Viewer** | 읽기 전용 |
| **Public** | 비로그인 / 권한 없음 (사용자 등록 유지하면서 접근 차단할 때 사용) |

리소스 기반 권한 모델. 예: `can delete on Connections` 권한이 있는 Role이 부여된 사용자는 Connection 페이지에서 삭제 가능.

### Composer 3 — 자동 사용자 등록

> "신규 사용자는 Managed Airflow 환경의 Airflow UI를 처음 열 때 **자동으로 등록**됩니다."

등록 시 부여되는 Role은 다음 Airflow 구성 옵션으로 결정:

| 섹션 | 키 | 기본값 |
|---|---|---|
| `webserver` | `rbac_user_registration_role` | `Op` |

→ **별도 설정 안 하면 모든 신규 사용자가 자동으로 `Op` Role** 부여. IAM Role 종류와 무관하게 일률적으로 적용됨. (즉 IAM이 `composer.admin` 이어도 Airflow 첫 로그인 시 Airflow Role은 `Op`)

### 초기 Admin 설정 절차 (공식 권장)

새 환경에서 첫 Admin을 만드는 표준 절차:

1. 환경 관리자가 새로 생성된 환경의 Airflow UI를 엽니다 (자동으로 `Op` 등록됨)
2. gcloud CLI로 본인 계정에 `Admin` Role 부여:
   ```bash
   gcloud composer environments run ENVIRONMENT_NAME \
         --location LOCATION \
         users add-role -- -e USER_EMAIL -r Admin
   ```
3. 그 다음부터는 Airflow UI에서 다른 사용자에게 Role 부여 가능

### 사용자 사전 등록 (Pre-register)

사용자가 아직 첫 로그인 안 한 상태에서 미리 등록 + Role 지정:

```bash
gcloud composer environments run ENVIRONMENT_NAME \
      --location LOCATION \
      users create -- \
      -r ROLE \
      -e USER_EMAIL \
      -u USER_EMAIL \
      -f FIRST_NAME \
      -l LAST_NAME \
      --use-random-password
```

- `--use-random-password` 는 필수지만 실제 사용되지 않음 (인증은 IAM이 처리)
- **Google 그룹은 사전 등록 불가**

### Username = Google 숫자 ID (감사 로그에서 중요)

> "사용자는 Google 사용자 계정의 **숫자 ID**(이메일 주소 아님)가 사용자 이름으로 자동 등록됩니다."

- 감사 로그의 `소유자` 필드 형식: `accounts.google.com:NUMERIC_ID`
- 이메일 ↔ 숫자 ID 매핑은 Airflow UI **보안 > 사용자 나열** 페이지에서 확인 가능 (`Admin` Role 필요)
- 사전 등록 시 이메일로 등록하면, 사용자 최초 로그인 시 숫자 ID로 자동 교체됨

### 사용자 삭제 = 액세스 취소 아님 (함정)

> "Airflow에서 사용자를 삭제하면 **다음에 Airflow UI에 액세스할 때 자동으로 재등록**되므로 해당 사용자의 액세스 권한이 취소되지 않습니다."

전체 액세스 취소 방법:
- 프로젝트 IAM에서 `composer.environments.get` 권한 회수
- 또는 사용자 Role을 `Public` 으로 변경 (사용자 등록은 유지, UI 권한은 모두 박탈)

### Airflow Role 관리는 Admin만

> "관리자 역할(또는 이에 상응하는 역할)이 있는 사용자가 Airflow UI에서 액세스 제어 설정을 확인하고 수정할 수 있습니다."

- 위치: Airflow UI **보안** 메뉴
- 이 메뉴는 `Admin` Role 사용자에게만 보임
- `Admin` 없는 사용자가 `/users/list/` 등 직접 URL 접근 시 404 또는 거부됨

### Self-managed

Airflow RBAC를 직접:
- 기본 5 Role 사용 또는 커스텀 Role 정의
- Webserver 설정에 `[webserver] rbac = True` (2.x), 3.x는 기본
- 사용자 생성:
  - 외부 IDP (Okta 등) 연동 시 → 첫 로그인 시 자동 등록 (`AUTH_USER_REGISTRATION = True`)
  - 또는 수동: `airflow users create ...` / UI Admin
- 외부 IDP 연동 시 `webserver_config.py` 에 OAuth provider 작성 (위 레이어 1의 Okta 예시 참고)
- **Okta 그룹 → Airflow Role 자동 매핑** (`AUTH_ROLES_MAPPING`): 운영 관점에서 Composer 3의 "IAM 그룹 권한" 모델과 거의 동등. 권한 관리를 Okta 한 곳에서 끝낼 수 있어 추천

## 레이어 2.5: DAG 수준 권한 (DAG-level permissions)

특정 DAG에 대한 접근을 Role 단위로 세분화하는 메커니즘. Composer 3는 두 가지 방법 제공.

### 방법 1: 폴더별 역할 등록 (자동)

`/dags` 폴더 내 하위 폴더 이름으로 자동으로 Role을 만들고, 그 폴더의 DAG들에 해당 Role 권한 부여.

활성화:

| 섹션 | 키 | 값 |
|---|---|---|
| `webserver` | `rbac_autoregister_per_folder_roles` | `True` |
| `webserver` | `rbac_user_registration_role` | `UserNoDags` (DAG 액세스 없는 default Role로 변경 권장) |

`UserNoDags`: 폴더별 역할 등록 활성화 시 자동 생성되는 Role. `User`와 동일하지만 DAG 액세스 없음.

동작 예시:
```
/dags/team-a/dag1.py     → "team-a" Role 자동 생성, 이 Role 가진 사용자만 dag1 보임
/dags/team-b/dag2.py     → "team-b" Role 자동 생성
/dags/top_level_dag.py   → 폴더별 역할 적용 안 됨 (최상위 DAG는 기본 Role로만 접근)
```

**주의:**
- 폴더 이름이 내장 Role(`Admin`, `Op`, `User`, `Viewer`, `Public`, `UserNoDags`)과 일치하면 해당 Role에 권한 부여됨. 예: `/dags/Admin/foo.py` → Admin Role에 권한 부여
- DAG가 100개 넘으면 스케줄러 파싱 시간 증가 가능 → CPU/메모리 증설 권장
- 폴더 만든 사용자가 자동으로 그 Role 받지는 않음. Admin이 수동으로 사용자에게 Role 할당해야 함

### 방법 2: DAG 코드의 `access_control` 속성

```python
dag = DAG(
    access_control={
        'DagGroup': {'can_edit', 'can_read'},
    },
    ...
)
```

스케줄러가 DAG 파싱 시 권한 적용.

### 충돌 주의 (공식 경고)

> "Airflow UI 또는 gcloud CLI를 통해 DAG 권한을 수동으로 부여하면 충돌이 발생할 수 있습니다. 폴더별 역할에 DAG 수준 권한을 수동으로 부여하면 DAG 프로세서가 DAG를 동기화할 때 이러한 권한을 삭제하거나 덮어쓸 수 있습니다. **DAG 권한을 수동으로 부여하지 않는 것이 좋습니다.**"

→ 폴더별 등록을 쓰기로 했으면 수동 권한은 모두 자동에 맡길 것.

### DAG 수준 권한이 제어하는 것 (스코프)

> "폴더별 역할 등록 기능은 **DAG에 대한 Airflow UI 권한만 제어**하고 다른 권한을 제어하지 않습니다."

DAG 수준 권한이 **제어하지 않는** 것:
- Connection, Variable 등 다른 Airflow 리소스 접근
- gcloud CLI 명령어 접근
- 환경 버킷에 대한 접근 (= DAG 코드 자체에 대한 접근)

## 레이어 3: Task → GCP 리소스 (Workload Identity)

DAG의 task가 BigQuery / GCS / Pub/Sub 등에 접근할 때 필요.

### 환경의 서비스 계정 (Environment Service Account)

- 환경 생성 시 지정, **생성 후 변경 불가**
- 역할:
  - Airflow worker, scheduler pod 실행 ID
  - DAG/task 코드 실행 시의 실제 신원
  - PyPI 커스텀 패키지 설치 시 이미지 빌드
  - 환경 버킷 객체 읽기/쓰기
  - KubernetesPodOperator 등을 통한 pod 실행 ID

**⚠ 경고**: "환경의 서비스 계정을 삭제하면 이 계정을 사용하는 모든 환경의 작동이 중지되고 **복구할 수 없습니다**."

### 사용자 관리형 SA 권장 (Composer 3 공식 권장)

> "**사용자 관리형 서비스 계정**을 설정하고 Managed Airflow 환경에 사용하는 것이 좋습니다."

설정 절차:
1. 새 GCP SA 생성
2. 그 SA에 `roles/composer.worker` 부여
3. DAG가 호출할 GCP 리소스 권한만 추가 부여

### 기본 Compute Engine SA를 쓰면 안 되는 이유 (공식 명시)

> "이 계정에는 일반적으로 Managed Airflow 환경 또는 DAG를 실행하는 데 필요한 것보다 많은 권한이 있습니다."

- 권한 과부여 상태로 시작
- 사후 권한 축소가 어려운 경우 많음
- → 신규 환경은 반드시 전용 user-managed SA로 만들 것

### `roles/composer.worker` 가 포함하는 권한 (광범위)

공식 문서에 나열된 주요 권한 카테고리:
- `artifactregistry.*` (거의 전체)
- `cloudbuild.builds.*`
- `cloudkms.keyHandles.*`
- `container.*` (광범위)
- `compute.*` (광범위)
- `datalineage.*`
- `logging.logEntries.*`
- `monitoring.timeSeries.*`
- `pubsub.*`
- `storage.buckets.*`, `storage.objects.*`, `storage.folders.*`

→ Worker 역할 자체가 이미 강력하므로, 추가로 부여하는 BQ 등의 권한도 최소 범위로.

### Cloud Composer 서비스 에이전트 계정

- 프로젝트의 모든 환경이 공동으로 사용
- 기본적으로 `roles/composer.serviceAgent` 만 보유
- 이 역할 그대로 유지 권장 (수정 비권장)

### Self-managed

직접 구성:

```bash
# 1. GCP SA 만들기
gcloud iam service-accounts create airflow-worker \
  --project=$PROJECT

# 2. GKE의 KSA(Kubernetes Service Account)와 매핑
gcloud iam service-accounts add-iam-policy-binding \
  airflow-worker@$PROJECT.iam.gserviceaccount.com \
  --role=roles/iam.workloadIdentityUser \
  --member="serviceAccount:$PROJECT.svc.id.goog[airflow/airflow-worker-ksa]"

# 3. Airflow worker Pod의 ServiceAccount annotation
kubectl annotate serviceaccount airflow-worker-ksa \
  -n airflow \
  iam.gke.io/gcp-service-account=airflow-worker@$PROJECT.iam.gserviceaccount.com

# 4. 권한 부여
gcloud projects add-iam-policy-binding $PROJECT \
  --member=serviceAccount:airflow-worker@$PROJECT.iam.gserviceaccount.com \
  --role=roles/bigquery.dataEditor
```

→ 설정 자체는 한 번이지만 **task가 여러 종류면 SA 분리 / queue별 SA 매핑** 등 추가 설계 필요.

### task별 SA 분리 (고급)

- task 종류별로 다른 권한이 필요할 때 (예: BQ만 / Pub/Sub만)
- Composer: `KubernetesPodOperator` 의 `service_account_name` 로 task별 KSA 지정
- Self-managed: 동일하게 가능, Pod 스펙 factory에 SA 포함 ([[4_Queue 라우팅과 Pod 스펙 설정]])

## 사용자 권한 부여 패턴 (공식 4종)

공식 문서가 제시하는 사용자 유형별 권한 묶음:

### 패턴 A: 환경 + 버킷 관리자 (DAG 배포까지)
환경 CRUD + 버킷 객체 관리 + Airflow 웹 UI/CLI 접근:
1. `roles/composer.environmentAndStorageObjectAdmin`
2. `roles/iam.serviceAccountUser`
3. 환경 SA에 `iam.serviceAccounts.actAs` 권한

### 패턴 B: 환경 관리자 (DAG 배포 제외)
환경 CRUD + Airflow UI/CLI/DAG UI 접근:
1. `roles/composer.admin`
2. `roles/iam.serviceAccountUser`
3. 환경 SA에 `iam.serviceAccounts.actAs` 권한

### 패턴 C: 환경 뷰어 + 버킷 쓰기 (DAG 배포만 가능)
환경 조회 + DAG 업로드:
1. `roles/composer.environmentAndStorageObjectViewer`
2. `roles/storage.objectAdmin` (DAGs 버킷에 한정)

### 패턴 D: 환경 뷰어 (일반 사용자)
환경 및 DAG 조회 + Airflow UI 접근 + DAG 트리거:
- `roles/composer.user`

## Connection / Variable / Secret 관리

Airflow에서 외부 시스템 접근 정보(DB 비밀번호, API 키 등)를 어떻게 보관하나.

### Composer 3

- **Secret Manager 자동 연동** 옵션 제공
- Airflow Connection / Variable 을 Secret Manager backend로 저장
- 설정 한 줄로 활성화: `[secrets] backend = airflow.providers.google.cloud.secrets.secret_manager.CloudSecretManagerBackend`
- task 코드에서 `Connection.get("my_conn")` 호출 시 자동으로 Secret Manager에서 가져옴
- 비밀번호가 Metadata DB에 평문으로 남지 않음

### Self-managed

- 동일 backend 사용 가능 (provider package 설치 필요)
- 또는 외부 vault (HashiCorp Vault, AWS Secrets Manager 등) backend 가능
- 직접 설정. Composer가 자동으로 해주던 게 수동

### Fernet Key

- Connection 비밀번호 암호화 키
- Composer: 자동 생성 + 자동 rotation
- Self-managed: **직접 생성 + 보관 + 회전 정책**

## 감사 / 로그

| 항목 | Composer 3 | Self-managed |
|---|---|---|
| UI 로그인 이벤트 | Cloud Logging 자동 | 직접 구성 |
| DAG 트리거 이벤트 | Airflow audit log (자동) | Airflow audit log (자동) |
| 권한 변경 이벤트 | Cloud Audit Logs (자동) | 직접 구성 |
| Connection/Variable 접근 | Airflow log | Airflow log |
| Airflow UI 감사 로그 위치 | UI **찾아보기 > 감사 로그** | 동일 (Airflow 기본 기능) |
| 사용자 식별자 | `accounts.google.com:NUMERIC_ID` (보안 > 사용자 나열에서 이메일 매핑) | 직접 등록한 username |

## 비교 요약

| 항목 | Composer 3 | Self-managed |
|---|---|---|
| UI 인증 | IAP 자동 (Google) | 직접 (IAP/OAuth/OIDC) |
| 사용자 자동 등록 | UI 첫 로그인 시 자동, default Role = `Op` (`rbac_user_registration_role` 로 변경 가능) | 직접 등록 |
| IAM Role → Airflow Role | 자동 매핑 없음. IAM은 1차 게이트만, Airflow Role은 `rbac_user_registration_role` 로 일률 적용 → Admin이 개별 승격 | 없음 (직접) |
| DAG 수준 권한 | 폴더별 자동 등록 + `access_control` 속성 | 동일 (Airflow 기본 기능) |
| Workload Identity | 자동 | 직접 설정 |
| 사용자 관리형 SA | 공식 권장 (default Compute Engine SA 비권장) | 직접 구성 |
| Secret Manager backend | 자동 옵션 | 직접 |
| Fernet Key | 자동 관리 | 직접 |
| 감사 로그 | Cloud Logging 자동 + Airflow audit log | 직접 |

## 흔한 실수 / 함정

- **IAM Role과 Airflow Role을 1:1로 생각하면 틀림**: Composer 3에서 IAM은 1차 게이트(UI 접근 허용/차단), Airflow Role은 별개 모델. IAM `composer.admin` 이어도 첫 로그인 시 Airflow Role은 default `Op` (`rbac_user_registration_role` 따름)
- **사용자 삭제 ≠ 액세스 차단**: Airflow UI에서 사용자 삭제해도 다음 로그인 시 자동 재등록됨. 차단하려면 IAM에서 `composer.environments.get` 회수 또는 Role을 `Public` 으로 변경
- **IAM 권한 회수 후 최대 12시간 캐시**: 즉시 차단 안 됨. 보안 사고 시 추가 조치 필요
- **DAG 코드 = SA 권한 위임**: DAGs 버킷 쓰기 권한 가진 사람은 환경 SA의 모든 권한을 사실상 행사 가능. 신뢰 가능한 사람만 부여
- **`composer.environments.update` 권한의 위험**: 환경 구성 변경 외에도 PyPI 패키지 설치를 통한 Python 코드 실행 가능. 단순 "설정 변경" 권한이 아님
- **`composer.environments.executeAirflowCommand` 권한의 위험**: Airflow CLI를 환경 SA로 실행시키는 권한. Python 코드 실행 가능
- **기본 Compute Engine SA 사용 금지**: 권한 과부여 상태. 사용자 관리형 SA로 변경
- **환경 SA 삭제 = 환경 영구 중단**: 복구 불가능
- **DAG 권한 수동 부여와 폴더별 자동 등록 충돌**: 둘 다 쓰면 자동 동기화가 수동 부여를 덮어쓸 수 있음. 하나만 사용
- **폴더 이름이 내장 Role과 겹치면 자동으로 권한 부여됨**: 예 `/dags/Admin/` 폴더에 DAG 두면 모든 Admin Role 사용자가 해당 DAG에 접근. 의도치 않은 권한 부여 위험
- **사용자 권한 출처가 프로젝트 IAM에 안 보일 수 있음**: Folder/Org 레벨 상속 또는 Google Group 경유 부여가 흔함. `gcloud projects get-iam-policy` 만 보지 말고 `gcloud asset search-all-iam-policies` 또는 상위 레벨 IAM 확인
- **Airflow UI 액세스 제어는 gcloud CLI / DAG 코드를 막지 못함**: UI에서 막혔다고 안전한 게 아님. CLI나 DAG 안에서 같은 작업 가능

## 의사결정에 주는 함의

- **권한 관리 부담 측면**에서는 Composer 압도. Self-managed는 1~2주 작업 + 지속적 운영 부담
- **사내 IDP(Okta 등) 통합 요구가 강하면** Self-managed가 유리. Composer는 Google 계정 SSO에 묶이는 반면 Self-managed는 Okta OIDC 직접 연동 가능 → 사내 권한 관리(입퇴사 자동화)와 자연스럽게 결합
- **multi-tenancy** (한 클러스터에 여러 팀) 가 필요하면 Self-managed가 유연. Composer는 환경 분리로 처리해야 함 (비용 증가)
- **DAG 수준 권한 분리가 핵심 요구사항**이면 Composer 3의 폴더별 역할 등록이 비교적 간편한 솔루션 (단, 100+ DAG 시 스케줄러 부하 고려)

## PoC / 검증 추가 항목

- [ ] 환경 생성 시 사용자 관리형 SA로 만들기 (기본 Compute Engine SA 회피)
- [ ] 초기 Admin 셋업 절차 (`gcloud composer environments run ... users add-role`) 검증
- [ ] `rbac_user_registration_role` 을 `UserNoDags` 로 변경 → 신규 사용자에게 자동으로 DAG 액세스 안 주는 패턴 검증
- [ ] 폴더별 역할 등록 (`rbac_autoregister_per_folder_roles=True`) 동작 확인
- [ ] DAG의 `access_control` 속성으로 Role 기반 가시성 제어 검증
- [ ] task별 SA 분리 패턴 설계 (Userlake용 / dbt run용 / extract용 등)
- [ ] Secret Manager backend로 기존 Connection 마이그레이션 절차
- [ ] IAM Role 회수 시 12시간 캐시 동작 확인 (실측)
- [ ] 사용자 삭제 후 재로그인 시 자동 재등록 동작 확인
- [ ] 감사 로그 보존 정책 (몇 개월 / 어떤 이벤트)
- [ ] (Self-managed 검토 시) Okta OIDC provider 등록 + `AUTH_ROLES_MAPPING` 으로 그룹 → Role 자동 매핑 PoC

## 미확정 / 확인 필요

- 폴더별 역할 등록 시 Role 이름 규칙 (특수문자 / 한글 / 공백 처리)
- `access_control` 과 폴더별 등록을 같이 쓰는 경우 정확한 우선순위
- Google Workspace SSO (SAML/OIDC) 와 Composer IAP의 정확한 통합 동작
- Self-managed 시 IAP를 GKE Ingress에 붙이는 권장 패턴 (Composer가 내부적으로 쓰는 방식과 동일한지)

## 관련 문서

- [[1_개요]]
- [[2_Cloud Composer vs Self-managed 비교]]
- [[3_Executor 종류 및 비교]]
- [[4_Queue 라우팅과 Pod 스펙 설정]]
- [[5_Metadata DB 운영]]
- [[6_Airflow 2 vs 3 비교]]
- [[7_Composer 비용]]

## 출처

- [doc-rbac]: Airflow UI 액세스 제어 사용 — https://docs.cloud.google.com/composer/docs/composer-3/airflow-rbac?hl=ko
- [doc-iam]: Managed Service for Apache Airflow의 액세스 제어 — https://docs.cloud.google.com/composer/docs/composer-3/access-control?hl=ko

[doc-rbac]: https://docs.cloud.google.com/composer/docs/composer-3/airflow-rbac?hl=ko
[doc-iam]: https://docs.cloud.google.com/composer/docs/composer-3/access-control?hl=ko
