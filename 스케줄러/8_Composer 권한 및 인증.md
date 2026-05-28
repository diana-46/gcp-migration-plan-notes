---
title: "Composer 권한 및 인증 (Airflow 3.x)"
status: verified
tags:
  - airflow
  - 스케줄러
  - composer
  - auth
  - security
created: 2026-05-14
updated: 2026-05-28
---

# Composer 권한 및 인증 (Airflow 3.x)

> Cloud Composer 3 + Airflow 3.1.7 기준. PoC (`test-airflow3`, asia-northeast3) 에서 실측 검증한 사실 기반.

## TL;DR (PoC 결과 한 줄)

> **권한 모델은 사실상 Airflow 2.x FAB RBAC 그대로.** Composer 3는 FAB Auth Manager 호환층을 그대로 살려두고, GCP IAM과 결합시켜서 운영. UI Security 메뉴도 Admin Role 사용자에게는 정상 노출.

- IAM = UI 접근 게이트 (Google SSO + `composer.*` Role)
- Airflow RBAC = UI 안에서 가능한 액션 (Admin/Op/User/Viewer/Public 5 Role)
- 신규 사용자 default Role = `composer_auth_user_registration_role` config (기본 `Op`, 권장값 `Viewer`)
- 관리는 **Airflow UI Security 메뉴** (Admin Role 보유자) 또는 **`gcloud composer environments run ENV users/roles ...`** 둘 다 가능

## 큰 그림: 3개의 권한 레이어

```
[Airflow UI 접근]    → 사용자가 UI에 들어올 수 있는가?
        ↓ (Google IAP + GCP IAM)
[Airflow RBAC]       → 들어와서 어떤 액션을 할 수 있는가?
        ↓ (FAB Role: Admin / Op / User / Viewer / Public)
[Task → GCP 리소스]  → task가 BigQuery / GCS 등을 호출할 수 있는가?
        ↓ (Workload Identity → 환경 SA)
```

| 레이어 | Composer 3 + Airflow 3.x | Self-managed |
|---|---|---|
| 1. UI 접근 | IAP 자동 통합 (Google 계정) | IAP / OAuth2 Proxy 직접 구성 |
| 2. Airflow RBAC | FAB Auth Manager (Composer가 통합 관리) | FabAuthManager / Custom |
| 3. Task → GCP | Workload Identity 자동 (환경 SA) | Workload Identity 직접 |

> **2.x → 3.x 실질 차이**: 거의 없음. config key 일부가 `composer_*` prefix로 추가된 정도. `airflow users` CLI, Security UI, FAB Role 모두 그대로.

## 레이어 1: Airflow UI 접근 (IAM)

UI에 들어올 수 있느냐 없느냐만 결정. Composer 3는 IAP를 자동 통합해서 Google 계정으로 SSO.

### IAM Role과 UI 접근

| IAM Role | UI 접근 | 비고 |
|---|---|---|
| `roles/composer.admin` | ✅ | 환경 전체 제어 + `executeAirflowCommand` 포함 |
| `roles/composer.editor` | ✅ | DAG / 환경 / operation 관리 |
| `roles/composer.user` | ✅ | DAG UI 보기·트리거 |
| `roles/composer.viewer` | ✅ | 읽기 전용 |
| `roles/composer.environmentAndStorageObjectAdmin` | ✅ | + DAG 업로드 |
| `roles/composer.environmentAndStorageObjectUser` | ✅ | + 버킷 객체 읽기 |
| `roles/composer.environmentAndStorageObjectViewer` | ✅ | 읽기 전용 + 버킷 객체 조회 |
| `roles/composer.worker` | ❌ | 서비스 계정 전용 |

핵심: `composer.environments.get` 권한이 있으면 UI 접근 가능.

### 권한 회수 시 12시간 캐시 (함정)

> "사용자에게 Airflow UI 액세스 권한을 부여하는 IAM 역할을 취소하면 이전에 할당된 권한이 웹브라우저에서 **최대 12시간 동안 캐시**될 수 있습니다."

→ IAM 회수만으론 즉시 차단 안 됨. 보안 사고 시 추가 조치 필요 (Workspace 계정 정지 등).

### IAM이 막지 못하는 것

- gcloud CLI를 통해 실행되는 Airflow CLI 명령어
- DAG 및 작업 코드
- → UI에서 차단해도 CLI / DAG 안에서 같은 작업 가능

## 레이어 2: Airflow RBAC (FAB)

PoC로 확인 — **2.x FAB RBAC 그대로 동작**.

### 기본 5 Role (PoC `roles list` 출력으로 확정)

| Role | DAG 보기 | DAG trigger/pause | Variable/Connection | 사용자/Role 관리 | 환경 설정 |
|---|---|---|---|---|---|
| **Admin** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **Op** | ✅ | ✅ | ✅ | ❌ | ❌ |
| **User** | ✅ | ✅ | ❌ | ❌ | ❌ |
| **Viewer** | ✅ 읽기만 | ❌ | ❌ | ❌ | ❌ |
| **Public** | ❌ | ❌ | ❌ | ❌ | ❌ |

### 실무 매핑 권장

| 역할 | Airflow Role |
|---|---|
| 일반 사용자 / 데이터 분석가 / 옆 팀 / 신규 입사자 default | **Viewer** ⭐ |
| DAG 개발자 (코드만 관리, 시크릿 안 만짐) | **User** |
| 데이터 엔지니어 (운영 풀세트) | **Op** |
| 플랫폼 관리자 (1~3명) | **Admin** |
| 의심 계정 임시 차단 | **Public** |

### 신규 사용자 자동 등록

- 첫 로그인 시 자동으로 metadata DB에 row 생성
- 부여되는 Role은 다음 config 값 (PoC 환경 기본값 `Op`):

| 섹션 | 키 | 기본값 | 권장 |
|---|---|---|---|
| `api` | `composer_auth_user_registration_role` | `Op` | **`Viewer`** |
| `api` | `rbac_user_registration_role` | `Op` | **`Viewer`** (양다리 안전) |

`composer_auth_user_registration_role` 가 Composer 3에서 추가된 신규 키. `rbac_user_registration_role` 은 2.x 호환 키. 둘 다 같이 바꿔두는 게 안전.

### Username 형식

PoC 실측: `accounts.google.com:NUMERIC_ID` (예: `accounts.google.com:109865159935253485663`)

- 감사 로그의 `소유자` 필드도 동일 형식
- 이메일 ↔ numeric ID 매핑은 **Security > List Users** 에서 확인 가능 (Admin Role 필요)

### Security UI 메뉴 (PoC 확인)

좌측 사이드바에 다음 메뉴들이 표시됨 (Admin Role 한정):

```
Home / Dags / Assets / Browse / Admin / Security / Plugins
```

**Role별 Security 메뉴 노출 동작 (PoC 실측)**:
- **Op / User / Viewer / Public** Role → Security 메뉴 안 보임
- **Admin** Role → Security 메뉴 노출 ✅

→ Composer가 UI를 강제로 숨기는 게 아니라, **표준 FAB 동작** (권한 없으면 메뉴 안 보임). 2.x와 완전 동일.

### Security UI에서 가능한 작업 (Admin Role)

**List Users 페이지** (`/users/list/`):

| 컬럼 | 내용 |
|---|---|
| First Name / Last Name | 사용자 이름 (자동 등록 시 이메일이 들어옴) |
| User Name | `accounts.google.com:NUMERIC_ID` |
| Email | 사용자 이메일 |
| Is Active? | 활성 여부 (false면 로그인 차단) |
| Role | 부여된 Role 목록 (배열, 여러 개 동시 보유 가능) |
| Groups | 사용자 그룹 (FAB Group 기능, 사용 안 하면 빈 배열) |

행별 액션:
- 🔍 보기 — 사용자 상세
- ✏️ 편집 — Role 변경 / Active 토글 / 이름 수정
- 🗑️ 삭제 — 사용자 row 제거 (⚠ 다음 로그인 시 자동 재등록)

상단 액션:
- ➕ 새 사용자 등록 (사전 등록)
- 🔎 검색 (이름/이메일/Role 등으로 필터링)

**List Roles 페이지**:
- 5개 기본 Role 확인 / 편집
- 새 Custom Role 생성
- Role의 action × resource 권한 매트릭스 직접 조회/수정

**Permissions 페이지**:
- 모든 action × resource 페어 목록
- Role과의 연결 관계 보기

→ **CLI 안 쓰고 UI에서 모든 권한 관리 가능.** 운영 효율성 측면에서 큰 메리트.

### 사용자 삭제 = 액세스 취소 아님 (함정, PoC 검증됨)

PoC에서 본인 계정 delete → 시크릿창에서 UI 접속 → 자동 재등록 + 새 default Role 부여 확인.

> "Airflow에서 사용자를 삭제하면 **다음에 Airflow UI에 액세스할 때 자동으로 재등록**되므로 해당 사용자의 액세스 권한이 취소되지 않습니다."

진짜 차단 방법:
- 프로젝트 IAM에서 `composer.environments.get` 권한 회수
- 또는 Airflow에서 Role을 `Public` 으로 변경 (UI 권한 모두 박탈, 사용자 row는 유지)
- 또는 Google Workspace에서 계정 정지

## 레이어 2.5: DAG 수준 권한 (PoC 미완)

### 두 가지 메커니즘 (Airflow 2.x 기능, 3.x에서도 살아있는지 검증 필요)

**방법 1**: DAG 코드의 `access_control` 속성
```python
with DAG(
    dag_id="restricted",
    access_control={'TeamA': {'can_edit', 'can_read'}},
    ...
):
```

**방법 2**: 폴더별 자동 Role 등록
| 섹션 | 키 | 값 |
|---|---|---|
| `webserver` | `rbac_autoregister_per_folder_roles` | `True` |
| `api` | `composer_auth_user_registration_role` | `UserNoDags` (DAG 액세스 없는 default 권장) |

→ `/dags/team-a/dag1.py` 가 자동으로 `team-a` Role 생성 + 권한 부여.

### 충돌 주의

> "폴더별 역할에 DAG 수준 권한을 수동으로 부여하면 DAG 프로세서가 DAG를 동기화할 때 이러한 권한을 삭제하거나 덮어쓸 수 있습니다."

→ 둘 다 쓰면 자동이 수동을 덮어씀. **하나만 사용**.

### DAG 수준 권한이 제어하는 것 / 못 하는 것

제어:
- Airflow UI에서의 DAG 가시성

제어 못 함:
- Connection, Variable 등 다른 Airflow 리소스 접근
- gcloud CLI 명령어
- 환경 버킷에 대한 접근 (= DAG 코드 자체에 대한 접근)

### 현실적 대안 — 환경 분리

DAG 단위 격리가 핵심 요구사항이면:
- 팀별로 Composer 환경 분리 (한 GCP 프로젝트 내 환경 N개)
- IAM Role을 환경별로 부여 (예: `composer.user` on environment X only)
- 격리 보장 ↑, **비용 증가**

## 레이어 3: Task → GCP 리소스 (Workload Identity)

DAG의 task가 BigQuery / GCS / Pub/Sub 등에 접근할 때 필요.

### 환경의 서비스 계정

- 환경 생성 시 지정, **생성 후 변경 불가**
- 역할: worker/scheduler pod 실행 ID, DAG 코드 실행 신원, 버킷 객체 접근, 커스텀 패키지 빌드, KubernetesPodOperator pod 실행 ID

⚠ "환경의 서비스 계정을 삭제하면 이 계정을 사용하는 모든 환경의 작동이 중지되고 **복구할 수 없습니다**."

### 사용자 관리형 SA 권장 (공식)

1. 새 GCP SA 생성
2. `roles/composer.worker` 부여
3. DAG가 호출할 GCP 리소스 권한만 추가

기본 Compute Engine SA 사용 금지 — 권한 과부여 상태.

### `roles/composer.worker` 가 포함하는 권한 (광범위)

`artifactregistry.*`, `cloudbuild.builds.*`, `container.*`, `compute.*`, `datalineage.*`, `logging.logEntries.*`, `monitoring.timeSeries.*`, `pubsub.*`, `storage.buckets.*`, `storage.objects.*` 등.

→ 추가로 부여하는 BQ 등의 권한도 최소 범위로.

### task별 SA 분리 (고급)

- task 종류별로 다른 권한이 필요할 때
- `KubernetesPodOperator` 의 `service_account_name` 로 task별 KSA 지정
- 상세 — [[4_Queue 라우팅과 Pod 스펙 설정]]

---

## 명령어 레퍼런스

> 환경: `test-airflow3` / location: `asia-northeast3`. 다른 환경이면 두 값만 바꾸면 됨.

### 사용자 조회

```bash
# 전체 사용자 목록 + Role
gcloud composer environments run test-airflow3 \
  --location asia-northeast3 \
  users list

# 모든 Role 종류
gcloud composer environments run test-airflow3 \
  --location asia-northeast3 \
  roles list
```

### Role 부여 / 회수

```bash
# Role 추가 (기존 Role은 유지됨, 누적)
gcloud composer environments run test-airflow3 \
  --location asia-northeast3 \
  users add-role -- -e EMAIL -r ROLE

# Role 제거
gcloud composer environments run test-airflow3 \
  --location asia-northeast3 \
  users remove-role -- -e EMAIL -r ROLE
```

⚠ `add-role` 은 추가만 함. 단일 Role로 만들려면 기존 Role을 `remove-role` 로 먼저 빼야 함.

### 사용자 생성 / 삭제 / 사전등록

```bash
# 사용자 사전 등록 (첫 로그인 전에 Role 지정)
gcloud composer environments run test-airflow3 \
  --location asia-northeast3 \
  users create -- \
    -r ROLE \
    -e EMAIL \
    -u EMAIL \
    -f FIRST_NAME \
    -l LAST_NAME \
    --use-random-password

# 사용자 삭제
# ⚠ 함정: 다음 UI 로그인 시 자동 재등록됨. 차단 수단으로 무의미.
gcloud composer environments run test-airflow3 \
  --location asia-northeast3 \
  users delete -- -e EMAIL
```

### default Role 변경 (신규 사용자에게 자동 부여될 Role)

```bash
# 둘 다 같이 변경 권장 (composer 신규 키 + 2.x 호환 키)
gcloud composer environments update test-airflow3 \
  --location asia-northeast3 \
  --update-airflow-configs=^::^api-composer_auth_user_registration_role=Viewer::api-rbac_user_registration_role=Viewer
```

적용 확인:
```bash
gcloud composer environments describe test-airflow3 \
  --location asia-northeast3 \
  --format="value(config.softwareConfig.airflowConfigOverrides)"
```

⏱ 적용까지 보통 2~5분 소요. **기존 사용자에겐 적용 안 됨** (신규 등록 시점에만).

### 기존 사용자 일괄 Role 변경

```bash
# Op → Viewer 일괄 다운그레이드 예시
for email in eric.next@kakaoent.com elena.27@kakaoent.com; do
  gcloud composer environments run test-airflow3 \
    --location asia-northeast3 \
    users remove-role -- -e $email -r Op
  gcloud composer environments run test-airflow3 \
    --location asia-northeast3 \
    users add-role -- -e $email -r Viewer
done
```

### IAM 부여 / 회수

```bash
# 부여
gcloud projects add-iam-policy-binding $PROJECT \
  --member="user:newbie@kakaoent.com" \
  --role="roles/composer.user"

# 회수
gcloud projects remove-iam-policy-binding $PROJECT \
  --member="user:leaver@kakaoent.com" \
  --role="roles/composer.user"

# Google Group 단위 부여 (스케일링)
gcloud projects add-iam-policy-binding $PROJECT \
  --member="group:airflow-users@kakaoent.com" \
  --role="roles/composer.user"
```

### Custom Role 만들기 (고급)

```bash
# 새 Role
gcloud composer environments run test-airflow3 \
  --location asia-northeast3 \
  roles create -- -r DagTriggerOnly

# 권한 추가 (action / resource 페어)
gcloud composer environments run test-airflow3 \
  --location asia-northeast3 \
  roles add-perms -- -r DagTriggerOnly -a can_read -r DAGs
```

→ Airflow UI Security > List Roles 에서도 동일 작업 가능 (Admin Role 필요).

---

## 운영 SOP

### 신규 입사자 권한 부여

1. **GCP IAM**: `roles/composer.user` (또는 Google Group 멤버 추가)
2. **첫 로그인** 시 Airflow가 자동 등록 + default Role (`Viewer`) 부여
3. **필요시 승격**: Airflow UI Security 또는 `users add-role` 로 User/Op/Admin

### 퇴사자 권한 회수

1. **GCP IAM** 에서 `composer.user` 회수 (또는 Group에서 제외)
2. ⚠ 최대 12시간 브라우저 캐시 — 즉시 차단 필요하면 Workspace에서 계정 정지

### 부분 차단 (계정 유지하되 UI 권한 박탈)

1. Airflow UI Security 또는 CLI 로 Role 을 `Public` 으로 변경
2. (선택) `composer.environments.get` IAM 권한도 회수

### 보안 사고 — 즉시 차단

1. Workspace 계정 정지 (IDP 레벨, 가장 강력)
2. Airflow Role 을 `Public` 으로 변경
3. IAM 권한 회수
4. (필요 시) DAGs 버킷의 쓰기 권한 회수 — DAG 코드를 통해 환경 SA 권한 행사 가능

### Connection / Variable / Secret 관리

- **Secret Manager backend** 사용 권장
  - 설정: `[secrets] backend = airflow.providers.google.cloud.secrets.secret_manager.CloudSecretManagerBackend`
  - 비밀번호가 Metadata DB에 평문으로 안 남음
  - Secret Manager 권한도 GCP IAM으로 통제
- Fernet Key: Composer가 자동 생성 + rotation (Self-managed면 직접)

### 감사

| 항목 | 위치 |
|---|---|
| UI 로그인 이벤트 | Cloud Logging |
| 권한 변경 이벤트 | Cloud Audit Logs (IAM 변경) |
| DAG / Variable / Connection 변경 | Airflow UI: **Browse > Audit Log** (Admin Role) |
| 사용자별 액션 | 동일. username = `accounts.google.com:NUMERIC_ID` |

## 사용자 권한 부여 패턴 (공식 4종)

### 패턴 A: 환경 + 버킷 관리자 (DAG 배포까지)
1. `roles/composer.environmentAndStorageObjectAdmin`
2. `roles/iam.serviceAccountUser`
3. 환경 SA에 `iam.serviceAccounts.actAs`

### 패턴 B: 환경 관리자 (DAG 배포 제외)
1. `roles/composer.admin`
2. `roles/iam.serviceAccountUser`
3. 환경 SA에 `iam.serviceAccounts.actAs`

### 패턴 C: DAG 배포만
1. `roles/composer.environmentAndStorageObjectViewer`
2. `roles/storage.objectAdmin` (DAGs 버킷에 한정)

### 패턴 D: 일반 사용자 (조회 + DAG 트리거)
- `roles/composer.user`

## IAM 권한 세분화 (덮어쓰기 / 좁히기)

표준 Role이 너무 광범위하면 4가지 도구:

### 1. Custom IAM Role
필요 권한만 모은 Role 정의:
```bash
gcloud iam roles create composerDagTriggerOnly \
  --project=$PROJECT \
  --title="Composer DAG Trigger Only" \
  --permissions=composer.environments.get,composer.environments.list,composer.dags.execute,composer.dags.get
```

### 2. IAM Conditions
환경 / 시간 / IP 조건 부여:
```bash
gcloud projects add-iam-policy-binding $PROJECT \
  --member="user:dev@kakaoent.com" \
  --role="roles/composer.user" \
  --condition='expression=resource.name.endsWith("/environments/dev-env"),title=dev-only'
```

→ **환경 단위 격리 가장 깔끔한 방법**.

### 3. IAM Deny Policy (진짜 덮어쓰기)
Allow 위에 Deny 얹어서 특정 권한 명시 차단. **Deny가 Allow를 항상 이김**.

```bash
gcloud iam policies create deny-composer-update \
  --attachment-point=cloudresourcemanager.googleapis.com/projects/$PROJECT \
  --kind=denypolicies \
  --policy-file=deny.json
```

### 4. Workspace SSO 정책
계정 자체를 무력화. 가장 강력하지만 운영팀이 관여해야 함.

---

## Self-managed 비교

| 항목 | Composer 3 + Airflow 3.x | Self-managed |
|---|---|---|
| UI 인증 | IAP 자동 (Google) | 직접 (IAP / OAuth / OIDC) |
| 권한 모델 | FAB RBAC (Composer가 통합) | FabAuthManager / Custom |
| 사용자 자동 등록 | UI 첫 로그인 시 (default `Op`, 변경 가능) | 직접 (또는 OAuth 연동 시 자동) |
| Workload Identity | 자동 | 직접 |
| 사용자 관리형 SA | 공식 권장 (default Compute Engine SA 비권장) | 직접 |
| Secret Manager backend | 자동 옵션 | 직접 |
| Fernet Key | 자동 관리 | 직접 |
| 감사 로그 | Cloud Logging + Airflow audit | 직접 |
| 사내 IDP(Okta) 직결 | ❌ (Google Workspace 경유만) | ⭕ FabAuthManager + `webserver_config.py` OAUTH_PROVIDERS |

### Self-managed Okta 통합 예시

FabAuthManager + `webserver_config.py`:

```python
from flask_appbuilder.security.manager import AUTH_OAUTH

AUTH_TYPE = AUTH_OAUTH
AUTH_USER_REGISTRATION = True
AUTH_USER_REGISTRATION_ROLE = "Viewer"

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

AUTH_ROLES_MAPPING = {
    "airflow-admins": ["Admin"],
    "airflow-ops": ["Op"],
    "airflow-viewers": ["Viewer"],
}
AUTH_ROLES_SYNC_AT_LOGIN = True
```

→ Okta 그룹 → Airflow Role 자동 매핑. Composer는 이걸 못 함.

## 흔한 실수 / 함정

- **Composer 3 = Airflow 권한 모델 완전 새 시스템?** → ❌ 거의 FAB RBAC 그대로. UI Security 메뉴도 Admin Role 사용자에겐 노출됨
- **IAM Role과 Airflow Role 1:1 매칭?** → ❌ 별개 레이어. IAM은 UI 접근만, 안에서의 권한은 Airflow Role
- **사용자 삭제 = 액세스 차단?** → ❌ 다음 로그인 시 자동 재등록. 차단은 IAM 회수 또는 Role을 `Public` 으로
- **IAM 회수 즉시 차단?** → ❌ 최대 12시간 브라우저 캐시
- **DAG 코드 = 환경 SA 권한 전체 위임** — DAGs 버킷 쓰기 권한 가진 사람은 환경 SA의 모든 권한 행사 가능
- **`composer.environments.update` 권한 = 단순 설정 변경?** → ❌ PyPI 패키지 설치를 통한 Python 코드 실행 가능
- **`composer.environments.executeAirflowCommand` 권한** = Airflow CLI를 환경 SA로 실행. Python 코드 실행 가능
- **기본 Compute Engine SA 사용 금지** — 권한 과부여 상태. 사용자 관리형 SA로
- **환경 SA 삭제 = 환경 영구 중단** (복구 불가)
- **사용자 권한 출처가 프로젝트 IAM에 안 보일 수 있음** — Folder/Org 레벨 상속 또는 Google Group 경유. `gcloud asset search-all-iam-policies` 또는 상위 IAM 확인
- **UI 접근 제어는 gcloud CLI / DAG 코드를 막지 못함**
- **`add-role` 은 누적** — 단일 Role로 만들려면 기존 Role을 `remove-role` 로 빼야 함

## 의사결정에 주는 함의

- **권한 관리 부담**: Composer 3가 매우 가벼움 — Workspace + IAM 한 곳, Airflow UI Security 메뉴 또는 CLI로 보조
- **사내 IDP(Okta) 직결**이 필수면 Self-managed가 유리
- **DAG 단위 격리가 핵심**이면 환경 분리(비용 ↑) 또는 Self-managed
- **multi-tenancy** 필요 시 환경 분리 필수 또는 Self-managed

## PoC 결과 — 검증 완료 (test-airflow3 / asia-northeast3)

| # | 항목 | 결과 / 증거 |
|---|---|---|
| 1 | Composer 3 + Airflow 3.1.7가 FAB RBAC 사용 | ✅ `users list` / `roles list` 동작 |
| 2 | 기본 5 Role 존재 (Admin/Op/User/Viewer/Public) | ✅ `roles list` 출력 5건 |
| 3 | `users list/add-role/remove-role/delete/create` CLI 동작 | ✅ 본인 Role 변경 사이클 검증 |
| 4 | `roles list` CLI 동작 / `--permissions` 옵션은 3.x에서 미지원 | ✅ list만, permissions 옵션은 거부됨 |
| 5 | `composer_auth_user_registration_role` config 으로 default 변경 | ✅ Op → Viewer 적용 확인 |
| 6 | 사용자 삭제 후 자동 재등록 + 새 default Role 부여 | ✅ diana.46 delete → 시크릿창 로그인 → Viewer로 재등록 |
| 7 | numeric ID 기반 사용자 식별 (`accounts.google.com:ID`) | ✅ `accounts.google.com:109865159935253485663` 등 |
| 8 | **Admin Role → Security UI 메뉴 노출** | ✅ 좌측 사이드바에 Security 항목 표시 (스크린샷 확보) |
| 9 | Security UI > List Users 페이지 정상 동작 | ✅ 3명 사용자 / Role / Groups / Is Active 컬럼 모두 정상 |
| 10 | Op Role 사용자 → Security 메뉴 비노출 | ✅ default Op 상태에서 Security 메뉴 없음 확인 |
| 11 | `composer.admin` IAM = `executeAirflowCommand` 보유 → CLI 만능 | ✅ Airflow Role 없는 상태에서도 gcloud 명령 정상 동작 |
| 12 | `add-role` 누적 동작 (기존 Role 유지) | ✅ Admin 추가 시 기존 Role 같이 존재 |

## 잔여 PoC (시간 되면)

| # | 항목 | 우선순위 |
|---|---|---|
| A | DAG `access_control` 속성 동작 여부 | ⭐⭐ |
| B | 폴더별 자동 Role 등록 (`rbac_autoregister_per_folder_roles`) 동작 여부 | ⭐ |
| C | IAM Role × Airflow Role 우선순위 (예: IAM `composer.viewer` + Airflow `Admin` = ?) | ⭐⭐ |
| D | IAM 권한 회수 후 12시간 캐시 실측 | ⭐ |
| E | Secret Manager backend로 Connection 마이그레이션 절차 | ⭐⭐ |
| F | task별 SA 분리 패턴 (Userlake용 / dbt run용 등) | ⭐⭐ |

## 관련 문서

- [[1_개요]]
- [[2_Cloud Composer vs Self-managed 비교]]
- [[3_Executor 종류 및 비교]]
- [[4_Queue 라우팅과 Pod 스펙 설정]]
- [[5_Metadata DB 운영]]
- [[6_Airflow 2 vs 3 비교]]
- [[7_Composer 비용]]

## 출처

- Airflow UI 액세스 제어 사용 — https://docs.cloud.google.com/composer/docs/composer-3/airflow-rbac?hl=ko
- Managed Service for Apache Airflow의 액세스 제어 — https://docs.cloud.google.com/composer/docs/composer-3/access-control?hl=ko
- PoC 실측 (`test-airflow3` / asia-northeast3, 2026-05-27)
