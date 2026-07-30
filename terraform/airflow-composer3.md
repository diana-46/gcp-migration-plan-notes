# Cloud Composer 3 (Airflow 3) 신규 서비스 구축 가이드

최종 수정: 2026-07-30
관련 이슈: DP-3137
관련 브랜치: `feature/DP-3137-gcp`
Terraform: `dp-terraform/{airflow,modules/gcp-composer}`

## 이 문서의 목적

팀별 Airflow (Composer 3) 환경을 새로 만들 때 처음부터 끝까지 어떻게 하면 되는지. `dev-berriz-airflow` 를 첫 케이스로 놓고 정리했다.

## 구조 개요

```
dp-terraform/
├── modules/
│   └── gcp-composer/            # 재사용 모듈
│       ├── main.tf
│       ├── variables.tf
│       ├── outputs.tf
│       └── versions.tf
└── airflow/
    ├── scripts/                 # 공용 스크립트
    │   ├── qssh                 #   QueryPie qssh wrapper (non-interactive shell 용)
    │   ├── setup-roles.sh       #   초기 Admin 승격
    │   └── migrate-from-airflow2.sh  # 온프렘 → Composer metadata 이관
    └── <gcp-project>/           # ex) dev-dp-project
        └── <env-name>/          # ex) dev-berriz-airflow  ← 팀별 root module
            ├── provider.tf      #   backend + provider 선언
            ├── configs.tf       #   project / region 상수
            ├── main.tf          #   module "this" 호출 + 팀별 config
            ├── roles.conf       #   setup-roles.sh 의 초기 Admin 리스트
            └── migration.conf   #   migrate-from-airflow2.sh 의 소스/대상 세팅 (이관 필요 시)
```

**원칙**
- **각 Composer env = 별도 root module = 별도 state**. Blast radius 격리, 롤링 업그레이드 자연스러움
- **코드 재사용은 `modules/gcp-composer/` 로**. root wrapper 는 얇게 유지
- **root 파일만 봐도 그 env 스펙 파악되게**. workloads 등 중요값은 module default 에 의존하지 말고 명시

## 신규 env 만들기 — 처음부터 끝까지

### Step 0. 결정할 것

| 항목 | 예시 (dev-berriz-airflow) | 참고 |
|---|---|---|
| **env 이름** | `dev-berriz-airflow` | 만든 뒤 변경 불가. `<phase>-<team>-airflow` 컨벤션 |
| **GCP 프로젝트** | `dev-dp-project-354904` | 변경 불가 |
| **region** | `asia-northeast3` | 변경 불가 |
| **VPC / subnet** | shared VPC (`dev-host-vpc` / `dev-dp-subnet-1`) | 사실상 변경 불가 |
| **Worker SA** | `dev-berriz-airflow@dev-dp-project-354904.iam.gserviceaccount.com` | 변경 불가. **팀에서 SA 못 만들면 별도 요청** |
| **Deployer SA** | `dev-berriz-airflow-deployer@...` | apply 후 GCS 버킷 이름 확정된 뒤 요청 |
| **필요 PyPI providers** | amazon, kafka, slack, datahub 등 | 팀 요구사항 |
| **DataHub 연동?** | O | plugin + config 세트로 |
| **Secret Manager backend?** | O | Airflow 밖 secret 관리 |

### Step 1. SA 요청 (기다리는 동안 아래 진행 가능)

**Worker SA** — DAG 실행 시 사용
```
SA: <phase>-<team>-airflow@<project>.iam.gserviceaccount.com
IAM (project level):
  - roles/composer.worker                  # Composer 필수
  - roles/bigquery.jobUser                 # 쿼리 · load · export job
  - roles/bigquery.dataEditor              # BQ dataset write (admin 대비 좁음)
  - roles/bigquery.readSessionUser         # BQ Storage Read API (Spark/pandas)
  - roles/storage.objectUser               # GCS 오브젝트 read/write
  - roles/secretmanager.secretAccessor     # Secret Manager 값 read
  - roles/artifactregistry.reader          # 내부 PyPI (dev-dp-python-registry) 접근
```

**Deployer SA** — CI/CD 배포 시 사용 (apply 후 요청)
```
SA: <phase>-<team>-airflow-deployer@<project>.iam.gserviceaccount.com
IAM:
  - Composer GCS 버킷 (apply 후 이름 확정) : roles/storage.objectAdmin
  - Project : roles/composer.user (Airflow UI/API 호출 필요 시)
```

**분리 근거**
- Worker SA 는 DAG 실행 = 임의 코드 실행 = 강한 권한
- 배포 시스템까지 이 SA 로 하면 CI/CD 침해 시 데이터 노출
- 따로 나누는 게 최소 권한 · 감사 로그 명확화

**받은 SA 검증** (요청 후 첫 확인)
```bash
gcloud projects get-iam-policy <project> --flatten="bindings[].members" \
  --format='table(bindings.role)' \
  --filter="bindings.members:<sa-email>"
```
경험적으로 SA 요청 시 **`roles/secretmanager.secretAccessor` 가 누락되는 경우** 발견 (SM backend 활성 시 필수). 전체 7개 role 다 있는지 확인 후 부족한 것만 추가 요청.

### Shared VPC (prod)

Prod 는 shared VPC 사용 (`prod-platform-host-project` 의 subnet). Composer Service Agent 에 subnet 접근 권한 추가 필요:

```
Composer Service Agent (service-<PROD_PROJECT_NUMBER>@cloudcomposer-accounts.iam.gserviceaccount.com)
  - roles/compute.networkUser on subnet <TBD>
  - (필요 시) roles/composer.sharedVpcAgent on prod-platform-host-project
```

Subnet 이름은 prod 인프라 담당자 문의 필요. `dp-kafka-kor` 는 `prod-dp-kr-subnet01` 을 쓰지만 Composer 용은 별도일 수도.

### Step 2. 디렉토리 준비

```bash
cd dp-terraform
mkdir -p airflow/<gcp-project>/<env-name>
```

`airflow/dev-dp-project/dev-berriz-airflow/` 파일 5개를 template 삼아 복사한 뒤 각각 수정:

| 파일 | 용도 | 수정할 부분 |
|---|---|---|
| `provider.tf` | backend + provider 선언 | `backend prefix` 를 `terraform/airflow/<gcp-project>/<env-name>/state` 로 |
| `configs.tf` | project / region 상수 | `project` 를 자기 GCP 프로젝트 id 로 |
| `main.tf` | module 호출 + 팀별 config | `name`, `service_account`, `labels`, 팀별 pypi_packages, airflow_config_overrides 등 |
| `roles.conf` | 초기 Admin 리스트 | `ENV`, `PROJECT`, `LOCATION`, `ADMINS` |
| `migration.conf` | 이관 소스/대상 (이관 필요 시만) | `SOURCE_HOST`, `TARGET_ENV`, `TARGET_PROJECT`, `TARGET_LOCATION`, `TARGET_BUCKET` |

(migration 필요 없으면 `migration.conf` 는 생략 가능)

### Step 3. Init + Plan

```bash
cd airflow/<gcp-project>/<env-name>
terraform init
terraform plan   # 1 to add, 0 to change, 0 to destroy 확인
```

### Step 4. Apply

```bash
terraform apply
# 25~30분 소요. Composer env + GCS 버킷 + network attachment 등 생성됨
```

### Step 5. Post-apply

1. Airflow UI 접속 확인 (output `airflow_uri` 로)
2. GCS 버킷 이름 확인 → Deployer SA 요청서에 사용
3. **온프렘 metadata 이관** (pool/variable/connection) — [메타데이터 이관](#메타데이터-이관-온프렘--composer) 참고
4. **팀원 role 승격** (신규 로그인은 Viewer 로 자동 등록됨) — [팀원 role 세팅](#팀원-role-세팅) 참고
5. Secret Manager 에 팀별 secret 등록 (필요 시)
6. CI/CD 파이프라인 연결 (DAG/dbt 배포)

## 팀원 role 세팅

Airflow UI 는 IAP 로 접근 제어 되지만, 첫 로그인 시 자동 등록되는 role 은 `Viewer`. **팀 관리자 최소 1명**은 CLI 로 Admin 승격 필요. 이후 팀원 role 관리는 Admin 이 UI 에서 진행.

**절차:**

1. 팀 관리자가 Airflow UI 한 번 로그인 → Viewer 로 자동 등록
2. 다른 관리자 (또는 자기 자신) 이 로컬에서 아래 실행:

```bash
cd airflow/dev-dp-project/<env-name>
../../scripts/setup-roles.sh              # roles.conf 기반 Admin 승격
../../scripts/setup-roles.sh --list       # 등록된 사용자 확인
../../scripts/setup-roles.sh --dry-run    # 실제 실행 없이 예상 명령만
```

**`roles.conf` 형식:**

```bash
ENV=<env-name>
PROJECT=<gcp-project>
LOCATION=asia-northeast3

# 초기 Admin (사전에 Airflow UI 로그인 필수)
ADMINS=(
  someone@kakaoent.com
)
```

**주의:**
- 대상 사용자는 사전에 한 번은 Airflow UI 접속해야 자동 등록됨. 안 되어 있으면 "user not found" 실패 (다른 사용자는 계속 진행됨).
- Idempotent — 이미 Admin 인 사용자에게 재실행 안전.
- Composer 상태가 `RUNNING` 이어야 함 (UPDATING 중이면 실패).

## 메타데이터 이관 (온프렘 → Composer)

Airflow 2 (온프렘) 의 `pool` / `variable` / `connection` 을 신규 Composer 로 이관.

**핵심 flow:**

```
[온프렘 airflow2 서버]                    [로컬]              [Composer]
    │                                        │                     │
    │  qssh + airflow export                 │                     │
    ├───────────────────────────────────────>│                     │
    │  (pools.json, variables.json,          │                     │
    │   connections.json)                    │                     │
    │                                        │  gsutil cp          │
    │                                        ├────────────────────>│
    │                                        │                     │  gcloud composer run
    │                                        │                     │   pools/variables/connections import
    │                                        │                     │
```

### 준비

**scripts:**
- `airflow/scripts/migrate-from-airflow2.sh` — end-to-end 스크립트
- `airflow/scripts/qssh` — QueryPie qssh alias 를 non-interactive shell 에서 쓰기 위한 wrapper

**per-env config: `<env-dir>/migration.conf`**

```bash
SOURCE_HOST=dp-airflow2-integration-manager01
# SOURCE_USER 기본 deploy
SSH_CMD=../../scripts/qssh          # QueryPie 사용 시. 표준 ssh 로 되면 이 줄 생략

TARGET_ENV=<env-name>
TARGET_PROJECT=<gcp-project>
TARGET_LOCATION=asia-northeast3
TARGET_BUCKET=<composer-bucket-name>
```

### 실행

```bash
cd airflow/<gcp-project>/<env-name>

# 1) Dry-run (실행 순서만 확인)
../../scripts/migrate-from-airflow2.sh --dry-run

# 2) Export 만 (JSON 로컬 확인)
../../scripts/migrate-from-airflow2.sh --export-only
# → /var/folders/.../airflow-migrate-* 에 3개 JSON 저장, 경로 출력

# 3) Full run (export → preview → confirm → import)
../../scripts/migrate-from-airflow2.sh

# 4) 이미 export 된 JSON 으로 import 만
../../scripts/migrate-from-airflow2.sh --import-only <path/to/json/dir> [--yes]
```

### Gotcha — Airflow 3 variables import 요구사항

Airflow 2 → 3 이관 시 알아둘 것:

1. **JSON 에 중복 키가 있으면 안 됨**
   - Airflow 3 는 `object_pairs_hook` 로 duplicates 검사 → 있으면 import 실패
   - 온프렘 export 결과에 duplicate 있는 경우 있음 (특히 오래 운영된 env)
   - **해결:** Python `json.load` 후 `json.dump` 로 재직렬화하면 자동 dedupe (마지막 값 채택)

2. **Variable value 는 flat string 이어야 함**
   - Airflow 3 는 배열/객체를 값으로 직접 저장 못 함
   - 온프렘에 `["a", "b"]` 같은 array 값이 있으면 → JSON string 으로 변환 후 저장:
     ```python
     for k, v in data.items():
         if isinstance(v, (list, dict)):
             data[k] = json.dumps(v)
     ```
   - DAG 사용 시: `json.loads(Variable.get("...", default_var="[]"))` 로 다시 파싱

3. **Composer 는 `RUNNING` 상태여야 import 가능**
   - `UPDATING` 중이면 `gcloud composer environments run` 실패
   - pypi install 진행 중이면 완료 대기 필요

### 실전: variables import 실패 → 재시도 절차

`migrate-from-airflow2.sh` 는 아직 위 후처리 (dedupe + array→string) 자동화 안 됨.
Import 시 다음 에러 뜨면 수동으로 재시도:
```
ERROR: The "...variables.json" file contains multiple values for keys: [...]
```

**단계별 처리 (dev / prod 공통):**

```bash
# 0) --export-only 로 얻은 로컬 임시 디렉토리 경로 확인
DIR=/var/folders/.../airflow-migrate-<TS>

# 1) variables.json 후처리 (dedupe + array/dict → JSON string)
python3 -c "
import json
with open('$DIR/variables.json') as f:
    data = json.load(f)
converted = 0
for k, v in list(data.items()):
    if isinstance(v, (list, dict)):
        data[k] = json.dumps(v)
        converted += 1
with open('$DIR/variables.json', 'w') as f:
    json.dump(data, f, indent=2, ensure_ascii=False)
print(f'{len(data)} keys, {converted} array/dict values → JSON string')
"

# 2) 후처리한 파일을 새 timestamp 로 GCS 재업로드
TS=$(date +%Y%m%d-%H%M%S)
gsutil cp "$DIR/variables.json"   "gs://<bucket>/data/migration/$TS/variables.json"
gsutil cp "$DIR/connections.json" "gs://<bucket>/data/migration/$TS/connections.json"

# 3) Composer 로 재 import
gcloud composer environments run <env> \
  --project=<project> --location=<location> \
  variables import -- /home/airflow/gcs/data/migration/$TS/variables.json

gcloud composer environments run <env> \
  --project=<project> --location=<location> \
  connections import -- /home/airflow/gcs/data/migration/$TS/connections.json
```

### 실전: prod-berriz-airflow 이관 사례 (2026-07-30)

- Source: `dp-airflow2-manager03.dakao.io` (story prod, `airflow-story` 서버)
- Target: `prod-berriz-airflow` on `prod-dp-project`
- 결과:
  - Pool: **100개** 이관 성공
  - Variable: **56개** (dedupe/변환 후처리 후 성공)
  - Connection: **34개** 이관 성공
- 주의: story prod 이라 berriz 무관 항목 다수 (mysql_buydb*, mysql_userinven*, kw_adfit_*, charlie_* 등). Import 후 UI 에서 정리 필요

### 필터/편집

Export JSON 을 그대로 import 하면 다른 팀 관련 pool/variable 도 다 들어옴. 편집이 필요하면 JSON 을 로컬에서 `jq` 로:

```bash
# 특정 pool 만 남기기
jq '{sensor_pool, default_pool}' pools.json > pools-filtered.json

# 특정 variable 제외
jq 'del(.athlon_api_token, .loupe_admin_token)' variables.json > variables-filtered.json

# 특정 connection 만 남기기
jq '{"dev-dp-project", "dp-kafka-dev", "dp-kafka-dev-consumer"}' connections.json > connections-filtered.json
```

편집한 JSON 을 폴더에 담고 `--import-only <dir>` 로 실행.

### Connection secret 처리

Export 된 `connections.json` 에는 **password 평문 포함**. 두 방향:

**(a) 그대로 metadata DB 저장 (default, 편함):** 스크립트가 하는 방식. DAG 코드 변경 없이 사용 가능.

**(b) Secret Manager 로 옮김 (권장, 아직 미구현):** password 만 Secret Manager 로 push, connection 은 껍데기만 남김. Composer secret backend 가 자동 조회. DAG 코드 동일.

지금 스크립트는 (a) 만 지원. Secret Manager 이관은 별도 스크립트 or 수동 필요.

**⚠️ 취급 주의:**
- `connections.json` 은 slack/git/email 에 절대 올리지 마세요
- 이관 후 로컬 임시 파일 즉시 삭제
- 로컬 저장 위치: `/var/folders/.../airflow-migrate-<TS>/`

## 설정 상세

### `main.tf` 필수 인자

```hcl
module "this" {
  source = "../../../modules/gcp-composer"

  project         = local.project
  name            = "<env-name>"                                    # 변경 불가
  service_account = "<worker sa email>"                             # 변경 불가

  # 사전 network attachment 없으면 network + subnetwork 지정 (Composer 자동 생성)
  network    = "projects/dev-host-project-353511/global/networks/dev-host-vpc"
  subnetwork = "projects/dev-host-project-353511/regions/asia-northeast3/subnetworks/dev-dp-subnet-1"

  labels = {
    env     = "dev"
    service = "<team>"
  }
}
```

### `airflow_config_overrides` — 언제 뭘 넣나

**항상 넣는 것 (팀 정책)**

| 키 | 값 | 이유 |
|---|---|---|
| `core-default_timezone` | `Asia/Seoul` | Airflow 기본 UTC. Seoul 표기 원함 |
| `webserver-default_ui_timezone` | `Asia/Seoul` | UI 도 통일 |
| `core-max_active_tasks_per_dag` | `80` | 기본 16, DAG 당 병렬성 확보 |
| `core-max_active_runs_per_dag` | `8` | 기본 16, 동시 run 제한 |
| `core-parallelism` | `10` (dev) | 기본 32, dev 리소스 절약 |
| `api-composer_auth_user_registration_role` | `Viewer` | 신규 사용자 최소 권한 |
| `api-rbac_user_registration_role` | `Viewer` | 위와 동일 |

**DataHub 연동 시 (세트)**

| 키 | 값 | 참고 |
|---|---|---|
| `datahub-cluster` | `DEV` or `PROD` | plugin 이 참조 |
| `openlineage-disabled` | `False` | listener 활성. Airflow 기본은 True |
| `cosmos-use_dataset_airflow3_uri_standard` | `1` | dbt on airflow 시 표준 URI |

그리고 pypi 에 `acryl-datahub-airflow-plugin` 반드시 넣어야 실제 emission 됨 (config 만 있으면 dead config).

**Secret Manager backend 쓸 때**

```hcl
"secrets-backend" = "airflow.providers.google.cloud.secrets.secret_manager.CloudSecretManagerBackend"
```

- kwargs 는 default 로 두면 됨 (`connections_prefix=airflow-connections`, `variables_prefix=airflow-variables`, `sep=-`)
- Composer SA 에 `roles/secretmanager.secretAccessor` 필요
- 조회 순서: Secret Manager → env var → metastore DB
- **Write 는 안 바뀜**: Airflow UI/CLI 로 만든 Variable 은 여전히 메타DB. Secret Manager 는 read-only 소스
- Secret 저장은 Airflow 밖에서 (Terraform / gcloud / 콘솔). 이름: `airflow-connections-<conn_id>`, `airflow-variables-<var>`

**넣지 말아야 할 것 (Airflow / Composer default 랑 같음)**

- `core-load_examples` : Composer 3 이미 False
- `scheduler-catchup_by_default` : test-airflow3 관찰 결과 이미 False
- `core-default_pool_task_slot_count` : Airflow default 128

### `env_variables`

```hcl
env_variables = {
  DATAHUB_TELEMETRY_ENABLED = "false"
  PIP_EXTRA_INDEX_URL       = "https://asia-northeast3-python.pkg.dev/dev-dp-project-354904/dev-dp-python-registry/simple/"
}
```

- `PIP_EXTRA_INDEX_URL` : 내부 pypi (`apache-airflow-providers-kakaoent-dataplatform` 등 kakao 내부 wheel) 조회용

### `pypi_packages` — 뭘 넣나

**모두 넣는 것**
```
apache-airflow-providers-kakaoent-dataplatform==0.1.0  # 팀 공통 operator/hook (내부 registry)
```

**팀 요구에 따라**
```
apache-airflow-providers-amazon        # AWS 접근 (S3 등)
apache-airflow-providers-apache-kafka  # Kafka 소비
apache-airflow-providers-slack         # 알림
```

**DataHub 연동 시**
```
acryl-datahub-airflow-plugin           # lineage emission (config 3종 + 이 plugin 세트)
```

**dbt on Airflow 사용 시**
```
astronomer-cosmos                      # 통합 layer
dbt-core                               # dbt 자체
dbt-bigquery                           # 대상 adapter (Trino/Snowflake 필요하면 추가)
```

**버전 정책**
- 신규 env 만들 때 PyPI 에서 최신 stable 버전 사용
- Airflow 3.1.7 호환 확인 (provider `apache-airflow>=2.10.0` 만 선언한 경우 Airflow 3.1 이후 릴리즈 확인)

**⚠️ pip resolver backtracking 방지 pin (필수)**

Composer 3 base image 에 이미 있는 것들을 우리 새 provider 가 다르게 요구 → pip 이 40+ 버전 조합 시도 → **20분 timeout**. 다음 pin 이 필수:

```
apache-beam                       ==2.74.0     # base image 기본. GCP 관련 dep 30+ 개 결정.
apache-airflow-providers-http     ==6.0.2      # base image 기본. 여러 provider 의 transitive dep.
apache-airflow-providers-ssh      ==5.0.1      # base image 기본.
asyncssh                          ==2.22.0     # base image 기본.
```

**amazon/slack 추가 시 필요할 수 있는 추가 pin (경험적)**
```
google-cloud-aiplatform           ==1.149.0    # base image. slack/amazon 이 인접 dep 이라 backtracking 유발
# litellm 은 dbt-core 의 click>=8.3.0 요구와 충돌 (litellm==1.83.14 는 click==8.1.8 필수)
#   → litellm 명시적 pin 하지 말고 pip 이 range 안에서 알아서 선택하도록
```

이 pin 없이 apply 하면 pypi install 이 반드시 timeout 실패. 배후 원리는 아래 "Gotcha: pypi backtracking" 참고.

**한 방에 다 안 됨 — 배치별 install 필요**

경험적으로 pypi 를 한 번에 다 넣으면 dep 트리 폭발로 실패. 배치별 접근:

1. **배치 1: pin + 가벼운 것** — 위 pin 목록 + `kakaoent-dataplatform` (deps 없음)
2. **배치 2: dbt 생태계** — `dbt-core`, `dbt-bigquery`, `astronomer-cosmos`
3. **배치 3: DataHub + Kafka** — `acryl-datahub-airflow-plugin`, `apache-kafka provider`
4. **배치 4: 무거운 provider** — `amazon`, `slack` (aiplatform 추가 pin 필요할 수 있음)

각 배치는 `gcloud composer environments update --update-pypi-package=... --update-pypi-package=...` 로. 실패 시 로그에서 backtracking 대상 확인 → pin 추가 → 재시도.

**Composer 3 pre-installed (직접 명시 안 해도 되는 것 — 위 pin 대상 제외하고)**
```
apache-airflow-providers-{google, common-sql, common-io, common-compat,
                         ftp, imap, smtp, sqlite, celery, fab, cncf-kubernetes}
```

### `workloads` — 팀별 명시

**dev 최소 (dev-berriz-airflow 기준)**

```hcl
workloads = {
  dag_processor = { cpu = 1, memory_gb = 4, storage_gb = 1, count = 1 }
  scheduler     = { cpu = 1, memory_gb = 4, storage_gb = 1, count = 1 }
  triggerer     = { cpu = 1, memory_gb = 2, count = 1 }
  web_server    = { cpu = 1, memory_gb = 4, storage_gb = 1 }
  worker        = { cpu = 4, memory_gb = 8, storage_gb = 10, min_count = 1, max_count = 1 }
}
```

**prod 참고 (아직 미구축)**
- worker: min 2 / max 5~10 으로 오토스케일
- scheduler: count 2 (HA)
- `environment_size = "ENVIRONMENT_SIZE_MEDIUM"` 또는 LARGE

**참고**: module default 는 test-airflow3 baseline (worker max 2 오토스케일). root 에 명시하면 override.

## 알아둘 것 (Gotchas)

### 1. 변경 불가 필드
- `name`
- `location` (region)
- `service_account`
- 세팅 잘못 잡으면 **destroy + recreate 필요** — 확정 후 apply

### 2. Composer 3 는 subnet 하나만 사용 가능
- 온프렘 airflow2 처럼 큐별로 subnet 분리 못 함
- Outbound (S3 등) 은 dev-host-vpc 의 Cloud NAT + 정적 IP 7개 (`dev-host-asia-northeast3-nat-ip[1-7]`) 로 SNAT
- AWS 쪽 IP allowlist 는 이 7개
- 팀 간 outbound IP 분리 원하면 → 팀별로 다른 subnet 배정

### 3. DataHub config 3종 + plugin 은 세트
- config 만 있고 plugin 없으면 dead config (test-airflow3 에도 초기에 이 상황이었음)
- plugin 만 있고 config 없어도 lineage emission 대상 클러스터 결정 안 됨

### 4. Secret Manager backend 는 read-only
- Airflow UI/CLI 로 만든 Variable/Connection 은 여전히 메타DB 저장
- Secret Manager 에 넣으려면 Airflow 밖에서 (Terraform 등)
- 같은 이름을 두 곳에 두면 Secret Manager 우선 → UI 수정이 반영 안 됨

### 5. GCS 버킷 커스텀 이름
- Composer default 이름은 `<region>-<env-name>-<random>-bucket` — 길고 hash 붙어 지저분
- 우리는 `storage_config.bucket` + `google_storage_bucket` 리소스로 커스텀 이름 사용 (`<env-name>-bucket`)
- **주의:** `storage_config` block 은 `config {}` 안이 아니라 리소스 top-level 임 (google-beta v6.50 기준)
- 커스텀 버킷 사용 시 Composer 서비스 에이전트에 `roles/storage.objectAdmin` 필요 (Terraform 에서 자동 부여)

### 6. Composer default 신뢰
- Airflow 자체 default 와 Composer 이미지가 override 하는 default 가 있음
- 우리가 명시적으로 override 하지 않으면 Composer 것 사용
- test-airflow3 관찰 결과: `catchup_by_default`, `load_examples` 등은 이미 False 상태 (안 넣어도 됨)
- **default_pool slots**: Airflow 2 는 128 이었지만 Composer 3 의 Airflow 3 는 10000. 이관 시 128 로 덮어쓸지 유지할지 판단 필요

### 7. pypi backtracking (⚠️ 중요)

**증상:** `terraform apply` 또는 `gcloud composer environments update --update-pypi-package=...` 실행 시 "PyPI packages installation timed out" 에러.

**원인:**
- Composer 3 base image 에 이미 다수 패키지 pre-installed (`apache-airflow-providers-http==6.0.2`, `apache-beam`, `asyncssh` 등)
- 우리 새 provider (`amazon`, `slack`, `acryl-datahub-airflow-plugin`) 의 transitive dependency 가 pre-installed 것과 다른 버전 요구
- Pip 20.3+ resolver 는 "모두 만족하는 조합" 을 찾으려고 **40+ 버전 조합 시도** (backtracking)
- 각 시도마다 metadata download → 20분 hard timeout 초과

**빠른 진단 명령:**

```bash
gcloud logging read 'resource.labels.environment_name="<env>" (textPayload=~"Downloading|Collecting|Requirement already satisfied")' \
  --project=<project> --limit=50 --freshness=30m \
  --format='value(timestamp,textPayload)' | head -40
```

로그에서 특정 패키지 여러 버전 `Downloading` 이 반복되면 backtracking 확정.

**해결:** Composer base image 에 이미 있는 버전으로 **명시 pin**. `pypi_packages` 에 다음 추가:

```
apache-beam                       ==2.74.0
apache-airflow-providers-http     ==6.0.2
apache-airflow-providers-ssh      ==5.0.1
asyncssh                          ==2.22.0
```

이 pin 은 pip 에게 "탐색 금지, 이 버전 그대로 써" 를 알림 → 즉시 결정 → build 시간 극적 단축.

**Composer 이미지 업그레이드 시 (예: 3.1.7 → 3.2.x)** 새 base image 의 pre-installed 버전 확인해서 pin 재조정 필요. 조회 방법:

```bash
gcloud logging read 'resource.labels.environment_name="<env>" textPayload=~"Requirement already satisfied"' \
  --project=<project> --limit=20 --freshness=15m --format='value(textPayload)'
```

**장기적 solution:** 커스텀 Composer 이미지 (Cloud Build 로 pre-build) — pip 를 이미지에 넣어두면 in-cluster build 불필요.

## 내부 PyPI 패키지 (`apache-airflow-providers-kakaoent-dataplatform`)

### Registry

| 환경 | Registry | Location |
|---|---|---|
| dev | `dev-dp-python-registry` | `dev-dp-project-354904` / `asia-northeast3` |
| prod | `prod-dp-python-registry` | `prod-dp-project` / `asia-northeast3` |

**둘 다 PYTHON format 의 Artifact Registry.** Prod 는 초기에 없어서 새로 생성 필요했음:

```bash
gcloud artifacts repositories create prod-dp-python-registry \
  --project=prod-dp-project \
  --location=asia-northeast3 \
  --repository-format=python \
  --description="Internal Python wheels (data platform)"
```

### 소스 & 빌드

- 소스 저장소: `/Users/diana.46/PycharmProjects/dp-airflow-provider`
- `pyproject.toml` 의 version 필드에서 관리
- **Runtime dependencies 는 비워둠 (`dependencies = []`)** — Composer base image 가 이미 airflow / providers-http / providers-google 다 제공. 우리 wheel 이 이들 재선언하면 pip resolver 가 apache-airflow-core → apache-beam 등 다시 검증하다 timeout

Build:
```bash
cd /Users/diana.46/PycharmProjects/dp-airflow-provider
rm -rf dist/ build/
python -m build --wheel
```

### 배포 (dev + prod)

```bash
# twine + keyring 세팅 (한 번만)
pip install --user twine keyrings.google-artifactregistry-auth

# Dev registry 로 publish
python3 -m twine upload \
  --repository-url https://asia-northeast3-python.pkg.dev/dev-dp-project-354904/dev-dp-python-registry/ \
  dist/apache_airflow_providers_kakaoent_dataplatform-<version>-py3-none-any.whl

# Prod registry 로 publish (같은 wheel)
python3 -m twine upload \
  --repository-url https://asia-northeast3-python.pkg.dev/prod-dp-project/prod-dp-python-registry/ \
  dist/apache_airflow_providers_kakaoent_dataplatform-<version>-py3-none-any.whl
```

**같은 wheel = 같은 코드**를 두 registry 에 → dev/prod 재현성 유지.

### 버전 관리 정책

- **Semver** 준수 (0.1.0 → 0.1.1 patch, → 0.2.0 minor, → 1.0.0 stable)
- Downgrade 금지 (0.3.0 → 0.1.0 은 초기 리셋 시에만 허용, 배포된 이후엔 절대 안 됨)
- **초기 dev 단계**: 0.1.0 부터 시작 (0.x = pre-stable 표시)
- **stable 선언 시**: 1.0.0 으로 승격 (breaking change 없이 안정 API)
- 옛 버전 삭제는 tombstone (재사용 불가 기간) 감안. 안전한 건 **새 번호 발행**

### Registry 에서 버전 삭제 (특별한 경우만)

배포 이력 없고 리셋 필요 시:
```bash
for v in 0.1.0 0.2.0 0.2.1 0.3.0; do
  gcloud artifacts versions delete "$v" \
    --project=dev-dp-project-354904 --location=asia-northeast3 \
    --repository=dev-dp-python-registry \
    --package=apache-airflow-providers-kakaoent-dataplatform \
    --quiet
done
```

**주의:** 삭제 직후 같은 버전 재발행 시도하면 tombstone 로 실패할 수 있음. 안 되면 30분~24시간 대기 or 다른 버전 사용.

## 온프렘 airflow2 에서 안 옮기는 것들

Composer 3 (Airflow 3) 가 자체 관리하거나 3.x 에서 사라졌음:

- `[celery]`, `[celery_kubernetes_executor]` — Composer managed executor
- `[database]` (metadata DB) — Composer 관리
- `[kubernetes]` — Composer 관리
- `[kerberos]` — GCP IAM 인증
- `[smtp]` — 온프렘 릴레이 접근 불가 가능성, 필요 시 SendGrid 등 별도
- `[metrics]` StatsD — Cloud Monitoring 대체
- `[webserver]` 대부분 — Airflow 3 UI 재작성
- `worker_concurrency` per queue (celery 전용)
- `doopey` worker (Hadoop + Kerberos) — Composer 3 에서 불필요
- `additional_packages` 로 배포하던 커스텀 Hive provider wheel — 온프렘 Hive 접속 안 함

## 백엔드 / State

- 버킷: `gs://dev-dp-terraform-bucket` (versioning: Suspended)
- Prefix 규칙: `terraform/airflow/<gcp-project>/<env-name>/state`
- 각 env 마다 별도 prefix, 서로 영향 없음

## 참고: 모듈 인터페이스 (`modules/gcp-composer/`)

**필수 인자**: `project`, `name`, `service_account`

**네트워크 (둘 중 하나)**
- `composer_network_attachment` — 미리 만들어진 attachment 붙임
- `network` + `subnetwork` — Composer 가 attachment 자동 생성

**주요 선택 인자와 default**
| 변수 | default | 언제 override |
|---|---|---|
| `region` | `asia-northeast3` | 다른 region 쓸 때만 |
| `labels` | `{}` | 항상 (env / service 라벨) |
| `environment_size` | `ENVIRONMENT_SIZE_SMALL` | MEDIUM / LARGE 필요 시 |
| `image_version` | `composer-3-airflow-3.1.7-build.9` | 특정 버전 pin 필요 시 |
| `composer_internal_ipv4_cidr_block` | `100.64.128.0/20` | 다른 CIDR 필요 시 |
| `airflow_config_overrides` | `{}` | 항상 (팀 정책) |
| `env_variables` | `{}` | 항상 (`PIP_EXTRA_INDEX_URL` 등) |
| `pypi_packages` | `{}` | 항상 (providers) |
| `web_server_plugins_mode` | `ENABLED` | 거의 안 바꿈 |
| `airflow_metadata_retention_days` | `60` | prod 는 길게 |
| `workloads` | test-airflow3 baseline | 팀별 명시 권장 |
| `web_server_allowed_ip_ranges` | `0.0.0.0/0`, `::0/0` | 웹 UI 접근 제한 원할 때 |

**Output**: `airflow_uri`, `dag_gcs_prefix`, `gcs_bucket`, `name`, `id`

## 현재 env 상태

### test-airflow3 (기존, destroy 예정)
- Composer 콘솔에서 수동 생성됨. Terraform import 로 편입
- 로컬 코드는 있지만 PR/커밋에는 미포함
- 향후 정리 예정

### dev-berriz-airflow ✅ 완료
- **상태**: RUNNING (2026-07-29 완성)
- URL: https://<hash>-dot-asia-northeast3.composer.googleusercontent.com
- 커스텀 GCS 버킷: `dev-berriz-airflow-bucket`
- Admin: `diana.46@kakaoent.com`
- **PyPI 13개 설치 완료** — 배치 4번으로 나눠 pin 조정하며 성공
- **Metadata 이관 완료** — Pool 6, Variable 30, Connection 16 (from `dp-airflow2-integration-manager01`)
- 참고 config: `dp-terraform/airflow/dev-dp-project/dev-berriz-airflow/main.tf`

### prod-berriz-airflow ✅ 완료 (초기 apply)
- **상태**: RUNNING (2026-07-29 완성, apply 소요 38분)
- URL: https://77af7e7bd45c472aa3f395987d78e0fe-dot-asia-northeast3.composer.googleusercontent.com
- 커스텀 GCS 버킷: `prod-berriz-airflow-bucket`
- Admin: `diana.46@kakaoent.com`
- **PyPI 13개 설치 완료** — dev 검증된 pin 목록으로 **배치 없이 한 번에** 성공
- **Metadata 이관 완료** — Pool 100, Variable 56, Connection 34 (from `dp-airflow2-manager03.dakao.io` = story prod)
- **미완**: shared VPC (지금은 Composer 자체 network attachment 사용), `secretmanager.secretAccessor` 요청 중

## 남은 작업

### 🔴 단기 (팀 사용 시작 위해 필요)

**팀 handoff:**
- Berriz 팀원들 Airflow UI 로그인 (dev + prod 각각) → 자동 Viewer 등록
- Admin 이 UI 에서 팀원별 Op role 부여
- 테스트 DAG 하나 배포해서 실행 검증 (dev 부터)
- DataHub lineage 실제 emission 확인 (`datahub-cluster=DEV/PROD` + `openlineage-disabled=False` + `acryl-datahub-airflow-plugin` 세트가 실제 작동하는지)

**CI/CD:**
- Deployer SA 관리 (별도 파이프라인에서 관리 중)
- DAG 자동 배포 파이프라인 구성 (`git push` → `gsutil rsync ./dags/ gs://<bucket>/dags/`)

### 🟡 중기 (Prod 완성 · 안정화)

**Prod Secret Manager backend 활성:**
- `prod-berriz-airflow@` SA 에 `secretmanager.secretAccessor` role 도착 확인
- Secret Manager 에 `airflow-connections-*` / `airflow-variables-*` prefix 로 실제 secret 등록 검증

**Prod shared VPC 이관:**
- 담당자에 Composer 3 용 subnet 확정 (`prod-dp-kr-subnet01` 유력)
- Composer Service Agent (`service-<PROD_PROJECT_NUMBER>@cloudcomposer-accounts.iam.gserviceaccount.com`) 에 `compute.networkUser` on subnet 부여 요청
- 필요 시 `composer.sharedVpcAgent` on `prod-platform-host-project` 도 요청
- `prod-berriz-airflow/main.tf` 의 network/subnetwork 주석 해제
- `terraform destroy` + `terraform apply` (기존 metadata 는 pre-destroy export 후 재 import 필요)

**스크립트 개선 (`migrate-from-airflow2.sh`):**
- Variables JSON dedupe + array/dict → JSON string 자동 후처리 로직 추가 (지금은 수동으로 python 실행)
- Import 하나 실패해도 다음 import 계속 진행 (skip-on-error 옵션)
- Secret Manager 로 connection secret 자동 이관 (지금은 metadata DB 저장)

**환경 정리:**
- `test-airflow3` destroy (초기 template, 이제 불필요)
- 이관된 metadata 중 berriz 무관 항목 UI 에서 삭제 (특히 prod 는 story 서버 것이라 100+ 개 무관)

### 🟢 장기 (별개 이슈)

**운영 안정성:**
- Pool/Variable/Connection 정기 export 스케줄 (재해 복구용)
- Composer 상태 모니터링/알림 (Cloud Monitoring)
- Composer 이미지 minor upgrade 정기 진행 (3.1.7 → 3.1.8+ 릴리즈 나올 때)

**미래 옵션 검토:**
- **Airflow 3.2/3.3 필요 시**: Composer 이미지 대기 (릴리즈 3~6개월 소요) or 특정 팀만 self-host
- **커스텀 Composer 이미지** (Cloud Build pre-build) — pypi backtracking 근본 해결. 여러 팀 env 늘어나면 검토 가치 있음
- **Kakaoent-dataplatform provider stable 승격** — 0.1.x → 1.0.0 (API 안정화 후)
