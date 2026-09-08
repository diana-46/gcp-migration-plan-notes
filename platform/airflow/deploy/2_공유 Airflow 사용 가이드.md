# 공유 Airflow 사용 가이드

**대상**: 통합 Airflow 환경에 DAG · dbt 를 올리는 모든 팀
**작성 근거**: `dev-data-airflow` 통합 운영 전환 (2026-08) + 실제 설정 실측
**관련**: [[0_Airflow Provider 배포 파이프라인 설계]] · [[1_Airflow Provider 배포 셋업 런북]]

> 위키 게시용 초안. 팀 상황에 맞게 다듬어 쓸 것.

---

## 0. 요약 — 지켜야 할 것

| | 규약 |
|---|---|
| `dag_id` | 상위 디렉터리명을 접두사로 (`dags/berriz/` → `berriz_...`) |
| Connection · Variable | `<서비스>_<이름>` 접두사 |
| DAG 파일 top-level | **I/O 금지** (`Variable.get()`, 네트워크, DB 조회) |
| 배포 경로 | 자기 레포 디렉터리에만 (`dags/<레포명>/`, `data/dbt/<레포명>/`) |
| `rsync -d` | 자기 경로 안에서만 |
| 패키지 추가 | 환경 전체 공유 → 요청 창구를 거쳐 dev 선검증 |
| 알림 채널 | Airflow 환경 단위 |

---

# Part I. 이 환경은 공유입니다

## 1.1 환경 구성

| 환경 | 용도 | 운영 |
|---|---|---|
| `dev-data-airflow` | 개발 | **모든 팀 통합** |
| `prod-berriz-airflow` | 운영 | 분리 |

dev 는 여러 팀이 같은 스케줄러 · 워커 · 메타DB · GCS 버킷을 씁니다.
**한 팀의 실수가 다른 팀에 그대로 전파됩니다.** 아래 규약은 그것을 줄이기 위한 것입니다.

## 1.2 공유되는 것 · 격리되는 것

| | 공유 | 격리 |
|---|---|---|
| `dag_id` 네임스페이스 | ✅ 전역 (평평함) | — |
| Connection · Variable · Pool | ✅ 전역 | — |
| Python 패키지 (`pypi_packages`) | ✅ 환경 전체 하나 | — |
| 워커 SA · 그 권한 | ✅ 하나 | — |
| 스케줄러 파싱 시간 | ✅ | — |
| `core-parallelism` | ✅ (dev 는 10) | — |
| GCS DAG · dbt 경로 | — | ✅ 레포별 디렉터리 |
| Airflow RBAC | ✅ 전역 role | (DAG 별 `access_control` 로 가능) |

**격리되는 것이 GCS 경로뿐**이라는 점이 이 문서의 출발점입니다.

---

# Part II. 이름 규약

## 2.1 `dag_id` — 상위 디렉터리명 접두사

```
dags/berrizdata-airflow-dags/berriz/berriz_0011_dw_hourly.py   dag_id="berriz_0011_dw_hourly"
dags/berrizdata-airflow-dags/bizberry/bizberry_0101_dw_hourly.py  dag_id="bizberry_0101_dw_hourly"
```

**Airflow 의 DagBag 은 평평합니다.** 디렉터리가 `dag_id` 를 네임스페이스하지 않으므로,
두 레포가 같은 `dag_id` 를 쓰면 **하나가 조용히 사라집니다.** 에러도 안 납니다.

서비스 디렉터리명을 접두사로 강제하면 레포 안에서는 충돌이 구조적으로 불가능합니다.
레포 간에는 **"서비스 디렉터리명은 조직 내 유일"** 이라는 합의에 의존합니다 —
다른 레포의 `dag_id` 는 CI 에서 볼 수 없어 자동으로 막을 수 없습니다.

> 테스트로 강제할 수 있습니다. `berrizdata-airflow-dags/tests/test_dag_integrity.py` 참고.

## 2.2 Connection · Variable — `<서비스>_<이름>`

```
❌ api_token           다른 팀과 겹칩니다
✅ berriz_loupe_api_token
✅ bizberry_gke_spark_cluster
```

전역 네임스페이스라 이름이 겹치면 **덮어씁니다.** UI 에서 누구나 수정할 수 있어
"내가 안 건드렸는데 값이 바뀌었다" 가 실제로 일어납니다.

공용으로 쓰는 것(`google_cloud_default` 등)은 **문서에 명시하고 함부로 고치지 않습니다.**

## 2.3 Pool

Pool 도 전역입니다. 한 팀이 slot 을 다 쓰면 다른 팀이 대기합니다.
동시 실행 제어가 필요하면 **팀별 pool 을 따로 만들어** 씁니다.

---

# Part III. DAG 작성 규약

## 3.1 top-level 에서 I/O 를 하지 않습니다 — 가장 중요

DAG 파일은 스케줄러가 **주기적으로 반복 파싱**합니다. 모듈 레벨에 무거운 작업이 있으면
파싱마다 실행되어 **환경 전체의 파싱이 느려집니다.**

```python
# ❌ 파싱마다 메타DB 를 때립니다
PROJECT = Variable.get("gke_spark_project")

# ✅ 템플릿으로 넘겨 실행 시점에 해석
PROJECT = "{{ var.value.gke_spark_project }}"
```

같은 이유로 모듈 레벨의 네트워크 호출 · BigQuery 조회 · 큰 파일 읽기를 금지합니다.
필요하면 태스크 안에서 합니다.

## 3.2 필수 속성

| 속성 | 이유 |
|---|---|
| `catchup=False` | `True` 면 `start_date` 부터 과거 구간이 한꺼번에 실행됩니다 |
| `owner` | 실패했을 때 누구를 찾을지 |
| `tags` | UI 에서 팀·용도로 거르기 |
| `description` | 목록에서 구분 |

의도한 backfill 은 수동으로 트리거합니다.

## 3.3 DAG 끼리 import 하지 않습니다

DAG 폴더가 `sys.path` 에 들어갑니다. 두 레포가 같은 모듈명을 쓰면 어느 쪽이 잡힐지 모릅니다.

```
dags/repoA/common/utils.py
dags/repoB/common/utils.py     from common import utils  →  ???
```

**공용 코드는 provider 패키지(`apache-airflow-providers-kakaoent-dataplatform`)로 올립니다.**
그것이 provider 레포의 존재 이유입니다.

---

# Part IV. 배포 규약

## 4.1 자기 디렉터리에만 배포합니다

```
gs://<bucket>/dags/
├── airflow_monitoring.py          ← Composer 가 만든 것. 건드리지 않음
├── <레포명>/                       ← 그 레포 소유
└── <다른레포>/

gs://<bucket>/data/dbt/
├── <레포명>/
│   └── target/manifest.json       ← cosmos 가 읽는 것
└── <다른레포>/
```

**`rsync -d` 는 destination 에만 있는 파일을 지웁니다.** 버킷 루트를 대상으로 하면
다른 레포의 DAG 과 `airflow_monitoring.py` 까지 삭제됩니다. 반드시 자기 경로로 한정합니다.

dbt 는 `-d` 를 아예 쓰지 않습니다. Composer 워커가 **실행 중에 참조**하므로 배포 도중
파일이 사라지면 돌고 있는 태스크가 깨집니다.

## 4.2 올릴 것만 올립니다

워크스페이스를 통째로 rsync 하고 제외 패턴으로 거르는 방식은 위험합니다.
새 파일이 생기면 조용히 함께 올라갑니다.

> 실제 사고: `google-github-actions/auth` 가 만드는 `gha-creds-*.json` 이 GCS 에 올라갔습니다.
> 점으로 시작하지 않아 dotfile 제외 패턴을 통과했습니다.
> (개인키 없는 WIF 설정 파일이라 즉시 위험은 없었으나 있을 자리가 아닙니다)

**포함 목록 방식**으로 뒤집습니다. 스테이징 디렉터리에 올릴 것만 모아서 rsync 합니다.

## 4.3 dbt → DAG 순서

dbt 를 **먼저** 배포합니다. cosmos 는 워커의 `manifest.json` 을 읽어 DAG 을 렌더하므로,
DAG 을 먼저 올리면 manifest 에 없는 모델을 참조해 parse 에 실패합니다.

---

# Part V. 패키지 추가

## 5.1 환경 전체가 목록 하나를 공유합니다

```hcl
# dp-terraform/airflow/configs.tf
"dev-data" = {
  pypi_packages = {
    "apache-airflow-providers-kakaoent-dataplatform" = "==0.1.0"
    "astronomer-cosmos"                              = "==1.15.0"
    "dbt-bigquery"                                   = "==1.12.0"
  }
}
```

**팀별 분리가 구조적으로 불가능합니다.** 한 팀이 다른 버전을 요구하면 한쪽이 양보해야 합니다.

이미 겪은 사례: `dbt-bigquery` 와 Airflow 공식 constraint 가 상호 배타적이라
`pip install -r requirements.txt` 가 resolve 되지 않습니다.

```
dbt-bigquery 1.9~1.11.1  → google-cloud-storage<3.2
dbt-bigquery 1.11.2+     → google-cloud-aiplatform>=1.148.0
constraints-3.1.7        → google-cloud-storage==3.9.0, google-cloud-aiplatform==1.135.0
```

## 5.2 절차

```
① 요청           패키지·버전·필요 이유를 창구에 전달
② dev 선검증     dev-data-airflow 에 먼저 적용해 다른 팀 DAG 이 깨지지 않는지 확인
③ prod 반영
```

- **버전은 반드시 pin 합니다.** 범위로 두면 어느 날 조용히 올라가 다른 팀이 깨집니다.
- 환경 업데이트는 20분 이상 걸리고 그동안 다른 팀 업데이트가 블록됩니다.
- pin 없이 추가하면 pypi resolver backtracking 으로 timeout 실패합니다.

---

# Part VI. 알림

채널은 **Airflow 환경 단위**로 나눕니다. 그 환경을 쓰는 사람이 그 채널을 봅니다.

| 환경 | 보는 사람 |
|---|---|
| dev | 그 환경에 배포하는 개발자 |
| prod | 운영 당번 · 서비스 오너 |

**dev 를 팀별로 쪼개지 않습니다.** 누가 언제 무엇을 배포했는지 서로 보이는 것이
공유 환경에서는 자산입니다. 파싱이 느려지거나 Variable 이 덮어써졌을 때 원인을 추적할 수 있습니다.

알림에 레포명·환경·브랜치가 찍히므로 여러 레포가 한 채널을 써도 구분됩니다.

---

# Part VII. 알아두어야 할 한계

지금 구조로는 막을 수 없는 것들입니다. **규약이 아니라 인지 사항**입니다.

## 7.1 워커 SA 공유 — 데이터 권한 경계가 없습니다

```
dev-data-airflow@dev-dp-project-354904   ← 모든 팀의 모든 DAG 이 이걸로 실행
```

**팀 A 의 DAG 이 팀 B 의 BigQuery 데이터셋을 읽고 쓸 수 있습니다.** 실수든 고의든 막을 수 없습니다.
워커 SA 에 준 권한 = 모든 DAG 작성자에게 준 권한입니다.

민감한 작업은 `KubernetesPodOperator` 로 별도 SA 에서 실행하는 방법이 있으나
DAG 작성이 무거워집니다.

## 7.2 RBAC 전역 — 서로의 DAG 을 트리거·clear 할 수 있습니다

Airflow 기본 role(Viewer/User/Op/Admin)은 **전역**입니다. `Op` 를 받으면 모든 팀 DAG 을 건드립니다.
현재 초기 Admin 승격만 자동화돼 있고 나머지 role 관리는 UI 수동입니다.

DAG 별 `access_control` 로 제한할 수 있으나 DAG 마다 선언해야 합니다.

## 7.3 자원 경쟁

`core-parallelism` 이 환경 전체 공유입니다 (dev 는 10). 한 팀이 태스크를 몰아 돌리면
다른 팀이 대기합니다. Pool 로 완화할 수 있으나 Pool 자체도 전역입니다.

---

## 부록. 레포별 상세 문서

| 레포 | 문서 |
|---|---|
| `dp-airflow-provider` | `.github/README.md` — 패키지 릴리즈 |
| `berrizdata-airflow-dags` | `.github/README.md` — DAG 배포 |
| `berrizdata-dbt` | `.github/README.md` — dbt 배포 |
| `dp-terraform` | `airflow/README.md` — 환경 생성·관리 (인프라 담당자용) |
