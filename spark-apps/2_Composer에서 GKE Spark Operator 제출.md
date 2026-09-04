---
title: "Composer 에서 GKE Spark Operator 로 Spark 잡 제출"
tags:
  - spark
  - spark-operator
  - gke
  - composer
  - airflow
status: verified
created: 2026-08-06
updated: 2026-08-07
---

# Composer 에서 GKE Spark Operator 로 Spark 잡 제출

> **검증 질문**: Composer 의 DAG 에서 GKE 의 Spark Operator 로 Spark 잡을 제출할 수 있는가? 무엇을 준비해야 하는가?
> **답**: 가능하다. **오퍼레이터 하나 + YAML 하나**면 되고, **권한을 추가로 받을 필요가 없다.** dev 에서 실제 제출·완주 확인 (2026-08-06 ~ 08-07).

## 한 줄 요약

```
SparkApplication YAML  +  GKECreateCustomResourceOperator  =  제출 끝
```

인증 코드는 쓰지 않는다. Composer 워커의 서비스 계정으로 자동 인증된다.

---

## 1. 배경 — CRD / CR / Operator

Kubernetes 에는 기본 리소스 종류(`Pod`, `Deployment` …)가 정해져 있고, **CRD**(CustomResourceDefinition)를 설치하면 새 종류를 추가할 수 있다. 그렇게 추가된 종류의 개별 오브젝트가 **CR**(Custom Resource)이다. Spark Operator 를 설치하면 `SparkApplication` 이라는 CRD 가 등록되고, 그때부터 `kubectl get sparkapplications` 가 된다.

**CR 자체는 아무것도 실행하지 않는다.** API 서버에 저장된 선언(데이터)일 뿐이고, 실제로 pod 를 만드는 건 클러스터에 상주하는 Operator 다.

```
CRD        "SparkApplication 이라는 종류가 있다"   (스키마 등록)
CR         "이런 Spark 잡을 원한다"                 (선언 = 그냥 데이터)
Operator   CR 을 watch 하다가 → 드라이버 pod 생성   ← 여기가 실행
```

그래서 **제출 성공 ≠ 실행 시작** 이다. 이 구분이 이 문서 전체의 핵심이다.

### ⚠️ "Operator" 가 두 가지를 뜻한다

| | Airflow Operator | Kubernetes Operator |
|---|---|---|
| 정체 | DAG 태스크 하나를 구현한 파이썬 클래스 | 클러스터에 상주하며 CR 을 watch 하는 컨트롤러 |
| 사는 곳 | Composer 워커 | GKE `spark-operator` 네임스페이스 |
| 이번에 쓴 것 | `GKECreateCustomResourceOperator` | kubeflow spark-operator 2.5.1 |
| 준비 주체 | 우리가 DAG 에 작성 | 이미 클러스터에 설치돼 있던 것 |

---

## 2. 제출은 두 축뿐이다

| 축 | 담당 | 정하는 것 |
|---|---|---|
| **SparkApplication YAML** | 무엇을 돌릴지 | 이미지, jar, `mainClass`, 리소스, 드라이버 SA, namespace |
| **`GKECreateCustomResourceOperator`** | 어디에·누구로 던질지 | 클러스터(project·location·cluster), 신분(워커 SA), CRD API 경로 |

DAG 전문이다. 태스크 하나뿐이다.

```python
SPARK_APPLICATION = """
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  generateName: diana-test-spark-pi-      # 이름은 API 서버가 붙인다 (§4-3)
  namespace: bq-dev                        # operator 가 watch 하는 ns 여야 한다 (§4-1)
  labels:
    app.kubernetes.io/managed-by: airflow
spec:
  type: Scala
  mode: cluster
  image: "apache/spark:3.5.1"
  mainClass: org.apache.spark.examples.SparkPi
  mainApplicationFile: "local:///opt/spark/examples/jars/spark-examples_2.12-3.5.1.jar"
  sparkVersion: "3.5.1"
  restartPolicy:
    type: Never
  driver:
    cores: 1
    memory: 512m
    serviceAccount: ingestion-sa
  executor:
    instances: 1
    cores: 1
    memory: 512m
"""


@dag(dag_id="berriz_0900_spark_submit_check", schedule=None, catchup=False, ...)
def build_dag():
    GKECreateCustomResourceOperator(
        task_id="submit_spark_application",
        project_id="{{ var.value.gke_spark_project }}",    # 어느 클러스터
        location="{{ var.value.gke_spark_location }}",
        cluster_name="{{ var.value.gke_spark_cluster }}",
        yaml_conf=SPARK_APPLICATION,                        # 무엇을 돌릴지
        custom_resource_definition=True,                    # CRD 경로로 가라
        namespaced=True,
    )


build_dag()
```

`custom_resource_definition=True` 를 빼면 오퍼레이터가 빌트인 리소스용 경로(`create_from_yaml`)로 가서 "모르는 종류" 로 실패한다. CRD 오브젝트는 `CustomObjectsApi` 로 가야 한다.

내부적으로 나가는 요청은 이것 하나다. `group`/`version`/`plural` 은 YAML 의 `apiVersion`·`kind` 에서 도출된다.

```
POST /apis/sparkoperator.k8s.io/v1beta2/namespaces/bq-dev/sparkapplications
Authorization: Bearer <워커 SA 의 OAuth 토큰>
```

> `mainApplicationFile` 의 `local://` 은 "**컨테이너 이미지 안에 이미 있는 파일**" 이라는 뜻이다. SparkPi 는 Apache Spark 가 공식 이미지에 넣어 배포하는 예제 잡이라 별도 업로드나 GCS 권한 없이 돈다. 실제 잡은 `berriz-ingestion-drain` 처럼 `gs://` 로 팀 jar 을 참조하면 된다.

---

## 3. 인증 — 코드를 쓰지 않는다

Airflow Variable 3개로 **어느 클러스터**인지만 알려주면, **누구로** 붙을지는 환경이 정한다.

```
gcp_conn_id 기본값 "google_cloud_default"  (빈 커넥션 = ADC 사용 선언)
   ↓
ADC → GKE 노드 metadata server → Workload Identity
   ↓
dev-berriz-airflow@dev-dp-project-354904.iam.gserviceaccount.com 의 OAuth 토큰
   ↓
Authorization: Bearer <토큰>
```

오퍼레이터는 kubeconfig 파일을 만들지 않는다. `project_id`/`location`/`cluster_name` 으로 GKE API 를 호출해 **API server endpoint 와 CA 인증서를 조회**하고, 그것 + 위 토큰으로 클라이언트를 메모리에서 조립한다. `gcloud container clusters get-credentials` 가 하는 일을 API 로 직접 하는 셈이다.

endpoint 와 CA 는 비밀이 아니다(CA 는 서버 검증용 공개키, endpoint 는 IP). 그래서 **kubeconfig 를 Secret Manager 에 넣는 접근은 필요 없다** — 그 방향은 exec plugin 바이너리나 k8s SA 장기 토큰을 새로 도입하게 되어 오히려 관리 부담이 늘어난다.

네트워크는 인증과 별개 축이다. Composer 3 워커는 우리 VPC 가 아닌 Google 관리 테넌트에서 돌고, 대상 클러스터의 **public endpoint 로 HTTPS** 로 나간다. 같은 VPC 를 공유해서 되는 게 아니다.

---

## 4. 함정 3개

### 4-1. namespace — operator 가 watch 하는 곳이어야 한다

```bash
kubectl -n spark-operator get deploy spark-operator-controller \
  -o jsonpath='{.spec.template.spec.containers[0].args}'
# [... "--namespaces=bq-dev" ...]
```

dev 의 controller 는 **`bq-dev` 만** 본다. `spark-jobs` 네임스페이스가 존재하고 SA 까지 있지만 watch 대상이 아니다. 거기에 제출하면:

- CR 은 정상 생성된다 (제출은 성공)
- `status` 가 영원히 비어 있고 드라이버 pod 도 안 뜬다

증상이 "조용한 무반응" 이라 원인 찾기가 어렵다. **prod 는 `--namespaces` 를 따로 확인해야 한다.**

### 4-2. 권한은 이미 있다 — 그런데 `auth can-i --as` 는 `no` 라고 답한다

워커 SA 의 프로젝트 IAM 에는 `container.*` 역할이 없어 보인다. 하지만 **`roles/composer.worker` 를 펼치면 다 들어 있다**:

| 권한 | 용도 |
|---|---|
| `container.clusters.get` / `.connect` | endpoint·CA 조회 및 접속 |
| `container.thirdPartyObjects.{create,get,list,update,delete}` | **CRD 오브젝트(SparkApplication) CRUD** |
| `container.pods.getLogs` | 드라이버 로그 조회 |

GKE 의 IAM authorizer 가 이 권한으로 통과시키므로 **RBAC RoleBinding 도 필요 없다.**

여기서 두 번 헷갈릴 수 있다.

1. **역할 이름 목록만 보면 없는 것처럼 보인다.** `gcloud iam roles describe <role>` 로 permission 을 펼쳐서 확인할 것.
2. **`kubectl auth can-i --as <SA>` 는 `no` 를 반환한다.** SubjectAccessReview 라서 RBAC 만 평가하고 IAM authorizer 를 못 본다. 정확히 보려면 그 SA 의 토큰으로 직접 물어야 한다.

```bash
gcloud auth print-access-token --impersonate-service-account=<SA>   # 이 토큰으로 kubeconfig 구성
kubectl --kubeconfig=<위 kubeconfig> auth can-i create sparkapplications -n bq-dev
# yes
kubectl --kubeconfig=<위 kubeconfig> create -f spark-app.yaml --dry-run=server
# sparkapplication.../... created (server dry run)
```

`--dry-run=server` 는 실제 생성 없이 CRD 스키마와 admission webhook 까지 검증한다. 배포 전 확인에 가장 확실한 방법.

### 4-3. 재실행하면 이름이 충돌한다 → `generateName`

이 오퍼레이터는 `apply` 가 아니라 **`create`** 다. 같은 `metadata.name` 으로 다시 제출하면 409 `AlreadyExists` 로 실패한다. 게다가 409 가 재시도 대상 상태코드에 포함돼 있어서 5회 백오프 후 **느리게** 실패한다.

`metadata.name` 대신 `metadata.generateName: diana-test-spark-pi-` 를 쓰면 API 서버가 매번 유니크한 이름을 붙여준다 (`diana-test-spark-pi-6dqb4`). 코드가 아니라 YAML 한 줄로 해결된다.

대가: **Airflow 는 생성된 이름을 모른다.** 태스크 로그에 `Resource was created` 만 찍힌다. 그래서 조회는 라벨로 한다.

---

## 5. 확인 방법

이 DAG 은 CR 생성까지만 한다. **태스크 success = 제출 성공이고, 잡이 성공했다는 뜻은 아니다.**

```bash
gcloud container clusters get-credentials dev-dp-kafka-gke \
  --region asia-northeast3 --project dev-dp-project-354904

# 제출됐나 + operator 가 픽업했나
kubectl -n bq-dev get sparkapplications -l app.kubernetes.io/managed-by=airflow
#   STATUS 가 비어 있으면 → operator 가 안 읽은 것 (§4-1)
#   SUBMITTED / RUNNING / COMPLETED → 정상

# Spark 잡 출력
kubectl -n bq-dev logs <name>-driver | grep "Pi is roughly"

# CR 은 생겼는데 pod 가 안 뜰 때
kubectl -n bq-dev describe sparkapplication <name>                          # Events
kubectl -n spark-operator logs deploy/spark-operator-controller --tail=100  # operator 시점

# 정리
kubectl -n bq-dev delete sparkapplication -l app.kubernetes.io/managed-by=airflow
```

### 무엇이 남고 무엇이 안 남나

| 리소스 | 잡 완료 후 |
|---|---|
| CR (SparkApplication) | 남는다 (`timeToLiveSeconds` 미설정) |
| **드라이버 pod** | `Completed` 로 남는다 → 로그 조회 가능 |
| **executor pod** | **사라진다.** 드라이버가 종료 시 정리한다. CR 의 `status.executorState` 에 이름만 기록으로 남는다 |

pod 를 찾다가 헷갈리기 쉬운 지점이 둘 있다.

- executor 를 찾으면 없다 (위 표)
- **CR 을 지우면 드라이버 pod 도 같이 사라진다.** 드라이버 pod 의 `ownerReferences` 가 CR 을 가리키므로 캐스케이드 삭제된다

pod 가 사라져도 로그는 Cloud Logging 에 남는다 (클러스터에 `WORKLOADS` 로깅 활성).

```bash
gcloud logging read 'resource.type="k8s_container"
  AND resource.labels.namespace_name="bq-dev"
  AND resource.labels.pod_name:"diana-test-spark-pi"' \
  --project dev-dp-project-354904 --limit 20 \
  --format="value(timestamp,resource.labels.pod_name,textPayload)"
```

### 누가 제출한 건지 구분하기

| 알고 싶은 것 | 보는 곳 | 신뢰도 |
|---|---|---|
| 클러스터 내부 컨트롤러가 만든 건가? | `ownerReferences` — Airflow 제출은 **비어 있고**, ScheduledSparkApplication 이 만든 것은 그 SSA 를 가리킨다 | 구조적 |
| 정확히 누가 요청했나? | 감사 로그 `principalEmail` + `callerIp` | 확정적 |
| 우리 DAG 것만 골라보기 | 라벨 `app.kubernetes.io/managed-by=airflow` | 관례 (누구나 붙일 수 있음) |

```bash
gcloud logging read 'resource.type="k8s_cluster"
  AND resource.labels.cluster_name="dev-dp-kafka-gke"
  AND protoPayload.resourceName:"sparkapplications"
  AND protoPayload.methodName:"create"' \
  --project dev-dp-project-354904 --limit 6 \
  --format="table(protoPayload.authenticationInfo.principalEmail,
                  protoPayload.requestMetadata.callerIp)"
```

실제 결과 예시:

| principalEmail | callerIp | 정체 |
|---|---|---|
| `system:serviceaccount:spark-operator:spark-operator-controller` | `10.152.78.145` | SSA 가 스케줄대로 만든 것 (클러스터 내부) |
| `diana.46@kakaoent.com` | `121.65.239.28` | 로컬에서 돌린 리허설 (사람 계정) |
| `dev-berriz-airflow@...` | Composer 워커 egress IP | **Composer 에서 나간 것** |

---

## 6. 환경 (dev 실측)

| 항목 | 값 |
|---|---|
| Composer 환경 | `dev-berriz-airflow` (asia-northeast3), `composer-3-airflow-3.2.2-build.0` |
| 워커 SA | `dev-berriz-airflow@dev-dp-project-354904.iam.gserviceaccount.com` |
| 대상 GKE 클러스터 | **`dev-dp-kafka-gke`** (이름과 달리 Spark Operator 가 여기 있다) |
| 네임스페이스 | **`bq-dev`** |
| Spark Operator | kubeflow spark-operator 2.5.1 (`spark-operator` ns) |
| CRD | `sparkoperator.k8s.io/v1beta2` |
| 드라이버 k8s SA | `ingestion-sa` (bq-dev 의 기존 Spark 잡이 쓰는 것) |
| control plane | public `34.64.112.10`, master authorized networks 비활성 |
| DNS endpoint | `allowExternalTraffic: false` → Composer 에서 이 경로는 사용 불가 |
| prod 대응 클러스터 | `prod-dp-spark-gke01` (`prod-dp-project`), **private endpoint** |

필요한 Airflow Variable 3개 (UI → Admin → Variables):

| Variable | dev 값 |
|---|---|
| `gke_spark_project` | `dev-dp-project-354904` |
| `gke_spark_location` | `asia-northeast3` |
| `gke_spark_cluster` | `dev-dp-kafka-gke` |

---

## 7. 검증 결과

로컬 `airflow dags test` 로 dev 클러스터에 실제 제출 (2026-08-07).

```
DagRun success (9.5초, 태스크 1개)
  submit_spark_application → "Resource was created"

클러스터:
  diana-test-spark-pi-6dqb4   SUBMITTED → RUNNING → COMPLETED
  드라이버 로그: Pi is roughly 3.1400957004785024
```

같이 확인된 것:

- `apache/spark:3.5.1` (Docker Hub) pull 정상
- `ingestion-sa` 로 executor 생성 정상
- CRD 스키마 / mutating webhook 통과, operator 즉시 픽업
- `generateName` 이 CRD 오브젝트에도 정상 동작

### 아직 확인 못 한 것

- **Composer 워커 → `34.64.112.10` egress.** 위 검증은 로컬 노트북에서 한 것이다. authorized networks 가 비활성이라 열려 있어야 하지만 워커 네트워크는 다르다. Composer 에서 실패한다면 1순위 후보.
- **Airflow 3.2.2 / google provider 20.0.0 에서의 동작.** 로컬은 3.1.7 / 19.5.0 이었다. 공개 API 만 썼지만 확인은 배포 후.

---

## 8. 다음 단계

### 실전 파이프라인이면 오퍼레이터를 바꾼다

지금 DAG 은 "제출만" 한다. 그래서 Airflow 는 잡의 성공/실패를 모르고, 드라이버 로그도 안 받고, 태스크를 kill 해도 잡은 계속 돈다. Spark 잡을 파이프라인의 한 단계로 쓰려면 `SparkKubernetesOperator` (cncf) 가 제출 + 로그 스트리밍 + 완료 대기 + 정리를 한 태스크에서 다 한다. GKE 인증 변종이 없어서 서브클래스 한 줄이 필요하다.

```python
class GKESparkKubernetesOperator(GKEOperatorMixin, SparkKubernetesOperator):
    """GKEOperatorMixin 이 hook 을 GKE 인증용으로 덮는다.
    google provider 가 GKEStartPodOperator 를 만든 것과 같은 조합."""
```

이때는 잡 스펙을 `.yaml` 파일로 빼고 `application_file=` 로 참조하는 편이 낫다 (그 오퍼레이터는 파일 내용도 Jinja 렌더한다 — `GKECreateCustomResourceOperator` 의 `yaml_conf_file` 은 렌더하지 않으니 혼동 주의).

### prod 로 갈 때

- `prod-dp-spark-gke01` 은 private endpoint → `use_internal_ip=True` 필요 (DNS endpoint 경로는 IAM `container.clusters.connect` 게이트 + dev 는 external traffic 차단)
- operator 의 `--namespaces` 확인 후 네임스페이스 맞추기

### 레포 쪽

- `apache-airflow-providers-cncf-kubernetes` 를 `requirements.txt` 에 추가해야 한다. google provider 의 GKE 오퍼레이터가 import 하므로 없으면 **DAG 파싱 자체가 실패**한다. `kubernetes` 파이썬 클라이언트는 이 프로바이더의 의존성으로 따라온다.
- Composer 환경의 `pypiPackages` 에는 cncf 가 없다 → 이미지 번들에 의존. 배포 후 import error 가 나면 terraform `pypi_packages` 에 추가.
- ⚠️ dev 환경은 Airflow 3.2.2 / google provider 20.0.0 인데 레포 `requirements.txt` 는 3.1.7 로 pin 돼 있다. 로컬 pytest 통과가 Composer 동작을 보장하지 않는 구간이 있다.

---

## 참고

- 코드: `berrizdata-airflow-dags` → `dags/berriz/berriz_0900_spark_submit_check.py`
- [[1_사용중인_spark_job]] — 이관 대상 Spark 앱 인벤토리
- [[스케줄러/8_Composer 권한 및 인증]] — Composer 의 3계층 권한 모델
