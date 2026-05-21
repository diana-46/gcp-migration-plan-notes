# 스케줄러 PoC — Composer 3 호환성 검증

> **목적**: 현재 사내 Airflow 셋업이 **Cloud Composer 3 (Airflow 3.1.7)** 에 옮겨질 수 있는지 검증.
> 회의 미션 답: "Composer 가 우리 use case 를 잘 받아들이는가".

## 컨텍스트

| 항목 | 값 |
|---|---|
| Composer 환경 | 3.1.7-build (이미 셋업 완료) |
| Airflow 버전 | **3.1.7 확정** (2 버전은 후보 X) |
| BQ project | _(채울 자리)_ |
| Region | asia-northeast3 |
| 사내 셋업 분석 대상 | `~/WebstormProjects/data-platform-settings/playbooks/roles/airflow2` |
| airflow-dags | `~/PycharmProjects/airflow-dags` |

## 검증 항목 (우선순위순)

| # | 항목 | 결정 영향 | 회의 시연 가치 | 시간 |
|---|---|---|---|---|
| 1 | **`airflow-dags` Airflow 3 호환성 grep** | ⭐⭐⭐ (코드 수정량 추정) | ⭐ (자료성) | 0.5일 |
| 2 | **Simple DAG 1개 Composer 3 실행** | ⭐ (baseline) | ⭐⭐⭐ (가장 빠른 시연) | 0.5일 |
| 3 | **자체 wrapper Operator → PyPI / Artifact Registry** | ⭐⭐⭐ ("사내 코드 가능?") | ⭐⭐⭐ | 1일 |
| 4 | **Worker / Queue / Pool 패턴 검증** | ⭐⭐ (sensor:40 deferrable 전환) | ⭐⭐ | 1~2일 |
| 5 | **DAG Bundles 동작 확인** | ⭐⭐ (배포 흐름) | ⭐⭐ | 0.5~1일 |
| 6 | **인증 — Google Workspace 통과 가능?** | ⭐⭐⭐ (Composer vs Self 결정) | ⭐ (인터뷰 위주) | 0.5일 (사내 정책 확인) |
| 7 | **실제 athlon DAG 1개 Composer 실행** ⭐ 시연용 | ⭐⭐ (검증) | ⭐⭐⭐ (회의 1순위) | 2~3일 |

총 예상: **~7~10일** (1~2주 sprint)

## 진행 상태

| # | 항목 | 상태 | 노트 |
|---|---|---|---|
| 1 | airflow-dags Airflow 3 호환성 grep | ✅ 완료 | [[01_airflow3_compat_grep]] |
| 2 | **DAG 배포 (GCS sync / Bundle / multi-repo)** | ✅ 완료 | [[02_dag_deployment]] |
| 3 | **PyPI 자체 패키지 install** | ✅ 완료 | [[03_custom_operator_pypi]] |
| 4 | **Queue / Worker / Pool 패턴** | ✅ 완료 | [[04_worker_pool_queue]] |
| 5 | **모니터링 / 알림 / callback** | ⬜ 대기 | `05_monitoring_alerts.md` |

각 항목은 진행하면서 별도 노트로 분리.

---

## 항목별 상세

### Step 1. airflow-dags Airflow 3 호환성 grep

**목표**: 코드 수정 인벤토리. "몇 곳에서 무엇이 깨지는지" 답.

**작업**:

```bash
cd ~/PycharmProjects/airflow-dags

# 폐기 패턴
grep -rn "SubDagOperator\|SmartSensor" .
grep -rn "schedule_interval=" .         # → schedule= 로
grep -rn "sla=timedelta\|'sla':" .     # SLA 제거
grep -rn "HiveOperator\|HDFS\|S3ToHdfs" .  # Hive 영역 → 폐기
grep -rn "Kerberos\|kerberos" .

# Task SDK 위반 가능성 (task 안에서 내부 모듈 import)
grep -rn "from airflow.models import" .
grep -rn "from airflow.utils.db" .

# 사내 operator 인벤토리
ls operators/
grep -rn "class.*Operator(" operators/
grep -rn "class.*Sensor(" operators/

# 사내 DB / 네트워크 의존
grep -rn "create_engine\|SQLAlchemy" .
grep -rn "kakaocorp.com\|onkakao.net" .
```

**결과 정리**:
- 폐기 패턴 발견 위치 / 개수
- 사내 operator 목록 + 의존성
- Task SDK 위반 가능 코드 위치
- 사내 DB / 네트워크 직결 위치
- Airflow `upgrade-check` 결과 (별도 도구)

**의문 / 발견**: (진행하면서)

---

### Step 2. Simple DAG 1개 Composer 3 실행

**목표**: Composer 환경 sanity check + DAG 배포 흐름 체감.

**DAG**:

```python
# simple_hello.py
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="hello_composer",
    schedule="@daily",
    start_date=datetime(2026, 5, 1),
    catchup=False,
    tags=["poc"],
) as dag:
    BashOperator(task_id="hello", bash_command="echo hello composer 3")
```

**작업**:
- DAGs GCS bucket 에 업로드
- Airflow UI 에서 DAG 보이는지 (1~2분 대기)
- 수동 실행 → 성공 확인
- Task log 확인

---

### Step 3. 자체 wrapper Operator → PyPI / Artifact Registry ✅ 완료

> 상세 결과: [[03_custom_operator_pypi]]

**검증 질문**: 사내 custom operator(wheel) 를 Composer 3 에 install 가능한가?

**결론**: ✅ **가능. 단 Artifact Registry + pip.conf 조합 필수.**

**발견한 함정 3개** (PoC 핵심 가치):

1. **wheel `file://` 참조 차단** — UI/CLI 둘 다 PEP 508 URL specifier 거부. self-hosted 식 GCS `file://` trick 불가 → AR 강제
2. **`PIP_EXTRA_INDEX_URL` env var 가 빌드 컨텍스트에 전달 안 됨** — 환경 변수 설정만으로 안 풀림. `gs://<env-bucket>/config/pip/pip.conf` 직접 업로드 필수 (콘솔 UI 진입점 없음 ⚠️). 실패 메시지가 `(from versions: none)` 으로 위장돼 디버깅 함정
3. **`keyrings.google-artifactregistry-auth` base image 에 없음** — requirements 에 명시 필수

**검증 흐름** (실제 통과):
```
wheel build → twine upload → AR push
  → pip.conf 업로드 (config/pip/pip.conf)
  → PyPI 패키지: keyrings.google-artifactregistry-auth + kakao-airflow-poc==0.1.0
  → SA 에 artifactregistry.reader
  → DAG trigger → log: "안녕 kakao from Composer (kakao-airflow-poc 0.1.0)" ✅
```

**회의 메시지**: 사내 wheel 운영은 **AR + pip.conf** 셋업 1회면 그 뒤로는 `twine upload` + PyPI 패키지 등록만으로 자동화 가능. 기존 sendbag-wheel 패턴 대비 인증/버전관리/공유 측면에서 깔끔.

**시연 자료**: AR 패키지 등록 화면 + DAG UI parsing 성공 화면 + task log success 화면 3장.

---

### Step 4. Worker / Queue / Pool 패턴 검증 ✅ 완료

> 상세 결과: [[04_worker_pool_queue]]

**검증 질문**: 사내 5종 queue (`hadoop`/`cloud`/`http`/`sensor`/`doopey`) 가 Composer 3 에서 어떻게 매핑되나?

**핵심 발견 5가지**:

1. ⚠️ **`task.queue='foo'` 가 묵음 실패** — `default` 외 queue 의 task 는 에러 없이 영원히 queued. 마이그레이션 시 모든 `queue=` 파라미터 제거 필수.
2. ⚠️ **KubernetesExecutor cold start 7분 46초 측정** — Autopilot 노드 provisioning + image pull. Idle 시 노드 즉시 deprovision → warm start 불가. **분 단위 task 에 사실상 사용 불가**.
3. ✅ **Celery worker autoscale 정상** (1→3 까지 관찰). 모니터링 탭의 "Celery Executor 작업자" 화면이 kubectl 의 실용적 대체.
4. ✅ **Deferrable sensor + Triggerer 정상** — worker 0 점유로 처리. `sensor:40` 패턴의 답.
5. ✅ **Airflow Pool 정상** — slot 수 정확히 강제. capacity 우회 가능.

**5종 queue 매핑 결과**:
- `hadoop` / `doopey` → 폐기 (Hive 종료)
- `cloud` / `http` → Celery worker 흡수 + **`queue=` 파라미터 제거 필수**
- `sensor:40` → deferrable Sensor + Triggerer (글로벌 `default_deferrable=True` 추천) ⭐
- heavy task → KubernetesExecutor 비추, Celery worker 사양 상향 권장

**마이그레이션 추정 (queue 영역)**: 2.5~4.5주

**회의 메시지**: 사실상 사내 케이스는 **Celery + Triggerer 만으로 충분**. K8sExecutor 안 써도 됨. 단 `queue=` 파라미터 제거가 묵음 함정의 핵심 작업.

---

### Step 5. DAG Bundles 동작 확인

**목표**: Airflow 3 의 DAG Bundles 가 Composer 에서 어떻게 동작하나.

**작업**:
- Composer 3 의 DAG Bundles 옵션 활성화 확인
- bundle 1개 정의 (git repo or OCI image)
- 환경 별 다른 bundle 사용 시도 (dev / prod)
- 기존 GCS sync 와의 차이 확인

---

### Step 6. 인증 — Google Workspace 통과 가능?

**목표**: 사내 LDAP 인증을 Composer IAP + Google Workspace 로 대체 가능한지 확인.

**작업** (대부분 인터뷰 / 사내 정책 확인):
- 카카오엔터 Google Workspace 계정 = 사내 ID 인지?
- Composer IAP 통과 가능한지 본인 계정으로 테스트
- IAM Role (`composer.user`) 부여 후 UI 접속 확인
- 보안팀 / IDP 팀과 LDAP 대체 가능성 협의

---

### Step 7. 실제 athlon DAG 1개 Composer 실행 (회의 시연용)

**목표**: 회의 1분 시연 ammunition.

**작업**:
- airflow-dags 에서 가장 simple DAG 선정
- 사내 의존성 최소화 (또는 mock)
- 자체 wrapper 포함 (Step 3 활용)
- Composer 에서 end-to-end 실행
- **화면 녹화** 또는 스크린샷 패키지

→ "기존 코드 그대로 Composer 3 에서 돌아갑니다" 시연.

---

## 회의 시연 시나리오 (최종 결과물)

**1~2분 데모**:

1. airflow-dags 의 작은 DAG 보여줌 (코드)
2. Composer 3 Airflow UI 에서 같은 DAG 동작 (Graph view)
3. Task log 에서 성공 확인
4. 자체 wrapper operator 가 거기서 실행됨 (사내 코드도 OK)

**1슬라이드 요약**:

- 호환성: ~80% 그대로 / ~15% 손봐야 (SLA / SubDAG / Hive) / ~5% 인프라 의존 (사내 DB / 네트워크)
- 자체 wrapper: **Artifact Registry + pip.conf 셋업 후 OK** (함정 3개 — [[03_custom_operator_pypi]] 참조)
- Worker queue 5종 → Composer 패턴 매핑 가능
- 마이그레이션 추정: 6~12주

---

## 관련 노트

- [[../1_개요]] — 스케줄러 메인 결정
- [[../2_Cloud Composer vs Self-managed 비교]] — 사내 셋업 호환성 분석 (Section "현 사내 Airflow 셋업 → Composer 3 호환성")
- [[../4_Queue 라우팅과 Pod 스펙 설정]] — Queue / Pod 패턴
- [[../8_Composer 권한 및 인증]] — 인증 관련
- [[../9_Airflow Asset과 Dataset]] — Airflow 3 Asset (Step 4~5 에 활용)
- [[../../애슬론/PoC/README]] — 별도 PoC (dbt / Asset-Centric / 패러다임 검증) — **본 PoC 통과 후** 진행


권한
네트워크 설정 상세 가능한지
dbt 테스트
