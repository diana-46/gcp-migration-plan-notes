---
title: "Cloud Composer 인프라 보고"
status: draft
created: 2026-06-25
청자: 인프라팀 담당자
용도: Composer 환경 / 네트워크 / IAM / 비용 / 운영 부담 협의
---

# Cloud Composer 인프라 보고

## 0. 한 줄 요약

> 사내 athlon (Neptune) + Hive 기반 데이터 파이프라인을 **Cloud Composer 3 (Airflow 3.1.7)** 위에 올림.
> dev 환경 (`test-airflow3`) PoC 검증 완료. **prod 환경 신규 구축 + 연관 GCP 리소스 (BQ / GCS / IAM / Secret) 설정** 협의 필요.

---

## 1. Composer 환경 구성

### 1-1. PoC dev 환경 (검증 완료)

| 항목 | 값 |
|---|---|
| Composer 환경명 | `test-airflow3` |
| Region | `asia-northeast3` (서울) |
| Composer image | `composer-3-airflow-3.1.7-build.9` |
| Airflow | 3.1.7 |
| Project (service) | `dev-dp-project-354904` |
| Project (host VPC) | `dev-host-project-353511` |
| Composer SA | `dev-dp-airflow@dev-dp-project-354904.iam.gserviceaccount.com` |
| Composer bucket | `gs://dev-airflow-test-bucket` |

### 1-2. 환경 분리 (dev / prod)

| 환경 | Composer 이름 | bucket | BQ project | 상태 |
|---|---|---|---|---|
| dev | `test-airflow3` | `gs://dev-airflow-test-bucket` | `dev-dp-project-354904` | ✅ 운영 중 |
| prod | (신규 구축 필요, 예: `prod-airflow3`) | `gs://prod-airflow-bucket` (예시) | `prod-dp-project-xxx` (예시) | ❌ 미구축 |

**환경 사이즈 권장** (PoC 측정 기반):
- dev: small (현재 PoC 와 동등)
- prod: medium 이상 권장 — DAG ~150개, worker 동시성 + dbt subprocess 메모리 고려

### 1-3. Worker / Executor

PoC 검증 결과:

- **기본**: CeleryExecutor (worker pod 공유)
- **메모리 격리 필요한 task**: KubernetesExecutor (task 마다 별도 pod)
  - 사례: dbt 모델이 많은 TaskGroup → worker OOM 위험 → K8s pod 로 격리
  - Composer worker 이미지 그대로 사용 (별도 이미지 빌드 불필요)
- **executor 라우팅**: Airflow 3 의 hybrid executor (task 별 `executor="KubernetesExecutor"` 지정)

### 1-4. 환경 변수 (필수)

| 변수명 | 용도 |
|---|---|
| `DBT_TARGET` | dbt 의 profile target 선택 (dev / prod) |
| `DBT_PROJECT_NAME` | dbt 프로젝트 디렉토리명 (`dbt_test`) |
| `DAGS_FOLDER` | Composer 자동 mount 경로 (`/home/airflow/gcs/dags`) |
| `DATAHUB_ENV` | DataHub URN 환경 식별자 (`DEV` / `PROD`) |

---

## 2. 연관 GCP 리소스

### 2-1. BigQuery

| 항목 | 권장 |
|---|---|
| Dataset | 환경별 분리 (`dbt_test` / `dbt_prod`) |
| Region | `asia-northeast3` (Composer 와 동일) |
| **Slot reservation** | prod 500~1500 slot (현재 Hive 쿼리량 기반 추정. 측정 필요) |
| Edition | Enterprise (reservation + autoscaling) 권장 |
| 권한 | Composer SA 에게 `roles/bigquery.dataEditor` + `roles/bigquery.jobUser` |

### 2-2. Cloud Storage

| 버킷 용도 | 예상 경로 | 비고 |
|---|---|---|
| Composer DAG bucket | `gs://<env>-airflow-bucket` | Composer 자동 생성 |
| dbt 산출물 (Avro export 등) | `gs://<env>-data-artifacts/neptune_poc/` 또는 도메인별 | PoC 에선 Composer bucket 안에 둠 |
| 로그 archive (선택) | `gs://<env>-airflow-logs-archive` | 30일 후 archive lifecycle |

권한: Composer SA → `roles/storage.objectAdmin` (해당 bucket 만)

### 2-3. Artifact Registry

| 용도 | 위치 |
|---|---|
| 공통 Operator Python package | `apache-airflow-providers-kakaoent` (사내 명명) |
| Region | `asia-northeast3` |
| 접근 | Composer 의 `pip install` 시 사용 |

### 2-4. Secret Manager

| Secret | 용도 |
|---|---|
| `dbt-profile` | dbt profiles.yml 시크릿 (필요 시) |
| `datahub-token` | DataHub REST emit 토큰 |
| `slack-webhook` | 알림 채널 |
| (사내 시스템 API 키들) | 사내 system 별 |

Composer SA → `roles/secretmanager.secretAccessor`

---

## 3. IAM / 권한 모델

### 3-1. Composer Worker SA

GCP 측 권한:

| Role | 대상 |
|---|---|
| `roles/composer.worker` | Composer 환경 |
| `roles/bigquery.dataEditor` | dbt 데이터셋 |
| `roles/bigquery.jobUser` | BQ project |
| `roles/storage.objectAdmin` | Composer bucket + 산출물 bucket |
| `roles/secretmanager.secretAccessor` | 사용할 시크릿 |
| `roles/artifactregistry.reader` | 공통 Operator package |
| `roles/logging.logWriter` | Cloud Logging |
| `roles/monitoring.metricWriter` | Cloud Monitoring |

### 3-2. WIF (Workload Identity Federation)

GitHub Actions 가 JSON key 없이 GCP 인증:

| 자산 | 용도 |
|---|---|
| WIF Pool + Provider | GitHub repo 와 binding |
| Deploy SA (`gha-deployer@<project>`) | `roles/storage.objectAdmin` on Composer bucket |
| GitHub repo secrets | `GCP_WIF_PROVIDER`, `GCP_SA`, `COMPOSER_BUCKET` |

→ 두 레포 (`dbt-test`, `dbt-test-airflow-dags`) 가 main 머지 시 자동 sync.

### 3-3. 사용자 권한 (DE / 플랫폼팀)

| 역할 | 권한 |
|---|---|
| DE 도메인 팀 | Composer UI 조회 + DAG trigger, BQ dataset 의 자기 도메인 조회 |
| 플랫폼팀 | Composer 환경 관리, IAM, Slot reservation 등 인프라 변경 |
| 매니저 / 디렉터 | 환경 인증 + 보고 dashboard |

---

## 4. 비용 추정

> PoC dev 환경 metric + GCP Pricing 기반 **러프 추정**. 인프라팀과 정확한 산정 필요.

### 4-1. 항목별 (월간)

| 항목 | dev | prod | 합계 |
|---|---|---|---|
| **Composer 환경** | $300~$500 (small) | $1,500~$2,500 (medium) | ~$2,000~$3,000 |
| **BigQuery slot reservation** | (on-demand) | $2,000~$5,000 (500~1500 slot) | ~$2,000~$5,000 |
| **BigQuery storage** | $100 | $500~$1,500 | ~$600~$1,600 |
| **BigQuery on-demand queries** | $100 | $200~$500 | ~$300~$600 |
| **GCS (Composer + 산출물)** | $20 | $50~$200 | ~$70~$220 |
| **Artifact Registry** | < $5 | < $10 | < $15 |
| **Secret Manager** | < $5 | < $10 | < $15 |
| **Networking (egress)** | $20 | $100~$500 | ~$120~$520 |
| **합계 (월)** | | | **~$5,100~$10,970** |

→ **연간 약 $61K ~ $132K (KRW 약 8천만 ~ 1억 7천만 원)**

### 4-2. 인프라팀에 확인 / 협의할 항목

- [ ] Composer 환경 사이즈 정확 견적 (Composer 의 노드 사양 + 시간당 가격)
- [ ] BQ slot reservation Edition (Standard vs Enterprise) 선택
- [ ] Composer 의 GKE 노드 가격 (Composer 3 는 일부 GKE 비용 포함)
- [ ] Networking 비용 (현재 VPC 와의 peering / egress)

### 4-3. dual-run 기간의 추가 비용

마이그레이션 중 Neptune (Hive) + GCP 동시 운영:
- 약 4~6개월 dual-run
- GCP 비용 + 기존 Hive 운영비 = **약 1.5~2배 일시 증가**
- Phase 3 완료 후 Hive 정리 시 정상화

### 4-4. 최적화 옵션

- **Slot reservation Flex / autoscaling** — idle 시간 절감
- **Partition expiration** — 옛 데이터 자동 long-term storage 전환
- **GCS lifecycle** — 30일 후 archive
- **Composer worker autoscaling** — 야간 worker 감축

---

## 5. 사내 시스템 연동

### 5-1. athlon DB (현재 운영 중)

- **현재**: athlon 내부 DB (DAG 정의 / ETL 메타 보관)
- **GCP 이관 시점**: athlon DB 자체를 Cloud SQL 로 이전할지, 사내 DB 유지하고 GCP 에서 접속할지 결정 필요
- **마이그레이션 완료 후**: athlon DB 의존도 ↓ (DAG / ETL 정의는 git 으로). athlon DB 의 잔존 역할 확정 필요

### 5-2. DataHub

- 사내 운영 중인 DataHub 인스턴스
- Composer 의 `acryl-datahub-airflow-plugin` 1.6.0 으로 lineage emit
- Composer ↔ DataHub 네트워크 연결 필요

### 5-3. Slack

- 운영 알림용 webhook
- Secret Manager 에 webhook URL 보관

### 5-4. Git 호스팅 (사내 GitHub)

- 사내 git enterprise (예상)
- WIF Provider 가 사내 GitHub 도메인 인식하도록 설정

---

## 6. 운영 / 모니터링

### 6-1. Cloud Logging

| 로그 종류 | 보존 |
|---|---|
| Composer worker logs | 30일 (Composer 기본) |
| DAG task logs | 30일 또는 GCS archive |
| BQ query logs | INFORMATION_SCHEMA (90일) |

### 6-2. Cloud Monitoring

권장 대시보드:
- DAG 성공률 / 실패율
- Worker pod CPU / 메모리 / OOM 빈도
- BQ 슬롯 사용률 / 큐 대기 시간
- 시간대별 task 분포 (자정 spike 확인용)

알림 (Alert Policy):
- DAG fail 비율 임계치 초과
- BQ 슬롯 100% 사용 지속
- Composer worker pod restart 빈도

### 6-3. SLA / SLO (협의 필요)

- DAG 실행 가용성 목표
- 일일 ETL 완료 시간 (SLA)
- 사고 복구 시간 (RTO)

---

## 7. 보안

### 7-1. 네트워크

- Composer 3 의 Private IP 모드 적용 권장 (외부 IP 노출 X)
- VPC Service Controls 적용 여부 검토 (사내 정책 따라)
- BQ / GCS 접근은 VPC 내부 또는 Private Google Access

### 7-2. 시크릿

- 모든 외부 인증 정보는 Secret Manager
- DAG 코드에 평문 시크릿 금지
- Composer Variables / Connections 도 Secret Manager backend 권장

### 7-3. 감사 로그

- Cloud Audit Logs 활성화 (Composer 환경 변경 / IAM 변경)
- BQ data access logs (선택, 비용 큼)

---

## 8. 배포 / CI/CD

### 8-1. 두 레포의 GCS sync

| 레포 | GCS 경로 | 빈도 |
|---|---|---|
| `dbt-test` (dbt project) | `gs://<bucket>/dags/dbt_projects/dbt_test/` | 모델 변경 시 |
| `dbt-test-airflow-dags` (DAG) | `gs://<bucket>/dags/` | DAG 변경 시 |

### 8-2. GitHub Actions 흐름

```
git push to main
    ↓ (paths filter)
GitHub Actions workflow
    ↓ WIF 인증 (gha-deployer SA)
    ↓ dbt parse + ls (lint)
    ↓ syntax check (Python ast)
gcloud storage rsync / cp
    ↓
Composer scheduler 자동 sync (1~2분)
```

### 8-3. 인프라팀 협의 항목

- [ ] WIF Pool / Provider 생성
- [ ] gha-deployer SA 권한
- [ ] 사내 GitHub repo 와 binding
- [ ] CI/CD pipeline 표준 (사내 정책 따라)

---

## 9. 리스크 (인프라 관점)

| 리스크 | 영향 | 완화 |
|---|---|---|
| Composer 환경 사이즈 부족 → DAG 지연 | 중 | autoscaling + 모니터링 |
| BQ slot reservation 부족 → 쿼리 대기 | 중 | reservation 점진 증액 |
| Worker pod OOM (PoC 에서 발견) | 중 | K8s executor 로 무거운 task 격리 |
| dbt 버전 mismatch (PoC 에서 발견) | 저 | Composer 의 dbt 버전 ↔ CI 의 dbt 버전 lock |
| Composer 사고 시 fallback | 저 | Phase 3 이전엔 Neptune 항상 fallback 가능 |
| 네트워크 / VPC 충돌 | 중 | host project / VPC peering 사전 협의 |

---

## 10. 인프라팀이 해야 할 것 (요청 사항)

### 10-1. 즉시 (1주 내)

- [ ] PoC 환경 (`test-airflow3`) 의 비용 metric 공유
- [ ] prod Composer 환경 사이즈 견적
- [ ] BQ slot reservation 견적 (Standard vs Enterprise)
- [ ] 사내 GitHub ↔ WIF 셋업 가능 여부 확인

### 10-2. Phase 0 (1개월 내)

- [ ] prod Composer 환경 구축 (예: `prod-airflow3`)
- [ ] prod 의 BQ project / dataset / slot reservation
- [ ] WIF + Deploy SA 설정
- [ ] 네트워크 (VPC peering, Private IP, Firewall)
- [ ] Composer 모니터링 / 알림 대시보드
- [ ] Artifact Registry repo
- [ ] Secret Manager 시크릿 등록

### 10-3. 의사결정 협의

- [ ] **prod 환경 사이즈** (small / medium / large)
- [ ] **BQ Edition** (Standard / Enterprise / Enterprise Plus)
- [ ] **네트워크 구성** (Public IP / Private IP / VPC SC)
- [ ] **비용 예산 승인 절차**
- [ ] **사내 시스템 연동 패턴** (athlon DB / DataHub / Slack)

---

## 11. 백업 자료

| 노트 | 인프라 관점에서의 가치 |
|---|---|
| [[애슬론/5_Neptune SQL 변환의 dbt-BigQuery 대체 검토]] | PoC 결론 + 기술 실현성 |
| [[애슬론/6_마이그레이션 플랜]] | Phase 별 일정 / 리소스 |
| [[스케줄러/15_관리 레포 인벤토리]] | 운영 모델 / 레포 구조 |
| [[dbt/6_배포와 환경 분리]] | 배포 자동화 / 실측 함정 5가지 |
