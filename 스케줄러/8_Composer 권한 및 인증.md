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
updated: 2026-05-14
---

# Composer 권한 및 인증

> Cloud Composer가 권한·인증을 어떻게 처리하는지, Self-managed로 갈 때는 무엇을 직접 구성해야 하는지. 사용자 로그인, task 권한, GCP 리소스 접근까지 3개 레이어로 정리.

## 큰 그림: 3개의 권한 레이어

```
[Airflow UI 접근]   → 사용자가 UI에 들어올 수 있는가?
        ↓ (Google IAP + IAM)
[Airflow 내부 RBAC] → 들어와서 무엇을 할 수 있는가? (DAG trigger / 변경 / 보기)
        ↓ (Airflow Role)
[Task → GCP 리소스]  → task가 BigQuery / GCS 등을 호출할 수 있는가?
        ↓ (Workload Identity → GCP SA)
```

| 레이어 | Composer 처리 | Self-managed에서 해야할 일 |
|---|---|---|
| 1. UI 접근 | IAP 자동 통합 (Google 계정) | IAP / OAuth2 Proxy 직접 구성 |
| 2. Airflow RBAC | Composer 3은 Google ID → Airflow Role 자동 매핑 (Composer 2도 부분 지원) | RBAC 직접 관리 (DB 또는 LDAP/OIDC) |
| 3. Task → GCP | Workload Identity 자동 (환경 SA) | Workload Identity 직접 설정 |

## 레이어 1: Airflow UI 접근

### Composer 2 / 3

- **Identity-Aware Proxy (IAP)** 가 Airflow UI 앞단에 자동 배치됨
- 사용자는 **Google 계정**으로 인증 (조직 SSO 자동 연동)
- IAP가 통과시켜야 Airflow UI 페이지에 닿음
- IAP 접근 권한은 **GCP IAM** 으로 부여:
  - `roles/composer.user` — UI 접근 + DAG 보기/트리거 가능
  - `roles/composer.admin` — 환경 설정 변경 가능 + UI 접근
  - `roles/composer.environmentAndStorageObjectAdmin` — DAG 코드 업로드 가능

### Self-managed (GKE)

직접 만들어야 함. 옵션:

| 옵션 | 장점 | 단점 |
|---|---|---|
| **IAP + Ingress** | Composer와 동일한 UX, Google SSO | Ingress + IAP 설정 손이 감 |
| **OAuth2 Proxy + Google** | 간단한 OAuth 흐름 | RBAC와 별도 매핑 필요 |
| **OIDC (Keycloak 등)** | 사내 IDP 통합 | IDP 자체 운영 비용 |
| **Basic Auth (개발용만)** | 제일 간단 | 운영 절대 비권장 |

> 카카오엔터처럼 사내 Google Workspace 있으면 **IAP가 가장 자연스러움** (Composer와 동일 UX).

## 레이어 2: Airflow 내부 RBAC

들어와서 어떤 행동을 할 수 있나. Airflow 자체의 권한 모델.

### Airflow 기본 Role (5종)

| Role | 권한 |
|---|---|
| **Admin** | 모든 권한 (Variable/Connection/User 관리 포함) |
| **Op** | DAG 트리거, Pause, Variable/Connection 보기·수정 |
| **User** | DAG 트리거, Pause, 자기 task 로그 보기 |
| **Viewer** | 읽기 전용 |
| **Public** | 비로그인 (거의 안 씀) |

### Composer 2 — Google ID → Airflow Role 매핑

- IAP 통과한 사용자가 Airflow에 들어가면 **이메일 기준으로 Airflow User 자동 생성**
- 기본은 **Op** 역할 (DAG 트리거 가능, Connection은 못 만짐)
- 더 강한 권한은 **Airflow UI Admin → Users**에서 수동 매핑 필요
- → **2단계 권한 관리** (GCP IAM + Airflow RBAC) 가 단점

### Composer 3 — 매핑 개선

- Google IAM Role → Airflow Role **자동 매핑 강화** (단일 권한 관리)
- `roles/composer.admin` → Airflow `Admin`
- `roles/composer.user` → Airflow `User` 또는 `Op`
- 커스텀 매핑 가능
- → 운영자 관점에서 권한 관리 통합되어 큰 장점

### Self-managed

Airflow RBAC를 직접:
- 기본 5 Role 사용 또는 커스텀 Role 정의
- Webserver 설정에 `[webserver] rbac = True` (2.x), 3.x는 기본
- 사용자 생성: `airflow users create ...` 또는 UI Admin
- 외부 IDP 연동 시 `webserver.config.py` 에 OAuth provider 작성

## 레이어 3: Task → GCP 리소스 (Workload Identity)

DAG의 task가 BigQuery / GCS / Pub/Sub 등에 접근할 때 필요.

### Composer 2 / 3

- **Workload Identity 자동 활성화**
- 환경 생성 시 **Environment Service Account** 가 자동 매핑됨
- task 코드는 별도 키 파일 없이 `from google.cloud import bigquery; bq = bigquery.Client()` 만으로 인증됨
- 추가 권한 부여: 그 SA에 IAM 역할 부여만 하면 끝
  - 예: `composer-env-sa@project.iam.gserviceaccount.com` 에 `roles/bigquery.dataEditor` 추가

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

## Connection / Variable / Secret 관리

Airflow에서 외부 시스템 접근 정보(DB 비밀번호, API 키 등)를 어떻게 보관하나.

### Composer 2 / 3

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

| 항목 | Composer | Self-managed |
|---|---|---|
| UI 로그인 이벤트 | Cloud Logging 자동 | 직접 구성 |
| DAG 트리거 이벤트 | Airflow audit log (자동) | Airflow audit log (자동) |
| 권한 변경 이벤트 | Cloud Audit Logs (자동) | 직접 구성 |
| Connection/Variable 접근 | Airflow log | Airflow log |

## 비교 요약

| 항목 | Composer 2 | Composer 3 | Self-managed |
|---|---|---|---|
| UI 인증 | IAP 자동 (Google) | IAP 자동 (Google) | 직접 (IAP/OAuth/OIDC) |
| Google IAM → Airflow Role 자동 매핑 | 부분적 | **강화됨** | 없음 (직접) |
| Workload Identity | 자동 | 자동 | 직접 설정 |
| Secret Manager backend | 자동 옵션 | 자동 옵션 | 직접 |
| Fernet Key | 자동 관리 | 자동 관리 | 직접 |
| 감사 로그 | Cloud Logging 자동 | Cloud Logging 자동 | 직접 |

## 흔한 실수 / 함정

- **Composer Environment SA 권한 과부여**: 한 SA에 모든 권한 → 보안 사고 시 영향 큼. task별 SA 분리 권장
- **Airflow RBAC의 default가 Op**: Composer에서 사용자 추가 후 RBAC 변경 잊으면 Connection 못 만짐
- **IAP는 ID는 검증하지만 권한 부여는 IAM**: IAP 통과 ≠ 모든 권한. `composer.user` IAM 별도 필요
- **Workload Identity와 GKE node SA 헷갈리기**: node SA는 노드가 GCS에서 이미지 풀할 때 쓰는 것. Pod 안 task 코드가 쓰는 건 KSA 매핑된 GSA
- **Secret Manager backend 활성화 후 기존 Connection 안 마이그레이션**: backend는 켰는데 값을 Secret Manager에 안 옮기면 못 찾음
- **사용자 권한을 Airflow UI에서만 관리**: GCP IAM과 따로 놀게 됨. Composer 3의 자동 매핑이 이 문제를 줄여줌

## 의사결정에 주는 함의

- **권한 관리 부담 측면**에서는 Composer 압도 (3은 더 좋음). Self-managed는 1~2주 작업 + 지속적 운영 부담
- **사내 IDP 통합 요구가 강하면** Self-managed가 유리할 수 있음 (custom OIDC 자유)
- **multi-tenancy** (한 클러스터에 여러 팀) 가 필요하면 Self-managed가 유연. Composer는 환경 분리로 처리해야 함 (비용 증가)

## PoC / 검증 추가 항목

- [ ] 우리 팀에서 필요한 Airflow Role 종류 정의 (Admin / Editor / Viewer 같이)
- [ ] 사내 Google Workspace 계정으로 Composer IAP 로그인 PoC
- [ ] Composer 3의 Google IAM → Airflow Role 자동 매핑 실제 동작 확인
- [ ] task별 SA 분리 패턴 설계 (Userlake용 / dbt run용 / extract용 등)
- [ ] Secret Manager backend로 기존 Connection 마이그레이션 절차
- [ ] 감사 로그 보존 정책 (몇 개월 / 어떤 이벤트)

## 미확정 / 확인 필요

- Composer 3의 IAM → Airflow Role 매핑 커스터마이즈 범위 (custom Role 매핑 가능 여부)
- 사내 SSO (Google Workspace) 가 Composer IAP와 어떻게 연동되는지 (정상적으로 SAML/OIDC 자동 통과되는지)
- Self-managed 시 IAP를 GKE Ingress에 붙이는 권장 패턴 (Composer가 내부적으로 쓰는 방식과 동일한지)

## 관련 문서

- [[1_개요]]
- [[2_Cloud Composer vs Self-managed 비교]]
- [[3_Executor 종류 및 비교]]
- [[4_Queue 라우팅과 Pod 스펙 설정]]
- [[5_Metadata DB 운영]]
- [[6_Airflow 2 vs 3 비교]]
- [[7_Composer 비용]]
