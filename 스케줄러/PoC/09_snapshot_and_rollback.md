---
title: "09. 환경 Snapshot 범위 + 버전 업그레이드 롤백 검증"
status: in_progress
tags:
  - poc
  - snapshot
  - backup
  - restore
  - upgrade
  - rollback
  - composer3
created: 2026-06-04
updated: 2026-06-04
---

# 09. 환경 Snapshot 범위 + 버전 업그레이드 롤백 검증

> **검증 질문 1**: Composer 3 의 환경 snapshot 이 **무엇을 저장하고 무엇을 저장하지 않나**? (DAG 파일 / Connections / Variables / DAG run history / RBAC / PyPI / 로그 ...)
>
> **검증 질문 2**: Airflow 버전 업그레이드 후 **문제 발생 시 이전 버전으로 롤백 가능한가**? snapshot 으로 복구 시 어디까지 회복되나?
>
> **답 (예상)**:
> - 1: ✅ Metadata DB (Connections/Variables/Pools/RBAC/DAG run history) + Airflow config + PyPI packages 는 포함. ⚠️ DAG 파일·로그·plugins 는 별도 (GCS bucket).
> - 2: ⚠️ 같은 환경 in-place 다운그레이드는 불가. **이전 버전 새 환경 생성 → snapshot restore** 패턴이 유일한 롤백 경로.

## 검증 의도

운영 측면 두 가지 risk 를 PoC 로 확정:

1. **백업 범위 명확화**: "Composer snapshot 만 있으면 환경 통째 복구되나?" 의 답. 만약 DAG 파일이 별도라면, 백업 전략을 **snapshot + GCS bucket 동기화** 이중으로 설계해야 함.
2. **업그레이드 안전망 확보**: Airflow 마이너 버전 올렸을 때 호환성 문제 / DAG 깨짐 발생 시 **롤백 경로**가 있는지. 없으면 업그레이드 정책 자체를 보수적으로 가야 함.

## 환경

| 항목 | 값 |
|---|---|
| Composer | 3.1.7-build |
| Airflow | 3.1.7 (시작 버전) |
| 업그레이드 목표 버전 | _(채울 자리 — 가용한 다음 마이너 버전)_ |
| Snapshot 저장 GCS bucket | _(채울 자리)_ |
| 테스트용 환경 | dev (별도 격리) |

---

## 시나리오 1 — Snapshot 범위 검증

### 1.1 준비: 환경에 다양한 state 심기

Snapshot 이 무엇을 잡는지 확인하려면 **먼저 환경에 식별 가능한 state 를 모두 심어둬야** 함. Snapshot 후 새 환경에 restore 했을 때 무엇이 살아남는지 1:1 확인.

| 항목                          | 심을 데이터                                                           | 식별 방법               |
| --------------------------- | ---------------------------------------------------------------- | ------------------- |
| **DAG 파일**                  | `poc_snapshot_marker_dag.py` (간단한 DAG)                           | UI 에서 dag_id 확인     |
| **Connection**              | `poc_snapshot_test_conn` (HTTP type, host=`https://example.com`) | Admin → Connections |
| **Variable**                | `poc_snapshot_var` = `2026-06-04-marker`                         | Admin → Variables   |
| **Pool**                    | `poc_snapshot_pool` (slot=5)                                     | Admin → Pools       |
| **DAG run history**         | marker DAG 을 수동 trigger 해서 success 1건 + fail 1건 만들기              | DAG run 화면          |
| **Task 로그**                 | 위 DAG 의 task log                                                 | Task 의 Log 탭        |
| **User & Role**             | `poc-test-user@kakaoent.com` 에 `Viewer` Role 부여                  | Security → Users    |
| **PyPI package**            | `requests==2.31.0` 같은 marker 패키지 설치                              | 환경 config           |
| **Airflow config override** | `core.default_timezone = Asia/Seoul` 같은 override                 | 환경 config           |
| **XCom**                    | marker DAG 안에서 XCom push (`xcom_marker=hello`)                   | XCom 화면             |
| **Plugins**                 | (있다면) plugin 1개 등록                                               | Plugins 화면          |

### 1.2 Snapshot 생성

```bash
gcloud composer environments snapshots save <env-name> \
    --location asia-northeast3 \
    --snapshot-location gs://<bucket>/snapshots/
```

→ 생성된 snapshot 경로 메모.

### 1.3 새 환경에 Restore

같은 Airflow 버전 (3.1.7) 의 **빈 환경** 을 새로 생성하고 거기에 restore:

```bash
# 새 환경 생성 (Airflow 3.1.7)
gcloud composer environments create <env-name>-restored \
    --location asia-northeast3 \
    --image-version composer-3-airflow-3.1.7-build...

# Snapshot 으로 복구
gcloud composer environments snapshots load <env-name>-restored \
    --location asia-northeast3 \
    --snapshot-path gs://<bucket>/snapshots/<snapshot-id>
```

### 1.4 관찰 포인트 — 무엇이 살아남았나

복구된 환경에서 1.1 의 각 항목 체크:

| 항목                                    | 복구 예상                                    | 실측 (체크) |
| ------------------------------------- | ---------------------------------------- | ------- |
| DAG 파일                                | ❌ 별도 (GCS DAG bucket 동기화 필요)             | [ ]     |
| Connection (`poc_snapshot_test_conn`) | ✅ 복구                                     | [ ]     |
| Variable (`poc_snapshot_var`)         | ✅ 복구                                     | [ ]     |
| Pool (`poc_snapshot_pool`)            | ✅ 복구                                     | [ ]     |
| DAG run history                       | ⚠️ 정책에 따라 다름 (확인 필요)                     | [ ]     |
| Task 로그                               | ❌ 별도 (GCS logs bucket)                   | [ ]     |
| User & Role                           | ⚠️ 확인 필요 (FAB metadata 포함 여부)            | [ ]     |
| PyPI package                          | ✅ 환경 config 에 포함                         | [ ]     |
| Airflow config override               | ✅ 환경 config 에 포함                         | [ ]     |
| XCom                                  | ⚠️ metadata DB 안에 있긴 한데 snapshot 에 포함되나? | [ ]     |
| Plugins                               | ⚠️ 확인 필요                                 | [ ]     |

### 1.5 핵심 결과물 (PoC 산출물)

**Snapshot 범위 매트릭스** — "백업 전략 설계 시 무엇을 추가로 백업해야 하나" 의 답:

| 보존 메커니즘 | 항목 |
|---|---|
| Snapshot 에 포함 | (실측 후 채움) |
| GCS bucket 동기화 필요 | DAG 파일, Task 로그 (예상) |
| 코드 / 인프라 코드로 재현 | PyPI 의존성, Airflow config (대안 경로) |

---

## 시나리오 2 — 버전 업그레이드 후 롤백

### 2.0 공식 정책 (정리 문서 참조)

Composer 3 공식 업그레이드 정책 사실은 **별도 정리 노트**: [[../13_Composer 3 환경 업그레이드 정책]]

본 PoC 시나리오 2 에서 활용하는 핵심 사실 (공식 명시):

- ❌ **Airflow 다운그레이드 불가** → 롤백 = 새 환경 + restore 만 가능
- **Airflow 버전/빌드 자동 업그레이드 ❌**, 사용자 수동 트리거만
- **DB > 20GB 면 업그레이드 불가** → 사전 사이즈 확인 필요
- **업그레이드 시 PyPI / config override 자동 재적용**, endpoint 유지

→ 시나리오 2 의 검증 목표는 공식 사실이 이미 답해준 부분 외, **"새 환경 + restore 의 실측 RTO / 손실 항목"** 측정.

### 2.1 검증 흐름 (정정판)

```
[1] 환경 A (Airflow 3.1.7) 에서 snapshot 생성 → S1
[2] 환경 A 를 다음 빌드로 in-place 업그레이드
[3] 업그레이드 후 동작 확인 (DAG 파싱 / 실행)
[4] 의도적으로 "롤백 필요" 시나리오 가정
[5] 롤백 경로 = 새 환경 B (이전 버전) 생성 + S1 restore  ← 유일 경로
[6] 어디까지 복구되나 + RTO 측정
```

(이전 버전의 Step B "in-place 다운그레이드 시도" 는 공식 문서가 ❌ 라 검증 불필요. 검증으로 reproduce 만 한 줄 짚으면 됨)

### 2.2 사전 확인

```bash
# 가용 Composer image 버전 확인
gcloud composer images list --location asia-northeast3

# 메타데이터 DB 사이즈 (20GB 제한 사전 확인)
# Airflow UI > Admin > Database, 또는:
gcloud composer environments describe <env-name> --location asia-northeast3 \
    --format="value(config.databaseConfig)"
```

### 2.3 단계별 실행

**Step A: snapshot 후 업그레이드**

```bash
# 1. Snapshot 생성 (필수 — 자동 snapshot 아님)
gcloud composer environments snapshots save <env-name> \
    --location asia-northeast3 \
    --snapshot-location gs://<bucket>/snapshots/

# 2. In-place 업그레이드
gcloud composer environments update <env-name> \
    --location asia-northeast3 \
    --image-version <target-version>
```

업그레이드 소요 시간 측정 + 그 동안 DAG 동작 상태 관찰.

**Step B: in-place 다운그레이드 시도 (공식 ❌ 재확인용, optional)**

```bash
gcloud composer environments update <env-name> \
    --location asia-northeast3 \
    --image-version <이전 버전>
```

예상: ❌ **거부**. 공식 문서가 다운그레이드 불가 명시. 실측 에러 메시지만 기록하고 다음 단계로.

**Step C: 새 환경 + restore (유일 롤백 경로)**

```bash
# 1. 새 환경 생성 (이전 버전)
gcloud composer environments create <env-name>-rollback \
    --location asia-northeast3 \
    --image-version <이전 버전>

# 2. 업그레이드 전 snapshot 으로 복구
gcloud composer environments snapshots load <env-name>-rollback \
    --location asia-northeast3 \
    --snapshot-path gs://<bucket>/snapshots/<pre-upgrade-snapshot>
```

→ 새 환경에서 시나리오 1 의 marker 항목 (Connection / Variable / Pool / DAG run) 다시 확인.

### 2.4 관찰 포인트

| 항목 | 확인 |
|---|---|
| 업그레이드 소요 시간 (in-place) | 분 단위 측정 |
| 업그레이드 중 DAG 실행 가능 여부 | 공식: "다른 작업 시작 불가하지만 DAG 는 계속 실행" — 실측 검증 |
| 업그레이드 중 환경 모니터링 가용성 | 공식: "단기간 사용 불가 가능" — 실측 |
| PyPI / config override 자동 재적용 확인 | ✅/❌ |
| In-place 다운그레이드 거부 에러 메시지 | (Step B 결과) |
| 새 환경 + restore 의 복구 범위 | 시나리오 1 매트릭스 그대로 |
| 새 환경 + restore 총 시간 | **환경 생성 (~25분) + restore 시간 합산** |
| **현실적 RTO** | 분 / 시간 단위 |
| 새 환경의 endpoint / URL 변경 | 새 환경 = 새 URL. 외부 시스템 / 자동화 영향 산정 |

### 2.5 핵심 결과물

**롤백 SOP** — 업그레이드 실패 시 실제로 따라갈 절차:

| 단계 | 액션 | 예상 소요 |
|---|---|---|
| 1 | 업그레이드 전 snapshot 강제 생성 | 분 단위 |
| 2 | 업그레이드 실행 + 검증 (DAG 파싱, sample run) | 시간 |
| 3 | 문제 발생 시 — 새 환경 (이전 버전) 생성 | 시간 (Composer 환경 생성 자체가 ~25분) |
| 4 | Snapshot restore | 분 ~ 시간 |
| 5 | DAG bucket 동기화 (snapshot 에 없으므로) | 분 |
| 6 | 트래픽 / 외부 시스템 endpoint 전환 | 환경 의존 |

→ 총 RTO **예상 1~3시간**. 즉시 롤백 불가능 — **업그레이드는 충분한 staging 검증 후** 진행해야 한다는 결론으로 이어짐.

---

## 실측 결과 (테스트 중)

> _(채울 자리 — 테스트 완료 후 추가)_

### 시나리오 1 결과

- [ ] Snapshot 범위 매트릭스 확정 (1.4 표 채우기)
- [ ] 발견된 함정 / 예상 외 항목
- [ ] 스크린샷: 복구 전후 비교

### 시나리오 2 결과

- [ ] In-place 다운그레이드 가능 여부 (✅/❌ + 에러)
- [ ] 새 환경 + restore RTO 측정값
- [ ] 롤백 SOP 확정

### 결론

- [ ] 백업 전략: snapshot 만으로 충분한가?
- [ ] 업그레이드 정책: 어느 정도 보수적이어야 하나?

---

## 함의 — 사내 적용 시

### 백업 전략

| 보존 대상 | 메커니즘 |
|---|---|
| DAG 파일 | GCS bucket cross-region 복제 또는 git 원본에서 재배포 |
| Snapshot 대상 (1.5 결과) | 정기 snapshot 스케줄링 (예: 일 1회) + retention 정책 |
| Task 로그 | GCS bucket lifecycle / 별도 archive |
| 환경 설정 (Terraform 등) | IaC 로 환경 자체 재생성 가능하게 |

### 업그레이드 정책

| 단계 | 액션 |
|---|---|
| 1. dev 환경에서 사전 검증 (1~2주) | 새 버전으로 dev 환경 만들어 DAG 전체 파싱 / 샘플 실행 |
| 2. snapshot 강제 생성 | 업그레이드 직전 |
| 3. 사용자 알림 + 다운타임 윈도우 | 업그레이드는 다운타임 발생 |
| 4. 업그레이드 실행 + 단계적 검증 | 핵심 DAG 우선 |
| 5. 문제 발생 시 롤백 SOP | 시나리오 2.5 따라 |

---

## 잔여 검증 (시간 되면)

- [ ] **Snapshot 자동 스케줄링** 설정 가능 여부 (Cloud Scheduler + gcloud)
- [ ] **Cross-region restore** 가능 여부 (DR 시나리오)
- [ ] **Snapshot 크기 / 저장 비용** 측정
- [ ] **Snapshot retention 정책** — 오래된 snapshot 자동 삭제
- [ ] **메이저 버전 (Airflow 2 → 3)** 업그레이드 시나리오 — 본 PoC 는 마이너 위주
- [ ] **DB 20GB 임계치 도달 시 동작** — Airflow metadata cleanup 정책 수립 필요성
- [ ] **자동 인프라 업그레이드** 실측 — maintenance window 안에서 언제 발생, 어떤 영향
- [ ] **인프라 자동 업그레이드 중 worker graceful shutdown** 동작 (24h 유예 실증)

## 관련 문서

- [[../7_1_실제 스펙 산정]] — 운영 비용 / SLA 산정
- [[02_dag_deployment]] — DAG bucket 동기화 패턴
- [[06_iam_workspace_rbac]] (필요 시) — RBAC snapshot 포함 여부
- [[../8_Composer 권한 및 인증]] — 사용자 / Role 복구 영향

## 참고

- [Cloud Composer 3 — Save and load snapshots 공식 문서](https://cloud.google.com/composer/docs/composer-3/save-load-snapshots)
- [Cloud Composer 3 — Upgrade environments 공식 문서](https://cloud.google.com/composer/docs/composer-3/upgrade-environments)
