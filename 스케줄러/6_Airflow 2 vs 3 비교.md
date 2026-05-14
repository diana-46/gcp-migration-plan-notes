---
title: "Airflow 2 vs 3 비교"
status: draft
tags:
  - airflow
  - 스케줄러
  - version
created: 2026-05-12
updated: 2026-05-12
---

# Airflow 2 vs 3 비교

> 2025년 4월 GA된 Airflow 3의 주요 변경점. GCP 이관 시 어느 메이저 버전으로 갈지는 Composer 선택과도 직결된다.

## 릴리스 / 지원 상태

| 버전 | GA | 최신 | EOL (예정) | Cloud Composer |
|---|---|---|---|---|
| **Airflow 2.x** | 2020-12 | 2.10.x 계열 | 2026년 내 점진적 EOL 전망 | Composer 2 |
| **Airflow 3.x** | 2025-04 | 3.x 계열 | 장기 지원 | **Composer 3** |

> Composer 2는 Airflow 2를 마지막으로 받고, 신규 환경은 Composer 3 (Airflow 3) 권장 흐름. 정확한 EOL/지원 정책은 GCP 공식 공지 확인 필요.

## 핵심 변경 한 줄 요약

> Airflow 3은 **"task가 더 이상 scheduler/메타DB에 직접 붙지 않는다"** 가 가장 큰 변화. 보안/격리/언어 확장이 따라옴.

## 주요 변경점 (high-level)

| 영역 | Airflow 2 | Airflow 3 |
|---|---|---|
| **Task 실행 모델** | task가 메타DB 직접 access | **Task SDK** 통해 API로만 통신 |
| **DAG versioning** | 없음 (수동 관리) | **first-class 지원** (UI에서 버전별 history) |
| **Webserver** | UI + 일부 API 결합 | **API-first**, UI는 분리된 React 앱 |
| **다국어 SDK** | Python 전용 | Python + **Go (preview)**, 향후 확장 가능 |
| **Asset / Dataset** | Dataset (2.4+) | **Asset / AssetWatcher** (이벤트 기반 강화) |
| **Edge Executor** | 없음 | **신규** — 원격/엣지 워커 지원 |
| **Backfill** | CLI 중심, 제한적 UI | **UI에서 직접 관리** (정식 기능) |
| **DAG 배포** | 파일 sync | **DAG Bundles** (버전 단위 배포) |
| **Python 최소 버전** | 3.8+ | **3.9+** (3.8 제거) |
| **SubDAG** | deprecated이지만 존재 | **제거** (TaskGroup만) |
| **SLA** | 있음 | **제거** — Deadline / Alert로 대체 |
| **Provider 패키지** | airflow에 묶이거나 분리 | **분리 강제** (core가 가벼움) |

## 아키텍처 차이: Task SDK 분리

Airflow 2:

```
[Scheduler] ─→ [Worker (task 실행)] ─→ [Metadata DB] (직접 SQL)
                                  └─→ [DAG 파일 access]
```

→ task 코드가 metadata DB나 Airflow 내부 모듈을 자유롭게 import/조작 가능. 편하지만 **보안/격리 위험**.

Airflow 3:

```
[Scheduler] ─→ [Task Execution API]
                      ↑
              [Worker (task 실행, Task SDK)] ─ HTTP/gRPC ─ API → DB
```

→ task는 **Task SDK**를 통해서만 Airflow와 통신. DB 직접 접근 X. 강한 격리.

영향:

- **장점**: 보안 강화, 멀티테넌시 가능, 언어 SDK 확장 용이 (Go 등), task 환경 완전 격리
- **단점**: task 코드에서 `from airflow.models import ...` 같이 내부 모듈 직접 import 못 함. 마이그레이션 시 **코드 수정 필요**
- **dbt 관점**: dbt를 task로 돌리는 데에는 영향 적음 (`KubernetesPodOperator`로 외부 프로세스 실행이라 격리는 이미 됨)

## 신규 기능 (3.x)

### 1. DAG Versioning

- DAG 파일 변경이 versioning 되어 UI에서 과거 버전 보기 / 비교 가능
- 백필 시 "그 시점의 DAG 코드"로 실행 가능 (재현성 ↑)

### 2. Asset / AssetWatcher

- Dataset → **Asset** 으로 개명 + 확장
- `AssetWatcher`: 외부 이벤트(파일 도착, BQ 테이블 갱신 등)로 DAG trigger
- BigQuery / GCS 트리거에 활용 가능 — Airflow가 polling 안 해도 됨

### 3. DAG Bundles

- DAG를 git repo / OCI image / S3 같은 "bundle" 단위로 배포
- 환경별 다른 bundle 사용 가능 (dev / prod 분리 쉬움)
- Composer의 GCS sync 방식과는 별개로 더 표준화된 배포 모델

### 4. Edge Executor

- 원격/엣지 워커가 scheduler 없이 동작. 네트워크 분리 환경에서 유용
- 우리 케이스(GKE 단일 리전)에서는 당장 불필요할 수 있음

### 5. Backfill UI

- CLI/SQL 안 거치고 UI에서 백필 범위·옵션 지정
- 운영자 손이 가벼워짐

### 6. Multi-language SDK

- Go SDK 첫 공개 (preview), 향후 JS/Rust 등 확장 가능성
- 실무 영향: **현재로서는 Python SDK가 압도적**. Go는 인프라성 task에 유용

## 제거된 / 변경된 기능

| 항목 | 변화 |
|---|---|
| **SubDAG** | 완전 제거. `TaskGroup`만 사용 |
| **SLA** | 제거. Deadline 알람 / 외부 모니터링으로 대체 |
| **Smart Sensor** | 제거 (이미 2.x에서 deprecated). `deferrable=True` Sensor 사용 |
| **DAG `default_view='graph'`** 등 일부 옵션 | 정리 / 제거 |
| **Python 3.8** | 지원 종료 (3.9+ 필수) |
| **MySQL backend** | 비권장 강화 — PostgreSQL 사실상 강제 |
| **`schedule_interval`** | `schedule` 로 통일 (2.4+ 이미 변경, 3에서 굳어짐) |
| **Provider 패키지** | core와 완전 분리. 별도 설치 필요 (`apache-airflow-providers-*`) |

## 호환성 / 마이그레이션

### 호환되는 것

- DAG 파일 대부분의 문법 (operator import 경로는 일부 변경)
- TaskFlow API (`@task`)
- Pool / Variable / Connection 개념과 데이터
- 대부분의 official provider operator

### 깨지는 것

- task 코드 안에서 `from airflow.models import DagRun` 같이 **내부 모듈 import**
- task 안에서 SQLAlchemy로 metadata DB 쿼리
- SubDAG / SLA / Smart Sensor 사용 DAG
- Python 3.8 환경
- 자체 작성한 plugin 중 내부 API 사용한 것
- 일부 deprecated operator/sensor (provider 이전 분리 안 된 것들)

### 마이그레이션 도구

- **`airflow db migrate`**: DB 스키마 자동 마이그레이션
- **`airflow upgrade-check`** (2.x 후반에 도입된 도구의 3.x 버전): 호환 안 되는 부분 사전 검사
- **Provider 패키지 호환 매트릭스**: provider 버전과 Airflow 코어 버전 호환표 확인 필수

## Cloud Composer 관점

| Composer 2 | Composer 3 |
|---|---|
| Airflow 2.x | **Airflow 3.x** |
| GKE 자동, CeleryKubernetesExecutor 고정 | 동일 + Edge Executor 옵션 가능성 |
| Webserver / Scheduler 결합 deployment | API-first 구조 반영 |
| DAG sync: GCS bucket | DAG Bundles 지원 (GCS 호환 유지) |
| Backfill: CLI/API 위주 | UI Backfill 사용 |

> **결정에 주는 영향**: GCP가 Composer 3을 새 환경의 기본으로 밀고 있어, 신규 환경 만든다면 Composer 3 (Airflow 3) 선택이 합리적. Self-managed라면 자유롭지만, **2026년 시점에 신규를 2.x로 가는 건 비추**.

## 우리 케이스 권장 (잠정)

1. **신규 GCP 환경은 Airflow 3 (= Composer 3)** 으로 가는 것이 기본 선택지
2. Airflow 2 → 3 마이그레이션 비용은 **DAG 코드보다 plugin / 내부 API 의존 코드에 집중**됨 — 현재 athlon이 Airflow 내부 모델을 import해 쓰는 지점 있는지 사전 점검 필요
3. dbt 통합은 **버전 영향 적음** (`KubernetesPodOperator` 사용 시) — 안심하고 3.x로 가도 됨
4. SLA → Deadline / Alert로 갈아끼울 운영 패턴 미리 정의

## PoC / 검증 추가 항목

- [ ] 현재 운영 중인 DAG에서 Airflow 내부 모듈 import 지점 grep (`from airflow.models`, `airflow.utils.db` 등)
- [ ] 사용 중인 provider 패키지 목록 추출 + Airflow 3 호환 버전 확인
- [ ] SubDAG / SLA / Smart Sensor 사용 여부 점검
- [ ] athlon 측 코드가 Airflow 내부 SQLAlchemy/DB에 직접 붙는지 점검
- [ ] Python 버전 호환 (3.9+) 점검
- [ ] Composer 3 환경에서 샘플 DAG 1개 + dbt run task 1개 실행 PoC

## 미확정 / 확인 필요

> 아래는 빠른 변경 가능성이 있어 GCP/Airflow 공식 공지로 최종 확인 필요.

- Composer 2 EOL 일정 / Composer 2 → 3 마이그레이션 도구 제공 여부
- Airflow 2.10.x의 마지막 패치 일정
- Edge Executor의 GKE Workload Identity 연동 성숙도
- Multi-language SDK(Go)의 production-ready 시점

## 관련 문서

- [[1_개요]]
- [[2_Cloud Composer vs Self-managed 비교]]
- [[3_Executor 종류 및 비교]]
- [[4_Queue 라우팅과 Pod 스펙 설정]]
- [[5_Metadata DB 운영]]
