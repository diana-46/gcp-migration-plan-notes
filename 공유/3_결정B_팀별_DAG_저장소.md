# 3. 결정 B — Orchestration: Python DAG + 팀별 저장소

> Orchestration 축은 **두 개의 하위 결정**을 세트로 가짐:
> 1. **Actions UI → Python DAG** (authoring 모델 변경)
> 2. **Centralized `airflow-dags` → 팀별 저장소** (org / 배포 모델 변경)
>
> 관련: [[스케줄러/15_관리 레포 인벤토리]], [[스케줄러/14_Composer 3 비용 구조]]

---

## 하위결정 1: Actions UI → Python DAG (authoring)

### `kwargs 스트링 JSON` 이 부딪히는 실제 불편

Actions / Workflows 의 오퍼레이터 kwargs 는 자유 형식 JSON:
```json
{"bucket": "my-bkt", "prefx": "path/*.parquet"}
                       ↑ 오타. 저장 통과 → Airflow 트리거 → 런타임에서 처음 발견
```

DE 관점 6가지 실제 시나리오:

**a) 필드 이름 힌트 없음** — 오퍼레이터가 무슨 필드를 받는지 UI 에 안내 없음. 다른 ETL 코드 카피해서 관례로 익힘. 새 오퍼레이터는 감 자체가 없음.

**b) 오타 검증 시점** — 저장 단계 통과 → Airflow 스케줄 → 런타임 실행 시점에 처음 발견. 사이클 하나가 실패로 날아감.

**c) 타입 힌트 없음** — 필드가 `str` 인지 `list[str]` 인지 `int` 인지 모름. `bucket="my-bkt"` 로 넣어야 하는지 `bucket=["my-bkt"]` 인지 시도해봐야 앎.

**d) Jinja 매크로 escape 이슈** — 문자열 안에 `{{ ds }}` 같은 매크로 넣을 때 JSON escape 규칙과 Jinja 규칙이 충돌. 백슬래시 이스케이프 실수 잦음.

**e) DP-821 케이스** — Slack 알림 채널 이름 하나 바꿨는데 kwargs 변경이 태스크 재생성으로 이어져 예상 밖의 대량 재실행 발생. UI + kwargs JSON 모델이 태생적으로 갖는 커플링 이슈.

**f) 문서화 drift** — 사내 위키의 필드 명세와 오퍼레이터 소스가 별도 관리. 최신 상태 유지 어려움.

### Python DAG 로 옮기면

```python
from airflow.providers.google.cloud.transfers import GCSToBigQueryOperator

upload = GCSToBigQueryOperator(
    task_id="load_raw",
    bucket="my-bkt",
    source_objects=["path/*.parquet"],   # ← IDE 자동완성 + 오타 시 빨간 줄
    destination_project_dataset_table="proj.raw.user",
)
```

DE 가 얻는 것:

| 관점 | Actions UI (kwargs JSON) | Python DAG |
|---|---|---|
| 편집 위치 | 브라우저 UI 텍스트 필드 | IDE (PyCharm / VS Code) |
| 자동완성 | 없음 | O — 필드명 / 타입 즉시 |
| 오타 검증 | 배포 후 런타임 | IDE 편집 중 빨간 줄 |
| 타입 체크 | 없음 | O — mypy / IDE inspection |
| 소스 확인 | 사내 위키 검색 | Ctrl+클릭 으로 오퍼레이터 소스 |
| 로컬 실행 | 불가 | `python my_dag.py` 로 파싱 검증 |
| 조건분기 · 반복 | 우회 필요 | Python `if` / `for` / 함수 자연 |
| 재사용 | 카피 & 붙여넣기 | 함수 / 모듈 / 패키지 |
| 라이브러리 | 사내 operator 만 | pip Provider 수백 개 (Slack, Snowflake, HTTP 등) |
| 디버깅 | Airflow 로그만 | breakpoint, traceback, IDE 스텝 |

### 사내 operator 는 사라지지 않음

기존 사내 operator 14개 (Nabi / Loupe / Hive / BQ 등) + helper 는 **Python 패키지 `apache-airflow-providers-kakaoent-dataplatform`** 로 재배포:

```python
from airflow.providers.kakaoent.dataplatform.operators.loupe_kafka_batch import LoupeKafkaBatchOperator
```

`pip install` + `import`. 오픈소스 provider 와 동일 인터페이스. 상세: [[5_3layer_배포_아키텍처]] § Layer 1.

---

## 하위결정 2: Centralized → 팀별 저장소 (org / 배포 모델)

### 지금 (Centralized) 모델의 문제

현행 `airflow-dags` = 여러 팀이 공유하는 단일 저장소.

- **배포 페이스 강제 동기화** — 한 팀 급하면 다른 팀 검토 끝나기 전 push. 반대도.
- **팀 간 이슈 전파** — 한 팀 DAG 파싱 실패 → 스케줄러 전체 노이즈
- **Ownership 흐림** — 파일별 CODEOWNERS 로 커버하기 번거로움
- **리뷰어 풀 혼란** — cross-team 리뷰 부담. 결국 리뷰 없이 merge.
- **dbt 프로젝트와 비대칭** — dbt 는 팀별 (`storydata-dbt` 등), DAG 만 통합. 관리 모델 불일치.

## 이게 왜 지금 (GCP 이관 시점) 특히 중요한가 — 비용·리소스 축

### 지금 athlon 은 DB 기반 스케줄링

- MySQL 에 저장된 DAG 정의 / 스케줄 상태를 매 분·초 폴링
- 측정치: **300~900 queries/minute** ([[스케줄러/15_관리 레포 인벤토리]] § athlon DB 한계)
- DAG / 팀 수 늘어날수록 부하 선형 증가 → DB CPU / 네트워크 / 잠금 경합
- SPOF (Single Point of Failure) — athlon DB 이슈 시 모든 팀 영향

### GCP Composer 요금 모델

Composer 3 요금 = **DCU (vCPU + RAM 시간)** ([[스케줄러/14_Composer 3 비용 구조]]):

- Scheduler / DAG processor / triggerer 는 **24×7 상주** → floor cost 발생 (DAG 0 개여도 ~$200-300/월)
- DAG 밀도·파싱 부하가 그대로 **스케줄러 스펙 = 요금**
- 관련: [[스케줄러/7_1_실제 스펙 산정]] (실측 스펙 산정), [[스케줄러/7_2_리소스 다이어트 포인트]] (다이어트 포인트)

### 팀별 저장소가 두 축을 동시에 해결

**1. git 기반 DAG = DB 폴링 자체가 사라짐**

Composer scheduler 는 GCS 파일 + 파싱된 manifest 캐시로 동작:
- athlon MySQL 을 매 분·초 찌르던 구조 **소멸**
- 300~900 qpm 부하 → 0 qpm
- GCP 이관 리소스 절감 목표의 **큰 지렛대**

**2. 팀별 스케일 격리 (팀별 Composer 로)**

저장소가 팀별로 나뉘고 **Composer 도 팀별로 운영** (아래 § 팀별 Composer 참조):
- 각 팀 scheduler 가 자기 DAG 만 파싱 → 스펙을 팀 DAG 밀도에 맞춤
- **DCU 요금이 도메인별로 명확** → 팀 자기 예산 · 사용량 대응
- Blast radius 완전 격리 (다른 팀 Composer 이슈 무관)

**핵심**: 팀별 저장소 = **팀 자율성 + 비용·리소스 최적화 실행 수단** (관련: [[스케줄러/0_결론]] 의 Phase 1/2 비용 절감 목표).

## 팀별 저장소가 주는 이득 (비용 외)

비용·리소스 축 (위 섹션) 이 핵심이지만, 그 외에도 여러 축의 이득이 있음:

### 1. 배포 자율성 · 속도

- 팀 자기 페이스로 push (다른 팀 검토 대기 없음)
- 배포 사고 시 롤백도 팀 내에서 완결
- 릴리즈 노트 · 배포 이력이 팀 저장소에 남음 → 팀 히스토리 축적

### 2. Ownership 명확

- **저장소 = 팀** 이라는 대응이 명확
- CODEOWNERS 파일 세팅 단순 (팀 GitHub 그룹 하나)
- 새로 온 사람이 "이 파이프라인 담당 팀이 누구지" 를 저장소 이름으로 즉시 답 얻음

### 3. Blast radius 격리

- 한 팀 DAG 이 파싱 실패해도 다른 팀 저장소는 안 밀림
- 한 팀 배포 실수 (예: `git push -f`) 가 다른 팀 코드에 영향 X
- 사고 조사 범위 자동 축소

### 4. 리뷰 문화 팀 내 정착

- Cross-team PR 리뷰 부담 없음 (모르는 도메인 코드 리뷰 X)
- 팀 안에서 리뷰어 페어링 · 규약 · 스타일 정립 자유
- 팀별 PR template, checklist 등 커스터마이즈 여지

### 5. dbt 프로젝트와 대칭 구조

- dbt 는 팀별 (`storydata-dbt`, `kpayment-dbt`, ...) → DAG 만 통합이면 관리 모델 비대칭
- 저장소 셋이 팀 단위로 정렬:
  - `<team>-dbt` (모델)
  - `<team>-airflow-dags` (스케줄)
  - 공용 `dp-airflow-provider` (operator)
- **팀 하나 = 저장소 2 개 (자기 팀) + 1 개 (공용 provider)** 라는 깔끔한 대응

### 6. 팀 규약 자유

- 커밋 메시지 컨벤션, PR template, 브랜치 전략 팀별 자율
- 팀별 CI 추가 (예: dbt 테스트 커버리지 gate) 자유
- 다른 팀 스타일에 강제 맞출 필요 없음

### 7. 확장성

- 팀 하나 새로 오면 저장소 template 복사 → 하루 안 셋업
- 저장소 크기가 특정 임계값 넘어서 파싱 · 리뷰 부담 커지는 문제 없음
- Provider 패키지 버전 lock 도 팀별로 자기 페이스

### 자율성 축 요약

| 관점 | Centralized | 팀별 |
|---|---|---|
| 배포 페이스 | 팀 간 동기 필요 | 팀 자율 |
| Ownership | 파일별 CODEOWNERS | 저장소 = 팀 |
| Blast radius | 저장소 전체 | 팀 내 국한 |
| 리뷰 | cross-team 부담 | 팀 내부 완결 |
| 팀 규약 | 통일 강제 | 팀 자율 |
| dbt 대응 | 비대칭 | 대칭 |
| 확장 | 저장소 크기 폭증 | 팀 단위 자연 확장 |

## 어떻게 실현되나 (실제 구현)

### 저장소 구조

각 팀이 자기 저장소 소유:
```
storydata-airflow-dags/     ← story-team 소유
├── dags/
│   └── storydata/           ← 팀 서브디렉토리
│       ├── berriz_0101_bizberry_hourly_integration.py
│       └── ...
├── .github/workflows/deploy.yml
└── README.md

kpayment-airflow-dags/       ← kpayment-team 소유
├── dags/
│   └── kpayment/
│       └── ...

otherteam-airflow-dags/      ← ...
```

### Composer GCS 배포 컨벤션

```
gs://COMPOSER_BUCKET/dags/
├── storydata/               ← storydata-airflow-dags 관리
├── kpayment/                ← kpayment-airflow-dags 관리
└── otherteam/               ← 각자 관리
```

### CI/CD 규칙

`.github/workflows/deploy.yml` (팀 저장소마다):
- Sync 범위: `dags/{TEAM}/` → `BUCKET/{TEAM}/`
- **`gsutil rsync -d`** — 팀 서브디렉토리 안에서만 delete propagation
  - 이 저장소가 `dags/{TEAM}/` 의 source of truth
  - 로컬에서 파일 삭제 → GCS 에서도 사라짐
- 다른 팀 서브디렉토리는 **안 건드림** (범위 격리)

### 왜 Composer 는 이걸 자연스럽게 지원하나

- Composer scheduler 는 **`dags/` 를 재귀적으로 스캔** — 서브디렉토리 자동 인식
- 별도 config 없이 `dags/storydata/*.py` 도 정상 파싱
- 팀별로 저장소 나누고 GCS 서브디렉토리 sync 하면 그대로 동작

## PoC 실증 — storydata 팀 이관 (2026-07)

- `storydata-airflow-dags` 저장소 신설
- `dags/storydata/` 서브디렉토리 컨벤션 확립
- `rsync -d` delete propagation 검증
- **Producer DAG** (`berriz_0101_bizberry_hourly_integration`) + **Consumer DAG** (`berriz_bizberry_downstream_demo_integration`) 로 Asset 기반 cross-DAG 트리거 실증
- 다른 팀 (kpayment 등) 이관 시 이 저장소 template 로 하루 안 셋업 가능

## Composer 도 팀별로 운영

**팀별 저장소 + 팀별 Composer 조합으로 감**. 시연 단계 (`test-airflow3` 공유) 를 지나
Phase 1 확산 시점부터 팀 별 Composer 인스턴스로 분리.

### 팀별 Composer 의 이득

- **DCU 요금 도메인별 명확** — 각 팀 Composer 가 자기 예산 · 사용량 대응
- **Blast radius 완전 격리** — 한 팀 Composer 이슈가 다른 팀에 절대 영향 X
- **Scheduler 부하 팀 단위 확장** — 팀별 DAG 밀도에 맞춰 스펙 조정
- **환경 (dev / integration / production) 도 팀별 매트릭스** — 각 팀 자기 진도로 promotion
- **PoC / 실험 자유** — 다른 팀 안 건드리고 새 Airflow 버전 · 새 Provider 실험

### Trade-off

- Floor cost × 팀 수 발생 (팀당 ~$200-300/월, 관련: [[스케줄러/14_Composer 3 비용 구조]])
- 팀 수 늘어나면 floor 총합 증가 → 하지만 blast radius 격리 · 요금 visibility 이득이 더 큼
- 공용 리소스 (예: 사내 DataHub, GAR provider registry) 는 그대로 공용 유지

### 저장소 ↔ Composer 대응

```
Story 팀:
    storydata-dbt         ──┐
    storydata-airflow-dags ──┼── Composer: storydata-composer
                              │
Kpayment 팀:
    kpayment-dbt          ──┐
    kpayment-airflow-dags ──┼── Composer: kpayment-composer
                              │
플랫폼팀 (공용):
    dp-airflow-provider   ──── GAR (모든 팀 참조)
```

- 각 팀 저장소 CI 는 자기 Composer 버킷만 sync
- 배포 · 스케줄러 · 워커가 팀 단위로 완전 분리
- Provider 패키지만 공용 (모든 Composer 가 `pip install` 로 소비)

## 흔한 질문

**"팀별로 저장소 만드는 부담"**
- 플랫폼팀 template 로 하루 내 셋업 (`.github/workflows/deploy.yml` + `dags/{team}/`)
- 이후엔 팀 자율. 오히려 조율 부담 감소.

**"공용 helper / 상수는 어디에"**
- `apache-airflow-providers-kakaoent-dataplatform` 패키지에 (관련: [[스케줄러/7_3_공통 Custom Operator 제공 방안]])
- 팀 저장소는 얇게 유지

**"cross-team dependency 는"**
- Airflow Asset 으로 (관련: [[7_Lineage와_관측성]])
- Producer DAG 이 Asset URI emit → Consumer DAG 이 `schedule=[Asset(...)]` 로 subscribe
- 저장소가 달라도 같은 Composer 안에서 자동 wiring

## 관련 문서

- [[스케줄러/15_관리 레포 인벤토리]] — 3-layer 저장소 구조 + athlon DB 한계
- [[스케줄러/14_Composer 3 비용 구조]] — DCU 요금 모델
- [[스케줄러/7_1_실제 스펙 산정]] — 리소스 실측
- [[스케줄러/7_2_리소스 다이어트 포인트]] — 다이어트 축들
- [[스케줄러/11_DAG Bundles와 배포 전략]] — 배포 전략
- [[애슬론/8_배포 시 유의할 점]] — 실전 배포 함정
