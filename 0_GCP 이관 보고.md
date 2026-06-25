---
title: "데이터 GCP 이관 계획 보고"
status: draft
created: 2026-06-25
청자: 의사결정자 (디렉터 / 매니저)
용도: 기술 스택 / 비용 / 선택 근거 보고
---

# 데이터 GCP 이관 계획 보고

## 0. 한 줄 요약

> 사내 athlon (Neptune) + Hive 기반 데이터 파이프라인을 **GCP 의 Cloud Composer 3 + BigQuery + dbt** 로 이관.
> PoC 완료 — 기술 실현 가능성 확정. 단계적 전환 (약 9~10개월).
> 운영 모델은 **athlon DB-driven → DE 자율 git-based** 로 동시 전환.

---

## 1. 기술 스택

### 1-1. 핵심 컴포넌트

| 영역 | 선택 | 역할 |
|---|---|---|
| **워크플로우 오케스트레이션** | Cloud Composer 3 (Airflow 3.1.7) | DAG 스케줄링 / 실행 / 모니터링 |
| **데이터 웨어하우스** | BigQuery | OLAP 쿼리 + 데이터 저장 |
| **변환 도구 (ETL)** | dbt 1.9 (`dbt-bigquery`) | SQL 변환, 모델 관리, 테스트, lineage |
| **Airflow ↔ dbt 통합** | Cosmos 1.14 | dbt 모델 → Airflow task 자동 변환 |
| **외부 산출물 저장** | GCS | Avro export, 로그, 임시 파일 |
| **카탈로그 / lineage** | DataHub (사내 운영 중) | 메타데이터 검색, 영향도 분석 |
| **CI/CD** | GitHub Actions + WIF | 자동 배포 (JSON key 없이) |
| **컨테이너 이미지** | Artifact Registry | 공통 Operator Python package |
| **시크릿 관리** | Secret Manager | dbt profile, 외부 API 키 |

### 1-2. 운영 모델 (레포 3개)

| # | 레포 | 책임 | 담당 |
|---|---|---|---|
| 1 | 공통 Operator 패키지 | 사내 공통 Operator / Hook / Sensor | 플랫폼팀 |
| 2 | dbt project | ETL/변환 로직 (SQL + schema.yml) | DE 도메인 팀 |
| 3 | DAG repo | Airflow DAG / 스케줄 / 백필 | DE 도메인 팀 |

상세: [[스케줄러/15_관리 레포 인벤토리]]

---

## 2. 기술 스택 선택 근거

### 2-1. 왜 Cloud Composer 3 (Airflow 3)

- **현재 athlon factory 의 자동 대체** — Airflow 가 표준, athlon 의 사내 전용 factory 종속성 제거
- **GCP 매니지드** — VM / DB 직접 운영 없음. 인프라 부담 ↓
- **Airflow 3 의 Asset 기반 cross-DAG 트리거** — Neptune 의 sensor 패턴 자연 대체
- **Hybrid executor (Celery + Kubernetes)** — 무거운 task 만 K8s pod 격리 가능 (PoC 검증)

### 2-2. 왜 BigQuery

- **사내 멀티 워크로드 표준** — 이미 인프라 / 권한 / DataHub 연동 정착
- **partition / clustering 자동 관리** — Neptune 의 `ALTER TABLE ADD PARTITION` 같은 명시 작업 불필요 (PoC 에서 `insert_overwrite` 패턴이 Neptune 시맨틱과 1:1 매핑 검증됨)
- **SQL 풀, 옵티마이저 강함** — Presto 대비 튜닝 부담 ↓
- **time travel (7일)** — rollback 안전 장치

상세: [[애슬론/5_Neptune SQL 변환의 dbt-BigQuery 대체 검토]]

### 2-3. 왜 dbt

- **업계 표준** — DE 학습 ROI 높음. 외부 이전 가능한 스킬
- **자동 lineage / 의존성** — Neptune 의 수동 upstream 선언 대체. `ref()` 자동 그래프
- **schema 강제 (contract)** — Neptune 의 자동 ALTER 보다 보수적·명시적 가드 (PoC § 8 검증)
- **테스트 / 문서 / 환경 분리** — 표준 도구로 깔끔
- **CI/CD 친화** — git PR 워크플로 자연스러움

상세: [[dbt/1_materialization]] ~ [[dbt/6_배포와 환경 분리]]

### 2-4. 왜 Cosmos

- **dbt 모델 → Airflow task 자동 변환** — boilerplate 제거
- **`ref()` 그래프 → task 의존성 자동 wiring**
- **Composer 표준 ExecutionMode.LOCAL** — admission webhook 충돌 없음
- **emit_datasets 로 Asset outlet 자동 부착** — DataHub / cross-DAG 연동 0 코드

상세: [[dbt/5_의존성 관리]]

### 2-5. 왜 DE 자율 git 모델

- **현재 단일 athlon factory 가 SPOF** — DE 가 자기 도메인 자율 → bottleneck 해소
- **review / 테스트 / rollback** — DB row 기반 변경엔 불가능했던 표준 git 워크플로 적용
- **GCP 이관 시 어차피 athlon DB 자체가 Cloud SQL 로 가야 함** — 이 기회에 git 으로 옮기면 동시 작업

상세: [[스케줄러/15_관리 레포 인벤토리]] § 2

---

## 3. 비용 추정

### 3-1. 추정 범위 (월간)

> 정확한 수치는 인프라팀과 협의 + 현재 사용량 metric 기반 추정 필요. 아래는 **PoC 환경 + 사내 유사 워크로드 추정치**.

| 항목 | 추정 비용 (월) | 산정 근거 |
|---|---|---|
| **Cloud Composer 3 (prod)** | $1,500 ~ $2,500 | medium 사이즈. scheduler / worker / web. 현재 PoC dev 환경 metric 기반 |
| **Cloud Composer 3 (dev)** | $300 ~ $500 | small 사이즈 |
| **BigQuery — 슬롯 reservation** | $2,000 ~ $5,000 | 500~1500 slot reservation (현재 Hive 쿼리량 기반 추정) |
| **BigQuery — 스토리지** | $500 ~ $1,500 | 525 ETL × 일별 partition. active vs long-term storage 비율 가정 |
| **BigQuery — 쿼리 비용 (on-demand)** | $200 ~ $500 | 슬롯 외 ad-hoc 쿼리 |
| **GCS — 산출물 / 로그** | $50 ~ $200 | Avro export + Composer bucket + DAG 로그 |
| **Artifact Registry** | < $10 | Python package |
| **Secret Manager** | < $10 | 시크릿 수십 개 |
| **Networking (egress)** | $100 ~ $500 | 내부 네트워크 사용량 따라 |
| **합계 (월간)** | **$4,660 ~ $10,720** | |

→ **연간 약 $55K ~ $130K (KRW 7천만 ~ 1억 7천만 원)**

### 3-2. 현재 운영 비용과 비교 (측정 권장)

이관 결정을 위해 비교해야 할 현재 비용:

| 항목 | 현재 비용 | 비고 |
|---|---|---|
| Hive 클러스터 (운영) | (측정 필요) | HDFS + Presto + YARN |
| athlon DB (RDS / 사내 DB) | (측정 필요) | |
| Jenkins / Git 인프라 | (측정 필요) | |
| Airflow 자체 운영 | (측정 필요) | 현재 athlon-Airflow |
| 운영 인력 비용 | (측정 필요) | athlon 팀 운영 + DE 대기 시간 |

→ **TCO 비교가 핵심**. 단순 GCP 청구액보다 운영 인력 / 사고 대응 비용까지 포함.

### 3-3. 비용 최적화 옵션

- **Slot reservation Flex / Edition** — autoscaling 으로 idle 시간 절감
- **Partition expiration** — 옛 데이터 자동 long-term storage 전환
- **GCS lifecycle** — 로그 30일 후 archive
- **Composer worker autoscaling** — 야간 task 적을 때 worker 감축
- **dbt 모델의 `is_incremental()` 강제** — 백필 외 full-refresh 안 하게

### 3-4. dual-run 기간의 추가 비용 (Phase 1~2)

마이그레이션 중 Neptune + GCP 동시 운영 기간:
- 약 4~6개월 dual-run
- GCP 비용 + 기존 Hive 운영비 = **약 1.5~2배 일시 증가**
- Phase 3 완료 후 Hive 정리 시 정상화

→ 이관 예산에 **dual-run 기간 추가 비용** 명시 필요.

---

## 4. 리스크 / 미확정

### 4-1. 핵심 리스크

| 리스크 | 영향 | 완화 |
|---|---|---|
| 비용 산정 부정확 → 예산 초과 | 중 | Phase 0 에서 정확한 측정 / 모니터링 셋업 |
| DE 시간 확보 못함 → 일정 지연 | 고 | OKR 인정 + 매니저 합의 |
| SQL 방언 번역 (Presto → BigQuery) 비용 과소평가 | 중 | Phase 1 에서 실측 |
| AVRO 컨슈머 호환성 (다운스트림 시스템) | 중 | 컨슈머 사전 합의 |
| BQ 슬롯 부족 → 쿼리 대기 | 중 | reservation 점진 증액 + 모니터링 |
| Composer 사고 시 fallback | 저 | Phase 3 이전엔 Neptune 항상 fallback 가능 |

### 4-2. 의사결정 미확정 항목

- [ ] **이관 일정** (Phase 0 시작 시점)
- [ ] **얼리어답터 DE 팀** (Phase 1 참여)
- [ ] **DE 마이그레이션 시간 OKR 인정 방식**
- [ ] **플랫폼팀 리소스** (Phase 0 풀타임 1~2명 + 이후 pair)
- [ ] **dbt project 구조** (단일 monorepo vs 도메인별 N 개)
- [ ] **비용 예산 승인**

---

## 5. 다음 단계

### 5-1. 단기 (보고 후 ~ Phase 0 시작)

1. **방향 / 일정 승인** (이 보고)
2. **현재 운영 비용 측정** — Hive / athlon DB / 인력
3. **DE 팀 합의** — 얼리어답터 1~2팀 선정
4. **인프라팀 협의** — Composer prod / BQ reservation / 비용 예산

### 5-2. Phase 0 (1개월)

- Composer prod 환경 셋업
- 공통 Operator 패키지 분리 + AR 배포
- CI/CD 자동화
- ETL 인벤토리 + 우선순위 매기기
- DE 온보딩 워크샵
- 자세한 활동: [[애슬론/6_마이그레이션 플랜]] § 4

### 5-3. Phase 1 ~ Phase 3 (8~9개월)

[[애슬론/6_마이그레이션 플랜]] 참조.

---

## 6. 백업 자료

| 노트 | 내용 |
|---|---|
| [[애슬론/5_Neptune SQL 변환의 dbt-BigQuery 대체 검토]] | PoC 결론 — 기술 실현 가능성 확정 |
| [[애슬론/6_마이그레이션 플랜]] | Phase 별 활동 / RACI / Gate / 일정 |
| [[스케줄러/15_관리 레포 인벤토리]] | 운영 모델 (3 레포) / AS-IS vs TO-BE |
| [[dbt/1_materialization]] ~ [[dbt/6_배포와 환경 분리]] | dbt 패턴 상세 |
| [[애슬론/2_Git 동기화·dbt 전환 계획]] | 초기 전환 계획 |
| [[애슬론/3_dbt 능력 경계와 영역 분담]] | dbt vs Airflow 책임 분리 |
| [[애슬론/4_Asset-Centric 아키텍처 안]] | Asset 기반 아키텍처 |
