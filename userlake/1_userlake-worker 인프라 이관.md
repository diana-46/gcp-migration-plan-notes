---
title: "userlake-worker GCP 이관 — 인프라 인벤토리"
status: draft
created: 2026-06-26
대상: userlake-worker 모듈
용도: 이관 범위 식별 / 기술 선택 검토
---

# userlake-worker GCP 이관 — 인프라 인벤토리

## 0. 한 줄 요약

> userlake-worker 는 RabbitMQ 큐에서 코호트 stage 실행 요청을 받아 Presto / Spark / HDFS / Kafka 로 처리하는 **메시지 드리븐 워커**.
> on-prem 의존이 무거움 (Kerberos + HDFS + Presto + RabbitMQ).
> **확정 방향**: Presto → **BigQuery**, RabbitMQ → **Pub/Sub**, HDFS → **GCS**. Hive 미사용으로 고려 대상 제외.
> 큰 코드 작업 3축 — (1) 쿼리 엔진 SQL/JDBC 재작성, (2) `consumer/` 패키지 Pub/Sub 재설계, (3) `core/util/filerw` 의 GCS 구현.
> **Spark Connect 는 Dataproc Serverless 로 lift 권장** — 제거하려면 sync/gate 재설계 수준이라 이관 범위 외로 두는 게 합리적.
> **사용량 데이터 (2026-06-29)** → [[11_사용량 분석 (한달 데이터 기반)]] — 일 평균 코호트 230 / stage 1,500. 75% 가 10만 행 이하 (다운사이즈 가능). 시간대 균일 (24h 상주 필요).

---

## 1. 모듈 역할 요약

- 8종의 stage (TARGET / GATE / SYNC / SLACK / CSV / COHORT / EXTRACT / EXTRACT_RUN) 를 RabbitMQ 큐에서 컨슘
- stage 별로 Presto 쿼리, Spark 집합 연산, 파일 복사, Kafka/HTTP 송신 등을 수행
- 결과를 파일 (CSV) 로 저장하고 result exchange 로 발행
- `@Scheduled` 없음 — 완전 이벤트 드리븐
- 동시성 / 재시도 / 타임아웃 / ack 가 큐 단위로 세밀하게 튜닝됨

---

## 2. 이관 대상 인프라 (현재 → GCP)

### 2-1. 메시지 브로커 — **Pub/Sub 확정**

| 항목 | 현재 | GCP |
|---|---|---|
| **RabbitMQ → Pub/Sub** | `dev-ay2-rabbitmq.dev.onkakao.net` / vhost `ent-dp-athlon` / quorum queue, manual ack, requeue | **Cloud Pub/Sub** (8개 stage 별 topic + subscription) |

상세 → **[[5_Pub-Sub 이관 (consumer 패키지 재작성)]]** (별도 문서)

요약:
- 작업량 **3~5주** — consumer/producer 패키지 전면 재작성 + userlake-api 의 result listener / request publisher 동반 재작성
- 영향: `consumer/`, `producer/`, `config/RabbitMQ*StageConfig` 8개 (worker) + `CohortRunStageResultListener` 등 (userlake-api)
- 핵심 리스크 4가지: basicNack ≠ Pub/Sub nack / quorum queue ordering → ordering key / dual retry (`@Retryable` + nack) 통합 / `delivery_attempt` 헤더 (DLT 활성 시에만)
- 의사결정 분기 5개 (topic 8개 vs 1개 통합, Pull vs Push, DLT 설계, ordering key, dual retry 통합) 별도 문서 § 5 참고

### 2-2. 분산 쿼리 엔진 — **BigQuery 확정**

| 항목 | 현재 | GCP |
|---|---|---|
| **Presto → BigQuery** | `kakaoent-presto-athlon.kakaodev.io:443`, Kerberos 인증, 카탈로그 `hadoop_kentdev` | **BigQuery** (사내 GCP 이관 표준, [[0_GCP 이관 보고]] 와 정합) |

상세 → **[[4_BigQuery 이관 (Presto 쿼리 엔진 전환)]]** (별도 문서)

요약:
- 작업량 **3~6주** — 이관 전체에서 가장 큰 코드 작업
- 영향: `TargetStageProcess`, `ExtractStageProcess`, `PrestoQueryCreator`, `PrestoTargetQueryBuilder`, **`distributed-query-engine` 공통 모듈 (athlon 전사)**
- 핵심 리스크 4가지: SQL 방언 / JDBC behavior (streaming·queryId·cancel·monitor) 보존 / 카탈로그 매핑 / `PrestoQueryFailureAnalyzer` 재구성
- 의사결정 분기 5개 (JDBC vs Storage Read API, 인터페이스 유지 vs 재설계 등) 별도 문서 § 8 참고
- 선결 의존: athlon 전사 데이터 GCP 이관 진행도 ([[0_GCP 이관 보고]]) — userlake-worker 의 BQ 이관은 그 위에 올라감

### 2-2-1. Spark Connect — 별도 의사결정 필요

userlake-worker 는 Spark Connect 를 **3가지 용도**로 씀:

| 위치 | 작업 | 본질 |
|---|---|---|
| `GateStageProcess` | CSV 읽어 `union` / `intersect` / `except` | 순수 SQL 가능 |
| `SyncStageProcess.copy` | CSV 읽고 `coalesce(1)` 후 다시 쓰기 | 단순 파일 처리 |
| `SyncStageProcess` 본 흐름 | CSV → splitter → senders (Kafka / Loupe / HTTP) | 분할 + 분배 |
| `SparkKafka.write` | DataFrame → Kafka topic | 외부 송신 |
| `CachedDataset` | `cache().count()` 패턴 | Spark API 의존 |
| ~~`HiveSync`~~ | parquet write + 파티션 | **미사용 — 제거 대상** |

본질은 **GCS 의 CSV 에 대한 (a) 집합 연산과 (b) 분기·송신**.

#### 옵션 비교

| 옵션 | 설명 | 코드 변경 | 운영 |
|---|---|---|---|
| **A. Dataproc Serverless + Spark Connect** | URL만 교체. Spark 3.4.2 호환 이미지 확인 | 최소 | 매니지드 (세션 콜드스타트 / idle 비용 주의) |
| B. Dataproc 일반 클러스터 | 클러스터 상주 | 최소 | 비용 균질 트래픽일 때 유리 |
| C. GKE + Spark Operator | GKE 일원화 | 최소 | Spark 직접 운영 — 권장 안 함 |
| **D. Spark 완전 제거** | Gate→BQ SQL, Kafka write→Pub/Sub publish, split/send→네이티브 | **큼** | Spark 의존 완전 제거. BQ + Pub/Sub + GCS 일관 |

#### 솔직한 평가 — Spark 제거는 "migration" 이 아니라 "redesign"

옵션 D 를 가볍게 적기 쉬운데, 실제 영향 받는 파일을 세보면 **재설계 수준**.

**Spark 직결 클래스 (5개)**: `SparkConnect`, `CachedDataset`, `SparkKafka`, `HiveSync`(미사용), `UserIdSchema`

**Spark Dataset 타입에 묶인 인터페이스 / 구현 (16 파일 + 테스트 4개)**:
- `SyncSender` + 구현 3개 (Default/Loupe/Convert) — 시그니처가 `Dataset<Row>`, `CachedDataset`, `SparkConnect`
- `SyncSplitter` + 구현 3개 (Default/Group/Sample) — `CachedDataset` 받음
- `SyncConverter` + 구현 (`IdConvertSyncConverter`, `IdConverterMap`) — `sparkSession` 받음
- `SyncSendFactory`, `SyncSplitFactory`, `SyncConvertFactory`
- `GateStageProcess`, `SyncStageProcess`

인터페이스 시그니처가 `Dataset<Row>` / `CachedDataset` 에 묶여 있어서 부분 교체가 불가. sender 하나 바꾸려 해도 데이터 컨테이너 추상을 새로 디자인해야 함.

Gate 의 union/intersect/except 도 1:1 같아 보이지만 실제로는:
- 입력이 GCS CSV → BQ 임시 테이블 load / external table 등록 / SQL 실행 / 결과 export 의 orchestration 새로 짜야 함
- BQ job 라이프사이클 (jobId, slot, 취소) ≠ Spark session 의미
- `canceled()` 매핑: `sparkSession.stop()` → BQ job cancel

#### 추천 — Spark Connect 유지 (Dataproc Serverless)

| | Spark 유지 (Dataproc Serverless) | Spark 제거 |
|---|---|---|
| 작업량 | URL · 스키마 교체 수준 | sync/gate 핵심 로직 **재설계** |
| 위험 | 낮음 (검증된 코드) | 높음 (코호트 결과 정합성 회귀) |
| 운영 표면 | Dataproc Serverless 1개 더 | 없음 (BQ + Pub/Sub 만) |
| 비용 | Spark Connect 세션 비용 | BQ slot + Pub/Sub |
| cloud-native 정렬 | 약간 어긋남 | 완벽 |

**결론**: Spark Connect 를 굳이 걷어낼 강한 이유 (성능/비용 문제 발견 등) 가 없다면 **Dataproc Serverless 로 유지**. 운영 표면 1개 늘어나는 비용 << 코호트 로직 재설계 + 회귀 테스트 비용.

> Spark 제거는 별도 의사결정으로 다루되, GCP 이관의 범위에는 포함하지 않는 것이 합리적.

#### 왜 Spark Connect 가 대체 불가능한가 (본질적 이유)

코드 자산 (16개 파일) 의 재설계 비용이 큰 것은 **결과**일 뿐, 본질은:

> **Spark Connect = "카탈로그 기반 동적 SQL + 인-메모리 임시 데이터 처리 + 분산 fanout" 한 API 통합**

userlake-worker 의 sync/gate 가 이 3가지를 동시에 활용:

1. **IdConvert**: dijkstra 로 런타임 동적 JOIN 체인 SQL 생성 + Hive Metastore 카탈로그 외부 테이블 JOIN
2. **Gate**: GCS 의 CSV path 만으로 즉시 union/intersect/except (BQ 면 매번 external table 등록 + cleanup)
3. **Sync**: `CachedDataset` 로 메모리 1번 캐시 → N 갈래 split → 동시 fanout (코루틴 + Spark distributed write)

BQ 는 강력하지만 **ad-hoc 임시 데이터 + fanout pipeline 에 ceremony 큼**. 외부 분해 (Pub/Sub, Cloud Run, Dataflow) 하면 운영 표면 증가.

→ 상세: [[2_Spark Connect → Dataproc Serverless 검토]] § -1

#### Dataproc Serverless 상세 검토

종류 선택 / 리소스 / 작업량 / PoC 검증 포인트 → **[[2_Spark Connect → Dataproc Serverless 검토]]**
비용 계산 / Pricing Calculator 입력값 / PoC 비용 전략 → **[[3_Spark Connect on Dataproc Serverless 비용 계산]]**

요약:
- 작업량 **1~2주** (이관 전체에서 가장 작음, critical path 아님)
- 운영 모델은 **Interactive Session 만 가능** — Gate/Sync 평균 4~10초 < cold start 30~60초, batch 불가
- 비용 (사용량 데이터 기반 [[11_사용량 분석 (한달 데이터 기반)]]): 다운사이즈 + 24h 셋팅 월 **~$900~1,300**
- **24h 상주 사실상 필수** — 시간대별 호출 거의 균일 (peak/lowest = 1.87×), idle 절약 효과 없음
- 75% 코호트가 10만 행 이하 → **현재 사이즈 (126 DCU) 4~6배 과다**, 다운사이즈 강력 권장

---

### 2-3. 분산 처리 / 데이터 저장

| 항목                 | 현재                                                         | GCP                                                        |
| ------------------ | ---------------------------------------------------------- | ---------------------------------------------------------- |
| **Spark Connect**  | `sc://localhost:15002`, Spark 3.4.2                        | **Dataproc Serverless** (Interactive Session) — 상세 [[2_Spark Connect → Dataproc Serverless 검토]] |
| **Hadoop / HDFS**  | hadoop-client 3.0, HDFS 경로                                 | **GCS** + `core/util/filerw` 의 `GCS` 타입 추가 — 상세 [[6_HDFS → GCS (FileSystemType 확장)]] |
| **로컬 파일 저장소**      | `USERLAKE_BASEDIR=/team/athlon/userlake/local/run/result/` | **GCS 버킷**                                                 |

HDFS → GCS 요약:
- 작업량 **3~4주** — `GcsFileReadWriter` 구현 + atomic rename 보상 + api 의 raw Hadoop FS 호출처 마이그레이션 + 회귀 검증
- 영향: athlon 전체 (`core` 모듈) — userlake-worker 의 모든 stage + api 일부
- 핵심 리스크 3가지: GCS atomic rename 없음 (Gate/Sync 의 finalize 흐름) / mtime 없음 / api 의 raw Hadoop FS 직접 호출
- **PoC 우선순위 1번** — 영향 범위 가장 넓고 atomic rename 가정 검증 필요

> Hive Metastore 는 sync 의 결과 저장 기능 (`userlake.stage.sync.hive.enabled=false`) 미사용. 단 Spark catalog 백엔드로는 Hive Metastore 사용 중 ([[2_Spark Connect → Dataproc Serverless 검토]] § 4-0).

### 2-4. 인증 (Kerberos 제거)

| 항목 | 현재 | GCP |
|---|---|---|
| **Kerberos** | `/etc/krb5.conf`, `.keytab`, realm `KAKAO.HADOOP`, user `hadoop-kent-data`, kinit sidecar | **GCP Service Account + Workload Identity** |

상세 → **[[7_Kerberos 제거 (인증 흐름 재설계)]]**

요약: 작업량 **3~5일** (자체) + 다른 이관 작업 (BQ § 4 / GCS § 1-4 / Spark Connect § 4-2) 에 분산. **5개 컴포넌트 인증 흐름 재설계** (worker pod 의 kinit sidecar, Presto JDBC, HDFS UGI, Spark keytab, Vault).

### 2-5. 데이터베이스 / 시크릿

| 항목 | 현재 | GCP |
|---|---|---|
| **MySQL** | `jdbc:mysql://localhost:3306/athlon...` | **Cloud SQL for MySQL** |
| **Vault** | `vault-beta.onkakao.net`, policy `data_platform_common`, `@VaultPropertySource` | **Secret Manager** (또는 Vault 유지) |

상세 → **[[8_MySQL Cloud SQL · Vault Secret Manager]]**

요약: 작업량 **2~3주** (Vault → Secret Manager 가 큼 — athlon 전사 `VaultDatabaseConfig` 영향: api / cdc-consumer / userlake-search-worker / userlake-worker). MySQL 자체는 1주 미만.

### 2-6. Sync stage 의 송신 대상 — 주력은 Loupe (사내 Kafka topic)

| 항목 | 현재 | GCP |
|---|---|---|
| **LoupeSyncSender → 사내 Kafka** (Spark Kafka write) | 사내 Kafka | (A) 사내 Kafka 유지 + Interconnect / (D) Pub/Sub 전환 |
| Convert / Default sender | HTTP / wrapper | 변경 없음 |

상세 → **[[9_Sync Kafka 송신 (Loupe destination)]]**

요약: **Loupe sender 가 HTTP 가 아니라 사내 Kafka topic write**. 작업량 **1~2주** (옵션 A 면 코드 변경 0, 네트워크 인프라만). 옵션 D 는 Loupe 팀 동반 이관 필요.

### 2-7. 컴퓨트 / 배포 / 운영

| 항목 | 현재 | GCP |
|---|---|---|
| **배포** | dp-gitops kustomize, K8s Deployment 1 replica | **GKE** + kustomize 그대로, **HPA 추가 권장** |
| 이미지 | `idock.daumkakao.io/kakaoent-dp/*` | **Artifact Registry** |
| 메트릭 / 로그 | micrometer / 사내 시스템 | **Cloud Monitoring + Logging** |
| 사내 망 ↔ GCP | — | **Cloud Interconnect / VPN** |

상세 → **[[10_컴퓨트 배포 운영 (GKE Monitoring)]]**

요약: 작업량 **1~2주**. 핵심 4가지: base image 사내 → public + Artifact Registry / kinit sidecar 제거 (§ 2-4) / 메트릭·로그 Cloud 전환 / **HPA Pub/Sub backlog 기반 추가** (현재 prod 도 1 replica 라 처리량 한계).

---

## 3. 코드 변경 영향도

### 3-1. 큰 작업 (구조 변경) — 4축

1. **Presto → BigQuery**
   - `PrestoQueryCreator` SQL 방언 전환 (Presto → BQ Standard SQL)
   - `TargetStageProcess` / `ExtractStageProcess` 의 JDBC 흐름 → BigQuery Java client / Storage Read API
   - `distributed-query-engine` 모듈 영향 (athlon 전사)
   - 카탈로그·테이블 구조 매핑
2. **RabbitMQ → Pub/Sub**
   - `consumer/` 패키지 8개 listener 재작성 (`@RabbitListener` → Pub/Sub subscriber)
   - `StageSubmitter` 의 ack/nack/requeue → ack deadline + DLQ + retry policy 로 재설계
   - `producer/StageResultProducer` Pub/Sub publish 로 교체
   - `config/RabbitMQ*StageConfig.kt` 8개 → Pub/Sub subscription 설정
   - 큐 길이 기반 HPA 트리거 재구성
3. **HDFS → GCS**
   - `core/util/filerw/` 에 `FileSystemType.GCS` 추가
   - `GcsFileReadWriter` 신규 (`HdfsFileReadWriter` 와 유사 패턴)
   - 모든 stage 의 결과 파일 경로가 여기 의존 — regression 위험 큼
4. **Kerberos 제거**
   - Presto 인증 흐름 폐기 (BQ 전환과 동시 진행)
   - Hadoop 인증 흐름 폐기 (HDFS→GCS 와 동시 진행)
   - keytab 관리 → Workload Identity / Service Account 키 없는 패턴
   - `application.yml` 의 `hadoop.kerberos.*` 4개 키 제거

### 3-2. 작은 작업 (URL / config 교체)

- MySQL URL (Cloud SQL), Spark Connect URL (Dataproc), Vault URI (이전 시)

### 3-3. 남은 결정 의존

- **Vault → Secret Manager** 가면 `VaultDatabaseConfig` 와 secret 조회 로직 전면 교체
- **Sync 의 Kafka 송신 대상** — 사내 Kafka 유지 / Managed Service for Kafka / Confluent Cloud on GCP / Pub/Sub 중 선택

---

## 4. 남은 결정 분기

| # | 분기 | 옵션 | 영향 |
|---|---|---|---|
| 1 | **Spark Connect 후속** | **Dataproc Serverless 로 유지** (제거는 sync/gate 재설계 수준이라 이관 범위 외 권장) | §2-2-1 참조 |
| 2 | **Vault 후속** | Secret Manager vs Vault 자체 호스팅 유지 | secret 조회 코드 + 정책 재구성 |
| 3 | **Sync Kafka 송신 대상** | Pub/Sub (broker 와 일관) / Managed Kafka / 사내 Kafka 유지 | `SparkKafka` + `SyncSendFactory` |

> 큰 분기 (쿼리 엔진 · 메시지 브로커) 는 BigQuery · Pub/Sub 로 확정. lift-and-shift 가 아닌 **cloud-native 전환** 방향으로 정렬됨. Spark Connect 는 단계별 (lift → 분해) 권장.

---

## 5. 다음 단계 (제안)

1. **§4 남은 분기 2개** 의사결정
2. PoC 우선순위 (영향 범위 큰 순):
   1. **HDFS → GCS** — `core/util/filerw` 가 모든 stage 의 결과 경로 진입점, 가장 먼저 검증
   2. **Presto → BigQuery** — 쿼리 의미·성능 검증 (특히 동적 SQL 패턴)
   3. **RabbitMQ → Pub/Sub** — ack/nack/retry 의미 매핑, 1개 stage 로 파일럿
3. 모듈별 이관 단계 plan 분리 작성

---

## 6. 참고

- 모듈 위치: `/userlake-worker`
- 핵심 의존: `core`, `distributed-query-engine`
- 관련 보고: [[0_GCP 이관 보고]], [[0_Cloud Composer 인프라 보고]]