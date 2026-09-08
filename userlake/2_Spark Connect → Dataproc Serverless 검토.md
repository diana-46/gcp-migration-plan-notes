---
title: "Spark Connect → Dataproc Serverless 이관 검토"
status: draft
created: 2026-06-26
대상: userlake-worker 의 Spark Connect 사용 부분
용도: Dataproc 종류 선택 / 리소스·비용 견적 / 작업량 산정
부모: [[1_userlake-worker 인프라 이관]]
---

# Spark Connect → Dataproc Serverless 이관 검토

## -1. 왜 Spark Connect 를 대체할 수 없는가 (가장 중요)

이전 § 2-2-1 (상위 문서) 에서 옵션 D (Spark 완전 제거) 를 "재설계 수준이라 이관 범위 외" 로 정리했는데, **그 비용적 결론의 근본 원인** 을 명확히 정리.

### 핵심 명제

> **Spark Connect 는 "카탈로그 기반 동적 SQL + 인-메모리 임시 데이터 처리 + 분산 fanout" 을 한 API 로 묶어주는 게 본질.**
> userlake-worker 의 sync/gate 가 이 세 가지를 동시에 활용하기 때문에 BigQuery 만으로 깔끔하게 대체 불가.

### 6가지 구체적 이유

#### 1. 카탈로그 기반 **동적** SQL (IdConvert 의 본질)

`IdConverterQuery` 가 dijkstra 그래프 탐색으로 런타임에 JOIN 체인 SQL 동적 생성 → `spark.sql(query)` 실행. JOIN 대상은 Hive Metastore 카탈로그의 외부 테이블.

BQ 대체 시: 매 실행마다 임시 외부 테이블 등록 + JOIN + cleanup lifecycle 관리. 또는 그래프의 모든 테이블을 BQ 로 사전 동기화. **Spark 는 catalog + in-memory 통합이라 ceremony 0**, BQ 는 catalog 객체화 비용 큼.

#### 2. CSV 의 **in-place** set 연산 (Gate 의 본질)

`spark.read.csv(path).union(...).intersect(...).except(...)` — GCS 의 CSV path 만 알려주면 즉시 처리.

BQ 대체 시: CSV 마다 external table 등록 → SQL UNION/INTERSECT/EXCEPT → 결과 export → 임시 테이블 cleanup. **Spark 는 path 기반, BQ 는 카탈로그 객체화 비용**.

#### 3. Lazy evaluation + **cache 로 재사용** (Sync 의 본질)

```kotlin
CachedDataset(df).use { cached ->
  val splits = splitter.split(cached)        // N 갈래 분할
  splits.mapIndexed { i, s -> senders[i].send(s) }  // 동시 송신
}
```

같은 source 를 메모리 캐시 1번 → N 번 재사용. BQ 로는 stage 임시 테이블 + N 번 query + cleanup 필요.

#### 4. 분산 **fanout write** (Sync 의 destination 송신)

`SyncSender` 가 코루틴 async/await + Spark partition 단위 distributed write 로 Kafka / Loupe / HTTP 동시 송신. BQ + Pub/Sub + Cloud Run 으로 분해하면 데이터 흐름 흩어짐 + lifecycle / 에러 처리 복잡.

#### 5. 데이터 사이즈에 맞는 처리 모델

코호트 user ID 수십만~수천만 행 (수십 MB~수 GB) → Spark executor 메모리 안에서 한 번 read + 처리 완료. BQ 는 import/export 왕복 비용 (네트워크 + 스토리지 + 시간).

#### 6. 코드 자산 (실용적 이유)

`sync/`, `gate/`, `sparkconnect/` 의 16개 파일 + 인터페이스 (`SyncSender`, `SyncSplitter`, `SyncConverter`, `CachedDataset`) 가 모두 Spark `Dataset<Row>` / `SparkSession` 에 묶임. BQ + Pub/Sub + 외부 fanout 분해 = **재설계** = 코호트 결과 회귀 위험.

### 결론

BigQuery 는 강력한 분석 엔진이지만 **ad-hoc 임시 데이터 + fanout pipeline 에는 ceremony 가 큼**. 그걸 외부 시스템 (Pub/Sub, Cloud Run, Dataflow) 으로 분해하면 운영 표면이 더 늘어남.

→ **Dataproc Serverless 로 Spark Connect 를 lift 하는 게 정답.**

---

## 0. 결론

> **Dataproc Serverless for Spark** + **Interactive Session 모델** 이 정답 (현재 GKE long-lived Spark Connect 서버와 매핑).
> 작업량: PoC 1.5~2주, 운영 단계 (GCP 전용 이미지) 포함 시 **2~3주**.
> 비용은 별도 문서 → **[[3_Spark Connect on Dataproc Serverless 비용 계산]]**. 요약: **GKE 직접 ~$964/월** (CUD 3년 시 ~$618), Dataproc Cluster ~$1,490, Serverless ~$5,443. **GKE 직접이 사내 패턴 + 최저 비용**.
> **이미지 자체엔 사내 좌표 baked-in 없음** (prod 3.4.2 빌드 = `eclipse-temurin:17-jre` public base + ConfigMap 으로만 conf 주입). ConfigMap 만 GCP 용으로 새로 만들면 됨.
> 단 이미지에 **사내 patched Hadoop (`khp-p7`)** 이 들어있어 PoC 에서 GCP 호환성 검증 필요. (`kent-dataplatform-jars` 는 표준 lib, Hudi 는 BQ 표준이라 무관)
> **Spark catalog 백엔드 = `spark-bigquery-connector`** (Dataproc Serverless 기본 포함). Hive Metastore / Dataproc Metastore 불필요. BQ 이관과 Spark Connect 이관이 **독립적**.
> **§-1 에 Spark Connect 가 대체 불가능한 6가지 이유** 정리 (의사결정 근거).

---

## 1. Dataproc 종류 선택

| 종류 | 특징 | userlake-worker 적합도 |
|---|---|---|
| **Dataproc 일반 클러스터** | VM 항상 떠 있음. 시간당 과금 | ❌ idle 시간 큼 — 낭비 |
| **Dataproc Serverless for Spark** | 잡 단위 자동 스케일. 초 단위 과금 (1분 최소) | ✅ **최적** |
| Dataproc on GKE | Spark on K8s. GKE 일원화 | ❌ Spark Operator 직접 운영 |

**선택 근거**:
- userlake-worker 는 **버스트성** 워크로드 — Pub/Sub 메시지 도착 시에만 Spark 호출, 사이사이 idle
- 각 stage 가 timeout 기반의 짧은 잡, 동시성 변동 큼
- 일반 클러스터면 야간/주말 idle 시간 비용 그대로 나감
- Serverless 는 잡 끝나면 0원, 콜드스타트 비용만 감수

---

## 2. 워크로드 특성 분석 (코드 기반 추론)

### 가벼운 편의 신호

- `coalesce(1)` — 단일 파티션 출력. 대용량 처리 패턴 아님
- 입력이 `userid` 컬럼 1개인 CSV
- 집합 연산 (union/intersect/except) — 메모리 효율 좋은 연산
- 각 stage 가 timeout 가지고 짧게 종료

### 무거워질 수 있는 신호

- `GateStageProcess` 의 fold 누적 — 코호트 크기 × 개수만큼 메모리에 들고 있음
- `CachedDataset` 의 `cache().count()` 패턴 — 메모리 점유
- `SyncStageProcess` 의 코루틴 fanout — 동시 송신으로 네트워크·CPU 동시 사용

### 짐작

코호트당 user ID 수십만 ~ 수천만 행 가정 시 **DCU 2~4개 × 분 단위 실행** 수준. 그리 무겁지 않을 것으로 추정.

> ⚠ **정확한 사이징은 현재 운영 데이터 필요**
> - 사내 Spark UI / 모니터링에서 executor 수, 메모리, 평균 job 실행 시간 확인
> - 코호트 크기 분포 (P50 / P95 / P99)

---

## 3. 비용

→ **[[3_Spark Connect on Dataproc Serverless 비용 계산]]** (별도 문서)

### 핵심 요약 (2026-06-29 사용량 데이터 기반 갱신)

- **운영 모델**: Interactive Session 만 가능 (Gate/Sync 평균 4~10초 < cold start 30~60초)
- **24h 상주 사실상 필수** — 시간대별 호출 거의 균일 (peak/lowest = 1.87×), stage 간격 89% 가 10초 이내
- **GKE 직접 운영 권장** — 사내 패턴 그대로 + 최저 비용
- **비용 비교** (n2-custom 8c/20G × 9 기준):
  - **GKE 직접 (zonal): ~$964/월 (SUD), ~$618 (CUD 3년)** ← 권장
  - Dataproc Cluster: ~$1,490/월 (Dataproc fee +$526)
  - Serverless: ~$5,443/월 (24h 상주 효과)

데이터 근거 → [[11_사용량 분석 (한달 데이터 기반)]]

상세 (시나리오 비교 / DCU 환산 / Pricing Calculator 입력값 / PoC 비용 전략 / 정확도 향상에 필요한 데이터) 는 별도 문서 참고.

---

## 4. 작업량 견적

**총 5~10일** — 이관 전체에서 가장 작은 작업.

| 작업 | 소요 |
|---|---|
| `spark.connect.url` 환경변수 교체 | 5분 |
| Dataproc Serverless runtime / 이미지 전략 (§4-1, §4-3) | 1~2일 |
| ~~Dataproc Metastore 셋업~~ | ~~불필요~~ — `spark-bigquery-connector` 가 카탈로그 역할 (§4-2 B) |
| `IdConvertSyncConverter` 의 JOIN 대상을 BQ temp view 등록 패턴으로 (B3) | 1~2일 |
| ConfigMap (hadoop-conf) GCP 용 작성 (hive-conf 는 제거 가능) | 0.5일 |
| 사내 이미지 GCP 에서 pull 가능 여부 검증 (옵션 1) | 0.5일 |
| (운영 단계) GCP 전용 이미지 빌드 파이프라인 (옵션 3) | 1주 |
| gcs-connector 통합 | 0.5일 |
| Spark Connect 서버 운영 모델 결정 (interactive vs batch) | 1일 (PoC) |
| GCS 경로 (`gs://...`) Spark 에서 읽기 검증 | 0.5일 (FileSystemType.GCS 작업과 묶음) |
| 네트워킹 (GKE → Dataproc, VPC, SA) | 0.5~1일 (인프라팀 의존) |
| Spark Connect 인증 (Workload Identity) | 0.5일 |
| Gate stage end-to-end 테스트 | 1~2일 |
| Sync stage end-to-end 테스트 (Kafka/Pub/Sub 연동) | 2~3일 |
| Cloud Monitoring 알람 / 메트릭 | 0.5일 |

### 다른 이관 작업과의 비교

| 작업 | 규모 |
|---|---|
| Pub/Sub 이관 (`consumer/` 전면 재작성) | 2~4주 |
| BigQuery 이관 (`PrestoQueryCreator` + JDBC) | 3~6주 |
| HDFS → GCS (`FileSystemType`, `GcsFileReadWriter`) | 1~2주 |
| **Spark Connect → Dataproc Serverless** | **1~2주** |

→ 이관 일정에서 **clitical path 아님**. BQ / Pub/Sub 이관에 묶여 자동으로 늦춰져도 OK.

---

## 4-0. 현재 사내 Spark Connect 배포 분석

GitOps 위치: `dp-gitops/athlon/spark-connect/` (base / qa / prod)

### 이미지 안 (`spark-connect-3.4.2-p3`)

빌드 소스: `WebstormProjects/spark-k8s-build`, 브랜치 **`spark-connect-khp-3.4.2-hadoop2.10.2`** (master 아님 — master 는 옛 Spark 3.1.3)

`RELEASE`: `Spark 3.4.2-khp-p1 ... built for Hadoop 2.10.2-khp-p5`, 빌드 옵션 **`-Phadoop-provided`** (Hadoop 외부 주입 모델)

#### 진짜 Dockerfile (3.4.2 브랜치의 메인 `Dockerfile`, upstream-style)

```dockerfile
FROM eclipse-temurin:17-jre              # ✅ public base image (Java 17)

COPY jars /opt/spark/jars                                       # Spark distribution (hadoop 없음)
ADD hudi-spark3.*-bundle_*.jar.tar.gz /opt/spark/jars          # Hudi 0.15.0 번들
COPY khp-hadoop-jars /opt/spark/jars                            # 사내 patched Hadoop 2.10.2-khp-p7
COPY kent-dataplatform-jars /opt/spark/jars                     # 사내 데이터플랫폼 라이브러리
COPY bin /opt/spark/bin
COPY sbin /opt/spark/sbin
# region-conf 는 COPY 안 함 → 사내 클러스터 좌표 baked-in 없음
```

**중요 정정**: 이전 § 4-0 에서 "사내 클러스터 좌표가 이미지에 baked-in" 이라고 한 건 master 의 옛 3.1.3 빌드 (`Dockerfile-production`) 얘기였음. **prod 3.4.2 빌드는 baked-in 없음**. ConfigMap mount 로만 들어옴.

#### 이미지 내용 정리

| 항목 | 상세 | GCP 영향 |
|---|---|---|
| base image | `eclipse-temurin:17-jre` (public) | ✅ GCP pull 가능 |
| Java | 17 | OK |
| Spark | 3.4.2-khp-p1 (사내 patch) | `-khp-p1` 패치 내용 확인 필요 |
| `khp-hadoop-jars` | Hadoop **2.10.2-khp-p7** (사내 patch) | HDFS jars 는 불필요. Hive Metastore Thrift 통신용 `hadoop-common`, `hadoop-auth` 는 필요. 표준 jar 로 교체 권장 |
| `kent-dataplatform-jars` | `spark-connect_2.12-3.4.2`, `spark-kubernetes_2.12-3.4.2`, `kubernetes-client-6.4.1`, guava/jackson 등 (37개) | **표준 Apache 라이브러리들** — `-Phadoop-provided` 빌드 보강용. 사내 lib 아님. "kent" 는 사내 Hadoop 클러스터 이름 (`hadoop-kent`) |
| `hudi-spark3.4-bundle_2.12-0.15.0` | Apache Hudi 0.15.0 | **GCP 에서는 BigQuery 표준이라 무관** — userlake-worker 가 Hudi 쓰는지 신경 안 써도 됨 |
| `region-conf/*` | baked-in 안 됨 | ConfigMap mount 로 주입 (dp-gitops) |

### dp-gitops 의 ConfigMap 마운트

`/opt/hadoop/conf`, `/opt/hive/conf` 에 ConfigMap mount 로 사내 좌표 주입.
**GCP 옮길 때는 이 ConfigMap 만 새로 만들면 됨** → 옵션 1 (§4-3) 이 매우 현실적.

### 이미지 밖에서 mount/주입되는 사내 의존성

| 소스 | mount | 용도 |
|---|---|---|
| Secret `athlon-kerberos-secret` | `/opt/kerberos/keytab` | Kerberos keytab |
| ConfigMap `athlon-hadoop-config` | `/opt/hadoop/conf` | core-site / hdfs-site / hive-site |
| ConfigMap `athlon-kerberos-config` | `/etc/krb5.conf` | Kerberos realm |
| ConfigMap `spark-connect-spark-config` | `/opt/spark/conf` | spark-defaults.conf, metrics.properties |
| ConfigMap `spark-pod-templates` | `/opt/spark/pod-templates/` | Executor pod template |

### `spark-defaults.conf` 의 핵심 설정

```
spark.master k8s://kubernetes.default.svc           # Spark on K8s
spark.sql.catalogImplementation hive                 # ⚠ Hive Metastore 사용
spark.sql.hive.metastore.version 2.3.2
spark.kerberos.principal hadoop-kent-data@KAKAO.HADOOP
spark.driver.cores 8 / memory 10G + 4G overhead
spark.executor.instances 8 / memory 10G + 4G / cores 8
*.sink.graphite.host=dp-vminsert.kakaoent.io         # 사내 VictoriaMetrics
```

### 현재: Spark catalog 백엔드가 Hive Metastore (사내)

`spark.sql.catalogImplementation hive` 명시. 이전에 "Hive 안 씀" 으로 정리한 건 **sync stage 의 `userlake.stage.sync.hive.enabled` 기능 (결과 저장)** 만이었고, **Spark catalog 자체의 백엔드는 Hive Metastore 사용 중**.

이게 §2-2-1 끝에 의심했던 `IdConvertSyncConverter` 의 JOIN 대상 테이블 출처. 즉 IdConvert 가 동적으로 SQL 짜서 JOIN 하는 테이블들이 **Hive Metastore 카탈로그에 등록돼있음**.

→ GCP 이관 시 **`spark-bigquery-connector`** 로 자연 해결 (§4-2 B 참조). Dataproc Metastore 등 별도 카탈로그 인프라 불필요.

---

## 4-1. Spark 버전 / 이미지 매핑

현재 사용 중: `idock.daumkakao.io/kakaoent-dp/spark-k8s-build:spark-connect-3.4.2-p3` (사내 커스텀 이미지)

### Dataproc Serverless 의 버전 모델

Dataproc Serverless 는 **Spark 버전 직접 지정이 아니라 "runtime version"** 으로 묶음.

| Runtime | Spark |
|---|---|
| 1.0 | 3.2.x |
| 1.1 | 3.3.x |
| **1.2** | **3.4.x** ← 가장 가까움 |
| 2.0 / 2.1 | 3.5.x |
| 2.2 | 3.5.x (LTS) |

> ⚠ 정확한 patch 버전은 [공식 runtime release notes](https://cloud.google.com/dataproc-serverless/docs/concepts/versions/spark-runtime-versions) 에서 매번 확인.

### 옵션 3가지

| 옵션 | 설명 | 사내 이미지 보존 | 매니지드 |
|---|---|---|---|
| **1. Runtime 1.2 그대로** | `runtime_version: "1.2"` 명시 | ❌ | ✅ |
| **2. Custom container on Dataproc Serverless** | 사내 `spark-connect-3.4.2-p2` 이미지를 Artifact Registry 로 푸시 후 사용 | ✅ | ✅ |
| 3. GKE 에 Spark Connect 서버 직접 | 현재 K8s manifest 그대로 GKE 로 | ✅ | ❌ |

### 추천: 옵션 1 PoC → 안 되면 옵션 2

`-p2` 패치가 무엇인지 (사내 jar / 인증 / 모니터링 등) 에 따라 다름:

- **사내 특화 부분 있음** → 옵션 2 (custom container) 가 답
- **단순 빌드 옵션 차이** → 옵션 1 (runtime 1.2) 로 충분

검증 순서:
1. 사내 이미지의 Dockerfile / 빌드 스크립트 확인 (`-p3` 차이점 파악)
2. [Dataproc Serverless custom container 요구사항](https://cloud.google.com/dataproc-serverless/docs/guides/custom-containers) 확인
3. PoC: 옵션 1 (runtime 1.2) 로 우선 시도 → 호환 이슈 발생 시 옵션 2

---

## 4-2. mount/config 별 GCP 이관 매핑

### A. 제거 가능 (사내 인프라 의존 폐기)

| 항목 | 이유 |
|---|---|
| Kerberos keytab Secret | Workload Identity / SA 로 대체 |
| krb5.conf ConfigMap | Kerberos 자체 폐기 |
| Hadoop conf 의 HDFS 부분 (core-site, hdfs-site) | HDFS 안 씀 |
| **Hive conf (hive-site.xml) ConfigMap** | `spark-bigquery-connector` 가 catalog 대체 (§4-2 B) |
| **`spark.sql.catalogImplementation hive` 설정** | 위와 동일 — 제거 |
| `purpose=spark-connect` 노드풀 affinity / toleration | Dataproc Serverless 자체 인프라 |
| Executor pod template | Dataproc Serverless 자동 관리 |
| Spark on K8s (`spark.master k8s://...`) 설정 | Dataproc Serverless 가 알아서 |

### B. Spark catalog 백엔드 — `spark-bigquery-connector` 가 답

이전 분석에서 "가장 큰 분기" 라고 했는데, **사내 데이터가 BQ 로 이관된다는 전제** 위에서 답이 명확해졌음.

**핵심 사실**: Spark 가 **`spark-bigquery-connector`** 통해 BQ 테이블을 직접 쿼리 가능. Dataproc Serverless runtime 에 **기본 포함**.

```kotlin
// Pattern 1: BQ 테이블을 temp view 로 등록
spark.read.format("bigquery")
  .option("table", "project.dataset.my_table")
  .load()
  .createOrReplaceTempView("my_table")

spark.sql("SELECT ... FROM csv_temp JOIN my_table ON ...")  // IdConvert 패턴 그대로

// Pattern 2: JOIN 자체를 BQ 에 푸시
spark.read.format("bigquery")
  .option("query", "SELECT ... FROM dataset.table1 JOIN dataset.table2 ON ...")
  .load()
```

→ **Hive Metastore 자체가 불필요해짐**. JOIN 대상이 BQ 로 가니까.

#### 옵션 재정리

| 옵션 | 설명 | 평가 |
|---|---|---|
| ~~B1. Dataproc Metastore~~ | 매니지드 Hive Metastore | ❌ 사내 Hive 가 BQ 로 가니까 불필요 |
| **B2. BigLake Metastore (BQ catalog)** | BQ 테이블이 Spark catalog 에 자동 노출 | ✅ `SELECT * FROM project.dataset.table` 그대로 |
| **B3. 매 잡마다 temp view 등록** | dijkstra 가 결정한 N개 테이블을 `createOrReplaceTempView` 로 등록 | ✅ IdConvert 코드 변경 작음. pragmatic |
| **B4. JOIN 을 BQ 푸시** | `spark.read.format("bigquery").option("query", ...)` | ✅ 가장 빠름 (BQ slot 사용). IdConvert 의 dijkstra 결과 SQL 을 BQ 에 던짐 |

**추천: B3 (매 잡마다 temp view)** — IdConvert 의 dijkstra 가 이미 필요 테이블 N개를 결정하니, 그 N개를 `spark.read.format("bigquery").load().createOrReplaceTempView(...)` 로 등록한 후 동적 SQL 실행. 코드 변경 작음.

추가로 **B4** 를 옵션으로 — 대용량 JOIN 이면 BQ 에서 처리하는 게 빠를 수 있음.

#### 동작 메커니즘

- **Read**: BigQuery Storage Read API (gRPC, Arrow columnar). HDFS read 보다 보통 빠름
- **Pushdown**: predicate / column pruning / partition filter 가 BQ 로 푸시됨
- **인증**: Workload Identity 자동
- **비용**: BQ Storage Read API ~$1.10/TB. 코호트 데이터 (수십만 ~ 수천만 행) 면 미미

#### 큰 의미

**BQ 이관과 Spark Connect 이관은 독립적**:
- 사내 Hive 테이블이 BQ 로 가는 일정 ([[0_GCP 이관 보고]]) 따라 진행
- Spark Connect 는 그동안 BQ 테이블 / GCS CSV / 뭐든 다 처리 가능
- userlake-worker 의 코호트 로직 (gate / sync / convert) **재설계 불필요**

#### 선결 확인 사항

- IdConvert 의 JOIN 대상 테이블이 사내 데이터 이관에서 BQ 로 옮겨질 일정
- 그 동안 사내 Hive 접근 유지 필요한지 (Phase 1 lift-and-shift) vs BQ 로 가는 테이블만 부분 PoC 가능한지

### C. 그대로 가져가는 것

| 항목 | GCP 대응 |
|---|---|
| `spark-defaults.conf` 의 driver/executor 사이징 | Dataproc Serverless spark properties 로 그대로 |
| 사내 Graphite 메트릭 (`dp-vminsert.kakaoent.io`) | Cloud Interconnect 통과 시 그대로, 또는 Cloud Monitoring 전환 |
| `spark.executor.instances 8` 등 동시성 설정 | properties 그대로 (또는 dynamic allocation 으로 전환) |

### D. 새로 추가

| 항목 | 작업 |
|---|---|
| `gcs-connector` | Dataproc Serverless stock runtime: 자동 포함 / Custom container: jars/ 에 직접 추가 필요 |
| **`spark-bigquery-connector`** | Dataproc Serverless stock runtime: 자동 포함 / Custom container: jars/ 에 직접 추가 |
| GCS / BQ 인증 | Workload Identity 자동 (또는 `spark.hadoop.fs.gs.auth.*`) |
| **`IdConvertSyncConverter` 의 BQ temp view 등록 로직** | dijkstra 가 결정한 N개 테이블을 `spark.read.format("bigquery").load().createOrReplaceTempView()` 로 등록 (B3 패턴) |
| VPC / Cloud Interconnect | userlake-worker (GKE) ↔ Dataproc Serverless ↔ 사내 Graphite |

---

## 4-3. 사내 이미지의 GCP 이전 전략

이미지에 사내 클러스터 좌표가 baked-in 이라 단순히 "옮기기" 가 안 됨. 3가지 옵션.

### 옵션 1: 사내 이미지 그대로 + ConfigMap 으로 override

prod 3.4.2 빌드는 **baked-in 사내 좌표 없음** → ConfigMap 만 GCP 용으로 새로 만들면 됨.

| ConfigMap | 사내 (현재) | GCP |
|---|---|---|
| `core-site.xml` `fs.defaultFS` | `hdfs://hadoop-kent` | 삭제 또는 `gs://<bucket>` (GCS 인증 추가) |
| `hdfs-site.xml` | HDFS HA + Kerberos | **삭제** (HDFS 안 씀) |
| `hive-site.xml` `hive.metastore.uris` | `thrift://hadoop-kent-rm1~3.dakao.io:9083` | `thrift://<dataproc-metastore-endpoint>:9083` |
| Kerberos 설정 | `KAKAO.HADOOP` realm | **삭제** (Workload Identity) |

장점: 이미지 빌드 안 함. PoC 가장 빠름. base image 가 public 이라 사내 망 의존 약함.
단점: 사내 레지스트리 (`idock.daumkakao.io`) 에서 GCP 가 이미지 pull 가능 여부 확인 필요. `khp-hadoop-jars` / `kent-dataplatform-jars` 의 사내 patch 가 GCP 환경에서 호환되는지 PoC 필요.

### 옵션 2: 사내 이미지를 Artifact Registry 로 복제

옵션 1 + 이미지를 Google Artifact Registry 로 복제.

장점: GCP 내부에서 빠른 pull, 사내 망 의존 제거.
단점: 이미지 사이즈만큼 복제 비용 + 버전 동기화 부담.

### 옵션 3: GCP 용으로 이미지 다시 빌드

`spark-k8s-build` 의 `spark-connect-khp-3.4.2-hadoop2.10.2` 브랜치를 fork 후:
- base image `eclipse-temurin:17-jre` 그대로 (이미 public)
- `khp-hadoop-jars` 를 **표준 Apache `hadoop-common-2.10.2`** 등으로 교체 (HDFS jars 제거, 사내 patch 제거)
- `kent-dataplatform-jars` 그대로 (표준 라이브러리 보강용, 사내 종속성 없음)
- **`gcs-connector` jar 추가** (사내 빌드엔 없음)
- Hudi 번들 — **제거 가능** (BQ 표준이라 무관)

장점: 깔끔. self-contained. 사내 patch 의존 완전 제거.
단점: 빌드 파이프라인 새로 만들어야 함. 사내 patch (`khp-p7`) 가 보장하던 기능 (HA, 인증 등) 이 표준 jar 로 호환되는지 검증 필요.

### 추천: 옵션 1 PoC → 운영은 옵션 3

- PoC: 옵션 1 로 빠르게 동작 검증 (ConfigMap 만 새로 만들어서 사내 이미지를 GCP 에서 끌어와봄)
- 운영: 옵션 3 로 GCP 전용 이미지 빌드 (사내 망 의존 완전 제거)

### 주의 사항 / 확인 필요 항목

1. **`khp-hadoop-jars` (Hadoop 2.10.2-khp-p7)** — HDFS jars 는 GCP 에서 불필요, hadoop-common / hadoop-auth 정도만 필요. **표준 Apache jar 로 교체 권장** (사내 patch `khp-p7` 이 GCP 환경과 충돌 가능성)
2. **`kent-dataplatform-jars`** — 확인 완료: Spark Connect + Spark Kubernetes + 표준 라이브러리들 (37개). 사내 lib 아님. **`-Phadoop-provided` 빌드 보강용**. GCP 에서도 그대로 가져가도 되거나, Dataproc Serverless runtime 이 자체 제공 가능
3. ~~Hudi 0.15.0 번들~~ — **무관**. GCP 에서는 BigQuery 가 표준이라 Hudi 호환성 신경 안 써도 됨. 이미지에 들어있어도 userlake-worker 가 안 쓰면 그만
4. **`gcs-connector` 없음** — 사내 빌드 `jars/` 에 google/gcs jar 0개. 옵션 1/2 도 ConfigMap 으로 spark.jars.packages 추가 또는 mount 로 주입 필요. 옵션 3 에서는 이미지에 baked-in
5. **`-Phadoop-provided` 빌드 옵션** — Spark 자체에 Hadoop 없음. `khp-hadoop-jars` 가 채워줌. Dataproc Serverless runtime 이 자체 hadoop classpath 제공할 가능성 있음 → 충돌 검증 필요
6. **사내 메트릭 sink** (`dp-vminsert.kakaoent.io`) — `metrics.properties` 에 있음. Cloud Interconnect 또는 Cloud Monitoring 전환 결정 필요
7. **사내 patch `khp-p1` (Spark) / `khp-p7` (Hadoop)** — 어떤 fix 들어갔는지 파악 필요. 예: HA, 인증, Kerberos 통합 등

---

## 5. PoC 검증 포인트 (4가지)

이 4개 검증하면 비용·리소스 답이 거의 다 나옴.

### 5-1. Spark Connect 세션 모델

- 현재 `SparkConnect(...).use { ... }` 패턴이 Serverless batch 모드와 어떻게 맞물리는지
- 잡당 세션 생성 비용 vs 공유 세션 운영 비용 비교
- 결정 영향: 운영 모델, 비용 모델

### 5-2. 콜드스타트 영향

- Serverless 첫 잡 startup 시간 (보통 30초 ~ 1분 추정)
- 현재 stage timeout 설정과 충돌 안 하는지 확인 필요
- `application.yml` 의 stage 별 timeout 확인 (소스에 따로 기재 안 됨, DB 에 stage 별 설정 있을 가능성)

### 5-3. GCS 읽기/쓰기 성능

- `coalesce(1)` 결과를 GCS 에 쓸 때 latency
- HDFS 대비 어느 정도 차이 나는지
- `gcs-connector` 가 Spark Connect 클라이언트에 자동 포함되는지 의존성 확인

### 5-4. 동시성 / 스케일

- 큐에서 5개 동시에 들어왔을 때 Dataproc Serverless 가 잘 스케일하는지
- GCP 프로젝트의 DCU quota 확인 (기본 12 DCU 정도일 것)
- Burst 시 잡 대기열 생기는지

---

## 6. 미해결 질문

PoC 전에 답할 수 없는 것들 — 데이터 / 인프라팀 협의 필요.

- [ ] 현재 Spark 클러스터의 executor 수 / 메모리 / 평균 잡 시간 (Spark UI / 사내 모니터링)
- [ ] 일일 Gate + Sync stage 실행 횟수 (사내 메트릭)
- [ ] 코호트 크기 분포 (P50 / P95 / P99 user ID 수)
- [ ] GCP 프로젝트의 Dataproc Serverless DCU quota 기본값 / 증액 가능 여부
- [ ] Dataproc Serverless 와 GKE (userlake-worker pod) 의 VPC 연결 방식
- [ ] Workload Identity 적용 가능한지 (Dataproc Serverless 가 K8s SA 매핑 지원하는지)

---

## 7. 참고

- 상위 문서: [[1_userlake-worker 인프라 이관]] § 2-2-1
- 코드 위치: `userlake-worker/src/main/kotlin/com/kakaopage/athlon/stage/sparkconnect/`
- 영향 받는 stage: `GateStageProcess`, `SyncStageProcess`