---
title: "Sync stage 의 Kafka 송신 — Loupe destination"
status: draft
created: 2026-06-28
대상: userlake-worker 의 SyncSender / SparkKafka / LoupeSyncSender
용도: Sync stage 의 외부 destination 송신 방식 / Kafka 이관 옵션
부모: [[1_userlake-worker 인프라 이관]]
---

# Sync stage 의 Kafka 송신 — Loupe destination

> 상위 문서: [[1_userlake-worker 인프라 이관]] § 2-6

## 0. 결론

> 중요 발견: **`LoupeSyncSender` 가 HTTP 가 아니라 사내 Kafka topic 으로 write**. Loupe = 사내 추천/세그먼트 서비스의 Kafka topic.
> → Sync stage 의 송신 = 사실상 **Kafka write 가 주력**, HTTP 는 부수적.
> 작업량 **1~2주** (옵션에 따라 다름). Kafka destination 결정이 핵심 분기.
> 추천: **사내 Kafka 유지 + GCP 에서 Cloud Interconnect 로 접근** (1단계) → 장기적으로 Pub/Sub or Managed Kafka 이전 (2단계, 사내 Loupe 가 같이 옮길 때).

---

## 1. Sync 의 송신 대상 정리

### 1-1. SyncSender 구현 3종

| Sender | 실제 동작 | 의존 |
|---|---|---|
| **`LoupeSyncSender`** | Spark Dataset → struct 구성 → **`SparkKafka.write(bootstrapServers, topic)`** | 사내 Kafka |
| **`ConvertSyncSender`** | ID 변환 후 destination 전달 (구체적 destination 은 wrapper) | 의존 |
| **`DefaultSyncSender`** | Spark Dataset 그대로 sender 의 doSend 호출 | 의존 |

→ **`LoupeSyncSender` 가 가장 빈번한 destination**. ConvertSync / DefaultSync 는 변환·기본 로직 wrapper.

### 1-2. LoupeSyncSender 의 실제 코드

```kotlin
// userlake-worker/src/main/kotlin/.../sync/send/LoupeSyncSender.kt
override fun doSend(dataset: Dataset<Row>, rowCount: Long) {
    // STEP 1-4: page / meta / data struct 구성
    // STEP 5: to_json 으로 value 컬럼 만듦
    val dfForKafka = dfWithMeta.withColumn("value", ...)
        .select(to_json(col("value")).alias("value"))

    sparkKafka.write(
        bootstrapServers = userlakeLoupe.bootstrapServers,
        topic = userlakeLoupe.topic,
        dataset = dfForKafka
    )
}
```

→ Loupe destination 의 `bootstrapServers` + `topic` 은 **`UserlakeLoupe` 모델에서 (즉 DB 에서) 동적 로드**. application.yml 에 박힌 게 아님.

### 1-3. SparkKafka 본체

```kotlin
class SparkKafka {
    fun write(bootstrapServers: String, topic: String, dataset: Dataset<Row>) {
        dataset.write()
            .format("kafka")
            .option("kafka.bootstrap.servers", bootstrapServers)
            .option("kafka.compression.type", "snappy")
            .option("topic", topic)
            .save()
    }
}
```

→ Spark 의 Kafka data source 사용. spark-sql-kafka jar 필요.

---

## 2. 옵션 비교

### 2-1. Kafka destination 후속 선택지

| 옵션 | 설명 | userlake-worker 영향 |
|---|---|---|
| **A. 사내 Kafka 유지 + GCP 에서 Interconnect 로 접근** | Loupe 가 사내에 있는 한 자연스러움 | bootstrap-servers 그대로, network 라우팅만 |
| **B. Managed Service for Kafka (GCP 신규)** | GCP 매니지드 Kafka | Loupe 가 같이 옮겨야 의미 있음 |
| **C. Confluent Cloud on GCP** | 외부 매니지드 | 비용 vs 안정성 |
| **D. Pub/Sub** | Pub/Sub 으로 전환 | `SparkKafka` 어댑터 + `SyncSendFactory` 재작성 + Loupe 측 consumer 도 변경 |

### 2-2. 추천: 단계별

**Phase 1 (이관 안정화)**: **옵션 A** — 사내 Kafka 유지
- 이유: Loupe 가 사내 서비스라 같이 옮기지 않으면 Kafka 만 옮겨도 의미 없음
- 작업: bootstrap-servers 의 network 도달성만 확보 (Cloud Interconnect / 사내 IP allow)
- 코드 변경 0

**Phase 2 (장기, Loupe / 사내 데이터플랫폼 GCP 이관 시 동시)**:
- 옵션 D (Pub/Sub) 으로 통합 — 이미 userlake 의 broker 가 Pub/Sub 이라 일관성
- `SparkKafka` 폐기 + Pub/Sub publisher 로 교체
- 사내 Loupe consumer 도 같이 이관 필요

---

## 3. 영향 받는 코드 (옵션 A 기준)

→ **거의 없음**.

| 항목 | 변경 필요? |
|---|---|
| `LoupeSyncSender` | ❌ |
| `SparkKafka` | ❌ |
| `UserlakeLoupe` model (DB) | ❌ — `bootstrapServers` 값을 사내 Kafka host 그대로 유지 |
| application.yml | ❌ (의외로) |
| **네트워크 / IAM** | ✅ Cloud Interconnect or VPN, 사내 Kafka 가 GCP egress IP 허용 |

### 옵션 D (Pub/Sub) 이라면 큰 변경

| 항목 | 변경 |
|---|---|
| `SparkKafka` | 폐기 |
| `LoupeSyncSender.doSend()` | Spark Kafka write → Pub/Sub Publisher 로 교체 |
| `SyncSendFactory` | Pub/Sub sender 변형 추가 |
| `UserlakeLoupe.bootstrapServers` | Pub/Sub topic name 으로 의미 변경 |
| **사내 Loupe 측** | Kafka consumer → Pub/Sub subscriber 로 재작성 |

---

## 4. 작업량 견적

### 4-1. 옵션 A (사내 Kafka 유지)

| 작업 | 소요 |
|---|---|
| Cloud Interconnect 설정 (사내 망 연결) — 인프라팀 의존 | 1~2주 (사내 협의 시간) |
| 사내 Kafka 가 GCP egress IP 허용 | 1일 |
| 네트워크 정합성 검증 (latency / throughput) | 1~2일 |
| **userlake-worker 코드 변경** | **0** |
| **합계** | **1~2주** (대부분 인프라팀 의존) |

### 4-2. 옵션 D (Pub/Sub 전환)

| 작업 | 소요 |
|---|---|
| `SparkKafka` 폐기 + Pub/Sub Publisher 통합 | 2~3일 |
| `LoupeSyncSender.doSend()` 재작성 (struct → JSON → publish) | 2~3일 |
| `SyncSendFactory` 의 sender 변형 추가 | 1일 |
| **사내 Loupe 측 consumer 재작성 (Loupe 팀 협의)** | 별도 (Loupe 팀 작업) |
| 테스트 (end-to-end loupe 데이터 flow) | 2~3일 |
| **합계** | **1~2주** (단 Loupe 팀 작업 별도) |

---

## 5. 의사결정 분기

| # | 분기 | 옵션 | 영향 |
|---|---|---|---|
| 1 | **Kafka destination** | A 사내 Kafka 유지 / B Managed Kafka / C Confluent / D Pub/Sub | userlake-worker 코드 + Loupe 팀 협의 |
| 2 | **Loupe 의 GCP 이관 일정** | 동시 / 후속 | 옵션 D 가능 여부 결정 |
| 3 | **사내 망 ↔ GCP 연결** | Cloud Interconnect / VPN / Public + IP allow | latency / 비용 |
| 4 | **다른 sender (ConvertSync, DefaultSync) 의 destination** | 확인 필요 | sender 별 옵션 |

---

## 6. PoC 검증 포인트

1. **GCP → 사내 Kafka 접속** — Cloud Interconnect 통한 bootstrap-servers 도달성
2. **Latency / throughput** — Sync stage 의 page-by-page 송신이 acceptable 범위인지
3. **인증** — 사내 Kafka 가 GCP egress IP 만 허용해도 충분한지, SASL/TLS 필요한지
4. **(옵션 D 만)** Pub/Sub 에서 Spark write 가능 여부 — `spark-pubsub` connector 라이브러리 검증

---

## 7. 미해결 질문

- [ ] **사내 Kafka 의 GCP 망 허용 정책** — egress IP allow 가능한지, 사내 망 안에서만 접근 가능한지
- [ ] **Loupe 의 GCP 이관 로드맵** — Loupe 팀 작업과 동기화 필요
- [ ] **`ConvertSyncSender` / `DefaultSyncSender` 의 실제 destination** — 코드에서 봤을 때 wrapper 라 실제 wrap 되는 sender 종류 확인 필요
- [ ] **`UserlakeLoupe` 외 다른 sync 대상 모델** — `cohortSync.pipeline.sends` 의 send 타입 enum 전체 목록

---

## 8. 참고

- 상위 문서: [[1_userlake-worker 인프라 이관]] § 2-6
- 관련:
  - [[5_Pub-Sub 이관 (consumer 패키지 재작성)]] (Pub/Sub 인프라 결정과 연계)
  - [[2_Spark Connect → Dataproc Serverless 검토]] (SparkKafka 폐기 vs 유지)
- 파일 위치:
  - `userlake-worker/src/main/kotlin/com/kakaopage/athlon/stage/sync/send/LoupeSyncSender.kt`
  - `userlake-worker/src/main/kotlin/com/kakaopage/athlon/stage/sync/send/DefaultSyncSender.kt`
  - `userlake-worker/src/main/kotlin/com/kakaopage/athlon/stage/sync/send/ConvertSyncSender.kt`
  - `userlake-worker/src/main/kotlin/com/kakaopage/athlon/stage/sync/send/SyncSendFactory.kt`
  - `userlake-worker/src/main/kotlin/com/kakaopage/athlon/stage/sparkconnect/SparkKafka.kt`