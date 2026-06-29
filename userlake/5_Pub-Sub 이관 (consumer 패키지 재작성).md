---
title: "Pub/Sub 이관 — consumer/producer 패키지 재작성"
status: draft
created: 2026-06-28
대상: userlake-worker 의 consumer/, producer/, config/RabbitMQ*StageConfig, userlake-api 의 CohortRunStageResultListener
용도: RabbitMQ → Pub/Sub 의미론 매핑 / 재작성 범위 / ack 흐름 차이
부모: [[1_userlake-worker 인프라 이관]]
---

# Pub/Sub 이관 — consumer/producer 패키지 재작성

> 상위 문서: [[1_userlake-worker 인프라 이관]] § 2-1

## 0. 결론

> RabbitMQ → Pub/Sub 전환은 **userlake-worker 의 consumer/producer 패키지 전면 재작성 + userlake-api 의 result listener 재작성**.
> 작업량 **2~4주**. 코드 변경은 크지만 의미론 매핑이 본질적 리스크.
> 가장 미묘한 4가지:
> 1. **basicNack(requeue=true) ≠ Pub/Sub nack** — 즉시 재배달 보장 안 됨
> 2. **Quorum queue 의 implicit ordering** → Pub/Sub 의 partition-based ordering 으로 매핑
> 3. **`@Retryable` (factory) + 메시지 nack (consumer)** 의 dual retry 모델을 단일 모델로 통합 필요
> 4. **delivery count** 가 RabbitMQ 의 `x-delivery-count` 헤더 → Pub/Sub 의 `delivery_attempt` 로 매핑 (DLT 활성 시에만 노출)

---

## 1. 영향 범위

### 1-1. userlake-worker (재작성 주력)

| 패키지 | 파일 | 역할 |
|---|---|---|
| `consumer/` | `StageConsumer` | 8개 `@RabbitListener` → Pub/Sub subscriber |
| `consumer/` | `StageSubmitter` | ack/nack/requeue 라이프사이클 + 타임아웃 + DB 검증 |
| `consumer/` | `StageMessage` | RabbitMQ `MessageProperties` → Pub/Sub `PubsubMessage` attributes |
| `consumer/` | `StageProcessFactory` | `@Retryable` 흐름 (spring-amqp 와 무관, 그대로 유지 가능) |
| `producer/` | `StageResultProducer` | RabbitTemplate.send → Pub/Sub Publisher.publish |
| `config/` | `RabbitMQ{Target,Gate,Sync,Cohort,Extract,ExtractRun,Csv,HookSlack}StageConfig` 8개 | 각 큐 Spring AMQP 설정 → Pub/Sub subscription 설정 |
| `config/` | `UserlakeRabbitListenerContainerFactory` | container factory 폐기 |
| `config/` | `UserlakeMessageListenerContainer` | 폐기 |

### 1-2. userlake-api (cross-module, 같이 재작성)

| 파일 | 역할 |
|---|---|
| `CohortRunStageResultListener` | result exchange consumer → Pub/Sub result subscriber 로 교체 |
| `application.yml` 의 `userlake.request-exchange`, `userlake.request-queues.*`, `userlake.result-queue.*` | Pub/Sub topic/subscription 으로 교체 |
| **stage request publisher** (분석에서 못 찾았지만 어딘가에 존재) | userlake-api → worker 로 메시지 보내는 producer 도 재작성 필요 |

### 1-3. 의존성 변경

- `org.springframework.boot:spring-boot-starter-amqp` 제거
- `com.google.cloud:google-cloud-pubsub` 또는 `org.springframework.cloud:spring-cloud-gcp-starter-pubsub` 추가

---

## 2. RabbitMQ → Pub/Sub 의미론 매핑

### 2-1. 큐/익스체인지 → topic/subscription

| RabbitMQ | Pub/Sub |
|---|---|
| Queue (큐 자체에 메시지 저장) | Topic (메시지 저장) + Subscription (구독자 view) |
| Exchange + routing-key + Queue binding | Topic + filter (attribute 기반) |
| **8개 stage 큐** (TARGET, GATE, SYNC, ...) | **8개 topic + 8개 subscription** (현재 구조 그대로) 또는 **1 topic + attribute filter** (재설계) |
| Result exchange + binding | Result topic + subscription |

**결정 분기 (§ 5-1 참조)**: 8개 topic 유지 vs 1개 통합 topic + attribute 분기.

### 2-2. Ack / Nack / Requeue

| 동작 | RabbitMQ | Pub/Sub |
|---|---|---|
| 성공 | `channel.basicAck(deliveryTag, false)` | `ackReply.ack()` |
| 실패 (DLQ 행) | `channel.basicNack(deliveryTag, false, false)` | `ackReply.nack()` + DLT 설정에 따라 N회 후 DLT 로 이동 |
| 실패 후 재시도 | `channel.basicNack(deliveryTag, false, requeue=true)` | **즉시 재배달 보장 없음**. `ackReply.nack()` 후 broker 가 적절히 재배달. ack deadline 만료 / `modifyAckDeadline(0)` 으로 강제 재배달 |
| 처리 중 lease 연장 | 불필요 (manual ack 모드는 lease 개념 없음) | **`modifyAckDeadline()` 으로 처리 시간 연장 필요** (긴 stage 의 경우) |

⚠ **핵심 차이**:
- RabbitMQ 의 `basicNack(requeue=true)` 는 **즉시 같은 큐 머리로 재배달**. Pub/Sub 의 nack 은 **언제 재배달할지 broker 결정** (exponential backoff)
- RabbitMQ 의 `basicReject(deliveryTag, true)` (InterruptedException 경로) 도 즉시 재배달이지만, Pub/Sub 은 동일하게 즉시 보장 없음

### 2-3. Prefetch / Concurrency → Flow Control

| RabbitMQ | Pub/Sub |
|---|---|
| `prefetch_count: 5` | `setMaxOutstandingMessageCount(5)` |
| `min_concurrency_consumers: 2` | (자동) subscriber 클라이언트 thread pool |
| `max_concurrency_consumers: 5` | `setExecutorThreadCount(5)` 또는 `setParallelPullCount(5)` |

→ 동등한 의미. 단 Pub/Sub 은 thread pool / pull count / outstanding count 3개로 표현되어 매핑 시 명확화 필요.

### 2-4. Quorum Queue → Pub/Sub guarantees

| Feature | Quorum Queue | Pub/Sub | userlake-worker 영향 |
|---|---|---|---|
| 메시지 영속성 | disk durable, 3+ replicas | managed (transparent replication) | 동등 |
| Ordering | FIFO per queue | best-effort, **ordering key** 명시해야 FIFO 보장 | ⚠ `cohortRunStageUuid()` lookup + `stageTerminated()` 검사가 cohort 내 stage 순서 가정. **ordering key 활성화 필요** (cohort run id 기준) |
| Delivery | at-least-once | at-least-once | 동등 |
| Dedup | 없음 (consumer 책임) | 메시지 ID 기반 + `messageOrderingKey` | 동등 — `CohortRunStageResultListener` 가 stage UUID 로 idempotent upsert |
| Leader election | 자동 | N/A (서버리스) | 동등 |

→ **Ordering key 활성화 (cohort run id 기준)** 가 quorum queue 의 implicit ordering 을 매핑하는 핵심.

### 2-5. Delivery Count (재시도 카운터)

| RabbitMQ | Pub/Sub |
|---|---|
| 헤더 `x-delivery-count` 자동 증가 | **DLT (dead letter topic) 설정 시에만** `delivery_attempt` 헤더 노출 |

→ `StageMessage` 의 `attemptCount = deliveryCount + 1` 계산 그대로 유지 가능. 단 **DLT 설정 필수** (안 하면 attempt count 안 옴).

---

## 3. Ack 흐름 / Exception 분류 매핑

현재 코드의 exception → ack 흐름:

| Exception | 현재 ack 동작 | Pub/Sub 매핑 |
|---|---|---|
| `JsonParseException` (파싱 실패) | `basicNack(false, false)` → DLQ | `nack()` + DLT (max delivery=1) |
| `Exception` (process 생성 실패) | `basicNack(false, false)` → DLQ | `nack()` + DLT |
| `StageProcessNoRetryException` | `basicNack(false, false)` → DLQ | `nack()` + DLT |
| `StageProcessInternalRetryException` | Spring `@Retryable(maxAttempts=5)` 내부에서 처리 (factory 단계) | 그대로 — Pub/Sub 과 무관, `StageProcessFactory.create()` 의 `@Retryable` 유지 |
| `StageProcessException` (일반) | `basicNack(false, requeue=true)` if attempts < max | `nack()` — broker 가 재배달, `delivery_attempt` 로 max 초과 시 DLT |
| `InterruptedException` | `basicReject(deliveryTag, true)` | `nack()` (즉시 재배달 보장 없음) |
| `OutOfMemoryError` | `basicNack(false, false)` → DLQ | `nack()` + DLT |
| Timeout (자체 처리) | `basicNack(false, false)` → DLQ + cancel | `nack()` + DLT |

⚠ **dual retry 모델 문제**:
- 현재: `@Retryable` (factory 5회) **+** message-level nack 재배달 (worker 의 maxAttempts) → **두 단계 retry**
- Pub/Sub: subscription 의 retry policy 가 메시지 단위로 통합 관리
- → **`@Retryable` 5회 + Pub/Sub 재배달 N회 = 총 5 × N 시도**가 의도된 건지 검토 필요. 보통 한 쪽으로 단일화해야 디버깅 가능

---

## 4. Cross-module 영향 (userlake-api)

### 4-1. CohortRunStageResultListener

현재:
```kotlin
@RabbitListener(queues = ["\${userlake.result-queue.name}"])
fun onMessage(...) {
  // updateCohortRunStage(stageResult, stageUuid)
  // basicAck on success, basicNack(false, false) on error
}
```

Pub/Sub 후:
```kotlin
@Bean
fun resultSubscriber(): Subscriber = Subscriber.newBuilder(
  subscriptionName,
  MessageReceiver { message, consumer ->
    // parse stageResult, updateCohortRunStage, consumer.ack() / consumer.nack()
  }
).build()
```

### 4-2. application.yml 변경 (userlake-api)

| 현재 (RabbitMQ) | Pub/Sub |
|---|---|
| `userlake.result-queue.name: local.userlake.stage.result.result.v1` | `userlake.pubsub.result-subscription: ...` |
| `userlake.result-queue.type: quorum` | 제거 |
| `userlake.request-exchange: local.userlake.stage.request` | 제거 |
| `userlake.request-queues.TARGET.routing-key: target.v1` (× 8) | `userlake.pubsub.request-topics.TARGET: ...` (× 8) 또는 1개 통합 |

### 4-3. Stage request publisher (userlake-api 어딘가)

분석에서 못 찾았지만, userlake-api 가 RabbitMQ exchange 로 stage request 를 publish 하는 코드가 있을 것. 그쪽도 같이 재작성 필요 → 사용자가 확인해줘야 함.

---

## 5. 새로운 의사결정 분기

### 5-1. Topic 구조: 8개 vs 1개

| 옵션 | 설명 | 장단점 |
|---|---|---|
| **A. 8개 topic + 8개 subscription** | 현재 RabbitMQ 와 1:1 매핑 | 코드 변경 최소, 큐별 동시성 튜닝 유지. 토픽 수 많음 |
| B. 1개 topic + attribute filter | type 을 메시지 attribute 로, subscription 에서 filter | 인프라 단순. 단 type 별 동시성 / DLT 분리 불가능 |

**추천: A** — 현재 코드의 타입별 동시성·prefetch 설정이 의미 있는 튜닝이므로 매핑 유지.

### 5-2. Subscription 모델: Pull vs Push

| 옵션 | 설명 | 적합도 |
|---|---|---|
| **Pull (StreamingPull / async pull)** | client 가 polling. flow control 가능 | ✅ userlake-worker 는 GKE pod 라서 endpoint 없음. Pull 이 자연스러움 |
| Push (HTTPS endpoint 로 broker 가 POST) | 서버리스 또는 endpoint 있는 환경 | userlake-worker 에 endpoint 만들면 가능. 추가 부담 |

**추천: Pull** (StreamingPull).

### 5-3. DLT 설계

- 8개 topic 모두 DLT 필요 (각 topic 별 dead-letter-topic + max delivery)
- `delivery_attempt` 헤더 노출 위해서도 DLT 필수
- max delivery: 현재 `max_attempt_count: 5` 와 정합

### 5-4. Ordering Key 활성화

- `cohortRunStageUuid()` lookup + `stageTerminated()` 검사가 cohort 내 stage 순서 가정
- **Ordering key 활성화 (cohort run id 기준)** 권장
- 단 ordering key 활성화 시 처리량 제약 (단일 partition) — 트래픽 보고 검증

### 5-5. Dual Retry 모델 통합

- Option 1: `@Retryable` (factory) 만 유지, Pub/Sub 은 max delivery = 1 (재배달 안 함)
- Option 2: `@Retryable` 제거, Pub/Sub retry policy 만 사용
- **추천**: `@Retryable` 은 transient infra 에러 (Vault, IOException) 용이라 유지 의미 있음. 하지만 max attempts 조합을 명시적으로 정리 필요

---

## 6. 작업량 견적

| 작업 | 소요 |
|---|---|
| Pub/Sub topic / subscription / DLT 인프라 구성 (8 + 1 result + DLT) | 1~2일 |
| `consumer/StageConsumer` 의 8개 listener → Pub/Sub subscriber 재작성 | 3~5일 |
| `consumer/StageSubmitter` 의 ack/nack/requeue → Pub/Sub ack/nack/lease 재설계 | 4~5일 |
| `consumer/StageMessage` 의 헤더/attempt count 매핑 | 1~2일 |
| `producer/StageResultProducer` → Pub/Sub Publisher 교체 | 1~2일 |
| `config/RabbitMQ*StageConfig` 8개 → Pub/Sub subscription 설정으로 교체 | 2~3일 |
| Container factory / Listener container 폐기 + 의존성 제거 | 1일 |
| **userlake-api** 의 `CohortRunStageResultListener` 재작성 | 2~3일 |
| userlake-api 의 stage request publisher 재작성 | 2~3일 |
| Workload Identity / IAM (Pub/Sub publisher / subscriber 권한) | 0.5일 |
| 메트릭 / 모니터링 (subscription backlog → HPA) | 1~2일 |
| 테스트 (단위 + 통합 + ack 흐름 시뮬레이션) | 4~6일 |
| **합계** | **22~36일 (3~5주)** |

> 의사결정 분기 (§ 5) 진행 정도에 따라 변동. Topic 8개 + ordering key + DLT 명시 결정 시 견적 안정화.

---

## 7. PoC 검증 포인트

1. **Ack 흐름 정합성** — Pub/Sub 의 nack 후 재배달 타이밍이 stage timeout 과 충돌 안 하는지 (실제 stage 1개 돌려서 측정)
2. **Ordering key** — 같은 cohort run 의 stage 들이 순서대로 처리되는지
3. **Max outstanding messages** — prefetch 5 와 동등한 backpressure 동작
4. **DLT 동작** — `delivery_attempt` 헤더가 정확히 노출되는지 + max delivery 초과 시 DLT 도착
5. **Result topic fanout** — userlake-api 의 result subscriber 가 정상 수신
6. **사내 → GCP 라우팅** — Cloud Interconnect 없이 IAM 만으로 Pub/Sub 접근 가능한지

---

## 8. 미해결 질문

- [ ] userlake-api 의 **stage request publisher** 위치 (분석에서 못 찾았음, 사용자 확인 필요)
- [ ] **`max_attempt_count`** 가 어디 정의돼있는지 — DB 의 stage 별 설정인지, application.yml 인지
- [ ] **Cohort run 내 stage 들이 정말 순서 의존인지** — ordering key 활성화 필요 여부
- [ ] **Dual retry 의도성** — `@Retryable` 5회 × Pub/Sub 재배달 N회 가 의도인지, 아니면 한 쪽만 의도였는지
- [ ] **사내 RabbitMQ 의 DLX 설정** — 현재 DLQ 가 어디 있는지 + 거기에 누가 붙어있는지 (Pub/Sub DLT 와 매핑 위해)
- [ ] **메시지 처리 평균 시간** — Pub/Sub `ack deadline` (기본 10초, 최대 10분) 충분한지

---

## 9. 참고

- 상위 문서: [[1_userlake-worker 인프라 이관]] § 2-1
- 코드 위치:
  - `userlake-worker/src/main/kotlin/com/kakaopage/athlon/consumer/`
  - `userlake-worker/src/main/kotlin/com/kakaopage/athlon/producer/`
  - `userlake-worker/src/main/kotlin/com/kakaopage/athlon/config/RabbitMQ*StageConfig.kt`
  - `api/src/main/kotlin/com/kakaopage/athlon/userlake/message/CohortRunStageResultListener.kt`
- application.yml 키: `spring.rabbitmq.*`, `userlake.stage.*`, `userlake.result-queue.*`, `userlake.request-exchange`, `userlake.request-queues.*`