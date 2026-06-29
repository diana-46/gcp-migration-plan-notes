---
title: "컴퓨트 / 배포 / 운영 — GKE + Cloud Monitoring"
status: draft
created: 2026-06-28
대상: userlake-worker 의 배포·이미지·메트릭·로그·사내 망 연결
용도: 인프라 이관 (deployment / 모니터링 / 네트워킹) 정리
부모: [[1_userlake-worker 인프라 이관]]
---

# 컴퓨트 / 배포 / 운영 — GKE + Cloud Monitoring

> 상위 문서: [[1_userlake-worker 인프라 이관]] § 2-7

## 0. 결론

> userlake-worker 는 **이미 K8s 기반 운영 중** (dp-gitops 의 kustomize 구조). GKE 이관은 자연스러움.
> 작업량 **1~2주** — 단 사내 종속성 (사내 base image, 사내 메트릭, kinit sidecar) 제거에 시간 필요.
> 핵심 변경 4가지:
> 1. base image 사내 → public + Artifact Registry
> 2. kinit sidecar 제거 ([[7_Kerberos 제거 (인증 흐름 재설계)]])
> 3. 메트릭 / 로그 → Cloud Monitoring / Logging
> 4. **HPA 트리거 재설계** — 현재 RabbitMQ 큐 길이 기반 → Pub/Sub subscription backlog 기반

---

## 1. 현재 배포 구조

### 1-1. 기본 구조 (dp-gitops)

```
dp-gitops/athlon/userlake-worker/
├── base/
│   ├── deploy.yaml          # Deployment (1 replica)
│   ├── configmap.yaml
│   └── kustomization.yaml
├── dev/   patch-deploy.yaml + patch-configmap.yaml
├── integ/  patch-deploy.yaml + patch-configmap.yaml
├── qa/    patch-deploy.yaml + patch-configmap.yaml
└── prod/  patch-deploy.yaml + patch-configmap.yaml
```

→ kustomize 기반 환경별 (dev/integ/qa/prod) overlay.

### 1-2. 리소스 / 컨테이너

```yaml
# base/deploy.yaml 발췌
spec:
  replicas: 1                     # ⚠ HPA 없음
  strategy: RollingUpdate (maxSurge 25%, maxUnavailable 0)
  containers:
    - name: athlon-userlake-worker
      resources:
        requests: {cpu: 3, memory: 6Gi}
        limits:   {cpu: 3, memory: 6Gi}
      env:                        # 사내 Hadoop 통합 변수 다수
        HADOOP_CLUSTER, KRB5CCNAME, PRESTO_HOME, HADOOP_CONF_DIR,
        HADOOP_HOME, HIVE_CONF_DIR, HIVE_HOME, SPARK_HOME,
        SPARK_KRB_REALM, KRB_REALM, JAVA_HOME, PATH
    - name: kinit                 # Kerberos 갱신 사이드카
      image: idock.daumkakao.io/kakaoent-dp/kinit-sidecar:0.0.1
```

### 1-3. prod 패치 핵심

```yaml
# prod/patch-deploy.yaml
spec:
  replicas: 1                     # ⚠ prod 도 1 replica, HPA 없음
  containers:
    - name: athlon-userlake-worker
      envFrom:
        - configMapRef: athlon-userlake-worker-config
      env:
        - HADOOP_CLUSTER: "hadoop-kent"
        - PRESTO_JDBC_URL: jdbc:presto://kakaoent-presto-athlon.kakaoent.io:443/...
```

→ **prod 가 단일 인스턴스로 운영 중** (HPA 없음). 처리량 한계 / SPOF 가능성. GCP 이관 기회에 검토.

---

## 2. GCP 이관 매핑

### 2-1. 컴포넌트별 매핑

| 항목 | 현재 | GCP |
|---|---|---|
| **K8s 클러스터** | 사내 K8s | **GKE** (Autopilot or Standard) |
| **Deployment 매니페스트** | dp-gitops 의 kustomize | 동일 (kustomize 호환) |
| **컨테이너 이미지** | `idock.daumkakao.io/kakaoent-dp/*` | **Artifact Registry** (`<region>-docker.pkg.dev/<project>/...`) |
| **base image** | (확인 필요) 사내 base 추정 | **public** (`eclipse-temurin:17-jre` 등) |
| **kinit sidecar** | `kinit-sidecar:0.0.1` 사내 이미지 | **제거** (Workload Identity) |
| **ConfigMap (hadoop-config)** | 사내 클러스터 좌표 | 제거 또는 GCP 좌표로 교체 |
| **Secret (kerberos-secret)** | keytab | 제거 |
| **메트릭** | micrometer → 사내 (`dp-vminsert.kakaoent.io`?) | **Cloud Monitoring** (OTLP) |
| **로그** | (확인 필요) | **Cloud Logging** (stdout 자동 수집) |
| **HPA** | **없음** | **추가 권장** (Pub/Sub backlog 기반) |
| **사내 망 호출** (Loupe Kafka, Slack, Vault 잔존 등) | 사내 내부 통신 | **Cloud Interconnect / VPN** |

### 2-2. HPA 재설계 (가장 가치 있는 변경)

현재 1 replica 운영의 한계:
- 처리량 = 단일 pod 의 `userlake.pool_size` × stage 별 동시성
- 메시지 burst 시 backlog 증가, SPOF

GCP 후:
```yaml
# HorizontalPodAutoscaler (예시)
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
spec:
  minReplicas: 1
  maxReplicas: 5
  metrics:
    - type: External
      external:
        metric:
          name: pubsub.googleapis.com|subscription|num_undelivered_messages
          selector:
            matchLabels:
              resource.labels.subscription_id: userlake-stage-target-sub
        target:
          type: AverageValue
          averageValue: 50
```

→ Pub/Sub subscription backlog 가 N 개 넘으면 pod 추가. 8개 stage 별 subscription 따라 별도 HPA 또는 단일 통합.

### 2-3. 사내 망 호출 패턴

GCP 후에도 사내 망 호출이 남는 곳:
- Loupe Kafka write ([[9_Sync Kafka 송신 (Loupe destination)]])
- Slack webhook (HookSlackStage)
- Vault (옵션 유지 시 [[8_MySQL Cloud SQL · Vault Secret Manager]])
- 사내 메트릭 (`dp-vminsert.kakaoent.io`) 유지 시
- 사내 HTTP destination (sync 의 default sender)

→ **Cloud Interconnect** 가 필수 인프라.

---

## 3. 모니터링 / 로그 / 알람

### 3-1. 메트릭

현재:
- `spark-defaults.conf` 에 Graphite sink → `dp-vminsert.kakaoent.io:2003` (사내 VictoriaMetrics)
- userlake-worker 자체는 micrometer 추정

GCP 옵션:
| 옵션 | 설명 |
|---|---|
| **A. Cloud Monitoring (managed Prometheus)** | OTLP 또는 Prometheus exporter |
| B. Datadog / NewRelic (외부) | 비용 추가 |
| C. 사내 VictoriaMetrics 유지 + Cloud Interconnect | 일관성 (사내 dashboard 그대로) |

→ **A** 권장. 단 사내 dashboard 자산이 많으면 C 도 고려.

### 3-2. 로그

현재: stdout / logback → 사내 ELK 추정.

GCP:
- GKE 기본 → **Cloud Logging** 자동 수집 (stdout/stderr)
- 사내 로그 시스템 유지 시 fluentd 사이드카

→ **Cloud Logging** 권장 (자동, 별도 설정 거의 없음).

### 3-3. 트레이싱 / APM

현재 명확히 없음. GCP 이관 기회에 추가 가능:
- **Cloud Trace** (OpenTelemetry SDK)
- Spring Boot 의 `micrometer-tracing` 통합

---

## 4. 이미지 / Artifact Registry

### 4-1. 이미지 이전

현재: `idock.daumkakao.io/kakaoent-dp/*`
- userlake-worker 본체 이미지 (PATCH_DOCKER_IMAGE)
- kinit-sidecar (제거 예정)
- spark-connect (별도 문서 [[2_Spark Connect → Dataproc Serverless 검토]])

GCP:
- **Artifact Registry** (`<region>-docker.pkg.dev/<project>/athlon/userlake-worker:<tag>`)
- CI/CD (GitHub Actions / Cloud Build) 가 build & push

### 4-2. Base image

현재 base image 확인 필요. 만약 사내 이미지 (`idock.daumkakao.io/openjdk:11-jre-slim` 등) 라면:
- public 이미지로 교체 (`eclipse-temurin:17-jre`)
- Java 17 권장 (Spark 3.4.2 호환)

---

## 5. 작업량 견적

| 작업 | 소요 |
|---|---|
| GKE 클러스터 프로비저닝 (or 기존 활용) | 인프라팀 의존 |
| KSA + Workload Identity 매핑 | 0.5일 ([[7_Kerberos 제거 (인증 흐름 재설계)]] 와 같이) |
| kustomize manifest 정리 (kinit 제거, ConfigMap/Secret 제거, 사내 env 제거) | 1~2일 |
| base image 교체 + Artifact Registry CI/CD | 1~2일 |
| Cloud Monitoring 통합 (OTLP exporter) | 1~2일 |
| Cloud Logging 통합 (자동, 단 사내 로그 시스템 출력 제거) | 0.5일 |
| **HPA 추가** (Pub/Sub backlog 기반) | 1~2일 |
| Cloud Interconnect 셋업 + 사내 망 호출 정합성 검증 | 1주 (인프라팀 의존) |
| 통합 검증 (end-to-end stage 실행) | 2~3일 |
| **합계** | **1~2주** (인프라팀 의존 제외) |

---

## 6. 의사결정 분기

| # | 분기 | 옵션 | 영향 |
|---|---|---|---|
| 1 | **GKE 모드** | Autopilot / Standard | 관리 부담 vs 유연성 |
| 2 | **메트릭 시스템** | Cloud Monitoring / 사내 VictoriaMetrics 유지 / 외부 (Datadog) | 일관성 vs 자산 활용 |
| 3 | **로그 시스템** | Cloud Logging / 사내 ELK 유지 | 자동 vs 통합 |
| 4 | **HPA 트리거** | Pub/Sub backlog / CPU / custom metric | 정확도 vs 단순성 |
| 5 | **사내 망 연결** | Cloud Interconnect / VPN | 비용 / latency |

---

## 7. PoC 검증 포인트

1. **GKE 에서 userlake-worker pod 가 정상 부팅** — Workload Identity 로 Cloud SQL / GCS / Pub/Sub 접근
2. **HPA 가 Pub/Sub backlog 보고 스케일** — 메시지 burst 시 replica 증가
3. **Cloud Monitoring 에 메트릭 도착** — micrometer → OTLP → Cloud Monitoring
4. **Cloud Logging 에서 stage 별 로그 검색** — 사내 ELK 대비 검색 성능
5. **사내 망 호출 latency** — Loupe Kafka write, Slack webhook 등

---

## 8. 미해결 질문

- [ ] **현재 base image** 정체 (Dockerfile / CI 확인 필요)
- [ ] **현재 prod 가 진짜 1 replica 인지** — patch-deploy.yaml 만 봤음. 실제 replica count 검증
- [ ] **사내 메트릭 (`dp-vminsert.kakaoent.io`) 의 athlon dashboard 양** — 이전 비용 vs 사내 유지
- [ ] **사내 로그 시스템 (ELK?) 위치** — 검색·alerting 의존도
- [ ] **Cloud Interconnect 의 사내 측 상태** — 이미 다른 워크로드용으로 깔려있는지

---

## 9. 참고

- 상위 문서: [[1_userlake-worker 인프라 이관]] § 2-7
- 관련:
  - [[7_Kerberos 제거 (인증 흐름 재설계)]] (kinit 제거 + Workload Identity)
  - [[5_Pub-Sub 이관 (consumer 패키지 재작성)]] (HPA 트리거 메트릭)
- 파일 위치:
  - `dp-gitops/athlon/userlake-worker/base/deploy.yaml`
  - `dp-gitops/athlon/userlake-worker/{dev,integ,qa,prod}/patch-deploy.yaml`