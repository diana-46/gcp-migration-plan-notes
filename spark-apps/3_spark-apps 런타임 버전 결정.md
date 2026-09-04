---
title: "spark-apps 런타임 버전 결정 (Spark 3.5.8 / Java 17 / Scala 2.12)"
tags:
  - spark
  - spark-apps
  - 런타임
  - 이관
status: verified
created: 2026-08-13
updated: 2026-08-13
---

# spark-apps 런타임 버전 결정

> **질문**: `spark-apps` (Spark 3.1.3 / Scala 2.12 / IDC Hadoop) 를 GKE Spark Operator 로 이관하려면 런타임을 어느 버전으로 맞춰야 하는가?
> **답**: **Spark 3.5.8 / Java 17 / Scala 2.12**, 이미지는 공식 `apache/spark:3.5.8-scala2.12-java17-ubuntu` 기반으로 직접 빌드. 3.1.3 → 3.5.8 로 올리면서 실제로 깨진 곳은 **파일 2개**뿐이었다.

앱별 구현 이슈는 [[4_LoupeKafkaBatchExporter 이관]] 참고.

## 결정

| 항목 | 값 | 근거 |
|---|---|---|
| Spark | **3.5.8** | `dp-spark-ingestion` 과 동일 대역. 기술적 강제는 아니고(아래) 팀과 트러블슈팅 경험을 공유하려는 선택 |
| Java | **17** | 팀·로컬 모두 17. CI 만 8 이어서 17 로 올림 |
| Scala | **2.12** | 유지. 2.13 의 관문은 `mongo-spark-connector 2.4.3` |
| 베이스 이미지 | `apache/spark:3.5.8-scala2.12-java17-ubuntu` | 공식 3.5.8 태그는 **scala2.12 변종만** 배포한다 |
| GCS 커넥터 | `gcs-connector 3.1.17-shaded` | 이미지에 추가. 공식 이미지에는 없어서 `gs://` 를 못 읽는다 |
| BQ 커넥터 | `spark-bigquery-with-dependencies_2.12:0.44.2` | fat jar 에 포함 |

## 🔑 오퍼레이터는 버전을 강제하지 않는다

이관 논의에서 가장 먼저 걸렸던 오해다. 같은 클러스터에서 **세 조합이 동시에 정상 동작**한다.

| 무엇 | Spark | Scala | 확인 방법 |
|---|---|---|---|
| spark-operator controller | **4.0.1** | 2.13 | `kubectl -n spark-operator exec deploy/spark-operator-controller -- cat /opt/spark/RELEASE` |
| dp-spark-ingestion 잡 | **3.5.8** | 2.13 | 드라이버 로그 `Running Spark version` |
| 검증 잡 (우리) | **3.5.1 → 3.5.8** | **2.12** | 같음. COMPLETED |

오퍼레이터는 자기 내장 Spark(4.0.1)로 `spark-submit` 을 수행해 드라이버 pod 스펙을 만들 뿐이고, **실제 런타임은 이미지가 정한다.** 드라이버 pod 라벨 `spark-version=4.0.1` 은 제출 측 버전이라 앱 런타임과 무관하다 — 이 라벨을 믿으면 안 된다.

> **유일한 하드 제약**: 앱 jar 의 Scala 바이너리 버전 = 그 잡이 쓰는 이미지의 Scala 버전. 이미지를 우리가 만들므로 자동 충족된다.

## Scala 2.12 를 유지하는 이유

2.13 전환 비용의 대부분이 **이관하지도 않을 앱** 때문에 발생한다.

| 항목 | 상태 |
|---|---|
| 코드 패턴 (JavaConverters 3, filterKeys 1, TraversableOnce 1, Stream 4) | 기계적 수정, 소규모 |
| **`mongo-spark-connector 2.4.3`** | 2.13 빌드는 **10.x 계열만** 존재. `ReadConfig`/`WriteConfig`/implicit 확장이 전부 삭제돼 3~5개 파일 재작성 필요 |
| 그 Mongo 앱들 | Datastream(CDC) 대체 대상 → 지금 재작성하면 버릴 코드에 비용 |
| 공식 이미지 | 3.5.8 은 scala2.13 태그가 없다 → 2.13 은 베이스 배포본부터 직접 조달해야 한다 |

즉 **2.13 이 오히려 이미지 조달도 번거롭다.** Mongo 앱이 정리되는 시점(또는 Spark 4 전환 시점)에 옮기면 되고, 이미지가 잡별로 독립이라 **앱 단위 순차 전환**이 가능하다.

## 3.1 → 3.5 로 깨진 곳 (2 파일)

| 파일 | 원인 | 대응 |
|---|---|---|
| `SlackUtils`, `Kafka2ParquetSparkTest` | Spark 3.5 가 쓰는 scala-parser-combinators 2.x 에서 `scala.util.parsing.json` 삭제 | Spark 가 이미 의존하는 json4s 로 대체 |
| `SparkMySqlExtensionUtils` | Spark 3.4 부터 `JdbcUtils.savePartition` 이 커넥션 팩토리 인자를 받지 않는다 (내부에서 dialect 로 생성). `createConnectionFactory` 도 이동 | 8인자 시그니처로 호출 변경 |

**깨지지 않은 것**:

- **Hudi** — 3개 앱(`TicketUseRecord`, `PushTargetUserImporter`, `KagePushTargetImporter`)이 `import` 없이 `format("hudi")` **문자열로만** 참조한다. 의존성을 제거해도 컴파일은 통과하고 실행 시에만 실패한다. 이관 대상이 아니므로 의도한 결과
- **Mongo 5개 파일** — Scala 2.12 유지 덕분에 영향 없음

## 의존성 함정 하나

`spark-3.5-bigquery`(Spark 3.5 전용 빌드)는 arrow / zstd-jni 를 전이 의존성으로 끌고 와서, 팀이 DP-3064 에서 명시적으로 pin 한 `kafka-clients 3.5.2` 와 충돌한다. sbt 가 binary incompatible 로 판단해 **update 단계에서 실패**한다.

```
[error] (update) found version conflict(s) ... suspected to be binary incompatible:
[error]   * com.github.luben:zstd-jni:1.5.6-3 (strict) is selected over 1.5.5-1
[error]       +- org.apache.arrow:arrow-compression:17.0.0   (BQ 커넥터)
[error]       +- org.apache.kafka:kafka-clients:3.5.2        (팀 pin)
```

→ 내부 의존성이 전부 shade 된 `spark-bigquery-with-dependencies_2.12` 를 쓴다. 팀의 pin 을 건드리지 않는 쪽을 택했다.

## 이미지 구성

```dockerfile
FROM apache/spark:3.5.8-scala2.12-java17-ubuntu
ARG GCS_CONNECTOR_VERSION=3.1.17
ADD .../gcs-connector-${GCS_CONNECTOR_VERSION}-shaded.jar /opt/spark/jars/gcs-connector-shaded.jar
COPY spark-apps.jar /opt/spark/app/spark-apps.jar
```

- **앱 fat jar 은 `/opt/spark/jars` 가 아니라 `/opt/spark/app`.** `jars/` 에 넣으면 fat jar 안의 모든 의존성이 항상 드라이버·executor classpath 에 올라가 Spark 라이브러리와 충돌할 수 있다. `mainApplicationFile: local:///opt/spark/app/spark-apps.jar` 로 명시 참조한다
- **GCS 커넥터 버전 문자열의 `hadoop3-` 접두사**: Hadoop 클러스터와 무관하다. Spark 가 파일시스템 접근에 쓰는 **Hadoop 클라이언트 라이브러리 세대**를 가리키는 이름이었고, Hadoop 2 지원이 끝난 3.x 계열에서는 사라졌다. 3.1.17 은 Hadoop 3.3.6 기준 빌드이고 이 이미지의 Spark 3.5.8 은 Hadoop 3.3.4 클라이언트를 번들하는데, 실제 `gs://` 읽기로 동작을 확인했다
- arm64 맥에서 빌드하려면 `--platform linux/amd64` 필요 (GKE 노드는 amd64)

이미지 경로는 기존 네이밍(`spark-jobs/<앱>`)을 따랐다. GAR 에는 앱마다 이미지가 따로 있고 **공용 base 이미지는 없다**.

```
asia-northeast3-docker.pkg.dev/dev-dp-project-354904/dev-kc-docker/spark-jobs/spark-apps:2.0.0-gcp
```

## Java 17 — 세 곳을 맞췄다

| 위치 | 이전 | 이후 |
|---|---|---|
| 로컬 | 17 (JAVA_HOME) | 그대로 |
| **CI (`.github/workflows/ci.yaml`)** | **8** | **17** |
| 런타임 이미지 | 17 | 그대로 |

JDK 8 로 컴파일한 바이트코드는 Java 17 런타임에서 돌기 때문에 지금까지 드러나지 않았다. 다만 최근 Google Cloud 라이브러리들이 Java 11+ 를 요구하고, Spark 4(Java 17 필수) 로 갈 때도 필요하다.

> `build.sbt` 에는 여전히 Java 버전 고정이 없다 (`-release:17` 미설정). 빌드하는 사람의 JDK 가 그대로 쓰이므로, 고정해 두는 것을 검토할 만하다.

## 검증 결과

```
sbt compile / Test/compile        성공 (68 sources, 에러 0)
이미지 빌드 → GAR 푸시            성공 (amd64, 1.16GB)
GKE 가 이미지 pull·실행           Running Spark version 3.5.8, SparkPi COMPLETED
이미지 안 fat jar 에서 앱 클래스   LoupeKafkaBatchExporter 로딩·실행 확인
gs:// 에서 jar 로딩               gcs-connector 3.1.17 + Spark 3.5.8 동작
```

## 커밋

```
31bad6e  DP-3156 CI 의 JDK 를 8 에서 17 로 올림
459cfb8  DP-3156 GKE Spark Operator 제출용 이미지 Dockerfile 추가
f72ecb2  DP-3156 Spark 3.5.8 로 상향하고 BigQuery 커넥터 추가
```

## 팀에 공유·확인이 필요한 것

1. **Scala 2.12 유지** — `dp-spark-ingestion` 은 2.13 이라 여기만 다르다. 공용 base 이미지를 만들거나 2.13 단일화 방침이 있으면 알려달라
2. **이미지 경로·태그 규칙** — 다른 앱은 `0.1.0` 스타일인데 `build.sbt` 의 `version`(`2.0.0-gcp`)을 따랐다
3. **GCS 커넥터 버전** — `dp-spark-ingestion` 이미지가 쓰는 버전과 맞출지
4. **이미지 빌드 CI** — 지금은 로컬 수동 빌드·푸시. 사내 표준(GitHub Actions / Cloud Build)이 있는지
5. **Spark 4 전환 로드맵** — 있으면 2.13 전환 시점을 그에 맞춘다

## 참고

- [[4_LoupeKafkaBatchExporter 이관]] — 앱별 구현 (Hive → BigQuery)
- [[2_Composer에서 GKE Spark Operator 제출]] — 제출 경로·오퍼레이터 구조
- [[1_사용중인_spark_job]] — 이관 대상 앱 인벤토리
