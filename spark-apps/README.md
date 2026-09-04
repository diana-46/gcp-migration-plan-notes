# spark-apps — 컨텍스트

## 풀고자 하는 문제 / 의사결정

IDC Hadoop에서 돌던 `spark-apps` (Spark 3.1.3 / Scala 2.12) 배치 잡들을 GCP로 이관한다.
실행 경로는 **Composer DAG → GKE Spark Operator 제출**로 확정, 런타임은 **Spark 3.5.8 / Java 17 / Scala 2.12**로 확정.

## 노트

- [[1_사용중인_spark_job]] — 이관 대상 인벤토리. active DAG 기준 실제 호출되는 `spark-apps/bin` 스크립트 ↔ Spark 앱(Scala 클래스) 매핑
- [[2_Composer에서 GKE Spark Operator 제출]] — 제출 경로 검증 (`status: verified`). SparkApplication YAML + `GKECreateCustomResourceOperator`면 끝, 추가 권한 불필요
- [[3_spark-apps 런타임 버전 결정]] — Spark 3.5.8 / Java 17 / Scala 2.12 결정과 근거 (`status: verified`)
- [[4_LoupeKafkaBatchExporter 이관]] — 첫 이관 앱 (Hive → BigQuery), 진행 중 (`status: draft`)

## 확정 사항

- DB 수집 파이프라인은 **Datastream**으로 대체 예정 → 소스 DB(MySQL/Mongo) 수집 앱은 이관 대상에서 제외
- 이미지는 공식 `apache/spark:3.5.8-scala2.12-java17-ubuntu` 기반 직접 빌드

## 용어

- **Spark Operator** — GKE에서 SparkApplication CR을 받아 Spark 잡을 실행하는 Kubernetes operator
- **Loupe** — 사내 로그/이벤트 수집 시스템 (Kafka 기반)
