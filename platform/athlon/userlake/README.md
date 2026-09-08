# userlake — 컨텍스트

## 풀고자 하는 문제 / 의사결정

athlon의 **`userlake-worker`(코호트 stage 실행 워커)를 온프렘 → GCP로 이관**하기 위한 인벤토리·기술 선택·비용 산정.
두 갈래: (a) 인프라 의존(Presto/RabbitMQ/HDFS/Kerberos/MySQL·Vault/Kafka/배포)별 GCP 대체와 작업량, (b) **Spark Connect를 어느 컴퓨트에 어떤 크기로 올릴 것인가** — 후자가 실질 중심이고 실측 기반으로 "GKE 직접 + 다운사이즈"까지 결론이 나 있다.

## 확정 사항

- Presto → **BigQuery**, RabbitMQ → **Pub/Sub**, HDFS → **GCS**, MySQL → **Cloud SQL**, Kerberos 폐기 → **Workload Identity**
- **Spark Connect는 제거하지 않고 lift** (제거는 sync/gate 재설계 수준이라 이관 범위 외)
- **Dataproc Serverless 부적합** (Interactive 강제 Premium, 3~4배 비쌈)
- 다운사이즈 확정: executor 3개 × 6 cores × 35G (72 vCPU/126GB → 26 vCPU)
- HPA는 Pub/Sub subscription backlog 기반으로 재설계

**팀 결정 대기**: 배포 모드 최종 선택 (권장: GKE 직접, Master n4-standard-16 + Worker n4-highmem-8 × 3, $917/월 Res CUD 3Y) · Vault→Secret Manager · Sync Kafka 송신 대상

## 노트

- [[1_userlake-worker 인프라 이관]] — **허브 문서**. 대체 확정 + 미해결 분기 3개 + PoC 우선순위 (GCS 1순위)
- [[2_Spark Connect → Dataproc Serverless 검토]] — Spark Connect가 대체 불가한 이유 6가지, BQ 이관과 Spark 이관은 독립
- [[3_Spark Connect on Dataproc Serverless 비용 계산]] — 배포 모드 3축 비교 → GKE 직접 + 다운사이즈 결론
- [[4_BigQuery 이관 (Presto 쿼리 엔진 전환)]] — 이관 전체 최대 코드 작업 (3~6주, athlon 전사 blast radius)
- [[5_Pub-Sub 이관 (consumer 패키지 재작성)]] — consumer/producer + 8개 StageConfig 재작성 (2~4주)
- [[6_HDFS → GCS (FileSystemType 확장)]] — 코드는 작지만 영향 범위 최대 → **PoC 1순위**
- [[7_Kerberos 제거 (인증 흐름 재설계)]] — 5개 컴포넌트 인증 흐름을 Workload Identity로
- [[8_MySQL Cloud SQL · Vault Secret Manager]] — Cloud SQL 확정, 실질 결정은 Vault 대체
- [[9_Sync Kafka 송신 (Loupe destination)]] — 1단계 사내 Kafka 유지 + Interconnect (코드 변경 0)
- [[10_컴퓨트 배포 운영 (GKE Monitoring)]] — dp-gitops 기반 GKE 이관 + HPA 재설계
- [[11_사용량 분석 (한달 데이터 기반)]] — 정기 잡 주류 → 24h 상주 + 다운사이즈가 답 (이용률 7.6%)
- [[12_Managed Service for Apache Spark 과금 체계 (공식)]] — DCU 단가 공식 정리 (reference)
- [[13_Spark Connect 다운사이즈 결정 (실측 기반)]] — 409일 실측 기반 스펙 확정 (decision)
- [[14_Spark Connect 다운사이즈 비용 & 노드 구성 (Seoul 실측)]] — 노드 구성·비용 확정, 모드 선택만 대기 (decision)

## 용어 / 약어

- **userlake-worker** — 코호트 stage(TARGET/GATE/SYNC/SLACK/CSV/COHORT/EXTRACT/EXTRACT_RUN) 실행 워커
- **Loupe** — 사내 추천/세그먼트 서비스. Sync의 주 destination (Kafka topic)
- **DCU / CUD** — GCP Data Compute Unit / Committed Use Discount (Res CUD 3Y)
- **distributed-query-engine** — Presto 접근 공통 모듈 (BQ 전환 시 blast radius의 중심)

## 외부 자료

- [Managed Spark 가격](https://cloud.google.com/products/managed-service-for-apache-spark/pricing)
- 레포: `userlake-worker`, `distributed-query-engine`, `dp-gitops/athlon/{userlake-worker,spark-connect}`, `spark-k8s-build`
- 근거 티켓: DP-2689 (OOM 조사)
