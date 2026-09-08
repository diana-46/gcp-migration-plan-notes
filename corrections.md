# corrections — 정정 이력 등록부

> **본문은 항상 현재형(최신 입장)만 서술한다.** "무엇이 왜 뒤집혔나"는 여기에만 기록하고,
> 본문에는 `(정정 R#)` 각주로만 연결한다. 미결 상태 표시(⚠️ 재검토 필요 등)는 이력이 아니라
> 현재 상태이므로 본문에 남긴다. — 팀 위키 `corrections.md`(R#)와 대응되는 L1 등록부.

## R1 — CDC 수집 도구: Datastream → Debezium + Kafka + BQ Sink Connector (2026-09-08)

- **틀림**: "DB 수집 파이프라인은 Datastream으로 대체 예정" — spark-apps 노트 전반의 전제 (2026-06 작성 당시 계획)
- **맞음**: CDC = **Debezium → Kafka → BigQuery Sink Connector** (Datastream 기각 — 수집 트랙 결정, 팀 위키 TDR-002)
- **파생 정정**: "Datastream은 BQ/GCS로만 랜딩 → 서비스→정산 직접 CDC 불가" 논리로 배제했던 옵션들은
  Kafka 경유 확정으로 **Kafka consumer 직결 경로가 새로 생겨 재검토 대상** (spark-apps/1 §5, 10 §5)
- **영향**: `platform/spark-apps/` 전반 (1·3·5·6·7·8·9·10·11·12·README)

## R2 — userlake-worker HPA: "RabbitMQ 큐 기반 HPA 재설계" 는 오기 (2026-09-08)

- **틀림**: 결론의 "현재 RabbitMQ 큐 길이 기반 HPA → Pub/Sub 기반으로 재설계"
- **맞음**: **현재 HPA 없음** — dev/prod 모두 `replicas: 1` 고정 (본문 §1 배포 yaml 실측). Pub/Sub backlog 기반 오토스케일은 "재설계"가 아니라 **신규 도입 검토**
- **영향**: `platform/athlon/userlake/10` §0, `userlake/1` §마이그레이션 항목

## R3 — HDFS→GCS 작업량: 1~2주 → 3~4주 (2026-09-08)

- **틀림**: 결론의 "작업량 1~2주"
- **맞음**: **3~4주 (16~22일)** — 초기 추정은 api 의 raw Hadoop FS 마이그레이션 + atomic rename 회귀 검증을 뺀 값. §6 상세 산정이 대표
- **영향**: `platform/athlon/userlake/6` §0 (상위 문서 userlake/1 은 처음부터 3~4주로 정합)

## R4 — Spark 런타임: "Dataproc Serverless 백필 병행" → 백필 포함 GKE 일원화 (2026-09-08)

- **이전**: 팀 결정(2026-07-29, 팀 위키 TDR-006) = 기본 GKE Spark Operator + Dataproc Serverless 는 백필·대규모 재처리 전용 병행
- **현재**: **백필·재처리 포함 GKE Spark Operator 일원화, Dataproc Serverless 미사용** — 모니터링 지점을 하나로 유지 (실행 환경 이원화 회피)
- **후속**: TDR-006 백필 조항과 상충 — 팀 위키 `_triage` 이의 등록 대상
- **영향**: `platform/spark-apps/1` 전제, `docs/shared/13` 로드맵, `CLAUDE.md` 확정 스택

## R5 — Composer 실행 방식: K8sExecutor 혼합안 → PoC 기각 (2026-09-08 반영, 실측은 2026-06)

- **틀림**: 초기 설계 "무거운 작업은 `executor=\"KubernetesExecutor\"` 로 Pod 분리 + 프리셋(SMALL~GPU)" (1_개요 §2.4)
- **맞음**: **기본 = Celery + Triggerer. 아주 무거운/특수 작업만 K8s Operator 로 별도 GKE 제출** —
  Composer 3 K8sExecutor 는 cold start **7분 46초 실측** + idle 즉시 deprovision(warm 불가)으로 기각 ([[platform/airflow/PoC/04_worker_pool_queue]])
- **영향**: `platform/airflow/1_개요` §2.4 (0_결론 §2·4_Queue 는 처음부터 정합)

## R6 — userlake Spark Connect 다운사이즈: 확정(decision) → 재검토(draft) 격하 (2026-09-08)

- **이전**: userlake/13·14 = `status: decision` (Dataproc Serverless/Managed Spark 전제 실측 결정)
- **현재**: 컴퓨트 선택(Dataproc 계열 사용 여부)이 **미정으로 회귀** — 결정이 아니라 후보 실측으로 격하.
  실측 데이터 자체(사용량·다운사이즈 산정)는 유효, **전제(과금 체계)만 미확정**
- **영향**: `platform/athlon/userlake/13·14` status, `userlake/3` archived(구판, 14가 비용 대표), wiki-manifest pitfalls
