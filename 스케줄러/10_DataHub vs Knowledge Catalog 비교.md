---
title: "DataHub vs Knowledge Catalog 비교"
status: draft
tags:
  - airflow
  - 스케줄러
  - lineage
  - catalog
  - datahub
  - decision
created: 2026-05-15
updated: 2026-05-15
---

# DataHub vs Knowledge Catalog 비교

> Composer 환경에서 lineage·카탈로그 도구로 무엇을 쓸까. 의사결정용.
>
> "Knowledge Catalog" = GCP Dataplex 의 현재 명칭 (Universal Catalog).

## 한 줄 결론

> **dbt 중심 + 사내 시스템 lineage 통합** 가 우리 미션 → **DataHub** ⭐
> **GCP-only + 운영 인력 zero + dbt 비중 작음** → Knowledge Catalog

## 비교 매트릭스

| 항목 | Knowledge Catalog | DataHub |
|---|---|---|
| **출처 / 유형** | GCP managed | LinkedIn 오픈소스 (Acryl Cloud SaaS 옵션) |
| **Composer 통합** | ✅ Native — 환경 옵션 1개로 자동 lineage | ⚠️ `datahub-airflow-plugin` 설치 |
| **BigQuery lineage** | ✅ audit log 기반 자동 | ⚠️ `datahub-bigquery` plugin |
| **GCS / Pub/Sub / Dataflow** | ✅ 자동 등록 | ⚠️ Plugin |
| **dbt 통합** | ⚠️ 부분적 (INFORMATION_SCHEMA 우회) | ✅ **`datahub-dbt` native** (manifest, test, docs, column-level) |
| **사내 시스템 lineage** (MySQL CDC / Kafka / 사내 API) | ❌ 약함 | ✅ Plugin 자유 작성 |
| **Custom operator OpenLineage** | ✅ 표준 | ✅ 표준 |
| **Column-level lineage** | ✅ (BQ 한정) | ✅ (dbt 통합 풍부) |
| **운영 부담** | ✅ Managed (zero ops) | ❌ self-host 0.2~0.5 FTE (또는 Acryl SaaS) |
| **비용** (서울 기준 추정) | ~$50~200/월 | self-host $200~500/월 + 인력 / Acryl $500~2,000/월 |
| **UI / UX** | GCP 콘솔 통합 | 자체 UI. 풍부한 카탈로그 |
| **DLP / 데이터 분류** | ✅ Cloud DLP 자동 통합 | ⚠️ plugin |
| **확장성** | GCP 종속 | 어떤 source 든 plugin |

## Knowledge Catalog 가 매력적인 시나리오 4가지

Knowledge Catalog 가 그냥 망한 도구가 아니라 **이런 시나리오들** 에서는 답:

### 1. 운영 인력 zero — managed 라는 강력함

DataHub self-host = Elasticsearch + Kafka + MySQL/Postgres + 백엔드 전체 스택. 한 명이 0.3 FTE 부담. 작은 팀에서는 진짜 큰 부담.

→ "lineage 가 좀 있긴 해야 하는데 운영할 사람 없음" 시나리오에서는 Knowledge Catalog 가 답.

### 2. GCP-only + BQ-centric 조직

모든 게 BQ 안에서 끝나고, dbt 비중 작거나 안 쓰고, 외부 시스템 lineage 안 필요한 조직 → Knowledge Catalog 충분.

### 3. GCP 거버넌스 통합

- Cloud DLP 와 자동 통합 (PII 자동 식별)
- IAM 으로 권한 일원화
- Cloud Audit Logs 자동
- 사내 정책이 "GCP 표준 도구 우선"

→ 거버넌스 시너지가 DataHub 운영 부담을 압도하는 케이스.

### 4. 일단 "최소 lineage 라도" 필요한 작은 규모

PoC / 초기 단계 → "켜기만 하면 BQ lineage 자동" 매력. 나중에 dbt 도입하면서 DataHub 로 옮겨도 됨.

## DataHub 가 압도하는 시나리오

### 1. dbt 가 중심

dbt 모델 / `ref()` / `source()` / test / docs / column-lineage 가 **`datahub-dbt`** 로 자동 흡수. Knowledge Catalog 는 BQ audit log 기반이라 dbt 메타데이터 손해.

### 2. 사내 시스템 lineage 통합

athlon-extract 가 다루는 외부 MySQL / Kafka / 사내 API → BQ raw chain 의 lineage. DataHub plugin 으로 가능, Knowledge Catalog 는 GCP 외부 약함.

## 우리 athlon 시나리오 솔직 평가

[[3_dbt 능력 경계와 영역 분담]] §5 의 lineage chain:

```
[외부 MySQL / 사내 시스템 / API]
     │
     │ athlon-extract operator                ← Knowledge Catalog: 약함
     ↓
[BQ raw 테이블]    ← 여기가 dbt 시작점       ← Knowledge Catalog: 자동
     │
     │ dbt 모델                                ← Knowledge Catalog: 약함
     ↓
[BQ staging / marts]                         ← Knowledge Catalog: 자동
     │
     │ GCS export / Slack notify             ← Knowledge Catalog: GCS 자동, Slack 약함
     ↓
[외부 consumer]                               ← Knowledge Catalog: 약함
```

→ **Knowledge Catalog 는 BQ 내부 만 강함**. 외부 source / 외부 consumer / dbt 모델 디테일은 약함. 우리 athlon 의 핵심 가치 (**수집 → ETL → consume 통합 lineage**) 와 정합성 낮음.

## 세 가지 옵션

### 🥇 Option 1: DataHub 단독 (우리 시나리오 추천)

- dbt 가 중심이라 `datahub-dbt` 가 진짜 가치
- athlon-extract 가 다루는 외부 시스템 lineage 흡수
- Asset URN ↔ DataHub Dataset URN 매핑으로 [[4_Asset-Centric 아키텍처 안]] 정합
- 운영: self-host ~0.3 FTE (또는 Acryl Cloud)

### 🥈 Option 2: Knowledge Catalog 단독 (가벼운 답)

- 운영 인력 정말 부족할 때
- "수집-ETL 통합 lineage" 야망 포기하고 BQ 내부 lineage 만으로 만족
- dbt 풍부 메타데이터 / 사내 시스템 lineage 손해

### 🥉 Option 3: Hybrid (가능하지만 복잡)

- Knowledge Catalog: GCP 영역 native (BQ / GCS / Pub/Sub)
- DataHub: dbt manifest + 사내 시스템 lineage

→ 두 시스템 운영 부담. 일반적으로 한쪽 선택이 깔끔.

## 결정 요인

| 질문 | Knowledge Catalog | DataHub |
|---|---|---|
| 사내에 이미 DataHub 운영 중? | | ✅ |
| GCP 외 시스템 lineage 필요? | | ✅ |
| 운영 인력 0.3 FTE 가능? | | ✅ |
| dbt 모델 메타데이터 풍부히 필요? | | ✅ |
| "켜기만 하면 lineage 자동" 매력? | ✅ | |
| 운영 인력 부족 / managed 강력 선호? | ✅ | |
| GCP 안에서만 데이터 처리? | ✅ | |
| GCP DLP 자동 분류 / 거버넌스 통합? | ✅ | |

**핵심 질문 3가지** (회의 전 확인):

1. **사내에 이미 DataHub / Knowledge Catalog 운영 중?**
2. **dbt 비중이 얼마나 클까?**
3. **운영 인력 0.3 FTE 가용?**

## 비용 비교 (서울 리전, 대략)

### Knowledge Catalog

- API call 기반: ~$0.10 / 1k calls
- 일반 규모: **~$50~200/월**
- 사용량 적으면 free tier

### DataHub Self-host

- GKE 노드 + Elasticsearch + Kafka + MySQL/Postgres
- 인프라: **~$200~500/월**
- 운영 인력: **0.2~0.5 FTE** (이게 진짜 비용)

### DataHub Acryl Cloud (SaaS)

- 규모별 plan: **~$500~2,000/월**
- 운영 인력 면제

## PoC / 검증 항목

- [ ] **사내 현황** 확인: DataHub / Knowledge Catalog 사용 팀
- [ ] Composer 3 sandbox 에서 Knowledge Catalog 자동 lineage 활성화 → 무엇이 자동 들어오는지
- [ ] 샘플 dbt project + dbt-bigquery 실행 → Knowledge Catalog 에 lineage 어떻게 보이는지
- [ ] DataHub local docker + `datahub-dbt` 로 동일 manifest 흡수 → 비교
- [ ] 사내 시스템 lineage 가 진짜 필요한 시나리오 인벤토리 (athlon-extract 다루는 외부 system)

## 미확정 / 확인 필요

- Knowledge Catalog 의 dbt 통합 최신 깊이
- Composer 3 + Knowledge Catalog 자동 통합 범위
- DataHub 의 GCP native 통합 (`datahub-bigquery`, `datahub-airflow-plugin`) Composer 환경 호환성
- 사내 다른 팀의 카탈로그 도구 현황

## 우리 vault 디자인에 주는 영향

[[3_dbt 능력 경계와 영역 분담]] §11 과 [[4_Asset-Centric 아키텍처 안]] §6 이 **DataHub 전제**.

Knowledge Catalog 채택 시:
- Asset URN ↔ DataHub Dataset URN 매핑 → Asset URN ↔ Catalog Entry 매핑 으로 일반화
- 5축 통합 중 "DataHub Lineage" → "Catalog Lineage (도구 선택)" 추상화

→ Asset-Centric 디자인은 도구 비종속. 어느 카탈로그를 골라도 핵심 미션 유효.

## 관련 문서

- [[1_개요]] — 스케줄러 메인 결정
- [[2_Cloud Composer vs Self-managed 비교]] — Composer 결정
- [[3_dbt 능력 경계와 영역 분담]] §11 — DataHub 통합 전략
- [[9_Airflow Asset과 Dataset]] — Asset URI 표준
- [[../애슬론/4_Asset-Centric 아키텍처 안]] §6 — Asset-Centric 디자인의 lineage 통합
