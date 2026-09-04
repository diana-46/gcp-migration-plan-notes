# 데이터플랫폼 이전

하둡 기반 데이터플랫폼을 GCP로 이전하는 프로젝트.

## 개요

하둡 기반의 데이터플랫폼을 GCP로 이전하는 작업.
아래 항목들에 대한 이전 계획을 세운다.

| 항목 | AS-IS | TO-BE | 조사가 필요한 것 |
|---|---|---|---|
| 쿼리 엔진 | presto | BigQuery | Presto 사용량 기반으로 요금제 확인 필요<br>BigQuery 아키텍처 파악 및 쿼리 호환성 검증<br> - Presto 사용량 기반 BigQuery 사용량 추정 및 slot 최적화 검토<br> - BigQuery 저장 포맷별 예상 비용 및 성능 |
| 데이터 스토리지 | hadoop + hudi | BQ Native<br>GCS + iceberg | 2개 안에 대한 비용 비교 후 선택 필요 (혹은 mixed?) |
| ETL | athlon (neptune) | dbt | 기존 athlon ETL을 변환하는 방법 조사 |
| 추출 | athlon (extract) | athlon (extract) | 플랫폼에서 제공하는 대체할 수 있는 기능이 있는지도 확인 필요 |
| Userlake | athlon (userlake) | athlon (userlake) | 생성하는 쿼리를 presto → BQ로 변경 필요 |
| 스케줄러 | airflow | airflow<br>composer | 비용 대비 운영 효율, 안정성 등 확인 필요 |
| 카탈로그 | datahub | datahub | 다른 항목들과 연계 방법 확인 필요 |

## 기본 조건

1. GCP의 **서울 리전**을 기준으로 조사한다.

## 이 레포의 역할

**팀용 LLM 위키의 소스 레포.** 조사·PoC·결정 기록을 여기 쌓고, 위키로 옮기기 좋은 형태(폴더별 결론 노트 + README 컨텍스트)로 유지한다.

**확정 사항** (이 레포에서는 재논의하지 않음):

- ETL: athlon (neptune) → **dbt** 로 전환 ✓
- 쿼리 엔진: presto → **BigQuery** ✓

데이터 스토리지(hudi→iceberg/BQ Native), 추출, 카탈로그(datahub)의 **인프라/도구 선택** 자체는 다른 담당자/레포에서 조사한다.

## 인덱스

각 폴더의 entry point는 `README.md`(컨텍스트)와 숫자가 가장 낮은 결론 노트(`0_결론` 또는 `1_개요`).

### 보고 (루트)

- [[0_GCP 이관 보고]] — GCP 이관 전체 보고
- [[0_Cloud Composer 인프라 보고]] — Composer 인프라 보고

### 주제 폴더

| 폴더 | 다루는 것 | 컨텍스트 |
|---|---|---|
| `스케줄러/` | Airflow 운영 (Composer vs Self-managed), 비용·권한·배포, Composer 3 신규 환경 구축(Terraform), PoC | [[스케줄러/README\|README]] |
| `애슬론/` | athlon 플랫폼 재구현 — dbt 수용 / operator 분담 / 이관 대상 Operator 인벤토리, PoC | [[애슬론/README\|README]] |
| `dbt/` | Neptune→dbt PoC 검증 (materialization·스키마·백필·의존성), incremental 전략, Presto→BQ 이관 규약, dp-dbt-utils | [[dbt/README\|README]] |
| `userlake/` | userlake-worker GCP 이관 — 인프라 대체 확정 + Spark Connect 컴퓨트·다운사이즈·비용 결정 | [[userlake/README\|README]] |
| `asset/` | Airflow 3 Asset scheduling 실전 (실측 + 제약 + 3.2/3.3 개선 + MDL aligning) | [[asset/README\|README]] |
| `spark-apps/` | spark-apps 배치의 GKE Spark Operator 이관 — 인벤토리·런타임 결정·첫 이관 앱 | [[spark-apps/README\|README]] |
| `deploy/` | Airflow Provider 배포 파이프라인 설계·런북 + 공유 Airflow 사용 가이드 | [[deploy/README\|README]] |
| `공유/` | DE/매니저/플랫폼팀 공유용 문서 세트 (결정 A/B, 로드맵, Before/After) | [[공유/README\|README]] |
| `베리즈 데이터 이관/` | 베리즈 데이터 이관 워크스트림 (날짜별 작업 로그) | [[베리즈 데이터 이관/README\|README]] |
| `attachments/` | 이미지·첨부 모음 | — |

---

## Vault 운영 규칙

이 vault에서 작업할 때 에이전트(Claudian)와 사용자가 함께 지키는 규칙.
새 노트를 만들거나 기존 노트를 정리할 때는 이 규칙을 먼저 확인한다.

> **Nested 규칙**: 작업 대상이 주제 폴더(예: `스케줄러/`) 안에 있을 때, 그 폴더에 `README.md`가 있으면 root `README.md`와 함께 반드시 읽는다. 카테고리별 컨텍스트·용어·외부자료·로컬 규칙이 거기 적혀 있다.

---

### 1. 폴더 / 파일 명명

- 주제별 폴더 하나에 관련 노트들을 모은다 (예: `스케줄러/`).
- 파일명은 **숫자 prefix + 언더스코어**로 정렬한다:
  - `1_개요.md` — **결론/의사결정 노트** (해당 주제의 entry point)
  - `2_...md`, `3_...md`, … — 근거/참고 자료 (논리적 흐름 순서)
- 파일명에 `[Airflow]` 같은 **카테고리 prefix는 쓰지 않는다.** 폴더명이 이미 컨텍스트.
  - ❌ `2_[Airflow] Executor 종류 및 비교.md`
  - ✅ `2_Executor 종류 및 비교.md`
- 폴더명은 **한국어**, 기술 용어는 영어 유지 (`Executor`, `Pod`, `Queue`).

---

### 2. `1_개요.md` 작성 지침

각 주제 폴더의 `1_개요.md`는 **결론 문서**다. 다음을 포함한다:

- 풀고자 하는 문제 / 의사결정 질문
- 핵심 결론 (한 줄)
- 결정 근거 (요약 표 또는 bullet)
- 트레이드오프
- 액션 아이템 / 다음 단계
- 참고 노트 링크 (`[[2_...]]`, `[[3_...]]` 등)

근거가 모이는 동안에는 `status: draft`로, 결정이 확정되면 `status: final`로.

---

### 3. Frontmatter 표준

모든 노트의 최상단에 YAML frontmatter를 둔다. 최소 필드:

```yaml
---
title: "노트 제목"
tags: [주제태그, 추가태그]
status: draft     # draft | review | final | archived
created: 2026-05-12
updated: 2026-05-12
source: <원본 URL, import한 경우만>
---
```

추가로 유용한 필드 (필요할 때):

- `related_prs: [URL1, URL2]` — 관련 GitHub PR
- `decision_date: 2026-05-12` — 결정이 내려진 날짜 (`status: final`일 때)

---

### 4. 링크 규칙

- vault 내부 노트 참조는 **항상 `[[wikilink]]`**. 평문(plain text) 금지.
  - ❌ `관련 문서: 2_Executor 종류 및 비교`
  - ✅ `관련 문서: [[2_Executor 종류 및 비교]]`
- 외부 링크는 마크다운 형식 `[제목](URL)`.
- import해온 노트의 `관련 문서` 같은 평문 참조는 wikilink로 자동 변환한다.

---

### 5. 외부 import 규칙 (Confluence / Notion / 기타)

외부 페이지를 vault로 가져올 때:

- 저장 위치는 **vault root 바로 아래의 주제 폴더**.
  - ❌ `confluence/DP/스케줄러/...`
  - ✅ `스케줄러/...`
- 원본 메타데이터는 frontmatter에 보존 (`source`, `confluence_id`, `version`, `updated`).
- import한 title이 본문 H1과 중복되는 경우 한 쪽을 제거한다.
- 본문 안의 `관련 문서` 섹션은 wikilink로 변환.
- 파일명에서 `[Airflow]` 같은 카테고리 prefix는 떼고 숫자 prefix만 남긴다.

---

### 6. 보안

- **API 토큰, 비밀번호, 계정 키, 인증 헤더 등 절대 vault에 저장 금지.**
- 환경변수(`export FOO=...`) 또는 macOS Keychain으로만 다룬다.
- 임시 파일에 토큰을 적었으면 작업 끝에 반드시 삭제.
- 실수로 vault에 들어갔으면 즉시 삭제하고, git 관리 중이면 history에서도 제거.
- 노트 안의 예시 코드도 placeholder(`<TOKEN>`, `xxxxxxxx`)로만.

---

### 7. 보조 폴더 규칙

- `_archive/` — 주제 폴더 안에 둘 수 있는 폐기/이전 버전 보관소.
  ```
  스케줄러/
  ├── 1_개요.md
  ├── 2_...
  └── _archive/
      └── 이전_검토안_2026-04.md
  ```
- `attachments/` — vault root에 단일 폴더로 두고 이미지 / 첨부 모음.
- `일일/` — 데일리 / 스크래치 노트. `일일/2026-05-12.md` 형식.
- `회의록/` — 외부 회의록. `회의록/2026-05-12-주제.md` 형식.

루트에 임시 노트가 쌓이지 않게 위 폴더 중 하나로 분류한다.

---

### 8. Nested `README.md` (카테고리별 컨텍스트)

각 주제 폴더에는 선택적으로 `README.md`를 둘 수 있다. 이 파일은 **해당 카테고리에서만 유효한 컨텍스트와 규칙**을 담는다.

권장 구조:

```markdown
# <카테고리명> — 컨텍스트

## 풀고자 하는 문제 / 의사결정
## 용어 / 약어
## 외부 자료
  - Confluence space / page URL
  - GitHub repo 경로 (절대경로 OK)
  - 사내 위키 URL
## 관련 사람 (Owner / 의사결정자 / 자문)
## Stack / 버전
## 로컬 규칙 (있다면, root 규칙을 보완)
## Anti-context (검토 끝나서 더 안 볼 옵션)
```

에이전트는 카테고리 폴더 안에서 작업할 때 **root `README.md` + 해당 폴더 `README.md`를 모두 컨텍스트로 사용**한다.

---

