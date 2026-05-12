# 데이터플랫폼 이전 — 인덱스

하둡 기반 데이터플랫폼을 GCP로 이전하는 프로젝트의 전체 작업 인덱스.

---

## Vault 운영 규칙

이 vault에서 작업할 때 에이전트(Claudian)와 사용자가 함께 지키는 규칙.
새 노트를 만들거나 기존 노트를 정리할 때는 이 규칙을 먼저 확인한다.

> **Nested 규칙**: 작업 대상이 주제 폴더(예: `스케줄러/`) 안에 있을 때, 그 폴더에 `AGENTS.md`가 있으면 root `AGENTS.md`와 함께 반드시 읽는다. 카테고리별 컨텍스트·용어·외부자료·로컬 규칙이 거기 적혀 있다.

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
updated: 2026-05-12
source: <원본 URL, import한 경우만>
---
```

추가로 유용한 필드 (필요할 때):

- `confluence_id`, `space_key`, `version` — Confluence import 시
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

### 8. 인용 / 참조

- GitHub PR / 이슈 인용:
  - 본문에 간단 인용 + frontmatter `related_prs:`에 URL 누적.
- 회의 결과 인용:
  - `[[회의록/2026-05-12-주제]]`로 wikilink.
- 외부 article / 블로그:
  - 본문 하단 `## 참고 자료` 섹션에 `[제목](URL) — YYYY-MM-DD`.

---

### 9. Nested `AGENTS.md` (카테고리별 컨텍스트)

각 주제 폴더에는 선택적으로 `AGENTS.md`를 둘 수 있다. 이 파일은 **해당 카테고리에서만 유효한 컨텍스트와 규칙**을 담는다.

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

에이전트는 카테고리 폴더 안에서 작업할 때 **root `AGENTS.md` + 해당 폴더 `AGENTS.md`를 모두 컨텍스트로 사용**한다.

---

## 작업 폴더

- [[스케줄러/1_개요]] — Airflow 스케줄러 (Composer vs Self-managed) · 컨텍스트: [[스케줄러/AGENTS]]
- (이후 추가)
