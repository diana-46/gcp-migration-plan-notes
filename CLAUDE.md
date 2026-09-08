# CLAUDE.md — 이 레포를 읽는 LLM을 위한 안내

이 레포는 **diana의 GCP 데이터플랫폼 이관 개인 위키(L1)** 다. 팀 위키가 읽기 전용으로 스크랩해가는 원천이며, 위키는 이 레포에 절대 쓰지 않는다.

**정본 라우팅** — 이 파일은 요약하지 않는다. 반드시 원본을 읽어라:
- 스크랩 규칙(범위·신뢰도 신호·제외·pitfalls·재인용) → **`wiki-manifest.md`**
- 작성 규칙(명명·frontmatter·링크·import·보안·보조 폴더) → **`README.md` § Vault 운영 규칙**
- 폴더별 컨텍스트(용어·외부자료·로컬 규칙·anti-context) → 각 폴더의 `README.md` (Nested 규칙: 폴더 안에서 작업하면 root README와 함께 반드시 읽는다)

## 구조 한눈에 (폴더 번호 = 읽기 순서)

```
00-보고 → 10-스케줄러 · 11-asset · 12-deploy   (플랫폼: Airflow/Composer)
        → 20-애슬론 · 21-dbt · 22-presto-to-bigquery   (ETL·쿼리 전환)
        → 30-userlake · 31-spark-apps   (워커·앱 이관)
        → 40-베리즈   (서비스별 실행)
        → 50-공유   (대외 설명자료)
90-wip = raw 작업장 (스크랩 제외) · attachments = 이미지
```

각 폴더 진입점 = 숫자 최저 노트(`0_결론`/`1_개요`). **폴더 내 수치·결론이 충돌하면 진입점 노트가 대표다.**

## 신뢰도 신호 (요약 — 정본은 wiki-manifest)

frontmatter `status` 4종: `wip`(제외) / `draft`(증언, 기본값 — frontmatter 없어도 draft) / `final`(확정 — 결론 인용 가능) / `archived`(제외).

## 확정 스택 (재논의 금지 — canonical)

- ETL: athlon(neptune) → **dbt** / 쿼리 엔진: presto → **BigQuery** / 스케줄러: **Cloud Composer 3**
- CDC 수집: **Debezium → Kafka → BigQuery Sink Connector** (Datastream 기각 — 수집 트랙 결정)
- Spark 런타임: **GKE Spark Operator 일원화** — 백필·재처리 포함, Dataproc Serverless 미사용 (2026-09-08, 모니터링 일원화. 팀 위키 TDR-006의 "백필 병행" 조항보다 이 선언이 최신)

## 함정 맵 (인용 시 주의)

- `30-userlake/2·3·12·13·14` (Spark Connect 컴퓨트): **Dataproc 전제 검토·실측 — 컴퓨트 선택 미정, 확정 인용 금지.** 3은 archived 구판(14가 비용 대표)
- 비용 수치는 basis(정가 추정 usage / 실측 measured / 약정 committed)를 병기해야 인용 가능

## 이 레포에서 작업하는 LLM 세션의 행동 규칙

1. 새 raw 노트는 `90-wip/`에 (`2026-MM-DD 주제.md`). 주제 폴더 승격 시 frontmatter 필수.
2. `status: final` 승격은 **사용자 확인 후에만** — final은 "팀 위키가 결론으로 인용해도 됨"의 선언이다.
3. 팀 위키에서 가져온 내용을 노트에 쓸 때 `출처: 팀위키 <페이지>` 한 줄 필수 (순환 인용 차단).
4. 파일 이동·리네임 최소화, 불가피하면 `git mv` (스크랩 인용이 경로@커밋 기반).
5. 결론과 본문 수치가 어긋나는 수정을 했다면 결론(진입점 노트)까지 함께 갱신한다 — 결론만 인용될 때 낡은 값이 퍼진다.
