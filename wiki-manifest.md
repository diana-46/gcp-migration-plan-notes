# diana/wiki-manifest.md — 팀 위키 스크랩 룰

> 팀 위키가 이 레포를 읽을 때 따르는 규칙. 작성자(diana)가 유지하며, 이 선언이 곧 신뢰도 신호다.
> (2026-09-08 확정판 — 팀 위키 `_manifest-drafts/diana.md` 초안 대체. 디렉토리 전면 재구성과 동시 적용이므로 **다음 사이클은 재시드 필요**)

```yaml
owner: diana
updated: 2026-09-08
track: >
  스케줄러(Composer/Airflow) · asset(Airflow Asset 스케줄링) · deploy(Provider 배포) ·
  애슬론 ETL 전환 · dbt 검증/규약 · presto-to-bigquery 쿼리 검증 ·
  userlake · spark-apps(GKE Spark Operator 이관) · 베리즈 이관 실행 · 공유(대외 설명자료)

rules:
  - "**/*.md 전량 포함 — 기본 신뢰도 draft(증언). 아래 exclude/승격 규칙만 예외"
  - "frontmatter status: final = confirmed (결론 인용 가능). 그 외 전부 draft"
  - "status: wip 또는 wip/** = 수집 제외 (아직 증언 아님)"
  - "status: archived 또는 **/_archive/** = 수집 제외 (폐기·구판)"
  - "*/PoC/** = reference (실행 상세 — 링크+요지만, 결론 인용은 status: final인 것만)"
  - "각 폴더 진입점 = 숫자 최저 노트(0_결론/1_개요). 폴더 내 수치가 충돌하면 진입점 노트가 대표"
  - "비용 수치 인용 시 basis 병기: 정가 추정(usage) / 실측(measured) / 약정(committed)"
  - "이미지 없음 — 캡쳐성 내용은 전부 본문 텍스트로 서술, 다이어그램은 mermaid 코드블록(스크랩 가능)"

exclude:
  - "wip/**"          # raw 작업장
  - "**/_archive/**"     # 폐기 보관소
  - "**/README.md"       # 디렉토리 안내·컨텍스트 (사람/에이전트용)
  - "**/*.html"          # 발표용 다이어그램 (내용은 노트 본문에 텍스트로 병기됨)

pitfalls:   # 인용 시 주의 — 원본에서 해소되면 여기서 제거한다
  - "platform/athlon/userlake/2·3·12·13·14 (Spark Connect 컴퓨트): Dataproc Serverless 전제 검토·실측 —
     컴퓨트 선택이 미정으로 돌아가 재검토 중. 확정 인용 금지 (3은 archived 구판, 14가 비용 대표)"
  - "결정 변경(2026-09-08): Spark 백필·재처리도 GKE Spark Operator로 일원화, Dataproc Serverless 미사용
     (모니터링 일원화) — 팀 위키 TDR-006의 '백필 전용 병행' 조항과 상충, _triage 이의 대상"
```

## 신뢰도 신호 체계 (위치 + status 혼합)

```
wip/2026-09-08 커넥터 삽질.md      ← 수집 제외 (raw)
        │ 내용이 정리되면 주제 폴더로 이동
        ▼
platform/airflow/17_커넥터 검토.md          ← draft: 증언으로 수집, 참고용
        │ 결론이 확정되면 status만 변경
        ▼
같은 파일, status: final              ← confirmed: 결론 인용 가능
```

- status 어휘는 4종만 쓴다: `wip | draft | final | archived` (frontmatter 없는 노트 = draft)
- 파일 이동·리네임은 최소화하고, 불가피하면 `git mv` (스크랩 인용이 경로@커밋 기반)

## 재인용 규칙 (순환 인용 차단)

팀 위키에서 가져온 내용을 이 레포 노트에 쓸 때는 `출처: 팀위키 <페이지>` 한 줄을 남긴다.
위키 문장이 출처 표기 없이 되돌아오면 "새 근거"로 오인돼 순환 인용이 생긴다.
