# 4. substr 인덱스 — 음수는 안전, 0 이 함정

- 검증일: 2026-09-03
- 검증자: diana (Presto: presto-cli UTC 세션, adhoc — 문자열 함수라 TZ 무관 / BigQuery: `dev-dp-project-354904`)
- 분류: ⚠ 조용히 결과가 달라지는 함정 (문법 에러 없음)

## 무엇이 다른가

| 입력 | Presto `substr` | BigQuery `SUBSTR` |
| --- | --- | --- |
| 음수 인덱스 (`-3`) | 끝에서부터 — `'llo'` | **동일** — `'llo'` |
| 음수 + 길이 (`-3, 2`) | `'ll'` | **동일** — `'ll'` |
| **인덱스 0** | **`''` (빈 문자열)** | **`'hello'` (0 을 1 로 취급 — 전체)** |
| 양수 (1-based) | `'ello'` / `'ell'` | 동일 |

검증 전 변환표에는 "음수 인덱스 세만틱 차이 주의"로 적혀 있었는데, **실측 결과
음수는 완전 동일**하고 진짜 함정은 **인덱스 0** 이었다.

## 검증 쿼리와 실측 결과

```sql
-- 양쪽 공통 형태
SELECT
    substr('hello', -3)     AS a,   -- Presto: 'llo'   | BQ: 'llo'
    substr('hello', -3, 2)  AS b,   -- Presto: 'll'    | BQ: 'll'
    substr('hello', 0)      AS c,   -- Presto: ''      | BQ: 'hello'  ← 함정
    substr('hello', 2)      AS d,   -- Presto: 'ello'  | BQ: 'ello'
    substr('hello', 2, 3)   AS e    -- Presto: 'ell'   | BQ: 'ell'
```

## 결론

1. **음수 인덱스·양수 인덱스는 양쪽 동일** — 변환 불필요.
2. **인덱스 0 만 다르다**: Presto 는 빈 문자열, BQ 는 1 로 취급해 **문자열 전체**를
   반환. 오류가 없어서, 원본이 `substr(x, 0)` 으로 (실수든 의도든) 빈 값을 만들고
   있었다면 BQ 에선 값이 통째로 살아난다.
3. 변환 시: 원본에서 `substr(..., 0)` 또는 **인덱스가 변수/계산식**인 경우
   (런타임에 0 이 될 수 있음) 를 발견하면 의도를 확인하고 명시적으로 처리.

## 반영

- neptune-to-dbt `references/presto-to-bq.md` 함정 표의 "substr 음수 인덱스" 행을
  "substr 인덱스 0" 으로 재작성 + ✅ (2026-09-03)
