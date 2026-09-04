# 4. 타입·JSON 세트 — CAST AS JSON 의 함정 발견

- 검증일: 2026-09-03
- 검증자: diana (Presto: presto-cli UTC 세션, adhoc / BigQuery: `dev-dp-project-354904`)
## 검증 쿼리

**Presto:**

```sql
SELECT
    typeof(CAST(1 AS DOUBLE))                              AS e1,
    typeof(CAST(1 AS REAL))                                AS e2,
    json_extract_scalar('{"a": 1}', '$.a')                 AS e3,
    json_extract_scalar(CAST('{"a": 1}' AS JSON), '$.a')   AS e4
;
```

**BigQuery:**

```sql
SELECT
    JSON_EXTRACT_SCALAR('{"a": 1}', '$.a')  AS e3,
    JSON_VALUE('{"a": 1}', '$.a')           AS e4
```

## 실측 결과

| 컬럼 | Presto 식 → 결과 | BigQuery 식 → 결과 | 판정 |
| --- | --- | --- | --- |
| e1 | `typeof(CAST(1 AS DOUBLE))` → `double` | (FLOAT64 대응) | ✅ |
| e2 | `typeof(CAST(1 AS REAL))` → `real` | (FLOAT64 대응 — BQ 는 단정밀도 없음) | ✅ |
| e3 | `json_extract_scalar('{"a": 1}', '$.a')` → `'1'` | `JSON_EXTRACT_SCALAR(...)` → `'1'`, `JSON_VALUE(...)` → `'1'` | ✅ varchar 직접 추출 동치 |
| e4 | `json_extract_scalar(CAST('{"a": 1}' AS JSON), '$.a')` → **빈 값 (NULL/'')** | — | ⚠ 예상('1')과 다름 |

## e4 분석 — CAST(varchar AS JSON) 은 파싱이 아니다

Presto 에서 `CAST('{"a": 1}' AS JSON)` 은 문자열을 **파싱하지 않고 JSON 문자열
값으로 래핑**한다 (`"{\"a\": 1}"` — JSON object 가 아니라 JSON string). 그래서
`$.a` 추출이 조용히 실패한다. 파싱하려면 **`json_parse()`** 를 써야 한다.

이관 관점 함의:
- 원본 쿼리가 `json_extract_scalar(CAST(col AS JSON), ...)` 패턴이면 **원본에서도
  NULL 이었을 가능성**이 크다 — 변환 전에 원본 세만틱부터 의심할 것.
- varchar 에 직접 `json_extract_scalar` 하는 관행 패턴(e3)은 양쪽 완전 동치라 그대로 이관.
- `json_parse(col)` 경유 패턴을 만나면 별도 검증 (미실측).

## 결론

1. `DOUBLE`/`REAL` 타입 존재 확인 → BQ `FLOAT64` 단일 매핑 (BQ 는 단정밀도 없음).
2. JSON 추출: **varchar 직접**이 정석이고 양쪽 동치. `CAST AS JSON` 경유는
   presto 쪽에서 이미 깨지는 패턴 — 발견 시 원본 의도 확인.

## 반영

- 변환표: DOUBLE/REAL ✅, CAST AS JSON 행을 실측 내용으로 재작성 + ✅ (2026-09-03)
- **이로써 변환표 전 행(41행) 검증 완료** 🎉
