# tiara 로그 timestamp 9h 밀림 · 파티션 경계 (2026-09-07, 양쪽 실측 종결)

## 배경

- 로그 적재 안내문의 함정 경고: hive `timestamp` 는 타임존 없는 KST 벽시계일 수
  있어 BQ `TIMESTAMP`(UTC 절대시각)로 읽히며 9시간 밀릴 수 있다 → 실값 대조 요구.
- 별개로 common.md 의 "berriz = UTC 경계" 인식과 BQ 실물(`collect_date` KST 경계)이
  충돌 → 레이크 직접 대조로 어느 쪽이 맞는지 확정 필요.

## 대상

- 레이크: `berriz_identified_production.o_tiara_log_identified` (presto, DataGrip
  세션 — `select now()` 가 `+00:00` 으로 UTC 세션 확인 후 실행)
- BQ: `prod-dp-project.berriz_identified_prod.o_tiara_log_identified`
  (collect_date DAY 파티션, 2026-08-26 부터 적재)

## 쿼리와 결과

BQ:

```sql
SELECT MIN(collect_dt), MAX(collect_dt), COUNT(*)
FROM `prod-dp-project.berriz_identified_prod.o_tiara_log_identified`
WHERE collect_date = '2026-09-05';
-- 2026-09-04 15:00:00 / 2026-09-05 14:59:59 / 5,060,741
```

presto (레이크, UTC 세션 — 클라이언트 렌더링 배제 위해 서버측 varchar 캐스팅):

```sql
SELECT CAST(min(collect_dt) AS varchar), CAST(max(collect_dt) AS varchar), count(*)
FROM berriz_identified_production.o_tiara_log_identified
WHERE collect_date = '2026-09-05';
-- 2026-09-04 15:00:00.007 / 2026-09-05 14:59:59.985 / 5060741
```

## 결론

1. **9h 밀림 없음 (종결)** — `collect_dt` 는 레이크부터 벽시계가 아닌 **UTC
   instant** 였고, BQ 적재는 값을 그대로 보존했다. hive 벽시계 함정은 tiara 에
   해당하지 않음 (다른 로그 테이블, 특히 `_history` 는 만나면 동일 방법으로 확인).
2. **파티션 경계는 레이크부터 KST** — `collect_date` 하루 = KST 00~24시의 UTC
   환산 범위(전일 15:00 ~ 당일 14:59:59 UTC). "berriz = UTC" 는 CDC(원본 시각
   컬럼 파티션) 얘기고, 로그는 적재기가 만든 KST 달력 컬럼 파티션 —
   **경계는 서비스가 아니라 파티션 컬럼의 출처를 따라간다** (common.md 프레이밍 교정).
3. **행수 5,060,741 정확히 일치** — 9/5 파티션 기준 레이크↔BQ 적재 정합 확인.
4. 부수 관찰: 파티션은 수집일 기준이라 `action_dt` 는 late arrival (해당 파티션
   내 최소 action_dt 가 08-28 — 최대 8일 전 이벤트 포함). 행동 시각 기준 집계
   변환 시 파티션 하루만 읽으면 누락.

반영: sources/log.md (함정 절 ✅·파티션 절·미확정 체크), sources/common.md
(경계 규칙 프레이밍 교정 + 서비스x파이프라인 표) — 툴킷 커밋 7baac0c
(적재 실측·적재기 소스 대조 반영).
