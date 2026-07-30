# Incremental Strategy 세부 비교 (dbt-bigquery)

> 상세 규약·매커니즘은 `9_Presto-BigQuery 이관 규약.md` 참조

---

## 메인 비교표

| 전략 | 내부 SQL 매커니즘 | 필수 config | 원자성 | 파티션 프루닝 | 함정 | dbt-bigquery | 우리 표준 |
|---|---|---|---|---|---|---|---|
| **`merge`** (BQ 기본) | `MERGE ... ON T.uk = S.uk WHEN MATCHED UPDATE ... WHEN NOT MATCHED INSERT` | `unique_key` | ✅ | ❌ 자동 안 됨 → `incremental_predicates` 필수 | source 중복 실패, NULL uk = duplicates | ✅ | 신규 CDC/upsert만 (예외 승인) |
| **`insert_overwrite`** ⭐ | `MERGE ... ON FALSE WHEN NOT MATCHED BY SOURCE AND partition IN (...) THEN DELETE WHEN NOT MATCHED INSERT` | `partition_by` | ✅ | ✅ 자동 | Static 방식은 source WHERE 동기화 필수 (중복 폭탄 위험) | ✅ | ⭐ **주력** (파티션 통째 교체) |
| **`microbatch`** | 내부적으로 배치 단위 MERGE 반복 | `event_time`, `batch_size`, `begin`, `lookback` | ✅ (배치별) | ✅ | batch_size = 파티션 granularity 매칭 필요 | ✅ (1.9+) | Bronze 로그 계층 검토 |
| **`append`** | `INSERT INTO target SELECT ...` | 없음 | ✅ | N/A | 중복 제거 없음 (설계상) | ⚠️ 공식 미지원 (dbt-trino엔 있음) | 사용 안 함 |
| **`insert_only`** (storydata 커스텀) | `INSERT INTO target (SELECT ...)` + `pre_hook DELETE` | `partition_by` + `pre_hook` | ⚠️ 2단계 (DELETE+INSERT 사이 empty gap) | ✅ | pre_hook 조건 = SELECT WHERE 동기화 필수 | 커스텀 정의 필요 | (day 파티션에서) hour 블록 idempotent replace |

---

## 서브 비교: 파티션 요구사항

| Strategy | 파티션 필수? | 파티션 없이도 쓸 수 있나 |
|---|---|---|
| `merge` | 아니오 | ✅ 하지만 target 전체 스캔 위험 (predicates 필수) |
| `insert_overwrite` | ✅ 필수 (`partition_by` 없으면 dbt가 에러) | ❌ |
| `microbatch` | ✅ 필수 (event_time = partition 매칭) | ❌ |
| `append` | 무관 | ✅ |
| `insert_only` (커스텀) | 권장 (pre_hook DELETE 프루닝용) | ✅ (하지만 비용) |

---

## 서브 비교: 비용 위험도

| Strategy | 잘못 쓰면 폭탄? | 안전 장치 |
|---|---|---|
| `merge` | 🔴 큼 (target 전체 스캔 가능) | `incremental_predicates` 필수 규약 |
| `insert_overwrite` (dynamic) | 🟢 낮음 | 자동 파티션 프루닝 |
| `insert_overwrite` (static) | 🟡 중간 (source 동기화 실수) | 매크로화·리스트 동일 var |
| `insert_overwrite` (copy_partitions) | 🟢 매우 낮음 | scan 없음 (메타 조작) |
| `microbatch` | 🟡 중간 (배치 폭주) | `lookback`, `batch_size` 신중히 |
| `insert_only` (커스텀) | 🟢 낮음 | pre_hook 조건 정확 |

---

## 서브 비교: Presto → BQ 매핑

| Presto 현재 관행 | BQ 이관 후 |
|---|---|
| `delete+insert` + Hive 세션 트릭 + unique_key 없음 | **`insert_overwrite`** (정공법) |
| `delete+insert` + unique_key (표준 dbt-trino 용법) | `merge` + `incremental_predicates` |
| `append` (거의 미사용) | `insert_only` 커스텀 정의하거나 회피 |
| MERGE (Presto 0.272 불가) | `merge` ✅ 첫 사용 가능 |

---

## 매커니즘 한 줄 요약

| Strategy | 요약 |
|---|---|
| `merge` | "MERGE ON unique_key. target 전체 스캔 위험 → incremental_predicates로 파티션 프루닝 강제" |
| `insert_overwrite` (dynamic) | "임시 테이블에서 파티션 자동 감지 → `MERGE ON FALSE` 트릭으로 지정 파티션 원자적 교체" |
| `insert_overwrite` (static) | "사용자가 파티션 리스트 명시. IN 절로 인라인 → 파티션 프루닝. source WHERE도 같은 리스트 필수" |
| `insert_overwrite` (copy_partitions) | "MERGE 대신 Copy Table API로 파티션 복사. 스캔 비용 없음. 대량 백필 최적" |
| `microbatch` | "event_time 기준 자동 배치. 배치마다 내부 MERGE. dbt 1.9+ 신규" |
| `insert_only` (커스텀) | "pre_hook DELETE (블록 단위) + 순수 INSERT. 원자성 없지만 스코프 정확. Airflow retry 안전" |

---

## 우리 프로젝트 결정 매트릭스 (참고)

| 시나리오 | Strategy |
|---|---|
| Bronze 일별 스냅샷 | `insert_overwrite` (dynamic) |
| Silver 일별/시간별 집계 | `insert_overwrite` (dynamic) |
| Gold API 마트 | `insert_overwrite` (dynamic) |
| **대량 백필** (1년치 재빌드) | `insert_overwrite` (static) + **`copy_partitions=true`** |
| 신규 CDC dimension | `merge` + `incremental_predicates` 필수 (승인제) |
| day 파티션 + hour 블록 재삽입 | **`insert_only`** (커스텀) + pre_hook |
| Bronze append-only 로그 (신설) | `microbatch` 또는 `insert_only` |
