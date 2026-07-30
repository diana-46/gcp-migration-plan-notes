# 6. DE 관점 — 일상 워크플로우 Before / After

> DE 가 실제로 뭘 다르게 하게 되는지, 뭘 배워야 하는지, 뭐가 그대로인지.
> 관련: [[2_결정A_dbt로_왜_가는가]], [[3_결정B_팀별_DAG_저장소]]

## 하루 흐름 비교

### Before (Neptune)

```
09:00  UI 열고 어제 실패한 ETL 확인
09:15  athlon 검색, SQL 편집, kwargs JSON 수정
09:30  저장, Airflow 트리거 (~10분 대기)
09:40  실패, 로그 확인, SQL 다시 수정
10:00  저장, 트리거, 대기
10:15  성공. 다음 실험으로 이동
```
사이클 하나 = 15-30분.

### After (dbt + Python DAG)

```
09:00  IDE 로 어제 dbt run 실패 확인, 로컬에서 `dbt run --select model` 재현
09:03  실패 원인 파악, .sql 파일 수정
09:05  `dbt run --select model` (10초) → 성공
09:07  git commit, PR 열기, CI 통과 대기 (~3분)
09:12  merge → Composer 자동 sync
```
사이클 하나 = 5-10분. 로컬 반복이 대부분.

## 편집 위치 · 방법

| 작업 | Before | After |
|---|---|---|
| SQL 수정 | athlon UI 텍스트 필드 | IDE (`.sql` 파일) |
| kwargs 수정 | athlon UI JSON 필드 | IDE (Python DAG operator kwargs) |
| 로컬 실행 | 불가 | `dbt run --select` / `airflow dags test` |
| 저장 | athlon "저장" 버튼 | `git commit` |
| 리뷰 | 슬랙 스크린샷 | GitHub PR |
| 배포 | 저장 즉시 반영 | PR merge → CI → GCS sync (~수 분) |

## 뭘 배워야 하는가

### 필수 (2-4주 온보딩으로 충분)

1. **dbt 기초**
   - `.sql` + `schema.yml` 파일 구조
   - `{{ ref() }}`, `{{ source() }}`, `{{ var() }}`
   - `materialized`, `incremental_strategy`, `partition_by` 개념
   - `dbt run`, `dbt test`, `dbt compile` CLI
   - 학습 자료: [[dbt/0_dbt 기본 개념]] ~ [[dbt/7_테이블 아웃풋]]

2. **Airflow DAG 기초** (Python)
   - `with DAG(...) as dag:` 구조
   - Operator instantiation + `>>` dependency
   - `data_interval_start` / `data_interval_end` Jinja 매크로
   - Airflow UI 에서 로그 / 재실행 조작
   - 학습 자료: LLM 도구 (Claude, Copilot) + 사내 예제 DAG + Airflow 공식 문서

3. **git / GitHub PR 워크플로우**
   - `git clone`, `branch`, `commit`, `push`
   - PR 생성, 리뷰 요청, comment 대응, merge
   - 이미 대부분 익숙하실 것

### 있으면 좋음 (선택)

- Python 기본 문법 (`if`, `for`, 함수 정의)
- LLM 프롬프팅 (에러 트레이스 붙여넣기 → 해석 받기)
- BQ 콘솔에서 스키마 / partition / 쿼리 검사

### 그대로인 것

- **SQL 자체**: SELECT, JOIN, WINDOW 등 — 그대로
- **도메인 지식**: 데이터 이해, 비즈니스 로직 — 여전히 본질
- **파티션 · 스케줄 개념**: hourly/daily 등 — 그대로
- **사내 operator (Loupe / Kafka / BQ 등)**: 이름은 동일, `import` 방식만 다름

## 흔한 걱정과 답

### "내가 못 짜면 어쩌지"

- **CI 안전망**: DAG parse, dbt parse 자동 검증. 배포 전 오류 catch.
- **PR 리뷰**: 지금 UI 저장은 리뷰 자체가 없음. 오히려 안전판이 새로 생김.
- **첫 3 개 PR 은 플랫폼팀 pair 리뷰** (Phase 0-1 약속)
- **LLM 지원**: Claude / Copilot 으로 첫 뼈대. 에러 붙여넣고 원인 해석 받기.

### "디버깅이 낯설다"

- 사실 Neptune 이 오히려 어려웠음 (로그 흐름 제한적, 재현 힘듦).
- Python DAG: 로컬 파싱 즉시, IDE breakpoint, Airflow UI Task Instance 로그 grep 가능
- dbt: `dbt run --select model --full-refresh --debug`, `target/compiled/` 에서 컴파일된 SQL 확인
- **디버깅 가능성 자체가 개선**되는 방향.

### "플랫폼팀 지원 이어질까"

- `#dbt-support`, `#airflow-support` 실시간 응답 SLA (Phase 0-2 최소 6개월)
- **Pair 프로그래밍** — 첫 DAG 는 플랫폼팀과 함께
- 워크샵 팀별 최소 2회
- 지원이 안 되면 이관 자체가 성립 안 함 → 플랫폼팀 KPI

### "DE 정체성이 흔들리는 느낌"

- SWE 로 바뀌라는 게 아님. **DE 의 도구에 Python 이 하나 더** 추가되는 정도.
- 도메인 지식 + 데이터 감각이 여전히 본질.
- 국내외 여러 팀 DE 도 이 방향. 흐름 자체가 자연.

### "안 그래도 바쁜데"

- **마이그레이션 시간은 OKR 로 인정** ([[애슬론/6_마이그레이션 플랜]] § 9)
- 매니저 / 디렉터 합의 전제. 일반 업무에 얹으면 진척 어려움 명시.
- Phase 0 에서 이 조건 안 잡히면 Phase 1 진입 보류.

## 첫 이관 실전 예시 (Story 팀 bizberry 케이스)

**타겟 ETL**: Neptune workflow `berriz_0101_bizberry_hourly` (10 개 ETL)

**이관 방식**:
1. Neptune YAML / SQL 을 dbt `.sql` 로 포팅 (초안은 자동 변환 도구 + 수동 검수)
2. 파티션 / 세만틱 mapping (관련: [[dbt/1_materialization]], [[dbt/7_테이블 아웃풋]])
3. `storydata-airflow-dags` 저장소에 DAG 파일 생성 (cosmos + Loupe operator)
4. `_integration` target 으로 dev 테스트
5. Dual-run 후 prod 전환

**결과 (2026-07)**:
- 4 mart 이관 완료 (userpost / artistpost / overview_trend / contents_summary)
- 각 mart 가 서로 다른 dbt 전략 (insert_only / insert_overwrite × daily / hourly partition)
- Producer + Consumer DAG 이 Asset 기반 자동 트리거

## 학습 리소스

### 사내

- [[dbt/0_dbt 기본 개념]] — dbt 기본기 (1-2시간 리딩)
- [[dbt/1_materialization]] ~ [[dbt/8_insert_overwrite_매커니즘]] — 응용 상세
- [[애슬론/5_Neptune SQL 변환의 dbt-BigQuery 대체 검토]] — 이관 검증
- [[애슬론/8_배포 시 유의할 점]] — 실전 함정
- 사내 예제 DAG 10-15개 (Story 팀 실제 이관본 활용 가능)

### 외부

- [dbt Docs](https://docs.getdbt.com/) — 공식 문서 (가장 정확)
- [dbt Fundamentals 코스](https://courses.getdbt.com/) — 무료 (기본기)
- Airflow 공식 문서 — Task SDK / Asset / Operator
- LLM (Claude / Copilot) — 첫 뼈대, 에러 해석

## 관련 문서

- [[2_결정A_dbt로_왜_가는가]] — dbt 결정 근거
- [[3_결정B_팀별_DAG_저장소]] — DAG 결정 근거
- [[8_실행계획과_안전장치]] — Phase / 지원 체계
- [[dbt/0_dbt 기본 개념]] — 학습 시작점
