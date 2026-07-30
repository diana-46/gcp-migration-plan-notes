# 사용중인 spark-apps/bin 스크립트

Athlon `actions_prod` 기준으로 **active DAG (`dag_prod.is_paused=0 AND dag_prod.is_active=1`)** 안에서 실제로 호출되는 `spark-apps/bin` 하위 스크립트와, 그 스크립트가 실행하는 Spark 앱 (Scala 클래스) 매핑.

## 이관 전제

- 이관 대상: **GCP** (Dataproc / BigQuery / Cloud SQL / GCS 등)
- **DB 수집 파이프라인은 Datastream으로 대체 예정** — 소스 DB(MySQL/Mongo)에서 데이터를 끌어오는 부분은 CDC 기반 Datastream이 담당할 계획.

이관 검토 대상에서 제외한 것들 (기존 결정):
- `run_presto_sql_khp.sh` (Presto CLI, 이관 불필요)
- `merge_and_move_dataframes.sh` (이관 불필요)
- `run_hudi` 계열: `run_hudi.sh`, `run_kage_push_target_importer.sh`, `run_kage_target_group_importer.sh`

> ⚠️ **주의**: 아래 "이관 제안"은 코드만 보고 정리한 초안. **각 앱이 왜 이 방식으로 만들어졌는지 히스토리·의도는 파악하지 못한 상태**라서, 실제 이관 방향 결정 전에 팀장/팀원과 논의 필요.

## 이관 대상 Spark 앱 (중복 제거) — 9개

| # | 클래스 | 실행 경로 (스크립트) | 이관 제안 (논의 필요) |
|---:|---|---|---|
| 1 | `imports.MongoDataFrameImporter` | `run_mongo_dump.sh` | CDC 이관 검토 (아래 [특수 케이스](#-1-mongodataframeimporter-특수-케이스) 확인 후 결정) |
| 2 | `imports.MySqlDataFrameImporter` | `run_mysql_dump.sh`, `run_mysql_dump_ex5.sh` | CDC 이관 검토 (아래 [특수 케이스](#2-mysqldataframeimporter-특수-케이스) 확인 후 결정) |
| 3 | `imports.AgeGenderCategorizingImporter` | `run.sh` (직접 지정) | BQ SQL 재설계 (아래 [특수 케이스](#3-agegendercategorizingimporter-특수-케이스) 확인) |
| 4 | `exports.mysql.MySqlDataFrameExporter` | `run_mysql_export.sh` | **Reverse ETL 유지 필요** (아래 [특수 케이스](#4-mysqldataframeexporter-특수-케이스)) |
| 5 | `exports.mysql.MySqlDataFrameChangeApplier` | `run.sh` (직접 지정) | Hive→MySQL delta sync (아래 [특수 케이스](#5-mysqldataframechangeapplier-특수-케이스)) |
| 6 | `transform.DataFrameTransformer` (`-t trevi`) | `run_transformer_trevi.sh` | Dataproc lift 권장 (아래 [특수 케이스](#6-dataframetransformer-trevi-특수-케이스)) |
| 7 | `merge.UnifySchemaMerger` | `unify_schema_merger.sh` | **폐기 유력** (아래 [특수 케이스](#7-unifyschememerger-특수-케이스)) |
| 8 | `etl.TicketUseRecord` | `adhoc/run_ticket_use_record.sh` | **앱 폐기 + BQ SQL 재구현** (아래 [특수 케이스](#8-ticketuserecord-특수-케이스)) |
| 9 | `gc.HdfsGarbageCollector` | `run.sh` (직접 지정) | **폐기 유력** (dump 앱 이관되면 GC 대상 자체가 사라짐, 아래 [특수 케이스](#9-hdfsgarbagecollector-특수-케이스)) |

**초안 판정 요약 (앱별 특수 케이스 분석 후 갱신)**
- **폐기 유력**: #7 UnifySchemaMerger (fallback + upstream 이관제외), #8 TicketUseRecord (BQ SQL로 재구현), #9 HdfsGarbageCollector (대상 사라짐)
- **CDC로 대체 (Datastream)**: #1 Mongo Importer, #2 MySql Importer (특수 케이스 확인 후)
- **재설계 필요**: #3 AgeGenderCategorizingImporter (PII 정책 이슈), #5 MySqlDataFrameChangeApplier (조직 경계)
- **유지 (Dataproc lift 유력)**: #4 MySqlDataFrameExporter (reverse ETL 유지 필요), #6 DataFrameTransformer trevi (PII 마스킹)

## 앱별 특이점 (이관 관점)

| # | 앱 | 하는 일 | 특이점 | 이관 제안 | ❓ 논의 필요 |
|---:|---|---|---|---|---|
| 1 | MongoDataFrameImporter | Mongo → HDFS/Hive dump | aggregation pipeline 지원 · 파티션 키 커스터마이즈 · Hive view + Presto suffix 테이블 자동 생성 | CDC 이관 검토 (특수 케이스 확인) | 왜 CDC 대상에서 빠져있는지 (특수 케이스 섹션 참고) |
| 2 | MySqlDataFrameImporter | MySQL → HDFS dump | 커스텀 `KakaoPageMySQLDialect` (KS-7008 bit→binary 변환) · 추정 row count로 파티셔닝 · WHERE 필터 지원 | CDC 이관 검토 (특수 케이스 확인) | 왜 CDC 대상에서 빠져있는지 (특수 케이스 섹션 참고) |
| 3 | AgeGenderCategorizingImporter | User 테이블 → 연령·성별 카테고리 산출 | `old`/`new`/`new_global` 3종 쿼리 · MySQL 함수(YEAR, TIMESTAMPDIFF, COALESCE) 하드코딩 · MySQL slave에서 range partition read | BQ SQL (스케줄드 쿼리 / dbt) 재설계 | 3종 쿼리 분기가 왜 있는지, 지금도 다 쓰는지 · 카테고리 로직 재구현 시 결과 검증 |
| 4 | MySqlDataFrameExporter | DataFrame → MySQL write (reverse ETL) | 커스텀 SaveMode (`insertignore`/`replace`) · 기본 1 파티션 · 사내 Spark 확장 · 소비처가 타팀 서비스 | Reverse ETL 유지 (구현 방식만 결정) | GCP에서 어떤 방식으로 reverse ETL 구현할지 (Dataproc / Composer+Python / Dataflow) |
| 5 | MySqlDataFrameChangeApplier | Neptune snapshot delta → 정산 MySQL sync | 서비스 DB→정산 DB 마스터 데이터 hourly 복제 · Neptune hive snapshot 앞뒤 시간 비교 · `INSERT IGNORE`+`DELETE`+`REPLACE INTO` · 사내 확장 사용 | Dataproc lift 또는 BQ 경유 두 단계 | Neptune 대체안 · 정산 팀과의 sync 방식 협의 |
| 6 | DataFrameTransformer (`-t trevi`) | page_trevi 로그 JSON → Parquet + PII 마스킹 | PII 컬럼 exclude 필수 (`userInfo_birth`, `ifa`) · rewarded는 `postbackId` dedup · age 자동 카테고리화 UDF · backfill DAG 2개 hourly | Dataproc lift 권장 | backfill DAG 상태 · JSON에 age 필드 존재 여부 · 소스 JSON 생성 파이프라인 이관 방향 |
| 7 | UnifySchemaMerger | Tiara 로그 다중 스키마 파일 병합 (**fallback**) | **`trigger_rule: all_failed`** — 정상 흐름에 실행 안 됨 · upstream `DataFrameMerger` 실패 시에만 리커버리 · 파일명 index 11부터 substring 그룹핑 | 폐기 유력 (upstream이 이관 제외라 fallback도 무의미) | 실제 실행 빈도 · 스키마 다형성 원인 · GCP 이관 후 스키마 안정성 |
| 8 | TicketUseRecord | buydb Hudi 16샤드 + Neptune parquet 조인 → tmp 산출 | 5-스텝 chain 중 중간 계산 · 16 샤드 union · broadcast join · OLD_DB 코드 잔존 · Jira 3회 개정 | **앱 폐기 + BQ SQL 재구현** | Neptune·downstream export 이관 방향 · `boracay` 정체 · dead code 정리 |
| 9 | HdfsGarbageCollector | HDFS timestamp suffix 디렉토리 TTL GC | 파일 mtime 아니라 **디렉토리 이름의 10자리 timestamp** 기반 · dump 앱 출력 정리 · exclude prefix (`t_series_product` 등 Neptune 소스) · Spark job이지만 실제 병렬 처리 없음 (client 모드) | **폐기 유력** (dump 앱들 이관되면 대상 사라짐) | exclude prefix가 downstream 의존이면 GCP에서도 유지 필요 · 이관 완료 전까진 유지 |

---

## 특수 케이스 상세

이미 CDC가 적용된 곳도 있고 이 Spark 배치 dump가 유지된 곳도 있음. **CDC로 가지 않고 배치로 남아있다면 그 나름의 이유가 있을 가능성이 큼** → 각 앱마다 특이한 사용 패턴을 뽑아서 원인을 팀 논의로 확인해야 함.

### #1 MongoDataFrameImporter 특수 케이스

**전체 37개 태스크 중 대부분(36개)은 단순한 "전체 스냅샷 dump" 패턴이지만, 아래 케이스들은 다름.**

**A. 시간 범위 aggregation pipeline + 월별 컬렉션 (1건)**
- 태스크: `data_0200_dump_hourly.dump_mongo_contents_open_log`
- 사용 옵션:
  - `-c open_log_{{ YYYYMM }}` — **컬렉션 이름이 월 단위로 시프트됨**
  - `-p '{$match: {open_dt: {$gte: ISODate(...), $lt: ISODate(...)}}}'` — **aggregation pipeline로 시간 윈도우 필터**
  - `--partition-key open_dt` — 커스텀 파티션 키 (기본 `_id` 아님)
  - `--create-hive-table false` — Hive 테이블 자동 생성 안 함 (나머지는 true)
- 실질적으로 **hourly 증분 로드**를 aggregation pipeline으로 흉내내고 있음
- ❓ 논의 필요: 이 open_log 컬렉션이 왜 배치 pipeline 방식인지 (컬렉션 이름 시프트가 CDC 대응이 어려운지, 이벤트 로그라 별도 처리하는지)

**B. 특정 컬럼 제외 (`-x`) (3건)**
- 태스크와 제외 컬럼:
  - `dump_mongo_stat_DArgsPass` — `-x kwargs` 제외
  - `dump_mongo_stat_DServerState` — `-x kwargs` 제외
  - `dump_mongo_stat_DCashFriend` — `-x start_dt` 제외
- 이유 후보: 스키마 문제 (nested/mixed type), 큰 필드 회피, 파싱 오류 컬럼 스킵 등
- ❓ 논의 필요: 왜 이 컬럼들만 특별히 빼는지 (데이터 이슈? 개인정보? 스키마 다형성?)

**공통 관찰**
- 나머지 33개는 옵션 조합이 거의 동일 (`-s`, `-e`, `--cluster-id`, `-c`, `--output-dir-timestamp`, `--create-hive-table true`) → 단순 dump. CDC로 대체하기 가장 무난한 부류.

### #2 MySqlDataFrameImporter 특수 케이스

**전체 9개 태스크, 2개 DAG.** WHERE 필터, 컬럼 include/exclude, estimated-row-count 등 파워 옵션은 하나도 안 씀. 즉 코드에는 다양한 기능이 있지만 **실제 사용 중인 건 매우 단순한 full dump 패턴**.

**A. 샤드된 MySQL을 8-way로 dump (8건)**
- DAG: `data_0004_dump_mysql_userinven_daily`
- 옵션 조합: `-s page_userinven -d userinven{01..08} -t view_history_meta --max-records-per-partition 200000 --create-hive-table false`
- 특징:
  - **8개 물리 DB (userinven01 ~ userinven08)** 각각을 태스크로 나눠 병렬 dump
  - 모두 동일 테이블(`view_history_meta`)을 뽑아옴
  - `--create-hive-table false` — Hive 테이블 자동 생성 안 함 (아마도 병합 후 별도 스텝에서 처리)
  - `--max-records-per-partition 200000` — 기본값(2,000,000)보다 훨씬 작게 명시. 큰 row 사이즈 or memory 이슈 회피용일 가능성
- ❓ 논의 필요:
  - 샤드된 DB 그대로 CDC 걸 수 있는지 (샤드마다 커넥터 필요)
  - `view_history_meta` 크기·row 특성 (`max-records-per-partition`을 명시적으로 낮춘 이유)
  - Hive 테이블 생성을 skip한 뒤 후속 처리가 뭔지

**B. `--contain-db-name-in-hive-table false` 사용 (1건)**
- 태스크: `data_0007_dump_mysql_page_userpublic_daily.dump_mysql_userpublic_t_waitfree_user`
- 옵션: `-s page_userpublic -d userpublic -t t_waitfree_user --contain-db-name-in-hive-table false`
- 특징: **유일하게 이 옵션 사용**. 기본은 true여서 Hive 테이블명이 `userpublic_t_waitfree_user`로 만들어지는데, 이 태스크만 `t_waitfree_user`로 만들도록 오버라이드
- ❓ 논의 필요: 왜 이 테이블만 DB 이름 prefix를 빼는지 (다른 소비처와 이름 맞춰야 하는지, 이관 이력이 있는지)

**공통 관찰**
- `-w` (where 필터), `-i`/`-x` (컬럼 필터), `--estimated-row-count` 등 코드에 있는 고급 옵션은 **아무도 안 씀** → 이관 시 이 기능들은 재현하지 않아도 될 수 있음
- 두 케이스 모두 **매일 fresh full dump** 구조 → CDC로 옮기면 훨씬 효율적일 수 있음
- 반대로 지금 배치로 남아있다는 건 CDC 걸기 어려운 요건(예: userinven 샤드 관리 복잡도)이 있을 가능성

### #3 AgeGenderCategorizingImporter 특수 케이스

**전체 1건, 1개 DAG** (`data_2000_categorize_age_gender.dump_categorize_age_gender`)

**실제 호출 커맨드:**
```
run.sh AgeGenderCategorizingImporter
  -s page_user -e {phase}
  --num-partitions 200
  --output-dir /team/kakaopage_c1/{phase}/categorized_age_gender/create_date={YYYYMMDD}
  --query-option new_global
```

**A. 이건 dump가 아니라 ETL**
- MySQL에서 `t_user + t_user_auth_info + user_private_info` 조인 read
- 나이를 10개 구간으로 버킷팅 (`15세 미만`, `20세 미만`, ... `60세 이상`)
- 성별 `남`/`여`/`NONE` 매핑
- `adult_flag` 컬럼(`미성년`/`18세 이상`/`19세 이상`) 추가
- **도메인 로직이 전부 SQL에 하드코딩**

**B. `new_global` 쿼리만 실사용**
- 코드에 3종 (`old`, `new`, `new_global`) 존재하지만 프로덕션은 `new_global`만 사용
  - `old`: 한국 나이 (`YEAR(CURRENT) - YEAR(birthday) + 1`)
  - `new`: 서양 나이 (`TIMESTAMPDIFF`)
  - `new_global`: **글로벌 대응** — `user_private_info` LEFT JOIN + `COALESCE(auth.str_birthday, upi.birthday)` 폴백, `adult_flag` 추가
- ❓ `old`/`new`는 legacy → 이관 시 폐기 가능

**C. team-specific 출력 경로**
- 출력: `/team/kakaopage_c1/{phase}/categorized_age_gender/create_date={YYYYMMDD}`
- 표준 데이터 레이크가 아니라 **팀 소유 디렉토리** (`/team/kakaopage_c1/`)
- Hive 테이블 생성 안 함
- ❓ 소비처(어떤 팀이 쓰는지) 확인 필요

**D. 수동 JDBC 파티션 최적화**
- `SELECT MIN(uid), MAX(uid) FROM t_user`로 범위 조회 → 200 파티션 병렬 read
- **slave에서 read**
- WHERE 필터 없이 전체 조인 (INNER/LEFT + `WHERE ... IS NOT NULL`)

**존재 이유 (가설 — 검증 필요)**
- 이 앱이 "importer" 이름인데 실제로는 read 시점에 범주화하는 이유는, **성별·생년월일 raw 값을 하둡으로 가져올 수 없는 정책 제약** 때문일 가능성이 큼
- 즉 카테고리화(`남`/`여`, `20세 미만`, `adult_flag` 등)는 **PII 노출 최소화(anonymization) 목적**의 read-time 변환
- 이 가설이 맞다면 CDC로 대체 불가한 근본 이유가 여기에 있음 (단순히 배치라서가 아니라, raw PII를 데이터 레이크로 옮길 수 없어서)

**CDC/이관 관점 함의**
- 위 가설이 맞으면 **Datastream이 raw birthday/gender를 BQ에 랜딩하는 것도 같은 정책 위반**이 됨
- 이관 시 옵션:
  1. Spark/Dataproc에서 이 read-time 범주화 그대로 유지
  2. 소스 DB에 view/materialized view로 범주화된 값만 노출하고, 그 view를 CDC 대상으로
  3. Datastream 컬럼 exclude + 이후 별도 안전한 채널로 birthday만 read → 범주화 → 결과만 BQ에 저장
- **CDC로 못 옮기는 게 정책 이유라면 이관 후에도 같은 형태의 배치 ETL이 필요**

**❓ 논의 필요**
- 위 가설(범주화 = anonymization) 자체가 맞는지 확인
- 개인정보 정책 owner (보안·컴플라이언스 팀?) 어디인지
- `/team/kakaopage_c1/categorized_age_gender/` 소비처
- `user_private_info` (글로벌 연령정보 테이블)가 CDC 대상에 포함돼 있는지
- `old`/`new` 쿼리 잔재 폐기 가능 여부

### #4 MySqlDataFrameExporter 특수 케이스

**전체 13번 호출, 2 DAG, 8 태스크** (Case B는 태스크당 export 2번 chain).

이건 **reverse ETL** — Hive/Spark SQL 결과를 다시 MySQL로 내보내서, **타팀 서비스**(정산, 파트너 사이트, BI 대시보드 등)가 MySQL을 read함. **소비처가 서비스 시스템이라 BQ direct 접근으로 대체하기 어려움** (스캔 비용, 레이턴시, 쿼터, 인증 복잡도, 타팀 코드 변경 협의) → **reverse ETL 자체는 이관 후에도 유지 필요**.

**사용 옵션**
- `-i` (import-sql): 13/13 — Spark SQL 쿼리로 소스 로드 (Hive 테이블 read)
- `-m` (save-mode): `replace` 3건, `insertignore` 10건 (사내 확장 SaveMode)
- `-n 1 --batch-size 100`: Case B에서만 명시. 트랜잭션 안전 위해 write는 1 파티션.
- `update` 모드와 관련 옵션(`where-columns`, `additional-where-clause`)은 **활성 DAG에서 안 씀**

**Case A: settlement 3개 지역 → 한 MySQL 테이블** (`data_8007_kakaowebtoon_settlement_daily`)
- kor(v1) / tha / twn 3개 태스크
- Hive `page_settlement_production.kw_billing_settlement_{region}_daily` → MySQL `settlement.kw_billing_settlement`
- 모드: `-m replace` (`REPLACE INTO`)
- 3 지역이 모두 같은 MySQL 테이블에 씀 (아마 region 컬럼으로 구분)
- 소비: **정산 시스템 (타팀)**

**Case B: BI 마트 → opsinsights DB** (`data_8010_common_dw_kor_daily`, 5 태스크)
- Hive `partner_site.kp_*_production` → MySQL `opsinsights.*`
- 모드: `-m insertignore` (`INSERT IGNORE`)
- **매 태스크가 export 2번 chain (`&&`)**:
  1. 먼저 `stat_relay_history` 테이블에 audit row 삽입 (`table_name, create_date, record_count, created_dt`)
  2. 그다음 실제 데이터 export
- Bash `NOW="$(TZ=UTC date ...)"`로 `created_dt` 주입
- 소비: **파트너 사이트, BI 대시보드 추정**

**커스텀 SaveMode `SparkMySqlExtensionUtils` 의존**
- `INSERT IGNORE`, `REPLACE INTO`는 MySQL 전용 문법
- 사내 Spark 확장이라 다른 툴로 옮기면 semantics 재구현 필요
- Cloud SQL for MySQL을 계속 쓰면 문법 그대로 유지 가능

**이관 옵션 (reverse ETL 유지 전제)**

Dataflow는 비용 이슈로 제외. 실질적으로 A, B 둘 중 선택.

| 옵션 | 방식 | 장단 |
|---|---|---|
| **A. Dataproc lift** | Spark 앱 그대로 + 사내 jar 유지 | 로직 그대로 재사용, 커스텀 SaveMode(`insertignore`/`replace`) 재구현 불필요 · Dataproc이 이미 로그 수집용으로 존재하면 부담 최소 |
| **B. Composer + Python** | Airflow(Composer)에서 BQ read → SQLAlchemy로 Cloud SQL write. `INSERT ... ON DUPLICATE KEY UPDATE`로 `insertignore`/`replace` 표현 | 심플, 저비용, 소량 데이터에 적합 · 대용량 시 성능 이슈 |

**로그 수집 파이프라인이 이미 Dataproc을 쓸 예정이라 → 옵션 A가 가장 자연스러움.** 인프라 공유 + Spark 코드 그대로 재사용. 별도 인프라 관리 부담 없음.

옵션 B는 이 앱 하나만 별도로 뽑아 이관하고 싶을 때 유효.

**❓ 논의 필요**
- 각 타겟 MySQL 테이블의 소비 시스템/팀 확인 (정산 시스템? 파트너 사이트? BI?)
- `stat_relay_history` audit 패턴이 실제로 어디서 소비되는지 (필요한 로그인지)
- Case A 지역별 3 태스크가 왜 분리돼 있는지 (BQ에선 region 컬럼 파티션으로 통합 가능)
- Cloud SQL for MySQL vs 다른 DB 옵션 결정 (문법 호환성 영향)

### #5 MySqlDataFrameChangeApplier 특수 케이스

**전체 5 태스크, 2 DAG.** CDC 역방향이라기보다 정확히는 **"hive snapshot 델타 → MySQL sync"** 파이프라인. 서비스 DB의 마스터 데이터를 정산 DB로 hourly 복제.

**코드 동작**
- `-b`(before parquet) vs `-a`(after parquet) 두 스냅샷 read
- 지정된 컬럼(`-c`)만 select
- 키(`-k`) 기준으로 diff 계산 → `added` / `removed` / `modified`
- MySQL master에 apply:
  - added → `INSERT IGNORE`
  - removed → `DELETE ... WHERE key = ?`
  - modified → `REPLACE INTO`
- `SparkMySqlExtensionUtils` 사내 확장 사용 (Exporter와 공유)

**Case A: Neptune snapshot hourly delta (4건, `data_0200_dump_hourly`)**
- 대상 테이블: `t_category`, `t_publisher`, `t_series_product`, `t_ticket_info_product`
- 소스: `/page_service|page_user/production/raw/neptune/snapshot_{table}/snap_date={YYYYMMDD}/snap_hour={HH}`
  - **Neptune**: Presto CTAS로 서비스 MySQL의 `_ro` 테이블에서 명시적 컬럼 CAST로 parquet 스냅샷 생성 (`run_presto_sql_khp.sh` 사용)
  - 예: `CREATE TABLE snapshot_t_series_product_{ts} WITH (format='PARQUET') AS SELECT CAST(uid AS decimal(20,0)), ... FROM t_series_product_ro`
  - 앞 시간 vs 현재 시간 스냅샷 비교로 변경분 추출
- 타겟: `page_settlement.settlement.{same_table}`
- 즉 **서비스 쪽 원본 → 정산 쪽 복제본으로 hourly sync**
- 컬럼 subset만 반영 (타겟에 필요한 컬럼만)
- `t_ticket_info_product`는 복합키 `series_id,position`

**전체 chain (현재)**
```
Service MySQL (page_service)
   ↓ CDC (기존 시스템)
_ro Hudi 테이블 (Hadoop) ← 원천 테이블 (source-of-truth)
   ↓ Neptune = Presto CTAS hourly
snapshot_{table}_{unix_ts} (hive parquet)
   ↓ MySqlDataFrameChangeApplier (앞뒤 스냅샷 diff)
정산 MySQL (page_settlement)
```

**GCP 이관 후 예상 chain**
```
Service Cloud SQL
   ↓ Datastream (기존 CDC의 GCP 버전)
BQ landing table ← _ro (Hudi) 대체
   ↓ BQ scheduled query / view / dbt ← Neptune 대체
snapshot BQ 테이블 (또는 단순 view)
   ↓ Reverse ETL (Dataproc lift or Composer+Python) ← ChangeApplier 대체
정산 Cloud SQL
```

**핵심 통찰**
- `_ro`는 이미 CDC로 수집한 원천 (지금은 Hudi/Hadoop). 즉 CDC 자체는 이미 존재. Datastream은 그 CDC의 GCP 버전.
- Neptune의 Presto CTAS + 명시적 CAST는 Hudi 스키마 drift 방지 목적. BQ 랜딩 스키마가 안정적이면 view 하나로 대체 가능해 매우 단순해질 수 있음.
- 정산 팀이 BQ 직접 접근 가능하면 ChangeApplier 자체가 필요 없어짐 (단, 서비스 read 패턴상 어려울 가능성).

**Case B: `_base` vs `_export` 비교 (1건, daily) — 같은 앱, 다른 사용 방식**
- 태스크: `apply_change_t_series_alarm_summary` (`data_0008_dump_mysql_page_service_daily`)
- 앞선 사이블링 태스크 `create_t_series_alarm_summary_export`가 Presto CTAS로 두 hive 테이블을 미리 만듦:
  - `_export`: 원천 알람 노티(`t_series_update_noti_ro`)에서 `COUNT(*)`로 **재계산한 정확한 값**
  - `_base`: 서비스 DB 현재 캐시(`t_series_alarm_summary_ro`)를 그대로 hive로 재구성한 **현재 상태**
- ChangeApplier가 이 둘 diff 계산 → 서비스 DB `t_series_alarm_summary` 테이블에 UPDATE/INSERT/DELETE
- 즉 **서비스 DB 안에서 집계 캐시 refresh** 하는 패턴. Case A(정산 DB로 replicate)와 완전히 다른 use case지만 앱 자체(applyDiff 로직)는 동일.

**Case A vs Case B — 같은 앱의 두 가지 사용 방식**

| | Case A (hourly, 4건) | Case B (daily, 1건) |
|---|---|---|
| `-b` before | Neptune snapshot 이전 시간 | 서비스 DB 현재 캐시 재구성 |
| `-a` after | Neptune snapshot 현재 시간 | 원천에서 재계산한 정확값 |
| diff 의미 | 시간 축 변경분 | 캐시와 정확값의 편차 |
| 타겟 | 정산 DB (다른 시스템) | 서비스 DB (자기 시스템) |
| 목적 | 조직 경계 넘어 마스터 데이터 복제 | 집계 캐시 refresh |
| 왜 diff apply | 시간별 변경분만 반영 | 서비스 실시간 참조 → TRUNCATE+INSERT 못함 |

이관 시 앱 자체는 하나로 다루되, 대체 방안은 케이스별로 다름:
- **Case A**: Datastream + BQ + 정산 팀 협의 (조직 경계 이슈)
- **Case B**: BQ scheduled query + Cloud SQL reverse ETL, 또는 Cloud SQL 안에서 직접 stored procedure로 재계산 (조직 경계 없어서 유연)

**존재 이유 — 조직 경계 추정**
- 정산 시스템이 자기 계산에 마스터 데이터가 필요
- 정산 시스템이 서비스 DB에 직접 접근 못하는 이유가 있어서 (조직/보안 추정)
- 그래서 서비스 DB의 CDC 결과(`_ro` Hudi) → Neptune 시간별 스냅샷 → ChangeApplier로 정산 DB에 delta sync 하는 chain 유지

**이관 옵션**

| 옵션 | 방식 | 장단 |
|---|---|---|
| **A. Dataproc lift** | Spark 앱 그대로 유지 (Neptune snapshot은 BQ scheduled query/view로 대체하되, diff+apply 로직은 Spark 유지) | 로직 재사용, 사내 jar 활용 · Neptune 대체안 필요 |
| **C. BQ 경유 두 단계** | 서비스 → BQ (Datastream) → 정산 Cloud SQL (reverse ETL, Composer+Python or Dataproc) | Datastream + Exporter 인프라 공유 · 지연 큼, 정합성 관리 복잡 |

Datastream은 BQ/GCS로만 랜딩하므로 **서비스 Cloud SQL → 정산 Cloud SQL 직접 CDC는 불가**. 필요하면 Cloud SQL External Replica나 DMS 등 별도 도구 검토 필요.

**❓ 논의 필요**
- 정산 시스템이 왜 마스터 데이터를 자기 DB에 두어야 하는지 (조직/보안/성능?)
- Case B의 `_base` vs `_export` 소스가 뭘 계산하는지 (같은 DAG 다른 태스크 확인)
- 정산 팀과 서비스 DB CDC 직접 replicate 협의 가능한지 (BQ 직접 read이든 Cloud SQL replica든)
- Neptune의 Presto CTAS를 BQ view/scheduled query로 대체 시 스키마 안정성 (Datastream 랜딩 스키마 검증)

### #6 DataFrameTransformer (trevi) 특수 케이스

**전체 5 태스크, 3 DAG** — 모두 `page_trevi` 광고 리워드/캠페인 로그 JSON을 Parquet로 변환하면서 **PII 마스킹**.

**사용 패턴**

| 태스크 | 소스 타입 | 특이 옵션 |
|---|---|---|
| `campaign_json_to_parquet` (data_8004) | campaign | `-x data.userInfo_birth` |
| `rewarded_json_to_parquet` (data_8004, backfill 0429, backfill 1865) | rewarded | `-x data.userInfo_birth,ifa --unique-keys postbackId` |
| `tessera_json_to_parquet` (data_8004) | tessera | `-x data.userInfo_birth,data.ifa` |

- 경로: `/page_trevi/{phase}/raw/json/{type}/{YYYY/MM/DD/HH}/*` → `.../json2parquet/{type}/{YYYY/MM/DD/HH}`

**Trevi transformer 코드 로직**
```
1. JSON read
2. -x 로 컬럼 drop (nested dot expression 지원)
3. Trevi-specific transform:
   a. parquet 저장 불가능 문자 제거
   b. flatten schema
   c. --unique-keys 있으면 dropDuplicates
   d. data.userInfo_age 컬럼이 있으면 UDF로 카테고리 컬럼 자동 생성 + 원본 age drop
4. Parquet write (overwrite)
```

**A. PII 제외가 모든 태스크의 필수 옵션**
- 5/5 태스크가 `userInfo_birth` (생년월일) 제외
- rewarded, tessera는 `ifa` (광고 식별자 IDFA/GAID) 도 제외
- **AgeGenderCategorizingImporter와 동일한 anonymization 목적**

**B. 코드에 age 자동 카테고리화 로직 내장**
- `data.userInfo_age`가 발견되면 자동으로 카테고리 컬럼(`userInfo_age_categorized`) 생성 후 원본 drop
- 지금 `-x`로 birth 제거 중인데, JSON에 age 필드가 실제로 있으면 이 로직도 트리거됨

**C. rewarded만 `--unique-keys postbackId`**
- 광고 postback은 네트워크 재시도로 중복 유입 가능 → dedup 필요
- campaign, tessera는 dedup 안 함

**D. backfill DAG 2개가 hourly로 running**
- `data_neptune_backfill_0429_etl_test2_for_khp_production` (매시간)
- `data_neptune_backfill_1865_create_trevi_report_hourly` (매시간 10분)
- 둘 다 `rewarded_json_to_parquet`만 실행 (data_8004의 rewarded와 동일 로직)
- **같은 시간대 데이터를 3번 처리하는 건지, 각각 다른 시간대인지 확인 필요**

**이관 옵션**

| 옵션 | 방식 | 장단 |
|---|---|---|
| **A. Dataproc lift** | Spark 앱 그대로 유지 | 로직 검증됨, 로그 수집 인프라 공유 · 재작성 부담 없음 |
| **B. BQ SQL로 이식** | GCS JSON → BQ external table → SQL로 exclude/dedup/카테고리화 | 인프라 단순 · UDF/전처리 재작성 필요 |

**옵션 A 권장** — 로그 수집이 Dataproc으로 갈 예정이라 인프라 공유 이점 큼.

**❓ 논의 필요**
- `page_trevi` 시스템 정체 (광고 리워드로 추정)
- **backfill DAG 2개 상태** — 진짜 backfill 중인지, 놓친 running인지, 언제까지 유지할지
- JSON 원본에 `data.userInfo_age` 필드 존재 여부 (age 카테고리화 코드 트리거 여부)
- PII exclude 목록의 policy owner (birth, ifa 외 추가 필요 컬럼)
- 소스 JSON 생성 파이프라인의 GCP 이관 방향 (`page_trevi/raw/json/` 원본)

### #7 UnifySchemaMerger 특수 케이스

**전체 2 태스크, 1 DAG** (`data_0910_merge_small_files`, daily 00:00). Tiara 로그 (사용자 행동 이벤트) 병합용. **`trigger_rule: all_failed`** — Airflow에서 upstream 태스크가 모두 실패했을 때만 실행되는 **fallback** 태스크.

**사용 옵션**
```
--filename-prefix {YYYYMMDD-HH}-merged
--file-size 90000000              (~90MB 타겟)
--src-dir /page_tiara_(non_)id/{phase}/raw/tiara/v1/collect_date={YYYYMMDD}
--dst-dirs /page_tiara_(non_)id/{phase}/unify_merged/tiara/v1/collect_date={YYYYMMDD}
--exit-empty-dir=true
--schema-table page_tiara_(non_)id_{phase}.tiara_(non_)identified_v1
--filename-suffix ''
```

Airflow 옵션: `"trigger_rule":"all_failed"` — **정상 흐름에선 실행 안 됨**

**두 태스크**
- `unify_merge_page_tiara_id_production` — 식별 사용자 로그
- `unify_merge_page_tiara_non_id_production` — 비식별 사용자 로그

**코드 동작 요약**
```
1. src-dir 파일들을 이름 접미어(index 11부터 substring)로 그룹핑
2. --schema-table의 Hive 스키마로 각 그룹을 재파싱 (default: parquet→JSON→parquet 라운드트립)
3. union
4. --file-size 기준으로 목적 파일 수 계산 (~90MB per file)
5. dst-dirs로 merge output (max 10개까지 복제 가능하지만 지금 1개만 사용)
```

**특이점**

**A. Fallback 태스크 — 정상 흐름에서는 안 돌아감**
- upstream (아마도 `DataFrameMerger`, `merge_and_move_dataframes.sh`)이 실패했을 때만 트리거
- 스키마 통일 병합으로 리커버리 시도
- **실제 실행 빈도가 극히 낮을 것** — active DAG 등록됐지만 조건부 실행

**B. 스키마 다형성 대응**
- Tiara 로그(사용자 행동 이벤트)에 여러 스키마 버전이 섞일 수 있음
- Hive 테이블의 스키마를 canonical로 삼아서 강제 병합
- 스키마에 없는 필드는 조용히 drop (코드 주석 CAUTION 있음)

**C. 성능 여지 있음**
- default가 `parquet → toJSON → JSON parse → parquet` 라운드트립 (무거움)
- `--reread-from-parquet` 옵션 있지만 사용 안 함 (fallback이라 최적화 우선순위 낮은 듯)

**D. 파일명 regex 취약**
- 파일명 index 11부터 substring해서 그룹핑 — Spark 기본 네이밍(`part-00001-...`)에 의존
- 컨벤션 바뀌면 깨짐

**E. upstream이 이관 제외 결정**
- upstream `DataFrameMerger` (`merge_and_move_dataframes.sh`)는 이미 이관 제외로 결정됨
- 즉 upstream이 사라지면 이 fallback도 존재 이유가 사라짐

**이관 옵션**

| 옵션 | 방식 | 장단 |
|---|---|---|
| **A. 폐기 (유력)** | upstream이 이관 제외라 fallback도 자연스레 사라짐 | 가장 단순 · upstream 대체 방안이 확정돼야 |
| **B. Dataproc lift** | 스키마 통일 로직 자체가 GCP에서도 필요하면 그대로 유지 | Spark 코드 재사용 · fallback 시나리오 재현 필요 · 아마 안 쓸 것 |

**폐기 유력** — upstream(`DataFrameMerger`)이 이관 제외로 결정된 시점에 이 fallback도 필요성이 사라짐. 다만 upstream 대체 방안이 확정되기 전까지는 존치.

**❓ 논의 필요**
- **실제 실행 빈도** — trigger_rule 특성상 얼마나 자주 트리거되는지 (거의 안 돌 가능성)
- Tiara 로그의 스키마 다형성 원인 (여러 소스인지, 시간축 변화인지, 이관 시에도 재발 가능성)
- `DataFrameMerger` 이관 제외 결정이 확정이면 → 이 앱도 자연 폐기
- GCP 이관 시 로그 데이터 스키마 안정성 (Datastream 랜딩 스키마 관리 방식)

### #8 TicketUseRecord 특수 케이스

**전체 1 태스크, 1 DAG** (`data_0203_dump_mysql_buydb_hourly`, hourly). buydb의 ticket use × ticket buy 도메인 조인 계산. **5-스텝 chain의 중간 스텝**이며, 소스가 Hudi + Neptune 두 아키텍처 결합.

**실행 커맨드 (buydb2 = NEW_DB)**
```
adhoc/run_ticket_use_record.sh
  -e production -v buydb2
  -d {YYYYMMDD}
  --from-timestamp "{data_interval_start} KST"
  --until-timestamp "{data_interval_end} KST"
  -o /page_buydb/production/tmp/ticket_use_record/{YYYYMMDD-HH}
```

**전체 DAG chain 흐름 (해당 앱 관련 부분)**
```
Sensor: buydb CDC 완료 대기 (16 샤드)
   ↓
Neptune ticket_buy_record ETL (4 태스크: Presto CTAS → merge → partition → cleanup)
   ↓
TicketUseRecord Spark 앱 (make_file) ← 우리가 보는 앱
   ↓ (tmp 출력)
merge_and_move → add_partition → cleanup
   ↓
kp_export_ticket_use_record ETL (4 태스크: 결과를 export 형태로 다시 가공)
```

**코드 동작**
1. **Ticket use history (Hudi, 16 샤드)**
   - 경로: `/page_buydb/production/raw/mysql/boracay_production/ticket_use_history/data/shard={01..16}/create_date={YYYYMMDD}/`
   - `spark.read.format("hudi")` + `created_dt` 시간 윈도우 필터
   - 컬럼 리네임 (`product_id → single_id`, `created_dt → use_dt`, `id → uid`)
   - **16 샤드 union**
2. **Ticket buy records (Neptune parquet)**
   - 경로: `/page_buydb/production/raw/neptune/ticket_buy_record`
   - `create_date <= targetDate` + `sale_type = 'S'` 필터
3. **`INNER JOIN ON ticket_uid`** — buy records를 broadcast join
4. `repartition(1)` → parquet overwrite → tmp 디렉토리

**특이점**

**A. Hudi + Neptune 두 아키텍처 결합**
- ticket_use_history는 buydb CDC 결과 Hudi 원천 (샤드 구조)
- ticket_buy_record는 Neptune snapshot parquet
- **한 앱에서 두 소스 계보 결합**

**B. 16 샤드 union**
- for loop로 각 샤드 read → reduce union
- Datastream으로 buydb 랜딩되면 이 union 자체가 불필요해질 수 있음

**C. OLD_DB (buydb1) 코드 잔존 — dead code**
- 8 샤드, `t_ticket_sales`, `pid → single_id`, `create_dt`
- 실사용은 buydb2뿐. 이관 시 정리 가능

**D. Jira 3회 개정**
- DD-3177, DD-3991, DD-5373 — 3번 수정된 복잡한 도메인 로직

**E. broadcast join 기본 활성**
- buy records를 broadcast (상대적으로 작음)
- `--disable-broadcast-join` 옵션 있지만 안 씀

**F. tmp 출력 + downstream chain**
- 이 앱은 최종 산출물이 아니라 중간 계산
- 후속 `merge_and_move_dataframes.sh` (이관 제외 결정된 앱) → `add_partition` → tmp cleanup → hive 테이블 완성
- 다시 `kp_export_ticket_use_record` ETL이 이 결과 소비

**G. `boracay_production` — 정체 불명**
- 경로에 등장하는 사내 시스템 이름 추정
- ❓ 확인 필요

**이관 결정: 앱 폐기 + BQ SQL 재구현**

- 로직 자체가 `union + filter + broadcast join` 단순 조합 → BQ SQL로 매우 자연스럽게 표현 가능
- Datastream이 buydb → BQ 랜딩하면 **16 샤드 union도 자연 해소** (BQ landing이 단일 테이블)
- Neptune `ticket_buy_record`도 BQ view/scheduled query로 대체되면 소스 두 개 모두 BQ에 있음
- **chain 전체가 어차피 재설계 대상** — `merge_and_move`가 이관 제외 결정된 시점에 5-스텝 chain 뒷부분이 사라지므로 앱만 lift하는 건 의미 없음
- downstream `kp_export_ticket_use_record` 도 함께 BQ SQL로 통합 재구현하는 게 자연스러움

**❓ 논의 필요**
- **buydb CDC 이관 방향** — Datastream으로 BQ? 아니면 Hudi 유지? 이 결정이 앱 존폐 결정
- Neptune `ticket_buy_record` 이관 계획 (다른 Neptune 소스들과 함께)
- **downstream `kp_export_ticket_use_record` ETL** — 이건 별도 export 파이프라인. 함께 이관 논의 필요
- OLD_DB(buydb1) dead code 정리 가능 여부
- `boracay`가 뭔지 (사내 시스템?)
- broadcast join 조건 재검증 (buy records 크기)
- NEW/OLD DB 시간 컬럼 차이 (`created_dt` vs `create_dt`) 이관 후 표준화 필요

### #9 HdfsGarbageCollector 특수 케이스

**전체 5 태스크, 1 DAG** (`data_0900_clean_hive_metastore`, daily 00:00). Mongo/MySql Importer가 만든 timestamp suffix 디렉토리(`_1783299600/` 같은)를 7일 TTL로 정리.

**사용 패턴**

| 태스크 | 대상 경로 | exclude prefix |
|---|---|---|
| `delete_hdfs_dirs_in_mongo_stat` | `/page_contentdb/{phase}/raw/mongo/stat` | 없음 |
| `delete_hdfs_dirs_in_page_billing` | `/page_billing/{phase}/raw/mysql/billing` | 없음 |
| `delete_hdfs_dirs_in_page_service` | `/page_service/{phase}/raw/mysql/service` | `t_series_product,t_single_product` |
| `delete_hdfs_dirs_in_page_user` | `/page_user/{phase}/raw/mysql/user` | `t_publisher,t_user` |
| `delete_hdfs_dirs_in_page_userpublic` | `/page_userpublic/{phase}/raw/mysql/userpublic` | 없음 |

**코드 동작**
```
1. basePath 하위 디렉토리를 listStatus로 훑음
2. exclude-dir-prefix 매치되면 skip
3. regex [\w\d_]*_([\d]{10}) — 디렉토리 이름 끝에 언더스코어 + 10자리 timestamp
4. 그 timestamp가 (현재 - 7일) 이전이면 DELETE
5. 아니면 KEEP
```

**특이점**

**A. 파일 mtime 아니라 이름의 timestamp 기반**
- Mongo/MySql Importer의 `--output-dir-timestamp` 옵션과 짝
- dump 앱들의 raw 출력 디렉토리 정리 목적

**B. exclude prefix — Neptune 소스 테이블 보호**
- `t_series_product`, `t_publisher` 등은 **ChangeApplier가 소스로 사용하는 Neptune snapshot 대상 테이블**
- 삭제하면 downstream 파이프라인 깨짐

**C. Spark job인데 실제 병렬 처리 안 함**
- 코드 주석: "SparkHadoopUtil을 쓰기 때문에 spark-submit 안에서 돌려야 함"
- HDFS FileSystem 접근을 위한 Kerberos/설정 획득 목적
- deploy-mode `client` (다른 앱들은 cluster)

**D. 대상은 결국 dump 앱들의 raw 출력**
- 5개 경로 전부 `raw/mysql/*` 또는 `raw/mongo/*`
- Mongo(#1), MySql(#2) Importer의 출력물

**이관 옵션**

| 옵션 | 방식 | 장단 |
|---|---|---|
| **A. 폐기 (유력)** | dump 앱들(#1, #2) 이관 완료 시 timestamp suffix 디렉토리 구조 자체가 사라짐 | 가장 단순 · dump 앱 이관 시점에 자연 소멸 |
| **B. GCS Object Lifecycle** | 만약 GCS에도 timestamp suffix 구조로 남으면 lifecycle rule로 대체 | 무료, 서버리스 · 파일명 timestamp vs mtime 시맨틱 차이 |
| **C. Cloud Scheduler + Cloud Run/Functions** | 파일명 기반 GC 로직 그대로 유지 | 현재 시맨틱 보존 · 인프라 부담 |

**폐기 유력**. 다만 dump 앱들이 이관 완료되기 전까지는 유지 필요.

**❓ 논의 필요**
- **exclude prefix 재확인**: `t_series_product`, `t_publisher` 등이 왜 예외인지 (Neptune snapshot 소스라서 추정) → 이관 후 downstream 재설계에 따라 필요성 달라짐
- Datastream 이관 후 GCS에 timestamp suffix 디렉토리 구조가 남는지 (남지 않을 것으로 예상)
- 재업로드/backfill 시나리오 유무 (있으면 GCS Lifecycle mtime 기준으로는 대응 불가)
- 이관 완료 전까지 앱 존치

---

## 앱별 사용 DAG · 태스크 · 스케줄 (검증용)

| # | Spark 앱 | DAG | 태스크 수 | Cron | 주기 |
|---:|---|---|---:|---|---|
| 1 | MongoDataFrameImporter | `data_0102_dump_mongo_stat_daily` | 36 | `0 0 * * *` | 매일 00:00 |
| 1 | MongoDataFrameImporter | `data_0200_dump_hourly` | 1 | `0 * * * *` | 매시간 |
| 2 | MySqlDataFrameImporter | `data_0004_dump_mysql_userinven_daily` | 8 | `0 18 * * *` | 매일 18:00 |
| 2 | MySqlDataFrameImporter | `data_0007_dump_mysql_page_userpublic_daily` | 1 | `0 0 * * *` | 매일 00:00 |
| 3 | AgeGenderCategorizingImporter | `data_2000_categorize_age_gender` | 1 | `0 0 * * 1` | **매주 월요일 00:00** ⚠️ |
| 4 | MySqlDataFrameExporter | `data_8007_kakaowebtoon_settlement_daily` | 3 | `0 0 * * *` | 매일 00:00 |
| 4 | MySqlDataFrameExporter | `data_8010_common_dw_kor_daily` | 5 | `0 0 * * *` | 매일 00:00 |
| 5 | MySqlDataFrameChangeApplier | `data_0008_dump_mysql_page_service_daily` | 1 | `0 0 * * *` | 매일 00:00 |
| 5 | MySqlDataFrameChangeApplier | `data_0200_dump_hourly` | 4 | `0 * * * *` | 매시간 |
| 6 | DataFrameTransformer (trevi) | `data_8004_trevi_data_hourly` | 3 | `10 * * * *` | 매시간 (10분) |
| 6 | DataFrameTransformer (trevi) | `data_neptune_backfill_0429_etl_test2_for_khp_production` | 1 | `0 * * * *` | 매시간 |
| 6 | DataFrameTransformer (trevi) | `data_neptune_backfill_1865_create_trevi_report_hourly` | 1 | `10 * * * *` | 매시간 (10분) |
| 7 | UnifySchemaMerger | `data_0910_merge_small_files` | 2 | `0 0 * * *` | 매일 00:00 |
| 8 | TicketUseRecord | `data_0203_dump_mysql_buydb_hourly` | 1 | `0 * * * *` | 매시간 |
| 9 | HdfsGarbageCollector | `data_0900_clean_hive_metastore` | 5 | `0 0 * * *` | 매일 00:00 |

**스케줄 관련 관찰**
- **#3 AgeGenderCategorizingImporter는 주 1회 (월요일)** — 배치 부하가 daily 대비 1/7. 소비처가 주간 리포트일 가능성.
- **#1 Mongo `data_0200_dump_hourly` (매시간)** — 앞선 특수 케이스의 open_log 증분 로드. daily 스냅샷 대비 21배 자주 도는 케이스.
- **#2 MySql userinven는 유일하게 18:00 스타트** — 다른 daily는 자정. 소스 DB 부하 회피 or 오프피크 활용으로 추정.
- **#5, #8 매시간 `data_0200_dump_hourly` DAG가 허브 역할** — 여러 앱(Mongo, ChangeApplier)을 함께 실행. 이관 시 이 DAG 재설계가 여러 앱에 영향.
- **#6 backfill DAG 2개가 여전히 매시간 running** — 실제 backfill 중인지, running 상태로 방치 중인지 확인 필요.

집계 쿼리 (앱별 DAG · 스케줄 매핑):

```sql
SELECT
  CASE
    WHEN a.kwargs LIKE '%run_mongo_dump.sh%' THEN 'MongoDataFrameImporter'
    WHEN a.kwargs LIKE '%run_mysql_dump.sh%' OR a.kwargs LIKE '%run_mysql_dump_ex5.sh%' THEN 'MySqlDataFrameImporter'
    WHEN a.kwargs LIKE '%run_mysql_export.sh%' THEN 'MySqlDataFrameExporter'
    WHEN a.kwargs LIKE '%run_transformer_trevi.sh%' THEN 'DataFrameTransformer(trevi)'
    WHEN a.kwargs LIKE '%unify_schema_merger.sh%' THEN 'UnifySchemaMerger'
    WHEN a.kwargs LIKE '%run_ticket_use_record.sh%' THEN 'TicketUseRecord'
    WHEN a.kwargs LIKE '%HdfsGarbageCollector%' THEN 'HdfsGarbageCollector'
    WHEN a.kwargs LIKE '%MySqlDataFrameChangeApplier%' THEN 'MySqlDataFrameChangeApplier'
    WHEN a.kwargs LIKE '%AgeGenderCategorizingImporter%' THEN 'AgeGenderCategorizingImporter'
  END AS spark_app,
  d.dag_id,
  w.schedule_interval AS cron,
  d.timetable_description AS schedule_desc,
  COUNT(*) AS task_count
FROM actions_prod a
JOIN workflows_prod w ON a.workflow_uid = w.uid
JOIN dag_prod d ON d.dag_id = w.name
WHERE a.operator_class = 'BashOperator'
  AND a.hidden = 0
  AND d.is_paused = 0
  AND d.is_active = 1
  AND ( a.kwargs LIKE '%run_mongo_dump.sh%'
     OR a.kwargs LIKE '%run_mysql_dump.sh%'
     OR a.kwargs LIKE '%run_mysql_dump_ex5.sh%'
     OR a.kwargs LIKE '%run_mysql_export.sh%'
     OR a.kwargs LIKE '%run_transformer_trevi.sh%'
     OR a.kwargs LIKE '%unify_schema_merger.sh%'
     OR a.kwargs LIKE '%run_ticket_use_record.sh%'
     OR a.kwargs LIKE '%HdfsGarbageCollector%'
     OR a.kwargs LIKE '%MySqlDataFrameChangeApplier%'
     OR a.kwargs LIKE '%AgeGenderCategorizingImporter%' )
GROUP BY spark_app, d.dag_id, w.schedule_interval, d.timetable_description
ORDER BY spark_app, d.dag_id;
```

전체 태스크 상세는 `/tmp/spark_app_usage.tsv` 참고 (동일 쿼리에서 `a.name` 포함).

## 비-Spark (HDFS 셸 유틸)

Spark 앱 아님. 이관 시 별도 처리 필요.

- `replace_merged_dir.sh` — HDFS 디렉토리 스왑 (`_MERGED` 마커 기반). GCS에서는 원자적 rename이 지원되지 않아 재설계 필요.
- `replace_org_dir.sh` — HDFS 디렉토리 백업 후 교체. 동일 이슈.

## 집계 기준

- `actions_prod.operator_class = 'BashOperator'`
- `actions_prod.hidden = 0`
- `actions_prod.kwargs`(JSON) 안에 `{{ var.value.spark_apps_bin }}/...sh` 참조가 있는 것
- `workflows_prod.name = dag_prod.dag_id` 로 조인해서 active DAG로 필터
- 조회일: 2026-07-03

집계 쿼리:

```sql
SELECT a.kwargs
FROM actions_prod a
JOIN workflows_prod w ON a.workflow_uid = w.uid
JOIN dag_prod d ON d.dag_id = w.name
WHERE a.operator_class = 'BashOperator'
  AND a.hidden = 0
  AND d.is_paused = 0
  AND d.is_active = 1
  AND a.kwargs LIKE '%spark_apps_bin%';
```

`run.sh`는 CLASS_NAME이 첫 인자로 파라미터화돼 있어서, 실제로 어떤 클래스가 지정됐는지는 kwargs 파싱으로 확인함.

## 참고: bin/ 에 있지만 active DAG에서 호출되지 않는 스크립트

- Generic: `run_via_proxy.sh`, `run_column_scanner.sh`, `run_sync_datahub.sh`
- imports: `run_json_dump.sh`, `run_push_recipients_importer.sh`
- exports: `run_kafka_export.sh`, `run_loupe_kafka_export.sh`, `run_mongo_export.sh`
- merge: `merge_and_move_dataframes_t_user_single_product.sh`
- transform: `run_open_log_non_id_backfill.sh`
- script/adhoc: `create_table_from_dataframe.sh`, `adhoc/page_openlog_migrator.sh`, `adhoc/page_view_history_migrator.sh`, `adhoc/farmsite_information_long_text.sh`
- SQL: `run_presto_sql.sh`, `run_impala_sql.sh` (전체 hidden=0 통계에는 잡히나 active DAG 조인 후엔 안 나옴 — paused/inactive DAG에서만 참조 중)
- 기타: `hdfs_copyToLocal.sh`, `hdfs_is_empty.sh`, `hdfs_rename_files.sh`, `env.sh`, `run_info.sh`, `run_jenkins_job.py`