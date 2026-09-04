---
title: "TargetGroupImporter — 앱 상세"
status: draft
tags:
  - spark-apps
  - gcp이관
  - hudi
  - kage
created: 2026-09-01
updated: 2026-09-01
---

# TargetGroupImporter — 앱 상세

> `com.kakaopage.spark.app.imports.kage.TargetGroupImporter` · 실행 스크립트 `bin/run_kage_target_group_importer.sh`
> 근거: 프로덕션 `actions` + `action_dependencies` + `task_instance`(90일) + 코드.
> 관련: [[11_KagePushTargetImporter]] (**같은 `KageFileLoader` 프레임워크**) · [[7_PushTargetUserImporter]]

## 한 줄

**타겟 그룹**(`t_target_group`)에 연결된 Kage CSV 파일을 받아 `target_group_uid × user_uid` 로 펼쳐 적재하는 앱.
현행 **1 태스크 / 1 DAG**, 매시간, 90일간 **1,440회 success**.

[[11_KagePushTargetImporter]] 와 **같은 프레임워크·같은 DAG** 지만 대상 도메인이 다르다
(푸시 발송 대상 vs 타겟 그룹 멤버).

## 1. 실행 형태

```bash
run_kage_target_group_importer.sh -e production \
  -w 'kage_upload_dt >= "{{ execution_date }}"
      AND kage_upload_dt <  "{{ next_execution_date }}"
      AND csv_kage_id != ""'
```
→ 내부적으로 `run_hudi.sh ... TargetGroupImporter **client** 3 4g 1`

| 항목 | 값 |
|---|---|
| DAG | `data_3200_kakaopage_analysis_hourly` (형제 앱과 **동일 DAG**) |
| cron | `0 * * * *` — 매시간 |
| action uid | 9581 (`collect_target_group_list_hourly`) |
| pool | `default_pool` |
| deploy-mode | **`client`** |
| executor | 3 × 4g × 1core |
| 90일 실행 | 1,440회 success |
| **의존성** | **upstream/downstream 모두 없음** |

`-e production` **하드코딩** (형제 앱과 동일하게 `{{ var.value.phase }}` 미사용).

## 2. 연결된 대상

### 읽기 ①  Hudi — 타겟 그룹 메타

```
/page_contentdb/{env}/raw/mysql/boracay_{env}/t_target_group/data
```
→ temp view `t_target_group` 등록 후

```sql
SELECT uid AS target_group_uid, csv_kage_id
FROM t_target_group
WHERE kage_upload_dt >= '{from}' AND kage_upload_dt < '{until}' AND csv_kage_id != ''
```

> **소스 DB 가 형제 앱과 다르다** — 이 앱은 `page_contentdb`, 형제 앱은 `page_service`.
> 경로는 둘 다 `boracay_{env}` 로 정상 치환된다.

### 읽기 ②  Kage edge server — UDF 안에서 HTTP GET

```scala
val url = s"$edgeServerUrl?kid=$kageId"      // kageId = csv_kage_id 컬럼값
Source.fromURL(url).getLines().toArray
```
edge 서버 주소는 설정 파일 `kakaopage/{env}/application-kage` 의 `edge.url` 에서 온다.

### 쓰기

| 저장소 | 대상 |
|---|---|
| HDFS | `/page_service/{env}/modeled/target_group` — parquet, `partitionBy(target_group_uid)`, `SaveMode.Overwrite` (dynamic) |
| Hive | `page_service_{env}.target_group_list` ← `addPartitionsSilently` |

출력 컬럼은 `target_group_uid`, `user_uid` **2개뿐**이다.

> 소스는 `page_contentdb` 인데 **출력은 `page_service` 로 간다.** 도메인 경계를 넘는다. ❓

## 3. 형제 앱과의 차이 — 프레임워크는 같지만 세부가 다르다

| | [[11_KagePushTargetImporter]] | **TargetGroupImporter** |
|---|---|---|
| 소스 Hudi | `/page_service/…/t_push_group/data` | **`/page_contentdb/…/t_target_group/data`** |
| kage id 컬럼 | `target_file` | **`csv_kage_id`** |
| 필터 | `target_type = 'FILE'` (쿼리에 고정) | **`csv_kage_id != ''`** (`-w` 인자로) |
| 시간 컬럼 | `send_start_dt` | **`kage_upload_dt`** |
| 파일 파싱 | `split(',')` → 첫 컬럼 | **캐스팅만** — 파일이 user_uid 한 줄씩 |
| 헤더 제거 | `.filter(!contains("user_uid"))` | **없음** ⚠️ |
| `data_type` 컬럼 | `'target'` 추가 | **없음** |
| 파티션 | `push_group_uid`, `data_type` (2단) | **`target_group_uid` (1단)** |
| 출력 테이블 | `page_service_{env}.push_user_v2` | `page_service_{env}.target_group_list` |

### ⚠️ 헤더 처리가 없다

```scala
// KagePushTargetImporter — 헤더 줄 제거
df.filter(!col(KAGE_COL).contains(USER_UID))
  .withColumn(SPLIT_COL, split(col(KAGE_COL), ","))
  .withColumn(USER_UID, col(SPLIT_COL).getItem(0).cast(LongType))

// TargetGroupImporter — 바로 캐스팅
df.withColumn(USER_UID, col(KAGE_COL).cast(LongType))
  .drop(KAGE_COL, KAGE_ID_COL)
```

Kage 파일에 헤더 줄(`user_uid`)이 있으면 `cast(LongType)` 이 **null 을 만들고 그대로 적재**된다.
형제 앱은 걸러내는데 여기는 안 거른다.

- 파일 형식이 서로 다르다면(형제=CSV 다중 컬럼, 여기=단일 컬럼 목록) 정상일 수 있다
- 같은 형식이라면 **null row 가 섞이고 있을 가능성**이 있다

> ❓ Kage 파일 형식이 두 앱에서 서로 다른지, 실제로 null 이 들어가는지 확인 필요.

## 4. 소비처가 athlon 밖이다

- `target_group_list` 를 **참조하는 athlon 액션이 0건**이다
- 이 태스크는 **upstream/downstream 의존성도 없다** — DAG 안에서 완전히 독립 실행

즉 만들어두기만 하고 athlon 파이프라인은 쓰지 않는다.
CRM/타겟팅 시스템이나 애드혹 분석에서 직접 쿼리하는 것으로 보인다.

> ❓ `page_service_{env}.target_group_list` 소비처 확인 필요.
> (형제 앱의 `push_user_v2` 도 동일한 상황이다)

## 5. 이관 관점

### 난점 — 형제 앱과 동일

| # | 항목 | 비고 |
|---|---|---|
| ① | **Spark UDF 안에서 Kage HTTP GET** | executor egress. 재시도·타임아웃 없음 |
| ② | **Kage 는 카카오 서비스 — 이관 대상 아님** | **GCP → 사내망 연결(Interconnect/VPN) 가능 여부에 종속.** `[[2_Cloud Composer vs Self-managed 비교]]` 미해결 질문 #2 |
| ③ | 소스가 Hudi (`t_target_group`) | Datastream + BQ 전환에 종속 |
| ④ | 파티션 `target_group_uid` 고카디널리티 | BQ 파티션 4,000 제한 → **클러스터링 전환** 필요 |

④ 는 형제 앱보다는 나을 수 있다 — 파티션이 1단(`target_group_uid`)이고,
타겟 그룹 수가 푸시 그룹 수(~38,700)보다 적을 가능성이 있다. ❓ 실측 필요.

### 옵션

| 옵션 | 방식 | 평가 |
|---|---|---|
| **A. Composer + Python** | Kage HTTP fetch → user_uid 파싱 → BQ 적재 | **유력.** 연산이 "파일 받아 숫자로 캐스팅"뿐. `client 3 4g` 규모라 작다 |
| B. Dataproc lift | Spark 그대로 | 변경 최소 |
| C. BQ SQL | — | **불가.** 외부 HTTP fetch 불가 |

> **kage 2종 + push 1종을 묶어서 설계하는 편이 낫다.**
> [[11_KagePushTargetImporter]] · [[7_PushTargetUserImporter]] 와 이 앱은
> "메타 테이블에서 파일 포인터 읽기 → 외부에서 파일 받기 → user_uid 전개 → 적재"
> 라는 **동일 패턴**이다. 다른 건 소스 테이블·파일 획득 방법·출력 테이블뿐이다.
> GCP 에서 공통 컴포넌트 하나로 만들고 설정만 달리하는 방식을 검토할 만하다.

## 6. ❓ 논의 필요

- **`page_service_{env}.target_group_list` 소비처** — athlon 안에 없다 (§4)
- Kage 파일 **형식이 형제 앱과 다른지** / 헤더 null 유입 여부 (§3)
- 소스가 `page_contentdb` 인데 **출력이 `page_service` 인 이유** (§2)
- `target_group_uid` **카디널리티** — BQ 파티션/클러스터링 설계 근거 (§5-④)
- `deploy-mode client` 인 이유 (형제 앱과 동일 항목)
- **kage 2종 + push 1종 통합 가능성** (§5)
- GCP → 사내망 Kage 연결 가능 여부 → **인프라 공통 과제**

## 재현

```sql
-- 현행 액션 (uid 9581)
SELECT a.uid, w.name dag, w.schedule_interval, a.name, a.pool, a.kwargs
FROM actions a
  JOIN workflows w ON w.uid = a.workflow_uid
  JOIN dag d ON d.dag_id = w.name
WHERE a.hidden = 0
  AND d.is_paused = 0 AND d.is_active = 1 AND d.next_dagrun IS NOT NULL
  AND a.kwargs LIKE '%run_kage_target_group_importer%';

-- 소비처 확인 (0건)
SELECT a.uid, w.name dag, a.name FROM actions a
  JOIN workflows w ON w.uid = a.workflow_uid
WHERE a.kwargs LIKE '%target\_group\_list%';

-- 의존성 (0건)
SELECT 'up' dir, up.uid, up.name FROM action_dependencies d
  JOIN actions up ON up.uid = d.upstream_action_uid WHERE d.action_uid = 9581
UNION ALL
SELECT 'down', dn.uid, dn.name FROM action_dependencies d
  JOIN actions dn ON dn.uid = d.action_uid WHERE d.upstream_action_uid = 9581;
```

코드: `TargetGroupImporter.scala` — 변환(13~16행), 설정 정의(59~81행).
공통 로직은 `KageFileLoader.scala` — Kage HTTP fetch UDF(37~50행), Hudi read(52~54행),
write + `addPartitionsSilently`(80~90행).
