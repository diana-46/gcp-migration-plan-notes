---
title: "KagePushTargetImporter — 앱 상세"
status: draft
tags:
  - spark-apps
  - gcp이관
  - hudi
  - push
  - kage
created: 2026-09-01
updated: 2026-09-01
---

# KagePushTargetImporter — 앱 상세

> `com.kakaopage.spark.app.imports.kage.KagePushTargetImporter` · 실행 스크립트 `bin/run_kage_push_target_importer.sh`
> 근거: 프로덕션 `actions` + `action_dependencies` + `task_instance`(90일) + 코드.
> 관련: [[7_PushTargetUserImporter]] (**형제 앱 — 같은 테이블을 `target_type` 으로 나눠 처리**)

## 한 줄

푸시 발송 그룹 중 **`target_type='FILE'`** 인 건에 대해, Kage 파일 서버에서 대상 유저 목록을 받아 적재하는 앱.
현행 **1 태스크 / 1 DAG**, 매시간, 90일간 **1,440회 success**.

[[7_PushTargetUserImporter]] 와 **같은 일을 하되 담당 `target_type` 만 다르다.**

## 1. 실행 형태

```bash
run_kage_push_target_importer.sh -e production \
  -w 'send_start_dt >= "{{ execution_date }}" and send_start_dt < "{{ next_execution_date }}"'
```
→ 내부적으로 `run_hudi.sh ... KagePushTargetImporter **client** 3 4g 1`

| 항목 | 값 |
|---|---|
| DAG | `data_3200_kakaopage_analysis_hourly` |
| cron | `0 * * * *` — **매시간** |
| action uid | 9582 (`collect_push_target_hourly`) |
| pool | `default_pool` |
| deploy-mode | **`client`** (다른 앱들은 대부분 `cluster`) |
| executor | 3 × 4g × 1core |
| 90일 실행 | 1,440회 success |
| **의존성** | **upstream/downstream 모두 없음** — 독립 실행 |

> `-e production` 이 **하드코딩**돼 있다. `{{ var.value.phase }}` 를 쓰지 않는다.

## 2. 형제 앱과의 분담 — `target_type` 으로 배타적으로 나뉜다

`t_push_group` 한 테이블을 두 앱이 나눠 읽는다.

| 앱 | 담당 `target_type` | 파일 획득 방법 | 주기 | DAG |
|---|---|---|---|---|
| [[7_PushTargetUserImporter]] | `URL`, `ATHLON` | HTTP GET / HDFS(→GCS) | 일 1회 | `data_2001_import_push_users` |
| **이 앱** | **`FILE`** | **Kage edge server HTTP** | **매시간** | `data_3200_kakaopage_analysis_hourly` |

```scala
// KagePushTargetImporter
WHERE target_type = 'FILE' AND {where}

// PushTargetUserImporter
WHERE send_status = 'SUCCESS' AND target_type IN ('URL','ATHLON') AND {where}
```

코드 주석에도 명시돼 있다 — `// NOTE: FILE 타입은 KagePushTargetImporter에서 처리`

> ⚠️ **이 앱은 `send_status` 를 안 본다.** 형제 앱은 `send_status='SUCCESS'` 로 거르는데
> 여기는 조건이 없다. 발송 실패/진행중 건도 대상 목록을 수집한다는 뜻이다. ❓ 의도인지 확인 필요.

## 3. 연결된 대상

### 읽기 ①  Hudi — 푸시 그룹 메타

```
/page_service/{env}/raw/mysql/boracay_{env}/t_push_group/data
```
→ temp view `t_push_group` 등록 후

```sql
SELECT uid AS push_group_uid, target_file
FROM t_push_group
WHERE target_type = 'FILE' AND {where}
```

형제 앱과 달리 **경로가 `boracay_{env}` 로 제대로 치환**된다
([[7_PushTargetUserImporter]] 는 `boracay_production` 하드코딩).

### 읽기 ②  Kage edge server — UDF 안에서 HTTP GET

```scala
val conf = ConfigFactory.load(s"kakaopage/$env/application-kage")
val edgeServerUrl = conf.getString("edge.url")

(kageId: String) => {
  val url = s"$edgeServerUrl?kid=$kageId"
  Source.fromURL(url).getLines().toArray
}
```

- `target_file` 컬럼값이 **kage id(`kid`)** 다. 테스트 샘플 기준 `sZcJp/ZSd535JCJV/qr4XjyDth093zzVckqohJ1`
  형태의 3단 슬래시 토큰이다.
- edge 서버 주소는 **설정 파일**(`kakaopage/{env}/application-kage`)의 `edge.url` 에서 온다.
  액션 kwargs 에는 안 나온다.
- 형제 앱과 마찬가지로 **Spark executor 에서 외부 HTTP 호출**이 발생한다.

### 변환

```scala
df.filter(!col(KAGE_COL).contains(USER_UID))        // 헤더 줄 제거
  .withColumn(SPLIT_COL, split(col(KAGE_COL), ","))
  .withColumn(USER_UID, col(SPLIT_COL).getItem(0).cast(LongType))
  .withColumn(DATA_TYPE, lit("target"))
  .drop(KAGE_COL, KAGE_ID_COL, SPLIT_COL)
  .repartition(col("push_group_uid"), col("data_type"))
```

> 헤더 제거 방식이 형제 앱과 다르다. 형제 앱은 `getLines().drop(1)` 로 첫 줄을 버리는데,
> 여기는 **`user_uid` 문자열이 포함된 줄을 필터**한다. 데이터에 `user_uid` 가 값으로 들어가면 오작동한다.

### 쓰기

| 저장소 | 대상 |
|---|---|
| HDFS | `/page_service/{env}/modeled/push_v2` — parquet, `partitionBy(push_group_uid, data_type)`, **`SaveMode.Overwrite` + `partitionOverwriteMode=dynamic`** |
| Hive | `page_service_{env}.push_user_v2` ← `addPartitionsSilently` 로 파티션 등록 |

출력 컬럼은 `push_group_uid`, `user_uid`, `data_type='target'` — 형제 앱과 **완전히 동일**하다.

## 4. ⚠️ 같은 파티션을 형제 앱과 공유한다

`/page_service/{env}/modeled/push_v2` 의 `data_type=target` 파티션을 **두 앱이 함께 쓴다.**

| 앱 | `data_type` | 쓰기 모드 | 주기 |
|---|---|---|---|
| **이 앱** | `target` | **`Overwrite` (dynamic partition)** | 시간별 |
| [[7_PushTargetUserImporter]] | `target` | `append` | 일 1회 |
| `kafka.PageKappusPushLogStreamingApp` | `recipient` | 스트리밍 append | 상주 |

`target_type` 이 배타적(`FILE` vs `URL`/`ATHLON`)이라 **같은 `push_group_uid` 를 다룰 일은 없어 보인다.**
다만 이 앱이 dynamic overwrite 라, 만에 하나 겹치면 형제 앱의 append 결과를 **덮어쓴다.**

> ❓ 두 앱이 같은 파티션 공간을 쓰는 것이 의도된 설계인지 확인 필요.
> 그리고 파티션 구조 자체가 BQ 로 그대로 못 간다 — [[7_PushTargetUserImporter]] §5-④ 참고
> (`push_group_uid` 파티션 ~38,700개, BQ 는 4,000 제한 → 클러스터링 전환 필요).

## 5. 이관 관점

### 난점

| # | 항목 | 비고 |
|---|---|---|
| ① | **UDF 안에서 Kage HTTP GET** | 재시도·타임아웃 처리 없음 |
| ② | **Kage 는 카카오 서비스 — 이관 대상 아님** | **GCP → 사내망 연결(Interconnect/VPN) 가능 여부에 종속.** `[[2_Cloud Composer vs Self-managed 비교]]` 미해결 질문 #2 |
| ③ | 소스가 Hudi (`t_push_group`) | Datastream + BQ 전환에 종속 |
| ④ | 파티션 `push_group_uid` 고카디널리티 | BQ 4,000 제한 → 클러스터링 |

형제 앱([[7_PushTargetUserImporter]])의 `ATHLON` 경로는 athlon 산출물이 GCS 로 가면서 해소됐지만,
**Kage 는 카카오(엔터 아님) 서비스라 그대로 남는다.** 우리가 옮기는 게 아니라 **닿을 수 있는지**가 문제다.

> **egress 주체는 실행 방식에 따라 다르다.**
> 지금은 Airflow 가 `spark-submit` 을 트리거만 하고 **실제 HTTP 호출은 Spark executor** 에서 일어난다.
> GCP 에서는:
>
> | 옵션 | fetch 실행 주체 | 사내망 egress 필요 대상 |
> |---|---|---|
> | Dataproc lift | Dataproc worker | **Dataproc** |
> | Composer + Python | Composer worker | **Composer** |
>
> 따라서 인프라에 물을 때는 "GCP 에서 사내망 되나요"가 아니라
> **"어느 컴포넌트에서 사내망에 닿을 수 있나요"** 로 물어야 한다.

### 옵션

| 옵션 | 방식 | 평가 |
|---|---|---|
| **A. Composer + Python** | Kage HTTP fetch·CSV 파싱을 Python 으로, 결과만 BQ 적재 | **유력.** 연산이 "파일 받아 첫 컬럼 뽑기"뿐. 이미 `client` 모드 3 executor 라 규모가 작아 보인다 |
| B. Dataproc lift | Spark 그대로 | 변경 최소 |
| C. BQ SQL | — | **불가.** 외부 HTTP fetch 불가 |

**형제 앱과 함께 설계해야 한다.** 둘이 같은 테이블을 읽고 같은 파티션에 쓰므로,
따로 이관하면 파티션 구조·쓰기 모드가 어긋난다.

> **통합 검토 여지** — 두 앱이 하는 일이 사실상 같다
> (푸시 그룹 → 대상 파일 fetch → user_uid 전개 → push_v2 적재).
> 다른 건 `target_type` 과 파일 획득 방법뿐이다.
> GCP 에서는 **한 파이프라인으로 합치고 `target_type` 분기만 두는 편**이 단순할 수 있다.

## 6. 코드 특이점

- **`deploy-mode client`** — 다른 배치 앱들은 `cluster` 인데 이것만 client 다.
  드라이버가 게이트웨이에서 돌기 때문에 UDF 의 HTTP 호출 경로가 다르다. ❓ 의도 확인 필요
- **`-e production` 하드코딩** — 액션이 `{{ var.value.phase }}` 를 안 쓴다
- **`addPartitionsSilently`** 로 Hive 파티션을 등록한다. MSCK REPAIR 이 아니라 명시적 등록이다
- **upstream/downstream 의존성이 없다** — DAG 안에서 독립 실행된다.
  형제 앱은 `data_2001` 에서 후속 MSCK 가 붙어 있는데 여기는 없다

## 7. ❓ 논의 필요

- **GCP → 사내망 Kage 연결** — Kage 는 이관 대상이 아니므로 "닿을 수 있는지"가 문제.
  **어느 컴포넌트(Dataproc / Composer)에서 닿는지**까지 물어야 한다 (§5)
- **`send_status` 를 안 거르는 것이 의도인지** (§2) — 형제 앱은 `SUCCESS` 만 본다
- 형제 앱과 **같은 파티션 공간을 공유하는 것이 의도인지** (§4)
- **두 앱을 하나로 합칠 수 있는지** (§5)
- `deploy-mode client` 인 이유 (§6)
- 시간별 처리 규모 — Composer+Python(A) vs Dataproc(B) 판단 근거
- `push_user_v2` 최종 소비처 (형제 앱 문서와 공통 항목)

## 재현

```sql
-- 현행 액션 (uid 9582)
SELECT a.uid, w.name dag, w.schedule_interval, a.name, a.pool, a.kwargs
FROM actions a
  JOIN workflows w ON w.uid = a.workflow_uid
  JOIN dag d ON d.dag_id = w.name
WHERE a.hidden = 0
  AND d.is_paused = 0 AND d.is_active = 1 AND d.next_dagrun IS NOT NULL
  AND a.kwargs LIKE '%run_kage_push_target_importer%';

-- 의존성 (없음을 확인)
SELECT 'upstream' dir, up.uid, up.name FROM action_dependencies d
  JOIN actions up ON up.uid = d.upstream_action_uid WHERE d.action_uid = 9582
UNION ALL
SELECT 'downstream', dn.uid, dn.name FROM action_dependencies d
  JOIN actions dn ON dn.uid = d.action_uid WHERE d.upstream_action_uid = 9582;

-- push_v2 를 쓰는 앱 전부
SELECT uid, name, kwargs FROM actions
WHERE kwargs LIKE '%PushTargetUserImporter%'
   OR kwargs LIKE '%run_kage_push_target_importer%';
```

코드: `KagePushTargetImporter.scala` — 쿼리·경로·테이블 정의(75~90행), 변환(13~20행).
공통 로직은 `KageFileLoader.scala` — Kage HTTP fetch UDF(37~50행), Hudi read(52~54행),
write + `addPartitionsSilently`(80~90행).
