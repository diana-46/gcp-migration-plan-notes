## 1. Pain Point 

### 프로젝트 특징

- 여러 DSP를 참조하여 지표·메타 생성 (CA, Melon, Youtube, Apple, Spotify, Chartmetric)
- DSP마다, 동일 DSP라도 리포트마다 수집 시간과 수집갭이 다름(weekly, monthly 갭으로 수집되는 데이터도 있음)       
  - spotify: 2일 전 데이터 정기적으로 제공. 단 stream 데이터가 10일에 1번정도 늦게 제공됨. 
  - apple: 2일 갭을 두고 정기적으로 데이터.
  - youtube: 비디오 데이터 3일 갭, 일부 asset 데이터는 2일 갭 (수집 시간도 랜덤)
  - melon: 보통 1일 전 데이터 (CDC는 준실시간 데이터)
  - chartmetric: 수집 시간 랜덤. gap도 랜덤. 가끔 데이터 오류 있어 재전송 함.
  - ca: 준실시간(CDC)
- duration과 누적/차이 계산 로직으로 인해 **전일 파티션 의존성**을 가짐. (chart, playlist)       
  - 해당 모델 관련 데이터 재수집되는 경우 재수집 일자부터 최신일자까지 순차적으로 재처리해야함
- 리포트가 도착하는 대로 데이터 가공/서빙을 빠르게 했으면 좋겠다. (Multi DSP 모델에서 특정 DSP가 수집 전이더라도 가공한 뒤에 데이터 전송해야한다.) 



## Airflow 운영 

### 시도했던 방법들

#### 1) Dataset 기반으로 트리거 (v2.10.2)

컨셉: 시각 기반이 아니라, **데이터가 들어온걸 판단해서 Dag 트리거하겠다.**

Dataset (v3.x.x의 Asset 과 동일개념, [https://airflow.apache.org/docs/apache-airflow/2.8.0/authoring-and-scheduling/datasets.html](https://airflow.apache.org/docs/apache-airflow/2.8.0/authoring-and-scheduling/datasets.html) )

데이터가 갱신됐다는 신호로 DagRun을 생성하는 방식. and(&amp;)조건과 or(||) 조건 제공함.

**한계 (v2.10.2 기준)**

Dataset을 선언해도 고유 ID일뿐임. 해당 Dataset을 지속적으로 탐색하는 것 아님. 상위 Dag를 추가로 생성, 해당 자산을 탐색하고 emit 시켜야함. *=&gt; v3에서 해소된 걸로 보임*

> 예) DAG의 구조는 아래와 같음.
>
> - 데이터를 탐색하고 들어온 경우 emit 시키는 Dag(Cron)
> - Emit된 Dataset을 Schedule 정보로 물고 있다가 실행되는 Dag



Dag 단위 트리거. Task 단위에 Dataset을 연결해서 트리거 불가. (emit은 task 단위로 가능함. schedule은 dag 단위)

> 예) youtube_silver DAG
>
> - Schedule: Dataset(youtube://content_owner_asset) || Dataset(youtube://content_owner_asset_traffic_source) (content_owner_asset 또는 content_owner_asset_traffic_source 이 수집되면 실행한다.)
> - 이슈: Dataset은 partition 을 구분할 수 없었음. extra 파라미터에 파티션 정보를 넣을 수 있긴함.
>   - 하지만 extra 까지 포함해서 Dataset을 구분하지 않는다. (content_owner_asset(partition_date=20260628), content_owner_asset(partition_date=20260627) 은 동일한 데이터셋으로 인식됨.)
>     - 이게 왜 무슨 문제인가? Airflow Dataset에도 동일 Dataset이 반복 수집되면 막는 로직이 있음. 이 데이터들 같은 데이터로 처리되기 때문에 한 파티션은 무시되는 문제 발생

youtube://content_owner_asset/{partition_date}로 하면 다른 파티션으로 인식하지 않는지? 스케쥴러가 파싱하는 타이밍(30초)에  partitition_date를 넣어야함. (변수로 선언이 안됨. **lazy** 가 아님.)

- Airflow는 Dag 코드를 주기적으로 읽어 파싱함. =&gt; Airflow의 그래프는 **정적**이다. =&gt; Dataset에 partition_date를 동적으로 넣는게 힘들었음.
- 수집된 DSP 이력 기반으로 2~3일 갭이 있는 source들을 나누어서 dag를 구성하고, partition_date를 스케줄러 서버 시간 기준으로 빼서 강제로 박아넣어봄
  - 데이터 수집 갭이 고정적이지 않음. 이 방법은 DSP에서 해당 날짜를 99% 이상 준다는 제공하에 동작 가능.
  - 재전송된 데이터는 처리 안함.
- Gold DAG 쪼개면 디펜던시 Dataset 모두 수동으로 선언 진행해야함(emit은 cosmos에서 해줌)



&nbsp;

#### 2) Kafka 메시지를 컨슘해서 트리거

Dag 구성(playlist, chart 제외)

- Kafka consume dag ( =&gt; TriggerDagRunOperator). 3시간마다 컨슘해서 처리하도록 구성
- silver dag
  - youtube 
  - chartmetric
  - spotify
  - apple
- gold, api



**한계**

- TriggerDagRunOperator 는 Dag 단위 trigger. 위 구조에선 silver dsp 단위로 트리거됨. (리포트 하나 수집되었는데 해당 dsp 실버가 모두 불필요하게 실행)
  - 위 구조로 유지 시 컨슘 주기마다 전체 silver dag 실행
  - 유튜브가 20260629, 20260630 리포트 수집되었다면 DagRun 두개 생김 =&gt; 전체 youtube 데이터 2일치를 갱신
    - chartmetric은 5일치 데이터를 제공하는 경우도 있었음. 
    - gold DAG도 5일치 실행
    - 컨슘마다 계속해서 dagrun이 쌓여서 루페 전송까지 데이터가 밀리는 현상 발생(3시간마다 kafka 메시지 컨슘해서 처리했는데 fan out 되서 계속해서 밀렸음)
  - Dag를 더 잘게 쪼개면 되지 않나? =&gt; DBT 디펜던시를 잃어버려서 사람이 수동으로 주입해야함. (DBT 코드를 하나 수정하면 Airflow 코드까지 수정해야함)
- silver dag =&gt; gold dag 디펜던시가 dag 단위로 관리됨.
  - 예) silver_melon dag 중 tr_album_melon_stream_cnt_daily 모델은 gold dag의 api_album_metrics 에서만 의존성이 있는 모델임. 디펜던시가 Dag 단위로 관리되므로.. tr_album_melon_stream_cnt_daily 이 빠르게 끝났더라도 다른 Silver dag가 모두 완료될때까지 대기 =&gt; 총 처리 시간 증가 (tr_album_melon_stream_cnt_daily 완료되면 바로  api_album_metrics 처리하고 서빙하는 걸 원함)
  - ExternalTaskSensor를 넣어서 silver dag, gold dag 동시에 실행하고 해당 silver dag의 task가 끝날때까지 대기시키면 되지 않나? =&gt; 마찬가지로 dag 분리되면 dbt 디펜던시 유지 불가로 ExternalTaskSensor을 수동으로 사람이 세팅해서 dbt 디펜던시에 맞게 넣어야함. 예) gold dag에 한땀한땀 ExtenralTaskSensor을 DBT 디펜던시에 맞게 넣어야함



#### 3) range_mode를 사용해서 5일치를 한번에 처리.

컨셉: DAG 최소화하고 DSP 데이터는 최소 5일 내에 도착하니 5일치 데이터를 range_mode로 매일 갱신해서 전송한다.

**한계**

- track_metrics 모델은 무거워서 5일치를 실행할 수 없었고
- 일부 diff/cum/duration 계산 로직에서 데이터 틀어짐 발생 (무조건 1일치를 5번 처리해야하는 구조)



### 현재 Airflow에서 처리하는 방법

컨셉: DSP x Silver, Gold/API 로 DAG 분리하고 SCHEDULED_TIME을 고정. 

- 데이터 가공 시 수집여부: dbt test
- 데이터 지연 알림: redash query =&gt; slack alert

- gold+
  - v2_api_daily
  - data_hourly
  - v2_partner_youtube_daily
  - v2_data_monthly
  - v2_playlist_daily
- silver
  - silver_v2_youtube_daily
  - silver_v2_spotify_daily
  - silver_v2_tiktok_daily
  - silver_v2_melon_daily
  - silver_v2_apple_daily
  - silver_v2_chartmetric_daily
  - silver_v2_merge_daily
  - silver_v2_chart_cm_daily
  - silver_v2_chart_melon_daily
  - silver_v2_chart_merge_daily
  - playlist_daily
- meta
  - meta_periodic

- Silver(Apple, Youtube, Spotify, Chartmetric, Melon)별로 DAG 생성. 처리할 partition_date를 하드코딩.
  - Apple / Spotify / Chartmetric: scheduled_time {{data_interval_start - 2days}}
  - Youtube: {{data_interval_start - 3days}}
  - Melon: {{data_interval_start - 1days}}
- API: Range mode로 실행 (data_interval_start-3 ~ data_interval_start-1)
- Chart: Nifi에서 Rest API로 ClearTaskInstance 처리 (data_interval_start-5 ~ data_interval_start-1)
- Playlist: 매일 오전 11시 {{data_interval_start - 1days}}

위와 같이 실행했을때 Chartmetric 에서 제공하는 일부 데이터에 3일 갭이 있었음 =&gt; Chartmetric은 2일전 데이터만 처리하므로 해당 데이터는 계속 누락된 채 제공되는 오류 발생.

### 임시 운영하면서 어려웠던 점

- **완벽한 이벤트 드리븐이 아님 —** 수집된 리포트와 관련된 모델만 실행하고 싶었으나 Airflow는 태스크를 정적으로 그려서 어려웠음. (=30초마다 Dag 코드를 파싱해서 UI 그래프를 그림. **파이프라인 실행 시 그래프를 그리는 lazy mode가 아님.**)
  - DynamicTaskMapping? → 내부 TaskInstance에서 모델 간 디펜던시를 그릴 수 없음.
  - 1개의 TaskInstance로 필요한 것들만 실행? → cron과 다를 바 없고, Airflow 컴포넌트(Clear task, 모델 간 의존성 관리, 센싱)를 못 씀
  - 
- 데이터 파티션 관리
  - Source가 어디까지 수집됐는지, 어느 파티션이 비었는지 알기 힘듦. (현재 Redash에 Alert을 붙여 지연 수집만 확인 중.)  Silver/Gold/API 는 TaskInstance 실패 여부로 관리 중
  - Gold/API는 여러 DSP를 혼합해서 가공 → 파티션이 있어도 어떤 DSP가 누락된 채 가공됐는지 직접 확인 필요.  (예) api_track_metrics 에 partition_date 20260628이 있지만 이 데이터에 spotify/apple/melon/youtube 중 어떤 것이 누락된 채 실행되었는지 알지 못함.
  
    
  
    Airflow에서 DBT Test를 붙여 해당 파티션 source가 수집되었는지 체크하고 있음.
- 데이터가 늦게 수집되면 수동 처리 필요 (데이터 수집 이벤트 감지 X) 서로 다른 Dag에 있는 경우 사용자가 디펜던시를 직접 파악해서 수동으로 실행
  - 예) 유튜브 비디오 데이터가 늦게 수집된 경우 
    - 해당 파티션을 처리하는 TaskInstance를 수동으로 Clear시킴 (Silver Dag)
    - 해당 파티션을 처리하는 TaskInstance를 수동으로 Clear시킴 (Gold/루페 전송 Dag)
    - TriggerDagRunOperator 로 Silver → Gold Dag 트리거하면 안되나?
      -  Trigger 단위가 DagRun 전체임. 특정 Task 및 downstream만 트리거할 수 없음.(유튜브 비디오 관련 Gold만 처리하는게 아니라, 모든 Gold 를 불필요하게 재처리하게 됨) 
- 처리한 파티션 정보를 흘리기 힘들다.
  - Airflow의 TaskInstance(=DBT model)는 **프로세스 단위**임. 처리 정보를 다음 TaskInstance로 넘기려면 XCom으로 DB에 써야함 (여러 DSP를 참조하는 모델은 처리할 파티션 계산 로직 계산하는 TaskInstance가 추가로 필요함. 예) 차트는 chartmetric·melon 두 DSP 중 min값을 파티션으로 순차 실행)
- 전일자에 의존해서 실행하는 sequenced 모델 처리가 힘듬 
  - chart/playlist는 수집된 데이터 파티션 이후를 모두 순차적으로 갱신해야함. 
    - 예) chart/playlist 2026/6/20일 데이터를 2026/6/24일에 수집되어 처리했음. 2026/6/30 기준으로 2026/6/28까지 데이터 정상 처리되어있는 상태 =&gt; 그런데 데이터 잘못되어서 외부 DSP 에서 2026/6/24 데이터를 재전송하는 경우. 2026/6/24 ~ 2026/6/28을 순차적으로 갱신해야함. 지금은 수동으로 갱신 필요
- 파티션과 DagRun 단위의 불일치 
  - MDL은 데이터 갱신이 주기적으로 필요한 구조 (eg. 리포트 수집 시간이 달라 silver DAG 하루에 4번 갱신). 하루에 4번 동일날짜를 처리한다면?
    - UI에서 특정 파티션 로그를 찾기 힘들고..
    - v3.xx이 제공하는 백필 기능을 이용하기도 힘들어보임. (백필 기간을 20260628~20260630으로 설정하더라도 12개의 DagRun이 생김)
- 

## . 검증 결과

아래 항목은 **모두 PoC로 동작을 확인한 것**입니다. (3-1~3-4는 Dagster UI 스크린샷, 3-5~3-6은 적용한 코드로 확인)

Dagster에서는 dbt 모델을 **자산(asset)**으로 올립니다. 아래가 기본 구조 — @dbt_assets로 dbt 모델 묶음을 정의하고, 파티션 키를 env로 넘겨 dbt를 빌드합니다.

```
.....
CHART_SELECT = (
    "tr_chart_cm_daily tr_chart_cm_adjust_daily tr_chart_melon_daily tr_chart_daily"
)


class _ChartDbtTranslator(DagsterDbtTranslator):
    def get_group_name(self, props) -> str:
        return "chart_dbt_real"


@dbt_assets(
    manifest=MANIFEST,
    select=CHART_SELECT,
    partitions_def=daily,
    dagster_dbt_translator=_ChartDbtTranslator(),
)
def chart_dbt_models(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    dt = context.partition_key.replace("-", "")
    # cosmos custom_env_vars 와 동일: START==SCHEDULED → dbt is_range_mode()=False(단일일).
    os.environ["START_PARTITION_DATE"] = dt
    os.environ["SCHEDULED_TIME"] = f"{dt}235959"
    yield from dbt.cli(["build"], context=context).stream()


@dg.definitions
def chart_dbt_definitions() -> dg.Definitions:
    return dg.Definitions(
        assets=[chart_dbt_models],
        resources={
            "dbt": DbtCliResource(
                project_dir=os.fspath(DBT_PROJECT_DIR),
                profiles_dir=os.fspath(DBT_PROJECT_DIR),
                target="dev",  # 라벨일 뿐 — 실제 대상은 DBT_HOST/DATABASE/SCHEMA env 가 결정
                dbt_executable=DBT_EXECUTABLE,
            )
        }
    )

```

코드 자유도가 높아, 센서·리소스 등 커스텀 로직을 직접 작성할 수 있습니다.

### 3-1. DBT 연동 &amp; Lineage 가시성

dbt와 연동하면 dbt yml에 선언해둔 source 정보, 모델별 lineage와 SQL을 Dagster UI에서 바로 볼 수 있습니다.



### 3-2. 파티션 가시성 — 어디까지 처리했고 무엇이 누락됐나

이벤트가 발생했을 때 그 파티션에 어떤 DSP가 들어와 있었는지, 가공 모델이 어디까지 처리했고 처리 시점에 빠진 DSP가 무엇인지 UI에서 확인됩니다. (현재 Airflow에서 가장 힘들었던 부분)



DBT Source에 파티션 키를 선언하면 센서가 meta에서 읽어 활용할 수 있습니다.

```
 sources:
    - name: mdl_dsp
      tables:
        - name: chartmetric_spotify_chart
          meta: { partition_column: partition_date }   # ← 이러면 센서가 meta에서 읽음
          # 또는 freshness/loaded_at_field 로 도착 지연까지 선언
```

### 3-3. 백필 — UI에서 클릭으로

Airflow에서는 백필 시 대상 모델을 추려 새 DAG를 만들어야 했습니다. Dagster는 UI에서 파티션을 직접 찍어 백필하고, 실행 로그·재실행 이력까지 추적됩니다.



**range_mode로 실행하면 range에 걸린 파티션을 모두 인식하는가?**

**이론상 가능.** single_run / multi_run 백필이면 context.partition_keys로 **범위 전체**가 들어옵니다. 여기서 START_PARTITION_DATE=min(keys), SCHEDULED_TIME=max(keys)로 넘기면 dbt가 is_range_mode()=True로 범위를 한 번에 빌드하고, Dagster는 BackfillPolicy.single_run() 기준으로 그 run이 커버한 범위 내 파티션을 **모두 materialized로 인식**합니다. (= 단일 run이 여러 파티션을 한꺼번에 충족)

**남은 실측 확인:** 각 파티션이 개별 materialization 이벤트로 찍히는지 / 범위가 한 덩어리로 찍히는지(=UI 타임라인·freshness 판정에 영향)는 dagster-dbt 동작을 직접 돌려서 확인 필요.

### 3-4. Dag / 그룹 관리

MDL에서 TaskGroup/Dag를 나누면 DBT 디펜던시가 유지되지 않아 매우 힘들었습니다. 

Dagster는 **그룹으로 나눠도 DBT 디펜던시가 유지**됩니다.

**그룹으로 되는 것**

- 선택 핸들: AssetSelection.groups("bronze_ca") / UI group:bronze_ca 필터로 그룹 전체 선택
- 그룹 단위 Materialize/백필: 그룹 선택 → Materialize → 범위 지정 → 그룹 내 자산 한 번에 백필 ✅
- job/스케줄의 대상: define_asset_job(selection=AssetSelection.groups("X")) → 스케줄·센서로 그룹 단위 자동화
- 큰 그래프에서 lazy 로딩

**안 되는 것 / 전제**

- 그룹은 의존성·실행 경계를 만들지 않음 (Airflow의 TaskGroup 단위 의존성 불가)
- ⚠️ 그룹 백필 기간 일괄 지정은 그룹 내 자산들이 **"같은 partitions_def"를 공유할 때**만 깔끔함. 파티션이 섞여 있거나(일부만 파티션) 서로 다른 파티션이면, 범위는 파티션 가진 자산에만 적용되고 나머지는 1회 materialize됨.

### 3-5. 동적 전파 &amp; 이벤트 드리븐 (tag: sequenced)

MDL에서 Chart/Playlist 데이터에는 유지 기간과 diff를 계산하는 로직이 있음.

**전일 파티션에 의존하기 때문에 데이터가 지연 수집(재수집)되는 경우 이후 파티션을 모두 갱신해야함.**

**(1) sequenced 모델 — 전일 파티션 의존**

전일 파티션에 의존하는 모델은 TimeWindowPartitionMapping으로 N → N-1 의존을 선언합니다.

```
dg.TimeWindowPartitionMapping(start_offset=-1, end_offset=-1)  # 파티션 N → N-1 에 의존
```

이걸 선언하면 Dagster가 0625 → 0626 → 0627 → 0628 순서를 강제합니다 — N-1이 성공해야 N이 실행됨. 병렬/역순 불가. 백필을 0625~0628로 걸면 알아서 오름차순 순차 실행합니다.

**(2) dbt 자산에 적용하는 법 (두 조각)**

dagster-dbt 소스 확인 결과 — self_partition_mapping AND has_self_dependency(meta) 둘 다 있어야 self-dep이 생성됩니다.

```
class _Translator(DagsterDbtTranslator):
    def get_partition_mapping(self, props, parent_props):
        if props["unique_id"] == parent_props["unique_id"]:   # 자기 자신 의존일 때
            return dg.TimeWindowPartitionMapping(start_offset=-1, end_offset=-1)
        return None
```

그리고 sequenced 모델의 dbt meta에 플래그를 답니다. **(dbt 모델 또는 폴더 단위 dbt_project.yml)**

```
meta:
  dagster:
    has_self_dependency: true
```

→ 이 둘이 있으면 dagster-dbt가 그 모델에 "전일 파티션 의존" 엣지를 자동 생성합니다.

**(3) 갭 감지 → max까지 자동 전파**

"0625 수집됨 → 0628(max)까지 순차 전파"는 self-dep + AutomationCondition(eager)로 자동화됩니다.

- 0625 source 도착 → 0625 materialize
- self-dep 덕분에 0626은 0625 완료 + 0626 source 충족 시 eligible → 실행
- … 0628(max)까지 순서대로 cascade

즉 갭이 생겼다가 채워지면 eager가 가장 오래된 미처리 파티션부터 max까지 자연스럽게 순차 backfill합니다. (수동 백필도 동일하게 순서 보장)

**(4) Kafka 메시지 폴링해서 처리 — 가능**

- Nifi로 데이터 수집 시 Kafka로 수집한 데이터 정보와 함께 메시지 프로듀싱 =&gt; Dagster에서 컨슘해서 전파 가능한지?

```
def _build_partition_days(context, dbt: DbtCliResource, kafka: KafkaResource, model: str):
    for day in context.partition_keys:
        dt = day.replace("-", "")
        os.environ["START_PARTITION_DATE"] = dt
        os.environ["SCHEDULED_TIME"] = f"{dt}235959"
        yield from dbt.cli(["build"], context=context).stream()   # 실패 시 raise → 아래 미실행
        kafka.publish({"model": model, "date": dt}, key=model)    # ✅ 성공분만 발행
        context.log.info(f"kafka published model={model} date={dt}")

# 각 자산은 model명과 kafka를 넘김:
def isrc_youtube_daily_dbt(context, dbt: DbtCliResource, kafka: KafkaResource):
    yield from _build_partition_days(context, dbt, kafka, "tr_isrc_youtube_daily")

def api_track_ranking_dbt(context, dbt: DbtCliResource, kafka: KafkaResource):
    yield from _build_partition_days(context, dbt, kafka, "api_track_ranking")

# 멀티파티션(api_track_metrics)은 period까지 실어서:
def api_track_metrics_dbt(context, dbt: DbtCliResource, kafka: KafkaResource):
    for key in context.partition_keys:
        dims = key.keys_by_dimension
        dt, period = dims["date"].replace("-", ""), dims["period"]
        os.environ["START_PARTITION_DATE"] = dt
        os.environ["SCHEDULED_TIME"] = f"{dt}235959"
        yield from dbt.cli(["build", "--vars", f'{{"DATA_PERIOD": "{period}"}}'], context=context).stream()
        kafka.publish({"model": "api_track_metrics", "date": dt, "period": period}, key="api_track_metrics")
```

**(5) 재수집 감지 — 가능**

- MDL DSP 데이터에 updated_dt가 없음 =&gt; hive 파티션 조회로 재수집을 감지할 수가 없음. =&gt; Kafka 메시지로 변환하면 감지 가능하지 않을까?

재수집은 어떻게? → NiFi가 또 메시지를 보냅니다. updated_dt도 없고 max도 안 변하지만, 수집 레이어(NiFi)가 0628에 0620을 재수집하면서 Kafka 메시지를 다시 발행합니다:

{"source": "apple_music", "date": "20260620", "offset": 84213} ← 0628에 도착한 재수집 이벤트

센서가 이 메시지를 읽으면 → 0620 파티션 재실행 → sequenced self-dep로 0620→0621→…→max 순차 재전파. warehouse를 안 보고도 정확히 트리거됩니다.

**run_key를 재수집마다 다르게 적용해야함.(고유 ID)**

Dagster는 **같은 run_key의 RunRequest를 중복 제거(이미 본 키면 skip)**합니다. 그래서 run_key를 "apple_music:20260620"로만 두면 재수집이 dedup돼서 재실행이 안 됩니다. → **Kafka offset(또는 수집 timestamp)을 run_key에 넣어** 매 수집 이벤트를 distinct하게 만들어야 재수집이 새 run을 띄웁니다.

```
from datetime import timedelta

@dg.sensor(minimum_interval_seconds=3600)   # 1시간 폴링
def kafka_ingest_sensor(context):
    consumer = get_consumer(...)
    msgs = poll(consumer, max_n=500)
    cutoff = now() - timedelta(days=14)
    requests, last_off = [], context.cursor
    for m in msgs:                                   # {"source","date","offset"}
        d = parse_date(m["date"])
        if d < cutoff:                               # ⬅️ 14일 초과 → 방어(무시)
            context.log.info(f"skip out-of-window {m['date']}")
            last_off = m["offset"]; continue
        requests.append(dg.RunRequest(
            partition_key=d.strftime("%Y-%m-%d"),
            run_key=f'{m["source"]}:{m["date"]}:{m["offset"]}',  # ⬅️ offset 포함 → 재수집=새 run
        ))
        last_off = m["offset"]
    context.update_cursor(str(last_off))             # 오프셋 cursor 저장(중복/누락 방지)
    return dg.SensorResult(run_requests=requests)
```

**먼 과거의 데이터가 와서 많은 데이터 처리하는 경우. 방어가 가능한가? 예) 14일 방어 (두 겹)**

1. 센서 필터 (위 if d &lt; cutoff): partition_date가 14일보다 오래되면 메시지를 버림. → 0628에 0620 재수집(8일 전)은 통과. 0601(28일 전) 재수집이 와도 무시.
2. automation 보강 (선택): 트리거된 자산에 ... &amp; in_latest_time_window(timedelta(days=14))를 걸면, 센서를 통과해도 14일 밖 파티션은 materialize 안 됨 (이중 안전).

**시나리오 (0620이 0628에 재수집)**

```
NiFi 0628 재수집 → Kafka {date:20260620, offset:84213}
  → 센서: 20260620 ∈ 최근14일? (8일 전, YES)
     → run_key="apple_music:20260620:84213" (새 offset → 중복제거 안 됨)
     → 0620 재실행 → sequenced 0620→0621→…→max 순차 재전파 ✅
  → 만약 20260601(28일 전) 재수집이면 → cutoff 미만 → skip (방어) ✅
```

**백필 시 하위 모델에도 partition 전파 가능**

단, downstream 모델에 AutomationCondition.eager() 를 부여해야함.

예) 상위 tr_chart_daily[N]가 재구체화되면 → api_track_ranking[N] 자동 재실행

### 3-6. 모니터링 — Freshness / SLA / 지연 알림

**SLA / Alert — 가능.** hourly 자산이 "그 시각 파티션을 40분 내에 채웠는가"를 체크합니다.

```
from datetime import timedelta
import dagster as dg

# hourly 자산이 "그 시각 파티션을 40분 내에 채웠는가" 체크
freshness_checks = dg.build_time_partition_freshness_checks(
    assets=[api_track_metrics_dbt],     # hourly 파티션 자산
    deadline_cron="0 * * * *",          # 매시 정각 기준 마감
    lower_bound_delta=timedelta(minutes=40),   # 40분까지 허용, 넘으면 실패
    timezone="Asia/Seoul",
)
```

아래 센서를 추가하면 됩니다.

```
freshness_sensor = dg.build_sensor_for_freshness_checks(freshness_checks=freshness_checks)
```

**source가 N일 이상 지연되면 알림 — 가능.**

```
from datetime import timedelta
import dagster as dg

# 1) 소스의 max(partition_date) 를 관측 (기존 PrestoProvider 쿼리 재사용)
@dg.observable_source_asset(name="youtube_source")
def youtube_source():
    max_dt = query_max_partition_date("mdl_dsp_production.chartmetric_youtube_video_daily_chart")
    return dg.ObserveResult(
        metadata={
            # ⚠️ 핵심: '데이터 날짜'를 freshness 기준으로 (관측 시각이 아니라)
            "dagster/last_updated_timestamp": dg.MetadataValue.timestamp(to_epoch(max_dt)),
            "max_partition_date": max_dt,
        }
    )

# 2) 4일 이상 지연되면 FAIL
yt_freshness = dg.build_last_update_freshness_checks(
    assets=[youtube_source],
    lower_bound_delta=timedelta(days=4),   # youtube: 정상 3일 → 4일 넘으면 위반
)

# 3) 체크 주기 평가 + (4) 위반 시 알림 (Slack/Kafka) — 앞서와 동일
freshness_sensor = dg.build_sensor_for_freshness_checks(freshness_checks=yt_freshness)
```

## 4. 운영 시 고려사항

### 4-1. k8s 배포 — Executor 선택

- Airflow K8sExecutor: task instance마다 pod 실행. image pull + pod 기동 오버헤드로 dagrun time 증가
- Dagster
  - K8sRunLauncher: run(=dagrun)마다 run worker pod 1개 (= 1 dagrun 1 pod)
  - Executor: run 내부의 op/step을 어떻게 실행할지 결정
    - in_process: run 파드 안 단일 프로세스 순차
    - multiprocess (default): run 파드 안에서 서브프로세스
    - k8s_job_executor: step(op)마다 별도 k8s job pod 생성 (= Airflow K8sExecutor 방식)

### 4-2. 동시성 제어 — Presto 부하 방지

Dagster에서 pod가 폭증하는 지점은 Run 개수(= Airflow DagRun)에 따릅니다.

- 예: 20260601~20260630 백필 시 기본값은 partition 1개당 run 1개 → pod 166개 (처리시간 느려짐)

**① Dagster 클러스터의 pod 수 limit**

```
# daemon의 QueuedRunCoordinator
  runCoordinator:
    type: QueuedRunCoordinator
    config:
      queuedRunCoordinator:
        maxConcurrentRuns: 10        # 동시에 10개 파드까지만
        tagConcurrencyLimits:
          - key: "dagster/backfill"
            limit: 5
```

**② BackfillPolicy.single_run() — 백필 파드를 1개로 접기**

```
# 1개의 pod에서 for문으로 실행하기
## 다만 아래처럼 실행하다가 80일째에 에러 발생하면 run 전체가 fail 처리
@dbt_assets(
      manifest=MANIFEST,
      select="tr_isrc_youtube_daily",
      name="isrc_youtube_dbt",
      partitions_def=isrc_daily,
      backfill_policy=dg.BackfillPolicy.single_run(),   # 범위 전체 → 파드 1개
      dagster_dbt_translator=_ChartDbtTranslator(),
  )
  def isrc_youtube_dbt(context: dg.AssetExecutionContext, dbt: DbtCliResource):
      # single_run 백필이면 partition_key 하나가 아니라 '범위'가 들어온다.
      keys = context.partition_keys          # ['2026-01-08', ..., '2026-06-22']
      context.log.info(f"single-pod backfill: {len(keys)}일치 순차 실행")

      for day in keys:                       # ← 파드 1개 안에서 하루씩 for문
          dt = day.replace("-", "")
          os.environ["START_PARTITION_DATE"] = dt
          os.environ["SCHEDULED_TIME"] = f"{dt}235959"
          context.log.info(f"  build {dt}")
          yield from dbt.cli(["build"], context=context).stream()
```

**1개 pod에서 7일치 for문 처리 → 중간에 에러 나도 7일 이전부터 재처리 가능?**

- multi_run(max_partitions_per_run=7)로 세팅하면 가능
- Backfills 페이지에서 실패한 run을 Re-execute하거나 "남은(미완료) 파티션만 다시 실행"을 누르면, Dagster가 실패한 파티션들을 다시 7일 묶음으로 재배치해 띄움



## 5. Airflow와의 비교

### Airflow 3.xx

3.x는 무엇이 달라졌나? (AssetWatcher가 가장 큰 차이로 보임)


|              | **2.10 Dataset**                   | **3.x Asset**                                                                                            |
| ------------ | ---------------------------------- | -------------------------------------------------------------------------------------------------------- |
|              | - Dataset - trigger_dataset_events | - Asset - triggering_asset_events                                                                        |
| AssetWatcher | 없음                                 | 이벤트 기반 스케줄링. 스케줄러가 외부 큐(SQS, 3.1부터 메시지 큐 확대)를 직접 watch해서 Asset 이벤트를 emit. “상위 DAG이 직접 emit 해야함” 제약 일부 해소 |
| AssetAlias   | O                                  | 런타임에 어떤 concrete asset을 emit할지 동적 결정 가능                                                                  |
| @asset       | 없음                                 | asset-centric 작성 모델                                                                                      |
| extra 전파     | outlet+extra                       | outlet_events[...].extra                                                                                 |




### **Airflow 3.x가 이 문제 풀 수 있을까?**


| **2.10.2 한계** ❌                                                      | **3.xx**                                                  | **Dagster**                                                                  |
| -------------------------------------------------------------------- | --------------------------------------------------------- | ---------------------------------------------------------------------------- |
| Dataset은 ID일 뿐 탐색 안함 → emit DAG 별도 필요                                | ✅ AssetWatcher + KafkaMessageQueueTrigger가 스케줄러에서 리스닝     | ✅ Sensor가 외부 상태 능동 폴링 → 특정 asset+partition으로 RunRequest                      |
| 태스크 단위 Dataset 트리거 불가 (DAG단위만)                                       | ❌ 여전히 DAG 단위. 스케줄 트리거는 DAG 레벨. 단 asset은 모델(태스크)별로 emit됨   | ✅ asset이 실행 단위. 단일 asset/부분집합만 materialize                                   |
| DAG 쪼개면 dbt 의존성 끊겨 수동 주입해야함                                          | ❌ cosmos가 cross-DAG 엣지를 manifest에서 자동 배선해주지 않음            | ✅✅ dagster-dbt가 모델=asset으로 그래프 통째 보존. 애초에 쪼갤 DAG이 없음                         |
| extra로 partition 못 구분 → Dataset 반복 emit 되는 경우 파티션 누락되는 문제            | ❌ 이벤트는 안 씹힘(전부 list로 전달). 단 **1파티션=1 DagRun 보장은 안됨**      | ✅✅ partition key가 1급. (asset, 20260627)·(asset, 20260628)은 별개 - 충돌, 누락 개념 없음 |
| 정적 그래프라 partition을 파싱 타임에 넣어야하는 이슈                                   | ✅ partition이 런타임 asset extra로 흐름. 파싱 타임 주입 안해도 됨.         | ✅ DynamicPartitionsDefinition 런타임 partition 추가                               |
| silver→gold로 partition 전파 필요                                         | ✅ 각 레이어가 extra 재 발행해야함                                    | ✅✅ PartitionMapping이 자동 전파 (수동 재발행 불필요)                                      |
| Kafka consume → TriggerDagRunOperator로 전체 DSP 불필요하게 실행               | ✅ AssetWatcher로 consume DAG 자체 제거. 리포트별 asset으로 트리거 범위 한정 | ✅ sensor가 영향받은 asset+partition만 RunRequest                                   |
| silver→gold 의존성이 DAG 단위 → 전체 대기                                      | ❌ 단일 cosmos DAG(DBT 프로젝트 전체) 또는 모델별 asset 수동 배선 둘 중 하나 필요 | ✅✅ AutomationCondition.eager() — 특정 upstream partition 끝나면 그 downstream만 즉시  |
| 모델별 파티션 처리 여부 조회 안됨                                                  | ❌                                                         | ✅                                                                            |
| missing/failed 파티션 식별                                                | ❌                                                         | ✅                                                                            |
| freshness SLA 체크용 DAG를 따로 운영해야함 eg) youtube 특정 리포트가 4일동안 수집되지 않은 경우 | 마찬가지로 별도 DAG로 운영해야함                                       | ✅ freshness check 선언 (dbt yaml 파일에 선언)                                       |


### Airflow VS DagSter


|     | **Airflow**                     | **Dagster**                     |
| --- | ------------------------------- | ------------------------------- |
| 중심  | Task/워크플로 (명령형: “이 작업들을 이 순서로”) | Data Asset(선언형: “이 데이터가 존재해야함”) |
| 모델  | 임의의 작업 오케스트레이션                  | 데이터 자산 + lineage                |
| 강점  | 범용 작업, 거대한 Operator 생태계         | 데이터 의존성/파티션/백필/DBT              |


**Airflow 의 강점**

“데이터 가공만 아니라 다른 Job들이 필요할때“

1. 데이터 자산이 아닌 범용 워크플로
  1. 인프라 작업, 배치 잡, 비즈니스 프로세스, CI/CD 오케스트레이션처럼 결과물이 데이터셋이 아닌 일
2. 이기종 시스템 glue - Operator 생태계가 핵심적일 때
  1. GCP 데이터 전송/가공
  2. DB 전송/Read
3. 순수 시간 기반 스케줄 
  1. 매일 3시에 특정 job 실행
4. 중앙 오케스트레이터 / 크로스 시스템

**Dagster의 강점**

“시간 기반 스케줄이 어려울때. 파티션끼리 의존성이 복잡할때.“

1. 파티션 단위 의존성/백필
2. DBT 통합(lineage)
3. 지연 데이터 수집 재처리 자동화



만약 옮긴다면? =&gt; Nifi 데이터 수집 부분 통합 필요

```
[NiFi 수집, **Dagster로 통합] → Kafka
   ├─→ [Dagster] 데이터 레이어 (dbt build, 파티션, 백필, 자동전파)  ← Dagster 강점
   └─ (처리완료 Kafka) → [Airflow] cron consume → Loupe 전송        ← Airflow 강점
```



## 내용 추가

### HA가 가능한가?


|           | **Airflow**                          | **Dagster**                                         |
| --------- | ------------------------------------ | --------------------------------------------------- |
| Scheduler | Active-Active 멀티 스케쥴러 지원             | Daemon 싱글톤                                          |
| 웹서버       | 수평 확장 가능 (Stateless)                 | 수평 확장 가능 (Stateless)                                |
| 실행 계층     | CeleryExecutor(현재 사용 중), K8sExecutor | Run 별 K8s Job/Process 격리 실행 run launcher가 파드 단위로 띄움 |
| 메타 DB     | PostgreSQL/MySQL                     | PostgreSQL                                          |
| 장애 복구     | 무중단(스케쥴러 다중화)                        | Daemon 죽으면 다시 뜰때까지 지연있을 수 있음.                       |


- Airflow: Scheduler를 2벌 운영
- Dagster: Daemon 1개로 운영해야하기 때문에 k8s pod 스케쥴로 해결해야함(replica 1)
  - multi zone: zone 무조건 2개 이상으로 세팅할 것 (ay가 가용불가인 경우 hn등 다른 존에서 뜰 수 있도록)
  - cursor 기반으로 세팅하면 유실 없음. cursor 없는 센서로 세팅하지 말것.
  - MDL 프로젝트는 수 초,수 분 지연에 민감하지 않음



### 프로젝트 확장

### 격리성

프로젝트 복잡해지면 Dagster 사이트를 분리해야하는지? =&gt; Code location 단위로 분리한다.

Code location은 무엇인가?

- Airflow: Webserver, Scheduler가 Dag 파일에 동시 접근 =&gt; Dag 파일에 에러가 있으면 둘 다 에러
- Dagster: Code location(Long running Pod) 가 가상환경 정보(dbt code)를 들고 있음
  - dbt code는 run pod가 뜰때 s3 에서 가져와도된다. image에 말아놔도 됨(권장). MDL은 7월 기준 수정이 잦으므로 추후 gcp로 이관하게 된다면 image로 교체해도될 것 같음.
  - Code location이 gRPC로 Daemon, Webserver에 응답함. (그래프, 스케줄, 센서 시작 시) 그래서 프로젝트마다 다른 dbt, python 버전을 쓸 수 있음.



다만, Web UI에서 모든 프로젝트의 Dependency가 한번에 보이므로.. 그룹을 잘 나눠야할 것 같음.



### Error가 다른 프로젝트로 전파되는지?

Code location에 따라 완전 격리된 Pod에서 실행. 전파 X



### SLA는 어떻게 계산하는지?

### Nifi의 데이터 수집 로직을 가져올 수 있을지

가져올 수 있음. 가져오고 바로 MDL 후속 job 실행 가능. (hadoop에 넣는 과정까지 포함되어야함.)

Youtube 수집 로직을 빠르게 할 수 없을지? 파일 1개를 조금씩 나눠서 s3에 저장 =&gt; duckdb로 csv 읽어서 parquet 변환 후 적재할 수 없을까