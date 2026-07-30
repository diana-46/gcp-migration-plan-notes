# 7. DE 설득 노트 — 왜 dbt, 왜 팀별 DAG 저장소

> 함께 일하는 동료 DE 여러분께. Neptune / Actions UI 에서 잘 굴러가던 걸 왜 두 축 모두
> 바꾸려 하는지 정리한 노트입니다.
>
> 이번 논의의 핵심 두 가지:
> 1. **왜 dbt** (SQL 축)
> 2. **왜 팀별로 airflow-dags 를 나눠 관리** (오케스트레이션 축)
>
> 관련: [[5_Neptune SQL 변환의 dbt-BigQuery 대체 검토]], [[6_마이그레이션 플랜]], [[스케줄러/15_관리 레포 인벤토리]]

---

## 0. TL;DR

- SQL 은 dbt 로 → **git 안, 로컬에서 초 단위 실험, lineage/테스트/문서가 코드 옆**
- DAG 은 팀별 저장소로 → **팀 자율 배포, 격리, ownership 명확**
- 공통 원리: **DB / UI 안에 갇힌 자산을 git + PR + CI 라는 SDLC 정공법으로**

---

## 1. Why dbt (SQL 축)

### Neptune 이 조금씩 불편했던 3가지

**1) 코드가 DB 안에 있음**  
Neptune SQL 은 MySQL TEXT 컬럼. 그래서:
- `git blame` 불가 — "3개월 전 이 WHERE 왜 이렇게 짰지" 답 어려움
- diff / PR 리뷰 없음 (슬랙에 스크린샷 붙이던 이유)
- 롤백 자연스럽지 않음
- "이 컬럼 쓰는 ETL 어디 있지" 를 grep 으로 못 찾음

**2) 로컬에서 미리 돌려보기 어려움**  
반복 사이클 ≈ 10분:
```
UI 에 SQL 넣기 → 저장 → Airflow 트리거 → 로그 대기 → 실패 → 다시
```

dbt 로는 10초:
```
$ dbt run --select my_model
Completed successfully in 8s.
```

**3) `kwargs` 가 스트링 JSON**  
Actions/Workflows 의 오퍼레이터 kwargs 는 자유 형식 JSON. 오타 검증도, 타입 힌트도, IDE 자동완성도 없음. 런타임에서 처음 발견.

### dbt 로 얻는 것

| Neptune | dbt |
|---|---|
| SQL 이 DB TEXT 컬럼에 | `.sql` 파일에 (git 안) |
| 히스토리·리뷰 불가 | `git blame` / GitHub PR |
| 저장 → 실행 → 10분 후 결과 | `dbt run --select` → 10초 |
| 의존성을 UI 에서 클릭 | `{{ ref('upstream_model') }}` — 자동 감지 |
| 파라미터 = EtlParameter | `{{ var('run_date') }}` |
| 파티션 = `ADD PARTITION` + HDFS 경로 | `partition_by` + `insert_overwrite` — BQ 가 처리 |
| 테스트 = 별도 구축 | `dbt test` (`unique`, `not_null`, custom) |
| 문서 = 별도 관리 | `schema.yml` — 자동 사이트 생성 |
| Lineage = 파편 / 컬럼 단위 없음 | dbt manifest → DataHub 자동 (column-level 포함) |

### 실증 — bizberry hourly 이관 (2026-07)

Story 팀 시연 대상 4 mart 이관 결과:

| Neptune ETL | dbt 결과 | 특징 |
|---|---|---|
| userpost (PLAIN 단일 쿼리) | 1 mart | daily partition + custom `insert_only` 전략 |
| artistpost (PLAIN 단일 쿼리) | 1 mart | hourly partition + `insert_overwrite` |
| overview_trend (YAML 3-temp) | 2 temp + 1 mart | nested STRUCT / `ARRAY<STRUCT>` |
| contents_summary (YAML 5-temp) | 5 temp + 1 mart | 5-way UNION ALL + hourly partition |

- Neptune 원본 세만틱 유지 (partition replace, block replace 등) — dbt-bigquery 매크로 override 로 커스텀 `insert_only` 전략 정의
- 로컬 반복 개발 사이클 검증 완료 (`dbt run --select` 10초 안팎)
- `ref()` 자동 lineage + DataHub column-level 시각화 확인

**부수 효과**: dbt 는 사내 툴 아니라 세계 표준. 익힌 스킬이 팀 밖·회사 밖에서도 그대로.

---

## 2. Why 팀별 airflow-dags 저장소

### 지금 (centralized) 모델의 문제

현행 `airflow-dags` 는 여러 팀이 공유하는 단일 저장소.

- **배포 페이스 강제 동기화** — 한 팀 이 급하면 다른 팀 검토 안 끝나도 push. 반대도 발생.
- **팀 간 이슈 전파** — 한 팀 DAG 이 파싱 실패하면 스케줄러 전체 노이즈. Composer 인프라 이슈 시 모든 팀 영향.
- **Ownership 흐림** — 누가 그 DAG 담당인지, CODEOWNERS 하나로 커버 어려움
- **리뷰어 풀 혼란** — 다른 팀 코드까지 리뷰해야 하는 부담. 결국 리뷰 없이 merge 되는 경우.
- **dbt 프로젝트와 불일치** — dbt 는 팀별 (`storydata-dbt`, `kpayment-dbt`, ...) 인데 DAG 만 통합. 대칭 안 맞음.

### 이게 왜 지금 (GCP 이관 시점) 특히 중요한가 — 비용·리소스 축

지금 athlon 은 **DB 기반 스케줄링**:
- MySQL 에 저장된 DAG 정의 / 스케줄 상태를 매 분·초 폴링
- 측정치: 300~900 queries/minute (관련: [[스케줄러/15_관리 레포 인벤토리]] § athlon DB 한계)
- DAG 수, 팀 수 늘어날수록 부하 선형 증가 → DB CPU / 네트워크 / 잠금 경합

GCP Composer 로 옮기면 요금 모델이 바뀜:
- Composer 3 요금 = **DCU (vCPU + RAM 시간)** — 관련: [[스케줄러/14_Composer 3 비용 구조]]
- Scheduler / DAG processor / triggerer 는 24×7 상주 → floor cost 발생
- DAG 밀도·파싱 부하가 그대로 스케줄러 스펙 = 요금 증가로 이어짐

**팀별 저장소가 이 두 축을 동시에 해결**:

1. **git 기반 DAG = DB 폴링 자체가 사라짐**  
   Composer scheduler 는 GCS 파일 + 파싱된 manifest 캐시로 동작. athlon MySQL 을 계속 찌르던 구조 소멸. GCP 이관의 리소스 절감 목표 (관련: [[스케줄러/7_1_실제 스펙 산정]], [[스케줄러/7_2_리소스 다이어트 포인트]]) 의 큰 지렛대 하나.

2. **팀별 스케일 격리·확장 여지**  
   저장소가 팀별로 나뉘면 **팀별 Composer 로 확장할 수 있는 옵션**이 열림. 각 팀 스케줄러가 자기 DAG 만 파싱 → 도메인별 요금 visibility 개선. 한 Composer 공유 케이스여도 팀별 스킴 유지 시 파싱 실패 격리, 이슈 blast radius 축소.

즉 팀별 저장소는 단순 "팀 자율성" 만이 아니라 **GCP 이관의 비용·리소스 목표와 직결된 실행 수단**.

### 팀별 저장소가 자연 fit

| 관점 | Centralized | 팀별 |
|---|---|---|
| 배포 페이스 | 팀 간 동기 | 팀 자율 |
| 이슈 격리 | 파싱 에러 전파 | 팀 내 국한 |
| Ownership | 파일별 CODEOWNERS 관리 | 저장소 = 팀 |
| 리뷰 | cross-team 부담 | 팀 내부 완결 |
| dbt 프로젝트 대응 | 비대칭 (dbt 팀별 / DAG 통합) | 대칭 (양쪽 팀별) |
| Composer 지원 | 필요 없음 | Composer `dags/` 재귀 스캔 지원 |

### 어떻게 실현되나 (실제 구현)

**저장소 구조**:
```
storydata-airflow-dags/           ← 팀별 저장소
├── dags/
│   └── storydata/                ← 팀 서브디렉토리
│       ├── berriz_0101_bizberry_hourly_integration.py
│       └── ...
```

**GCS 배포 컨벤션** (Composer bucket):
```
gs://COMPOSER_BUCKET/dags/
├── storydata/                    ← storydata-airflow-dags 가 관리
├── kpayment/                     ← kpayment-airflow-dags 가 관리
└── otherteam/                    ← 각자 관리
```

**Workflow 규칙** (`.github/workflows/deploy.yml`):
- Sync 범위: `dags/storydata/` → `BUCKET/storydata/`
- `gsutil rsync -d` — 팀 서브디렉토리 안에서만 delete propagation (다른 팀 안 건드림)
- 이 저장소가 `dags/storydata/` 의 source of truth

**결과**:
- 한 Composer 공유, 팀별 격리
- 각 팀 자기 pace 로 push
- Composer 는 `dags/` 재귀 스캔이라 서브디렉토리 자동 인식

### 실증 — storydata 팀 이관 (2026-07)

- `storydata-airflow-dags` 저장소 신설
- `dags/storydata/` 서브디렉토리 컨벤션
- rsync -d 로 delete propagation, `_integration` 환경 스위칭 검증
- Producer DAG (`berriz_0101_bizberry_hourly_integration`) + Consumer DAG (`berriz_bizberry_downstream_demo_integration`) 로 Asset 기반 cross-DAG 트리거 확인

---

## 3. 두 결정의 공통 원리

**DB / UI 안에 갇힌 자산 → git + PR + CI 로**

| | Neptune 시절 | dbt + 팀별 DAG |
|---|---|---|
| 편집 | UI 텍스트 필드 / kwargs JSON | IDE + `.sql` / `.py` 파일 |
| 저장 | MySQL 컬럼 | git 파일 |
| 리뷰 | 슬랙 스크린샷 | GitHub PR + CODEOWNERS |
| 검증 | 배포 후 런타임 | CI (parse, dbt test, DAG import) |
| 롤백 | 수동 재입력 | `git revert` + BQ time travel |
| 히스토리 | 없음 | `git blame` / `git log` |
| Ownership | 흐림 | 저장소 = 팀 |
| 확장 | 플랫폼팀 요청 대기 | 팀 자율 |

## 4. 걱정과 답

**"Python DAG 못 짜면 어쩌지"**  
사내 operator (Nabi/Loupe/Hive/BQ 등) 는 `apache-airflow-providers-kakaoent-dataplatform` 패키지로 재배포 — `pip install` 후 `import` 로 그대로 씀. LLM 도구 (Claude, Copilot) 진입 문턱 크게 낮춤.  
관련: [[스케줄러/7_3_공통 Custom Operator 제공 방안]]

**"팀별로 저장소 만드는 부담"**  
저장소 초기 셋업은 platform 팀 template 로 하루 안 걸림. 이후엔 팀 자율 배포. `airflow-dags` 공유 저장소보다 오히려 부담 줄어듦 (팀 내 리뷰만).

**"플랫폼팀 지원 이어질까"**  
`#dbt-support`, `#airflow-support` 실시간 응답 + pair 프로그래밍. 첫 3개 DAG 은 플랫폼팀이 함께 리뷰.

**"기존 자산 다 재작성?"**  
자동 변환 도구 (Presto → BQ 방언, Actions UI → Python DAG) 로 초안 생성. DE 는 검수 + 수정만.

**"시간 확보"**  
마이그레이션 시간은 OKR 로 인정 (매니저·디렉터 합의 전제). Phase 0 에서 이 조건 안 잡히면 다음 단계 안 감.

---

## 5. 마무리

Neptune 이 부족했다는 얘기가 아니라, Hive/Presto → BQ 환경 변화에 맞춰 도구도 함께 옮기는 결정입니다.

**핵심 요청**:
1. 얼리어답터 — Phase 1 에서 자기 팀 ETL 1~2개 이관
2. 피드백 — 이 노트, [[6_마이그레이션 플랜]] 에 대한 의견
3. 워크샵 참여 — Phase 0 온보딩

---

## 관련 문서

- [[5_Neptune SQL 변환의 dbt-BigQuery 대체 검토]] — 이관 기술 검증
- [[6_마이그레이션 플랜]] — Phase 0~3 실행 플랜
- [[8_배포 시 유의할 점]] — 실전 배포 함정 정리
- [[스케줄러/7_3_공통 Custom Operator 제공 방안]] — provider 패키지 설계
- [[스케줄러/15_관리 레포 인벤토리]] — 3-layer 저장소 구조
