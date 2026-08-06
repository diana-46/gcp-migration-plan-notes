# 11. 공용 라이브러리 (dp-dbt-utils)

> 여러 dbt 프로젝트에서 공통으로 쓸 매크로를 별도 레포 (`kakaoent/dp-dbt-utils`) 로 뽑아 관리한다. 소비자 프로젝트 (`berrizdata-dbt` 등) 는 `packages.yml` 로 태그 pin 해서 참조. dbt 는 PyPI 같은 별도 registry 가 없어 git 태그 자체가 릴리즈 단위.
> 관련: `dp-dbt-utils/README.md` (사용법·기여법), [[6_배포와 환경 분리]]

## 0. 다루는 범위

- 왜 뽑았는지 (rationale)
- 무엇이 어디 있는지 (매크로 배치)
- dbt 패키지 관리 모델 (git 태그 = 릴리즈)
- 소비자 워크플로우 (packages.yml, dbt deps 순서)
- 기여자 워크플로우 (PR, 태그, semver)
- 판단 기준 (공용 vs 프로젝트-로컬)

---

## 1. 왜 뽑았나

berrizdata-dbt 에 있던 매크로 2개가 다음 조건을 만족:

- **여러 프로젝트에서 재사용 가능한 규약** — 특정 서비스 (`berriz`) 도메인이 아님
- **Neptune 이관 공통 규칙** — 다른 팀 프로젝트도 같은 방식으로 이관될 것

따라서 프로젝트 안에 계속 두면 이관 다음 프로젝트에서 매번 복붙 → drift → 규약 붕괴. 라이브러리로 뽑아 한 곳에서 관리.

이관 대상:
- `generate_alias_name` — 환경별 테이블 alias suffix
- `insert_only` incremental strategy — Neptune merge_move 세만틱 재현

이관 결과:
- `dp-dbt-utils/macros/generate_alias_name.sql`
- `dp-dbt-utils/macros/bigquery_insert_only_strategy.sql`
- `berrizdata-dbt/macros/` 는 비어짐 (프로젝트 고유 매크로 생기면 여기에)

---

## 2. dbt 의 "패키지" 모델

Python 과 다르다.

| Python (kakaoent-dataplatform) | dbt (dp-dbt-utils) |
| --- | --- |
| PyPI (사내 GAR) 에 publish | git repo 에 태그 push |
| `pip install pkg==0.1.0` | `dbt deps` → git clone 태그 |
| twine upload 필요 | `git tag v0.1.0 && git push origin v0.1.0` |
| .whl 아티팩트 | 소스 그대로 |

즉:
- dbt 코드는 SQL + Jinja → 인터프리트라 빌드 개념 없음
- **git 태그 자체가 릴리즈**
- 소비자는 `dbt deps` 로 지정 태그를 git clone → `dbt_packages/` 아래로 저장

`hub.getdbt.com` 도 결국 GitHub 레포 링크 카탈로그일 뿐. 사내에서 별도 registry 필요 없음.

---

## 3. 소비자 워크플로우 (berrizdata-dbt 등)

### 3.1 최초 세팅

`packages.yml`:

```yaml
packages:
  - git: "https://github.com/kakaoent/dp-dbt-utils.git"
    revision: v0.1.0
```

**규약**: `revision:` 은 항상 태그. `main`/브랜치 이름 절대 금지 (재현성 붕괴).

### 3.2 로컬 개발 순서

```bash
dbt deps                          # ← GitHub 에서 fetch, dbt_packages/ 아래로
dbt parse --profiles-dir .        # ← 로컬만 읽음 (dbt_packages/ 포함)
dbt run --target dev              # ← 실제 실행
```

`dbt deps` 를 안 하고 `dbt parse` 하면 → 패키지 매크로 참조가 컴파일 에러.

### 3.3 CI (deploy-*.yml) 순서

```yaml
- run: dbt deps
- run: dbt parse --profiles-dir .
- run: gsutil -m rsync -r ... . <bucket>
```

`dbt_packages/` 는 rsync 에 포함 — Composer worker 가 `dbt run` 시점에 매크로 참조 필요.

### 3.4 매크로 사용

프로젝트 코드에서 그냥 이름으로 호출:

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='insert_only',
    ...
) }}
```

어느 패키지 소속인지 몰라도 됨. dbt 가 이름으로 dispatch. 단, **프로젝트 macros/ 에 같은 이름 매크로 만들지 말 것** — 프로젝트가 우선 순위라 라이브러리 매크로가 가려짐.

### 3.5 업그레이드

1. dp-dbt-utils 의 [tags](https://github.com/kakaoent/dp-dbt-utils/tags) 확인
2. `packages.yml` 의 `revision:` bump
3. `dbt deps` 재실행 → `dbt parse` 로 정합 확인
4. PR 리뷰 → 머지

---

## 4. 기여자 워크플로우 (dp-dbt-utils)

### 4.1 매크로 추가/수정

1. main 에서 브랜치 컷 (main 에 직접 커밋 금지)
2. `macros/*.sql` 편집. 상단 주석 필수:
    - 목적
    - 사용 예 (모델 config 스니펫)
    - 대안 (기본 dbt-bigquery 전략 등) 과의 차이
3. PR → 팀 리뷰 → main merge

### 4.2 릴리즈

```bash
git checkout main && git pull
git tag v0.2.0
git push origin v0.2.0
```

이 순간부터 v0.2.0 이 "배포된" 상태. 소비자는 자기 timing 에 packages.yml revision 업그레이드.

### 4.3 semver 규칙

- **0.x.y (현재)** — 초기. breaking 허용
- **1.0.0** 부터:
    - **major** — breaking (시그니처 변경, 삭제). 소비자에게 사전 공지 필수 (Slack)
    - **minor** — 신규 매크로 추가, 하위 호환 개선
    - **patch** — 버그 픽스, 주석 수정

### 4.4 breaking change 낼 때

- 태그만 bump 하고 끝내지 말고, Slack 등으로 소비자에게 공지
- 소비자는 아직 예전 태그에 pin 되어 있어 당장 안 깨짐 → 시간을 두고 revision 업그레이드
- 필요 시 마이그레이션 노트를 GitHub Release 본문에

---

## 5. 무엇을 어디에 두나 (판단 기준)

### `dp-dbt-utils` 에 두기

- 팀 여러 프로젝트에서 재사용 가능한 매크로
- 환경 규약 (alias suffix 등)
- Neptune 이관 공통 로직 (insert_only strategy 등)
- dbt-bigquery override 성 매크로 (사내 규약 반영)

### 프로젝트 (`berrizdata-dbt` 등) 에 두기

- 그 프로젝트/서비스에서만 쓰는 매크로 (특정 도메인)
- 실험적/미검증 매크로 — 프로젝트에서 최소 1회 실전 검증 후 필요하면 라이브러리로 승격
- 프로젝트 특유의 pre_hook / post_hook 유틸

### 승격 절차 (프로젝트 → 라이브러리)

1. 프로젝트 macros/ 에서 실전 검증 (여러 모델에서 안정 사용)
2. 다른 프로젝트에서도 필요하다는 시그널 확보 (팀 논의)
3. dp-dbt-utils 로 옮기고 PR + 태그 릴리즈
4. 원 프로젝트에서 로컬 매크로 삭제 + packages.yml 로 참조 전환

---

## 6. 참고 링크

- 레포: https://github.com/kakaoent/dp-dbt-utils
- 태그 목록: https://github.com/kakaoent/dp-dbt-utils/tags
- dbt 공식 packages 문서: https://docs.getdbt.com/docs/build/packages
- dbt-utils (레퍼런스 사례): https://github.com/dbt-labs/dbt-utils
