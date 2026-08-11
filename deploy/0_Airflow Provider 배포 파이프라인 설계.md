# Airflow Provider 배포 파이프라인 설계

**대상**: `kakaoent/dp-airflow-provider` (`apache-airflow-providers-kakaoent-dataplatform`)
**작성 근거**: `loupe` CI/CD 구조 참조 + dev/prod GCP 인프라 실측 (2026-08-04)
**상태**: 설계 확정. 인프라 셋업은 [[1_Airflow Provider 배포 셋업 런북]] 참조

---

## 0. 요약

| 항목 | 결정 |
|---|---|
| 산출물 | Python wheel → GCP Artifact Registry (Python format). 실제 배포는 소비 팀의 Composer 반영 시점 (§0.1) |
| 인증 | Workload Identity Federation (SA JSON key 미사용) |
| 환경 분리 | dev / prod 각 프로젝트에 별도 registry |
| 빌드 시점 | `rc/**` push (dev), `main` 머지 (dev → prod) |
| 릴리즈 게이트 | main 머지 PR 리뷰 (관례). GitHub Environment·브랜치 보호 규칙 미사용 (§3.5) |
| 버전 단일 소스 | `src/.../dataplatform/__init__.py` 의 `__version__` |
| rc 버전 형식 | `0.2.0.dev<KST ts>` (local label 은 Composer 가 거부 → 폐기, §2.3.1) |
| 릴리즈 이력 | prod 성공 후 `vX.Y.Z` 태그 + GitHub Release 자동 생성 |
| 버전 정책 | **main 머지 = 항상 registry 업로드.** PR 은 버전 bump 필수, 하락 금지 (§3.6) |

---

## 0.1 용어 — "업로드" 와 "배포" 는 다르다

이 파이프라인이 하는 일은 **wheel 을 Artifact Registry 에 올리는 것까지**다.
그 시점에 Airflow 에서 달라지는 것은 없다.

```
파이프라인 범위                        │  파이프라인 밖
──────────────────────────────────────┼──────────────────────────
rc push  → dev GAR 업로드              │
main 머지 → dev·prod GAR 업로드         │  소비 팀이 Composer 의
          + vX.Y.Z 태그/Release        │  requirements pin 을 변경
                                      │      ↓
                                      │  Composer 환경 업데이트
                                      │      ↓
                                      │  DAG 가 새 provider 를 사용  ← 여기가 실제 배포
```

소비 팀은 `===` 로 정확한 버전을 pin 한다(§1.3). 따라서 **우리가 `0.2.1` 을 prod registry 에 올려도
`===0.2.0` 을 pin 한 환경은 아무 영향을 받지 않는다.** 반영 시점과 대상은 각 팀이 정한다.

| 용어 | 뜻 | 주체 |
|---|---|---|
| 업로드 / 발행 | wheel 이 registry 에 적재됨 | 이 파이프라인 |
| 배포 | Composer 환경이 그 버전을 실제로 쓰기 시작함 | 소비 팀 |

`loupe` 는 gitops 로 파드까지 교체하므로 파이프라인이 곧 배포다. 이 프로젝트는 다르다.

> 문서·워크플로우에서 "머지하면 `0.2.1` 이 배포됩니다" 같은 표현이 보이면
> **"registry 에 올라간다"** 로 읽는다. Slack 알림은 이미 "업로드 완료" 로 표기를 정정했다.

---

# Part I. 인프라 실측

## 1.1 Artifact Registry

| 항목 | dev | prod |
|---|---|---|
| GCP project | `dev-dp-project-354904` | `prod-dp-project` |
| project number | `996471974382` | `398770835896` |
| Python registry | `dev-dp-python-registry` | `prod-dp-python-registry` |
| 리전 | `asia-northeast3` | `asia-northeast3` |

둘 다 **이미 존재**했다. 신규 생성 불필요.

## 1.2 Workload Identity Federation

| 항목 | dev | prod |
|---|---|---|
| pool | `pool` | `pool` |
| provider | `github` | `github` |
| attribute condition | `assertion.repository_owner == 'kakaoent'` | (동일 구조) |

**pool/provider 를 새로 만들 필요가 없다.** provider 조건이 조직 단위로 열려 있어 `kakaoent` 소속 레포면 모두 사용 가능하다. 레포별로 필요한 것은 SA 에 대한 `roles/iam.workloadIdentityUser` 바인딩뿐이다.

`loupe` 는 자기 프로젝트(`dev-loupe-project`, `prod-loupe-project`)의 pool 을 쓴다. 프로젝트가 다르므로 재사용은 불가하고, 구조만 같다.

## 1.3 소비 측 (Composer)

2026-08-04 관측 시점:

| 환경 | project | pin | 비고 |
|---|---|---|---|
| `dev-berriz-airflow` | dev | `===0.1.0` | |
| `test-airflow3` | dev | `===0.3.0` | ⚠️ registry 에 없는 버전 |
| `prod-berriz-airflow` | prod | `===0.1.0` | |

- 소비 팀은 `===` 로 **정확한 버전을 pin** 한다. 이 사실이 버전 설계의 전제가 된다 (아래 2.3).
- `prod-dp-airflow@prod-dp-project` 는 존재하지만 prod registry 의 `artifactregistry.reader` 에 **누락**돼 있다. prod DP Composer 가 이 패키지를 쓸 예정이면 조치 필요.
- `test-airflow3` 의 `0.3.0` 은 dev·prod 어디에도 없다. 레포의 초기 `__version__` 이 `0.3.0` 이었던 흔적으로, 올리려다 만 상태로 보인다.

---

# Part II. 버전 설계

## 2.1 단일 소스를 `__init__.py` 로 둔 이유

`pyproject.toml` 에서 `dynamic = ["version"]` + `[tool.setuptools.dynamic] version = {attr = ...}` 로 참조한다.

루트 `VERSION` 파일(사내 다른 프로젝트 관습)을 쓰지 않은 이유:

- 루트는 `src/` 밖이라 **wheel 에 포함되지 않는다.** `get_provider_info()` 가 런타임에 버전을 읽어야 하므로 파일 접근이 불가능하다.
- `importlib.metadata` 로 우회하면 패키지 미설치 상태(소스 직접 실행)에서 `PackageNotFoundError` 로 깨진다.
- wheel 에 넣으려고 패키지 안으로 옮기면 경로가 `src/airflow/providers/kakaoent/dataplatform/VERSION` 이 되어 **루트 `VERSION` 관습과 어차피 어긋난다.** 일관성 이득이 사라진다.
- Airflow provider 생태계 관습도 `__version__` 이다.

setuptools 가 `__init__.py` 를 AST 로 정적 파싱하므로 빌드 시 `airflow` import 가 불필요하다 (isolated build 환경에서 동작 확인).

## 2.2 PEP 440 제약 — loupe 태그 형식을 쓸 수 없다

`loupe` 는 **Docker 이미지 태그**를 만들고, 이미지 태그는 자유 문자열이다.
이 프로젝트는 **wheel 을 Python format registry** 에 올리므로 버전이 PEP 440 을 지켜야 한다.

```
❌ rc-v0.0.30-6c29da3-20260803-154006
❌ 0.0.30.6c29da3.20260803154006
✅ 0.0.30.dev20260803154006+6c29da3
```

**대시·언더바 자체는 제한이 아니다.** PEP 440 은 `-`, `_`, `.` 를 구분자로 허용하고 정규화한다 (`1.0-rc1` → `1.0rc1`). 실제 제약은:

| 요소 | 판정 | 이유 |
|---|---|---|
| `rc-` 로 시작 | ❌ | 버전은 숫자 릴리즈 세그먼트로 시작해야 함 (`v` 접두사만 예외) |
| 릴리즈 세그먼트 뒤의 hex | ❌ | 그 자리엔 숫자만 (`rc1`, `.post1`, `.devN`) |
| `+` 뒤 local label 의 대시 | ✅ | 자유. `.` 으로 정규화 |

즉 `0.0.30+6c29da3-20260803-154006` → `0.0.30+6c29da3.20260803.154006` 로 PEP 440 상 유효하다.

> 단, 유효한 것과 쓸 수 있는 것은 다르다. local label 은 결국 폐기했다 — §2.3.1 참조.

## 2.3 local label 만으로는 안 되는 이유

`0.2.0+6c29da3` 처럼 prerelease 마커 없이 local label 만 붙이면 두 가지가 깨진다.

```
정렬:  0.2.0.dev... < 0.2.0rc1 < 0.2.0 < 0.2.0+6c29da3
                                          ↑ 정식 릴리즈보다 높게 정렬됨

==0.2.0 pin:  0.2.0+6c29da3  →  ⚠️ 매칭됨
```

소비 팀이 `===0.2.0` 으로 pin 한 상태에서 rc 빌드를 받아갈 수 있다.
따라서 **`.dev` 또는 `rc` 를 반드시 함께 붙여야 한다.** 그러면 pip 후보 선정에서 기본 제외된다.

## 2.3.1 local label 은 쓸 수 없다 — Composer 가 거부 (2026-08-05 실측)

`0.2.0.dev20260805142851+350943a` 를 dev GAR 에 올리는 것까지는 **성공했다.**
GAR 은 local label 을 정규화 없이 그대로 저장한다.

그러나 **Composer 의 PyPI 패키지 입력 검증이 이를 거부한다.**

```
PyPI 패키지의 extras 및 버전에는 extras(선택사항)와 versionspec(선택사항)을
차례로 입력해야 합니다. 예를 들어 '>=1.10.3'과 같이 입력합니다.
자세한 내용은 PEP 508의 'extras' 및 'versionspec' 문법을 참조하세요.
```

PEP 508 문법 자체는 version 에 `+` 를 허용한다.

```
version = wsp* ( letterOrDigit | '-' | '_' | '.' | '*' | '+' )+
```

즉 Composer 의 검증이 **PEP 508 보다 좁고, `+` 에서 파싱이 끊긴다.**
`packaging.requirements.Requirement` 로는 `===0.2.0.dev20260805142851+350943a` 가 정상 파싱되므로 표준 준수 문제가 아니라 Composer 구현 제약이다.

**결론**: 소비 팀이 pin 을 넣을 수 없으면 rc 배포의 목적 자체가 사라지므로 버전에서 sha 를 제외한다.

```
0.2.0.dev20260805142851        ← 확정
0.2.0.dev20260805142851+350943a  ← 폐기 (GAR 은 OK, Composer 거부)
```

커밋 추적은 Slack 알림의 `Link: Code Open` 과 타임스탬프로 찾는 Actions run 으로 대체한다.

> 교훈: 산출물 저장소(GAR)가 받아준다고 소비 측이 받아준다는 보장이 없다.
> 버전 형식은 **생산 측과 소비 측 양쪽에서 검증**해야 한다.

## 2.4 유일성을 `.dev` 세그먼트에 둔 이유

`0.2.0.dev0+<sha>.<ts>` 처럼 유일성을 전부 local label 에 실으면, **GAR 이 local label 을 떼거나 정규화할 경우 모든 rc 빌드가 `0.2.0.dev0` 로 뭉개져 두 번째 push 부터 409** 로 실패한다.

타임스탬프를 `.dev` 세그먼트에 두면 label 처리 방식과 무관하게 유일성이 유지된다.

> 검증 결과: GAR 은 local label 을 수락했으나 **Composer 가 거부**하여 `+<sha>` 를 제거했다 (§2.3.1).
> 유일성이 `.dev` 세그먼트에 있었기 때문에 그 조각만 떼고 나머지는 그대로 동작했다.

## 2.5 타임스탬프 vs run_number

`github.run_number` 도 단조 증가라 정렬은 맞지만,

- **GAR 버전 목록에서 옛 버전을 골라 재배포할 때** `dev30` 은 정보가 없고 `dev20260804093015` 는 즉시 읽힌다
- run_number 는 워크플로우 파일 단위 카운터라 파일명 변경 시 1 로 리셋된다

→ KST 타임스탬프 채택. `loupe` 의 타임스탬프 생성 로직(`github.run_started_at` + 9h)을 그대로 재사용한다.

---

# Part III. 트리거 설계

## 3.1 왜 loupe 와 다른가

`loupe` 는 `develop` → dev, `release/vX.Y.Z` → prod, GitHub Release → prod 구조다. main 머지 트리거가 없다.
이는 제약이 아니라 **저장소 구조의 결과**다.

- loupe 태그는 배포 대상을 인코딩한다 (`v1.2.3`, `admin-v1.2.3`, `<tenant>-v1.2.3`). `trigger.on-release.yml` 에 validate job 이 3개인 이유다. **한 번의 머지로 "테넌트 X 의 api 만 배포" 를 표현할 수 없다.**
- gitops 배포라 `gitops/loupe/<module>/<phase>` 경로를 골라야 한다.
- 변경된 모듈만 빌드한다.

이 프로젝트는 **배포 대상이 하나**다. `1 머지 = 1 버전 = 1 wheel = 1 업로드` 로 대응되므로 태그 체계는 불필요한 간접층이다.

대신 loupe 가 GitHub Release 로 얻던 **릴리즈 노트**를 잃으므로, prod 업로드 성공 후 태그 + Release 를 자동 생성해 보완한다.

## 3.2 버전 변경 감지 — `HEAD^` 가 아니라 `event.before`

`main` push 시 `__version__` 이 바뀌었는지 비교하는데, `HEAD^` 를 쓰면 **rebase merge 에서 깨진다.**

```
버전 PR 이 커밋 3개, 버전 bump 가 첫 커밋인 경우

rebase 후 main:  ... ─ C1(0.2.0) ─ C2 ─ C3
                                    ↑     ↑
                                  HEAD^  HEAD
                     둘 다 0.2.0 → "변경 없음" → 릴리즈가 조용히 skip
```

실측 결과:

| 머지 전략 | main 새 커밋 | `HEAD^` | `event.before` |
|---|---|---|---|
| merge commit | 4 | publish ✓ | publish ✓ |
| squash | 1 | publish ✓ | publish ✓ |
| **rebase** | 3 | **skip ❌** | publish ✓ |

레포에 rebase merge 가 활성화돼 있어 `github.event.before`(push 이전 main SHA) 비교를 채택했다. 머지 전략·커밋 개수에 무관해진다.

## 3.3 버전 미변경 머지는 skip — 안전망

버전이 그대로면 업로드를 건너뛴다. 같은 버전 재업로드는 409 이므로 실패 알림이 뜨는 것을 막기 위함이다.
조용히 넘어가면 "버전 올리기를 잊은 것" 을 놓치므로 Slack 과 job summary 에 경고를 남긴다.

> **정상 경로에서는 도달하지 않는다.** §3.6 의 PR 검사(`version-diff`)가 버전 미변경 PR 을
> 먼저 실패시키기 때문이다. 체크를 무시하고 머지하거나 main 에 직접 push 한 경우의 안전망이다.

## 3.4 버전은 브랜치명에서 뽑지 않는다

초기 설계는 `rc/v0.2.0` 브랜치명에서 버전을 추출했으나, main 릴리즈는 `__init__.py` 를 쓰므로 **두 소스가 어긋날 수 있었다** (rc 브랜치만 만들고 `__version__` bump 를 잊은 경우 → dev 는 `0.2.0.dev...`, main 머지는 구버전).

일치 검사를 추가하는 대신 **rc 도 `__init__.py` 를 읽도록** 통일했다. 결과적으로 브랜치명 형식 제약이 사라져 `rc/` 접두사만 요구한다.

## 3.5 승인 게이트는 파이프라인 밖에 둔다

초기 설계는 GitHub Environment `prod` 에 required reviewers 를 걸어 prod 업로드 직전에 멈추게 했다. 철회했다.

- 팀 절차상 **버전업 PR 을 팀 리드가 approve** 해야 머지된다. 승인은 이미 PR 에서 이뤄지므로 Environment 승인은 같은 산출물에 대한 이중 승인이다.
- 보호 규칙 없는 `environment:` 선언은 게이트가 있는 것처럼 보이면서 실제로는 통과한다. 오해를 남기므로 선언 자체를 제거했다.

브랜치 보호 규칙도 쓰지 않는다. 한때 적용했으나 사내 다른 레포(`loupe`, `berrizdata-airflow-dags`)가 모두 쓰지 않아 관례를 통일했다. `.github/CODEOWNERS` 로 리뷰어를 자동 지정하고, 승인은 관례로 운영한다.

## 3.6 버전 하락 방지

### 왜 필요한가

버전 변경 감지(§3.2)는 **"값이 달라졌는가" 만** 본다. 낮추는 것도 "달라졌다" 이므로 그대로 배포된다.
실제로 첫 릴리즈에서 `0.3.0 -> 0.2.0` 이 통과했다 (main 의 초기 스켈레톤 값이 `0.3.0`,
실제 배포 최신은 `0.1.0` 이었으므로 그때는 의도된 조정이었다).

main 은 릴리즈 라인이 하나뿐이다. 이후의 하락은 사실상 실수(오타·잘못된 rebase·구버전 덮어씀)이고,
낮은 버전이 registry 에 올라가면 소비 팀의 "최신" 해석이 어긋난다. 따라서 낮추는 방향은 막는다.

### 어디서 막는가

같은 규칙을 **세 지점에서 확인**한다. 릴리즈 작업을 진행하는 순서다.

```
① 로컬에서 pytest        → 테스트 실패    "이미 배포된 v0.3.0 보다 낮습니다"
② PR 을 열면             → 체크 실패      "base(main) 의 0.3.0 보다 낮습니다"
③ main 에 머지되면        → 업로드 중단     "머지 직전 main 의 0.3.0 보다 낮습니다"
```

앞 단계에서 잡히면 뒤 단계까지 갈 일이 없다. ①이 가장 빠른 피드백, ③이 마지막 안전망이다.

### 왜 비교 대상이 셋 다 다른가

각 지점에서 알 수 있는 정보가 다르기 때문이다. 로컬에는 PR 정보가 없고, PR 시점에는 아직 머지 결과가 없다.
같은 기준을 억지로 맞추려면 각 지점이 필요 없는 정보를 끌어와야 한다.

| | 구현 | 무엇과 비교 |
|---|---|---|
| ① | `tests/test_version.py` | 최신 `v*` 태그 = **실제로 배포된 것** |
| ② | `version-diff` (`trigger.on-pr.yml`) | base 브랜치의 `__version__` |
| ③ | `check-version` (`trigger.on-push-main.yml`) | `event.before` 의 `__version__` (머지 직전 main) |

결과적으로 rc 브랜치에서 잘못 낮춘 것, PR 로 올라온 것, 머지 과정에서 어긋난 것이
각기 다른 지점에서 잡힌다.

### ①이 태그를 기준으로 삼은 이유

릴리즈 워크플로우가 **prod 업로드 성공 후에만** `vX.Y.Z` 태그를 만든다. 즉 태그 = 실제 배포 기록이다.
Artifact Registry 를 직접 조회하는 방법도 있으나 GCP 인증이 필요해 로컬 개발·CI 단위 테스트에서는 쓸 수 없다.

부수 효과로 `unit-tests.yml` 의 checkout 에 `fetch-depth: 0` 이 필요해졌다.
기본 checkout 은 태그를 가져오지 않아 해당 테스트가 **조용히 skip** 된다.

### ②를 추가한 이유 — 버전 bump 를 필수로

`check-version`(③)은 머지 뒤에 돌기 때문에 "이 PR 을 머지하면 무슨 버전이 올라가는가" 를 리뷰어가
판단할 근거가 PR 단계에 없었다.

**main 머지 = 릴리즈** 이므로 main 으로 가는 PR 은 버전 bump 를 반드시 포함해야 한다.
따라서 `version-diff` 는 안내에 그치지 않고 **미변경도 실패**로 처리한다.

```
🚀 머지하면 `0.3.0` 가 배포됩니다        통과. + base/head 대조표
❌ 버전을 올려주세요 (base 와 같은 0.2.0)  실패
❌ 버전이 하락했습니다 (0.3.0 -> 0.2.0)   실패
```

문서·CI 수정 PR 도 patch bump 를 요구한다. 버전이 그대로면 머지해도 아무것도 올라가지 않으므로,
"올리는 것을 잊은 릴리즈 PR" 과 구분할 방법이 없기 때문이다.

**부작용**: `version-diff` 가 실패하면 GitHub 이 같은 워크플로우의 `unit-test`·`build-check` 를
취소한다. 모든 step 은 success 인데 job 결론만 `cancelled` 이 되고 PR 체크에는 실패로 보인다.
버전만 고치면 함께 다시 도므로 그대로 두었다.

**③의 skip 경로는 이제 정상 흐름이 아니다.** PR 단계에서 먼저 막히므로, 체크를 무시하고 머지하거나
main 에 직접 push 한 경우에만 도달한다. 안전망으로 남긴다 (409 실패 대신 조용히 skip).

### 구현 함정 두 가지

- **문자열 비교 금지.** `0.10.0 < 0.9.0` 이 되어버린다. `packaging.version.Version` 으로 비교한다.
- **`|| exit 1` 명시.** heredoc 으로 실행한 python 이 실패해도 스크립트가 계속 진행돼 job 이
  exit 0 으로 끝난다. Actions 기본 셸이 `bash -e` 라 실제로는 중단될 수 있으나 그것에 의존하지 않는다.
  (실측 확인 — 로컬 bash 에서 exit 0 이 나왔다)

---|---|---|---|
| `tests/test_version.py` | 로컬 `pytest` / 모든 CI 체크 | 최신 `v*` 태그 | 테스트 실패 |
| `version-diff` (`trigger.on-pr.yml`) | PR 열림·갱신 | base 브랜치의 `__version__` | 체크 실패 |
| `check-version` (`trigger.on-push-main.yml`) | main 머지 후 | `event.before` 의 `__version__` | 업로드 중단 |

**기준이 서로 다른 것이 의도적이다.** 각각 "실제 배포된 것" / "머지 대상" / "머지 직전 main" 을 본다.
rc 브랜치에서 잘못 낮춘 것, PR 로 올라온 것, 머지 과정에서 어긋난 것을 각기 다른 지점에서 잡는다.

### 태그를 기준으로 삼은 이유 (테스트 층)

릴리즈 워크플로우가 **prod 업로드 성공 후에만** `vX.Y.Z` 태그를 만들기 때문에 실제 배포 기록과 일치한다.
Artifact Registry 를 직접 조회하는 방법도 있으나 GCP 인증이 필요해 로컬 개발·CI 단위 테스트에서는 쓸 수 없다.

부수 효과로 `unit-tests.yml` 의 checkout 에 `fetch-depth: 0` 이 필요해졌다.
기본 checkout 은 태그를 가져오지 않아 해당 테스트가 **조용히 skip** 된다.

### PR 층을 추가한 이유

`check-version` 은 머지 뒤에 돌기 때문에 "이 PR 을 머지하면 배포가 되는가" 를 리뷰어가
판단할 근거가 PR 단계에 없었다. `version-diff` 가 그 정보를 Actions job summary 에 표시한다.

```
🚀 머지하면 `0.3.0` 가 배포됩니다        + base/head 대조표
⏭️ 버전 미변경 — 머지해도 업로드되지 않습니다
❌ 버전이 하락했습니다 (0.3.0 -> 0.2.0)   ← 체크 실패
```

"버전 올리는 것을 잊은 릴리즈 PR" 이 조용히 머지되는 것도 여기서 드러난다.

### 구현 함정 두 가지

- **문자열 비교 금지.** `0.10.0 < 0.9.0` 이 되어버린다. `packaging.version.Version` 으로 비교한다.
- **`|| exit 1` 명시.** heredoc 으로 실행한 python 이 실패해도 스크립트가 계속 진행돼 job 이
  exit 0 으로 끝난다. Actions 기본 셸이 `bash -e` 라 실제로는 중단될 수 있으나 그것에 의존하지 않는다.
  (실측으로 확인 — 로컬 bash 에서 exit 0 이 나왔다)

---

# Part IV. 최종 파이프라인

| 트리거 | 테스트 | 빌드 | 업로드 | 버전 검사 |
|---|---|---|---|---|
| PR → `main` | ✅ | ✅ (검증만) | — | `version-diff` (업로드될 버전 안내 + bump 필수 + 하락 차단) |
| push `rc/**` | ✅ | `0.2.0.dev<ts>` | dev | 테스트 층만 |
| push `main` (버전 ↑) | ✅ | `0.2.0` | dev → prod → 태그 | `check-version` (하락 차단) |
| push `main` (버전 =) | ✅ | — | skip | — |

```
.github/workflows/
├── trigger.on-pr.yml                    PR → 테스트 + 빌드 검증
├── trigger.on-push-branches-for-rc.yml  rc/** → dev GAR
├── trigger.on-push-main.yml             main → dev → 승인 → prod → 태그
├── unit-tests.yml                       [재사용] ruff + pytest
├── upload-wheel-to-gar.yml              [재사용] WIF + 빌드 + twine
└── notify-on-slack.yml                  [재사용] 배포 알림
```

`trigger.*` 가 트리거만 정의하고 나머지를 `workflow_call` 로 조합하는 loupe 컨벤션을 따른다.

## 4.1 승인 게이트

`upload-wheel-to-gar.yml` 의 job 에 `environment: ${{ inputs.env-name }}` 를 선언했다.
GitHub Environment `prod` 에 required reviewers 를 설정하면 해당 job 이 승인 대기로 멈춘다.

**Environment 는 처음 참조될 때 보호 규칙 없이 자동 생성된다.** 즉 승인 게이트는 YAML 이 아니라 레포 설정에만 존재하며, 설정하지 않으면 prod 가 그냥 통과한다.

조직이 enterprise 플랜이라 private 레포에서도 protection rule 사용 가능하다.

## 4.2 rc 빌드 시 버전 주입

버전 단일 소스가 `__init__.py` 이므로, rc 빌드는 그 파일의 `__version__` 줄만 `sed` 로 치환한 뒤 빌드한다. 커밋하지 않으므로 레포에 영향이 없고, wheel 파일명 · METADATA · `get_provider_info()` 가 모두 따라온다.

파일 전체를 덮어쓰지 않고 해당 줄만 치환하는 이유는 나중에 `__init__.py` 에 다른 내용이 추가돼도 안전하게 하기 위함이다.

---

# Part V. 미결 사항

## 5.1 해소된 항목

| 항목 | 결과 |
|---|---|
| WIF 바인딩 | ✅ dev·prod 양쪽 `dp-ops-cicd` 에 `kakaoent/dp-airflow-provider` 추가 완료 (2026-08-05) |
| GAR local label 허용 여부 | ✅ GAR 은 수락. 단 **Composer 가 거부**하여 최종 폐기 (§2.3.1) |
| dev GAR cleanup policy | ✅ 1년 경과 버전 삭제 적용 ([[1_Airflow Provider 배포 셋업 런북]] Part II-B) |
| prod 승인자 | ✅ 불필요로 결론. GitHub Environment 를 쓰지 않고 main 브랜치 보호 규칙(Code Owner 리뷰)으로 단일화 (§3.5) |
| `sts.googleapis.com` (dev 미활성) | ✅ 활성화 완료 |
| main 파이프라인 | ✅ 2026-08-05 전 구간 성공. prod 업로드 · `v0.2.0` 태그 · Release 생성 확인 |
| dev Composer 실검증 | ✅ rc 버전 pin 하여 설치 확인 |

## 5.2 남은 항목

| 항목 | 내용 |
|---|---|
| `prod-dp-airflow` reader 누락 | prod DP Composer 가 이 패키지를 쓸 예정이면 `artifactregistry.reader` 부여 필요 |
| `test-airflow3` 의 `0.3.0` pin | registry 에 없는 버전. 현재 설치 불가 상태 |
| CHANGELOG | GitHub Release 자동 노트로 대체했으나 별도 파일이 필요한지 미정 |
| 버전 미변경 main 머지 → skip | 유일하게 미실측인 경로. 버전 안 올린 머지가 생기면 자연히 검증됨 |

---

## 관련 노트

- [[1_Airflow Provider 배포 셋업 런북]] — GCP IAM · GitHub 설정 절차
- [[0_Cloud Composer 인프라 보고]]
- [[6_배포와 환경 분리]] (dbt) — 환경 분리 관점 비교
