# Airflow Provider 배포 파이프라인 설계

**대상**: `kakaoent/dp-airflow-provider` (`apache-airflow-providers-kakaoent-dataplatform`)
**작성 근거**: `loupe` CI/CD 구조 참조 + dev/prod GCP 인프라 실측 (2026-08-04)
**상태**: 설계 확정. 인프라 셋업은 [[1_Airflow Provider 배포 셋업 런북]] 참조

---

## 0. 요약

| 항목 | 결정 |
|---|---|
| 산출물 | Python wheel → GCP Artifact Registry (Python format) |
| 인증 | Workload Identity Federation (SA JSON key 미사용) |
| 환경 분리 | dev / prod 각 프로젝트에 별도 registry |
| 빌드 시점 | `rc/**` push (dev), `main` 머지 (dev → prod) |
| prod 게이트 | GitHub Environment `prod` + required reviewers |
| 버전 단일 소스 | `src/.../dataplatform/__init__.py` 의 `__version__` |
| rc 버전 형식 | `0.2.0.dev<KST ts>+<short sha>` |
| 릴리즈 이력 | prod 성공 후 `vX.Y.Z` 태그 + GitHub Release 자동 생성 |

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

> **미검증**: GAR 이 local label(`+<sha>`) 업로드를 허용하는지 확인되지 않았다 (PyPI 는 거부).
> 거부될 경우 `+<sha>` 만 제거하면 되고 나머지 설계는 그대로 동작한다.

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

## 3.3 버전 미변경 머지는 skip

문서·CI 수정 머지마다 409 실패 알림이 뜨는 것을 막는다. 단 조용히 넘어가면 "버전 올리기를 잊은 것"을 놓치므로 Slack 과 job summary 에 경고를 남긴다.

## 3.4 버전은 브랜치명에서 뽑지 않는다

초기 설계는 `rc/v0.2.0` 브랜치명에서 버전을 추출했으나, main 릴리즈는 `__init__.py` 를 쓰므로 **두 소스가 어긋날 수 있었다** (rc 브랜치만 만들고 `__version__` bump 를 잊은 경우 → dev 는 `0.2.0.dev...`, main 머지는 구버전).

일치 검사를 추가하는 대신 **rc 도 `__init__.py` 를 읽도록** 통일했다. 결과적으로 브랜치명 형식 제약이 사라져 `rc/` 접두사만 요구한다.

---

# Part IV. 최종 파이프라인

| 트리거 | 테스트 | 빌드 | 업로드 |
|---|---|---|---|
| PR → `main` | ✅ | ✅ (검증만) | — |
| push `rc/**` | ✅ | `0.2.0.dev<ts>+<sha>` | dev |
| push `main` (버전 ↑) | ✅ | `0.2.0` | dev → 승인 → prod → 태그 |
| push `main` (버전 =) | ✅ | — | skip |

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

## 5.2 남은 항목

| 항목 | 내용 |
|---|---|
| `prod-dp-airflow` reader 누락 | prod DP Composer 가 이 패키지를 쓸 예정이면 `artifactregistry.reader` 부여 필요 |
| `test-airflow3` 의 `0.3.0` pin | registry 에 없는 버전. 현재 설치 불가 상태 |
| CHANGELOG | GitHub Release 자동 노트로 대체했으나 별도 파일이 필요한지 미정 |
| main 파이프라인 실측 | prod 업로드 · `v0.2.0` 태그 · Release 자동 생성 경로가 아직 실행되지 않음 |
| dev Composer 실검증 | rc 버전을 pin 해서 provider 가 실제로 import 되는지 확인 |

---

## 관련 노트

- [[1_Airflow Provider 배포 셋업 런북]] — GCP IAM · GitHub 설정 절차
- [[0_Cloud Composer 인프라 보고]]
- [[6_배포와 환경 분리]] (dbt) — 환경 분리 관점 비교
