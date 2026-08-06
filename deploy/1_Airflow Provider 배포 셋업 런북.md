# Airflow Provider 배포 셋업 런북

**대상**: `kakaoent/dp-airflow-provider`
**목적**: 워크플로우 커밋 전에 완료해야 하는 GCP · GitHub 설정 절차
**설계 배경**: [[0_Airflow Provider 배포 파이프라인 설계]]
**상태**: dev 경로 검증 완료. main 파이프라인 실측만 남음 (§4 체크리스트)

---

## 0. 왜 커밋 전에 해야 하나

워크플로우는 WIF 로 인증한다. IAM 바인딩이 없으면 **첫 배포가 인증 단계에서 실패**한다.

순서:

```
① GCP IAM 바인딩  →  ② GitHub Secret  →  ③ 워크플로우 커밋  →  ④ rc push 로 dev 경로 검증  →  ⑤ main 머지
```

④ 를 main 머지보다 먼저 두는 이유: rc 경로는 dev 만 건드리므로 실패해도 영향이 없고,
테스트·버전생성·빌드·인증·업로드를 순서대로 확인할 수 있다.

---

# Part I. GCP IAM

## 1.1 CI 서비스 어카운트 (확정)

| 환경 | SA |
|---|---|
| dev | `dp-ops-cicd@dev-dp-project-354904.iam.gserviceaccount.com` |
| prod | `dp-ops-cicd@prod-dp-project.iam.gserviceaccount.com` |

WIF pool/provider 는 양 프로젝트에 `pool` / `github` 로 **이미 존재**하므로 신규 생성 불필요
([[0_Airflow Provider 배포 파이프라인 설계]] §1.2).

## 1.2 실측 결과 — 필요한 작업은 WIF 바인딩 1건씩

작업 착수 시점(2026-08-04) 상태:

| SA | `artifactregistry.writer` | WIF 바인딩에 이 레포 |
|---|---|---|
| dev `dp-ops-cicd` | ✅ **project 레벨 보유** | ❌ `kakaoent/athlon`, `kakaoent/athlon-ui` 만 |
| prod `dp-ops-cicd` | ✅ **project 레벨 보유** | ❌ 없음 (`serviceAccountUser` 만) |

> 2026-08-05 현재 두 바인딩 모두 추가 완료. dev 에는 `berrizdata-airflow-dags`,
> `berrizdata-dbt` 도 함께 등록됐다.

**`artifactregistry.writer` 는 양쪽 다 project 레벨에 이미 있으므로 registry 단위 부여가 불필요하다.**
남은 것은 `roles/iam.workloadIdentityUser` 의 principalSet 에 `kakaoent/dp-airflow-provider` 를 추가하는 것뿐이다.

## 1.2.1 "레포 연결" 은 별도 절차가 없다

Cloud Build 처럼 GitHub App 을 GCP 에 연결하는 단계가 **없다.** GCP 는 GitHub 레포를 사전 등록하지 않는다.

```
① GitHub Actions (id-token: write) → OIDC 토큰 발급
     claims: { repository: "kakaoent/dp-airflow-provider", repository_owner: "kakaoent" }
② google-github-actions/auth → GCP STS 로 토큰 전송
③ provider 검증: issuerUri + attributeCondition(repository_owner == 'kakaoent')
④ SA 바인딩 확인 ← 레포별 인가 지점
     principalSet://.../attribute.repository/kakaoent/dp-airflow-provider
⑤ 통과 시 단기 토큰 발급
```

**§1.4 의 `add-iam-policy-binding` 이 곧 "레포 연결"** 이다.
provider 조건이 조직 단위라 kakaoent 소속 레포는 모두 토큰을 받을 수 있으나, 가장 가능한 SA 는 자기를 principalSet 에 명시한 것뿐이므로 실질 관문은 SA 바인딩이다.

## 1.2.2 API 활성 상태 (실측)

| API | dev-dp | prod-dp | loupe dev | loupe prod |
|---|---|---|---|---|
| `artifactregistry.googleapis.com` | ✅ | ✅ | ✅ | ✅ |
| `iam.googleapis.com` | ✅ | ✅ | ✅ | ✅ |
| `iamcredentials.googleapis.com` | ✅ | ✅ | ✅ | ✅ |
| `sts.googleapis.com` | ✅ | ✅ | ✅ | ✅ |

`sts.googleapis.com` 은 `dev-dp-project-354904` 에만 빠져 있었다. WIF 토큰 교환(STS) 경로에 관여하고, **동작이 검증된 loupe 는 dev·prod 양쪽 다 활성**이므로 기준선을 맞췄다.

```bash
# 2026-08-04 실행 완료
gcloud services enable sts.googleapis.com --project=dev-dp-project-354904
```

## 1.3 principalSet 형식

레포 단위로 바인딩한다. `attribute.repository` 값이 `<org>/<repo>` 다.

```
principalSet://iam.googleapis.com/projects/<PROJECT_NUMBER>/locations/global/workloadIdentityPools/pool/attribute.repository/kakaoent/dp-airflow-provider
```

| 환경 | PROJECT_NUMBER |
|---|---|
| dev | `996471974382` |
| prod | `398770835896` |

## 1.3.1 권한 실측 — IAM 바인딩은 직접 못 한다

`diana.46@kakaoent.com` 기준 (2026-08-04 실측):

| 작업 | 권한 | 가능 |
|---|---|---|
| SA IAM 바인딩 추가 | `iam.serviceAccounts.setIamPolicy` | ❌ |
| SA IAM 조회 | `iam.serviceAccounts.getIamPolicy` | ✅ |
| API 활성화 | `serviceusage.services.enable` | ✅ (dev·prod 모두) |

→ **`sts.googleapis.com` 활성화는 직접 가능. WIF 바인딩 2건은 프로젝트 owner 그룹에 요청 필요.**

| 프로젝트 | `roles/owner` |
|---|---|
| dev | `group:gi-admin-dev-dp-project@kakaoent.com` |
| prod | `group:gi-admin-prod-dp-project@kakaoent.com` |

### 요청서 (그대로 전달 가능)

> **제목**: `dp-airflow-provider` 레포에 WIF 바인딩 추가 요청
>
> **배경**
> `kakaoent/dp-airflow-provider` 에 GitHub Actions 배포 파이프라인을 구성했습니다.
> Airflow provider wheel 을 dev/prod Artifact Registry(Python) 에 업로드합니다.
> 인증은 사내 표준대로 Workload Identity Federation 을 사용하며, SA JSON key 는 쓰지 않습니다.
>
> **요청 내용**
> 기존 CI SA `dp-ops-cicd` 의 `roles/iam.workloadIdentityUser` principalSet 에
> 이 레포를 추가해주세요. 기존 멤버(`athlon` 등)는 유지되며 추가만 필요합니다.
>
> ```bash
> # dev
> gcloud iam service-accounts add-iam-policy-binding \
>     dp-ops-cicd@dev-dp-project-354904.iam.gserviceaccount.com \
>     --project=dev-dp-project-354904 \
>     --role=roles/iam.workloadIdentityUser \
>     --member="principalSet://iam.googleapis.com/projects/996471974382/locations/global/workloadIdentityPools/pool/attribute.repository/kakaoent/dp-airflow-provider"
>
> # prod
> gcloud iam service-accounts add-iam-policy-binding \
>     dp-ops-cicd@prod-dp-project.iam.gserviceaccount.com \
>     --project=prod-dp-project \
>     --role=roles/iam.workloadIdentityUser \
>     --member="principalSet://iam.googleapis.com/projects/398770835896/locations/global/workloadIdentityPools/pool/attribute.repository/kakaoent/dp-airflow-provider"
> ```
>
> **추가 권한 불필요**
> `roles/artifactregistry.writer` 는 두 SA 모두 project 레벨에 이미 보유하고 있어
> registry 단위 부여가 필요하지 않습니다.
>
> **참고**
> WIF provider 조건이 `assertion.repository_owner == 'kakaoent'` 이므로 조직 내 레포는
> 토큰 발급 자체는 가능하지만, 위 바인딩이 없으면 해당 SA 를 가장할 수 없습니다.
> 즉 이 바인딩이 레포별 인가 지점입니다.

## 1.4 실행할 명령 (2건)

기존 principalSet(`athlon` 등)은 유지되고 이 레포가 **추가**된다.

```bash
# dev
gcloud iam service-accounts add-iam-policy-binding \
    dp-ops-cicd@dev-dp-project-354904.iam.gserviceaccount.com \
    --project=dev-dp-project-354904 \
    --role=roles/iam.workloadIdentityUser \
    --member="principalSet://iam.googleapis.com/projects/996471974382/locations/global/workloadIdentityPools/pool/attribute.repository/kakaoent/dp-airflow-provider"

# prod
gcloud iam service-accounts add-iam-policy-binding \
    dp-ops-cicd@prod-dp-project.iam.gserviceaccount.com \
    --project=prod-dp-project \
    --role=roles/iam.workloadIdentityUser \
    --member="principalSet://iam.googleapis.com/projects/398770835896/locations/global/workloadIdentityPools/pool/attribute.repository/kakaoent/dp-airflow-provider"
```

## 1.5 검증

```bash
gcloud iam service-accounts get-iam-policy \
    dp-ops-cicd@dev-dp-project-354904.iam.gserviceaccount.com \
    --project=dev-dp-project-354904

gcloud iam service-accounts get-iam-policy \
    dp-ops-cicd@prod-dp-project.iam.gserviceaccount.com \
    --project=prod-dp-project
```

`attribute.repository/kakaoent/dp-airflow-provider` 가 `roles/iam.workloadIdentityUser` 멤버에 보이면 완료.

## 1.6 워크플로우 반영값 (완료)

`trigger.on-push-branches-for-rc.yml`, `trigger.on-push-main.yml` 에 반영됨.

| 환경 | provider | service account |
|---|---|---|
| dev | `projects/996471974382/locations/global/workloadIdentityPools/pool/providers/github` | `dp-ops-cicd@dev-dp-project-354904.iam.gserviceaccount.com` |
| prod | `projects/398770835896/locations/global/workloadIdentityPools/pool/providers/github` | `dp-ops-cicd@prod-dp-project.iam.gserviceaccount.com` |

---

# Part II. GitHub 설정

## 2.1 GitHub Environment 는 쓰지 않는다 (결정 변경)

초기 설계는 Environment `prod` 에 required reviewers 를 걸어 승인 게이트로 쓰려 했으나 철회했다.

이유:
- 팀의 릴리즈 절차상 **버전업 PR 을 팀장이 approve** 해야 머지된다. 즉 승인은 이미 PR 에서 이뤄진다. Environment 승인을 더하면 같은 산출물을 두 번 승인하게 된다.
- 보호 규칙 없는 `environment:` 선언은 게이트가 있는 것처럼 보이면서 실제로는 통과한다. 오해를 남기므로 선언 자체를 제거했다.

조치: `upload-wheel-to-gar.yml` 의 `environment:` 삭제, 자동 생성됐던 `dev` Environment 삭제.
릴리즈 게이트는 PR 리뷰 관례 한 곳으로 단일화했다 (§2.4).

## 2.2 Secret `SLACK_WEBHOOK_URL`

```
Settings → Secrets and variables → Actions → New repository secret
  이름: SLACK_WEBHOOK_URL
```

`loupe` 도 같은 이름을 **레포 레벨**에 두고 있다 (조직 레벨 아님). 값을 재사용하거나 provider 배포용 채널 webhook 을 새로 발급한다.

## 2.3 `GCP_SA_KEY_DEV` 삭제

WIF 전환으로 불필요해진 장기 크리덴셜. 방치하면 쓰이지도 않는 키가 남는다.

## 2.4 브랜치 보호 규칙 — 사용하지 않는다 (결정 변경)

한때 적용했다가 제거했다. 사내 다른 레포와 관례를 맞춘다.

```bash
# 실측: 두 레포 모두 main 보호 규칙 없음
gh api repos/kakaoent/loupe/branches/main/protection                    # 404
gh api repos/kakaoent/berrizdata-airflow-dags/branches/main/protection  # 404
```

**릴리즈 승인은 PR 리뷰 관례로 운영한다.** `.github/CODEOWNERS` 가 리뷰어를 자동 지정하고,
버전업 PR 은 팀 리드 승인 후 머지한다. 기술적 강제는 없다.

> 적용해봤다가 제거한 이유: 작성자는 자기 PR 을 approve 할 수 없어(GitHub 제약)
> 파이프라인 최초 검증 단계에서 머지가 막혔다. `--admin` 우회도 가능했으나
> 다른 레포와 관례를 통일하는 쪽으로 정했다.
>
> 다시 걸려면:
> ```bash
> gh api -X PUT repos/kakaoent/dp-airflow-provider/branches/main/protection --input protection.json
> # required_status_checks.contexts: ["unit-test / test", "build-check"]  (실측 확인된 이름)
> ```

---

# Part II-A. IAM 대기 중 우회 경로 (비상용)

## A.1 무엇이 막히고 무엇이 안 막히나

principalSet 에 이 레포가 없으면 **CI 업로드는 불가능**하다. 인증 4단계에서 거부되므로 `dp-ops-cicd` 가 `artifactregistry.writer` 를 갖고 있어도 그 SA 가 될 수 없어 무의미하다.

| 경로 | IAM 대기 중 |
|---|---|
| PR 테스트 · wheel 빌드 검증 | ✅ 동작 (GCP 인증 불필요) |
| rc/main 의 테스트 · 버전 생성 · 빌드 | ✅ 동작 |
| GAR 업로드 (CI) | ❌ 인증 단계에서 실패 |
| GAR 업로드 (수동) | ✅ 가능 (§A.2) |

즉 **파이프라인의 절반은 IAM 없이 검증 가능**하다. rc 브랜치를 push 하면 인증 실패 로그가 남고, 그 자체가 요청 근거로도 쓰인다.

## A.2 수동 업로드

`diana.46@kakaoent.com` 은 dev·prod 양 registry 에 `artifactregistry.repositories.uploadArtifacts` 를 보유한다 (실측). `0.1.0` 이 2026-07-28 에 올라간 경로가 이것으로 추정된다 — 당시 워크플로우 실행 이력이 0건이었다.

```bash
rm -rf dist build
python -m build --wheel

# dev
python -m twine upload \
    --repository-url https://asia-northeast3-python.pkg.dev/dev-dp-project-354904/dev-dp-python-registry/ \
    dist/*.whl

# prod
python -m twine upload \
    --repository-url https://asia-northeast3-python.pkg.dev/prod-dp-project/prod-dp-python-registry/ \
    dist/*.whl
```

`keyrings.google-artifactregistry-auth` 가 ADC 를 사용하므로 `gcloud auth application-default login` 이 되어 있어야 한다.

## A.3 수동 업로드를 기본 경로로 쓰지 않는 이유

- **버전이 소모된다.** 수동으로 `0.2.0` 을 올리면 파이프라인 첫 실행은 `0.3.0` 이어야 한다. IAM 이 곧 풀린다면 `0.2.0` 을 파이프라인 검증용으로 남기는 편이 낫다.
- 테스트 강제 · prod 승인 게이트 · Slack 알림 · 태그/릴리즈 노트 · 재현 가능한 빌드를 모두 잃는다. 파이프라인의 존재 이유가 그것이므로 대체재가 아니라 비상 경로다.

---

# Part II-B. Registry 보존 정책 (2026-08-04 적용)

| registry | cleanup policy | dry-run |
|---|---|---|
| `dev-dp-python-registry` | `olderThan: 365d` → DELETE | 아니오 (실제 삭제) |
| `prod-dp-python-registry` | 없음 (영구 보존) | — |

```bash
gcloud artifacts repositories set-cleanup-policies dev-dp-python-registry \
    --project=dev-dp-project-354904 --location=asia-northeast3 \
    --policy=policy.json --no-dry-run
# policy.json:
# [{"name":"delete-versions-older-than-1y","action":{"type":"Delete"},
#   "condition":{"olderThan":"365d"}}]
```

해제: `gcloud artifacts repositories delete-cleanup-policies dev-dp-python-registry --policynames=delete-versions-older-than-1y ...`

## B.1 왜 1년이고, KEEP 목록을 두지 않았나

cleanup policy 는 **접두사 매칭만** 지원한다(정규식 없음). dev registry 에는 rc 빌드(`0.2.0.dev...`)와 정식 릴리즈(`0.2.0`)가 섞이는데, `0.2.0` 은 `0.2.0.dev...` 의 접두사이기도 해서 **"`.dev` 만 삭제" 를 일반적으로 표현할 수 없다.**

따라서 정식 릴리즈도 함께 삭제 대상이 된다. 이를 KEEP 목록으로 보호하려면 pin 중인 버전을 계속 관리해야 하는데, **prod 가 영구 보존이라 복구 경로가 있으므로** 그 유지보수를 하지 않기로 했다.

기간을 1년으로 잡은 근거:
- 적용 시점 가장 오래된 산출물이 `0.1.0`(2026-07-28)이라 **향후 1년간 삭제되는 것이 없다.** 위험 구간 없이 걸 수 있어 dry-run 단계를 생략했다.
- 저장 용량은 애초에 문제가 아니다 (repo 전체 0.012MB, wheel 1개 ~20KB). 목적은 목록 정리다.

## B.2 삭제된 버전 복구

```bash
pip download apache-airflow-providers-kakaoent-dataplatform==0.1.0 \
    --index-url https://asia-northeast3-python.pkg.dev/prod-dp-project/prod-dp-python-registry/simple/ \
    --no-deps -d /tmp/recover

python -m twine upload \
    --repository-url https://asia-northeast3-python.pkg.dev/dev-dp-project-354904/dev-dp-python-registry/ \
    /tmp/recover/*.whl
```

재업로드하면 createTime 이 갱신되므로 다시 1년간 보존된다.

## B.3 관측된 기존 설정

`prod-dp-python-registry` 에 `cleanupPolicyDryRun: true` 가 이미 설정돼 있었다(정책은 없음). 정책이 없으므로 현재 동작에 영향이 없고, 향후 prod 에 정책을 걸면 dry-run 으로 시작된다 — 안전한 기본값이라 그대로 두었다.

---

# Part III. 첫 릴리즈 절차

`0.1.0` 은 dev·prod 양쪽에 **이미 배포돼 사용 중**이므로 재사용할 수 없다. `0.2.0` 부터 시작한다.

```bash
git checkout -b rc/v0.2.0
# __version__ = "0.2.0"  (이미 반영됨)
git push -u origin rc/v0.2.0
```

1. dev GAR 에 `0.2.0.dev<ts>+<sha>` 업로드 → Slack 확인
2. dev Composer 에 해당 버전 pin 해서 검증
3. `rc/v0.2.0` → `main` PR 머지
4. dev → prod 승인 → prod 업로드 → `v0.2.0` 태그 + Release

## 3.1 배포 이력 확인

```bash
for P in "dev-dp-project-354904 dev-dp-python-registry" "prod-dp-project prod-dp-python-registry"; do
  set -- $P
  echo "── $1 / $2 ──"
  gcloud artifacts versions list --project=$1 --location=asia-northeast3 \
      --repository=$2 --package=apache-airflow-providers-kakaoent-dataplatform \
      --format="table(name.basename(),createTime)"
done
```

---

# Part IV. 체크리스트

## GCP

- [x] CI SA 확정 — dev/prod 모두 `dp-ops-cicd@<project>`
- [x] `artifactregistry.writer` — 양쪽 project 레벨에 이미 보유 (추가 작업 없음)
- [x] 워크플로우에 `gcp-service-account` · `gcp-workload-identity-provider` 반영
- [x] dev: `sts.googleapis.com` 활성화 (2026-08-04 완료)
- [x] dev: `workloadIdentityUser` 바인딩 추가 (2026-08-05 완료)
- [x] prod: `workloadIdentityUser` 바인딩 추가 (2026-08-05 완료)

## GitHub

- [x] Secret `SLACK_WEBHOOK_URL` (2026-08-04 완료)
- [x] `GCP_SA_KEY_DEV` 삭제 (완료)
- [x] 워크플로우 커밋 + 테스트 PR (#1)
- [x] 브랜치 보호 규칙 — **사용하지 않음으로 결정** (사내 다른 레포와 통일, §2.4)
- [x] `.github/CODEOWNERS` — PR 리뷰어 자동 지정

## 별도 조치 (배포 파이프라인 외)

- [ ] `prod-dp-airflow` 에 prod registry `artifactregistry.reader` 부여 여부 판단
- [ ] `test-airflow3` 의 `===0.3.0` pin 정리 (registry 에 없는 버전)
- [x] dev GAR cleanup policy — 1년 경과 버전 삭제 (2026-08-04 적용, §B 참조)
- [x] local label `+<sha>` 검증 — GAR 은 수락하나 **Composer 가 거부**하여 버전에서 제거 (2026-08-05)

---

## 관련 노트

- [[0_Airflow Provider 배포 파이프라인 설계]] — 결정 근거
- 레포 내 실무 가이드: `.github/README.md`
