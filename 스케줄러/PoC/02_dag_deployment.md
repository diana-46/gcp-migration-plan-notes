---
title: "02. DAG 배포 검증 (GCS sync / DAG Bundle / Multi-repo)"
status: done
tags:
  - poc
  - dag-deployment
  - gcs-sync
  - composer3
created: 2026-05-19
updated: 2026-05-19
---

# 02. DAG 배포 검증

> **검증 질문**: Composer 3 에서 DAG 배포가 가능한가? Multi-repo 패턴은 어떻게?
> **답**: GCS bucket sync 표준. Multi-repo = sub-folder 분리로 OK. **DAG Bundle 은 Composer 가 lock**.

## 환경

| 항목 | 값 |
|---|---|
| Composer | `test-airflow3` (asia-northeast3) |
| Airflow | composer-3-airflow-3.1.7-build.8 |
| DAG bucket | `gs://dev-airflow-test-bucket/dags` |
| SA | `dev-dp-airflow@dev-dp-project-354904.iam.gserviceaccount.com` |
| 네트워킹 | 공개 IP |

## 🔒 발견 1: Composer 가 DAG Bundle 옵션을 명시적 lock

GCP 공식 문서 ["Airflow 3.1.7에서 차단된 구성"](https://cloud.google.com/composer/docs) 에서 확인:

| 섹션 | 차단된 옵션 |
|---|---|
| `dag_processor` | `dag_bundle_storage_path` |
| `dag_processor` | `dag_bundle_config_list` |
| `api_auth` | `jwt_*` (5개) |
| `cli` | 전체 |

→ Airflow 3 의 native DAG Bundles 기능 사용 불가. Composer 의 GCS sync 가 표준 강제.

## ✅ 발견 2: GCS sub-folder 로 Multi-repo 분리 동작

**검증 시나리오**:

```
로컬:
~/PycharmProjects/test-dag1/dags/hello_from_repo_a.py
~/PycharmProjects/test-dag2/dags/hello_from_repo_b.py

GCS:
gs://dev-airflow-test-bucket/dags/
├── test-dag1/hello_from_repo_a.py    ← gsutil rsync 로 sync
└── test-dag2/hello_from_repo_b.py    ← 별도 sync
```

**Sync 명령**:

```bash
gsutil -m rsync -r -d \
  ~/PycharmProjects/test-dag1/dags/ \
  gs://dev-airflow-test-bucket/dags/test-dag1/

gsutil -m rsync -r -d \
  ~/PycharmProjects/test-dag2/dags/ \
  gs://dev-airflow-test-bucket/dags/test-dag2/
```

**결과**:
- ✅ Airflow UI 에 `hello_from_repo_a` + `hello_from_repo_b` 둘 다 보임
- ✅ Schedule (`0 0 * * *`) 정상 동작
- ✅ Latest Run 성공 (2026-05-19 09:00:00 ✓)
- ✅ tags (`bundle-a` / `bundle-b`) 로 UI 필터링 가능

→ **Multi-repo / multi sub-folder 패턴 완벽 동작**.

## 💡 발견 3: 잡파일 (`.git`, README 등) 들어가도 Airflow 영향 X

- Airflow 는 `.py` 만 파싱 → `.git/`, `.md` 등 무시
- 단 깔끔하게 하려면 sync 시 `dags/` 폴더만 또는 `-x` exclude 사용

**권장 sync 패턴**:

```bash
# dags/ sub-folder 만 sync (권장)
gsutil -m rsync -r -d ~/PycharmProjects/REPO/dags/ \
  gs://dev-airflow-test-bucket/dags/REPO_NAME/
```

또는 `.airflowignore` 파일로 무시 지정.

## 회의 ammunition

| 질문 | 답 |
|---|---|
| Composer 환경 동작? | ✅ |
| DAG sync 가능? | ✅ GCS bucket 기준 |
| 사내 multi-repo (kakaopage/berriz/melon 등) 그대로? | ✅ sub-folder 분리 |
| Airflow 3 의 DAG Bundles 활용 가능? | ❌ Composer 가 lock |
| 사내 git 직결 가능? | ❌ → CI 자동화 (GitHub Actions / Jenkins) 로 git → GCS push 필요 |

## 사내 운영 시 매핑

```
사내 multi-repo (Jenkins or GitHub Actions)
  ├── airflow-dags-core
  ├── airflow-dags-kakaopage
  ├── airflow-dags-berriz
  └── airflow-dags-melon
        │
        │ CI: 각 repo 의 dags/ → GCS sub-folder
        ↓
gs://composer-prod-bucket/dags/
  ├── core/
  ├── kakaopage/
  ├── berriz/
  └── melon/
```

→ **사내 multi-repo 운영 패턴 그대로 옮겨갈 수 있음**.

## 미확정 / 추가 검증 필요

- [ ] 사내 git (`github.com`) → GCS sync CI 흐름 (외부 노출 / 인증)
- [ ] Composer DAGs bucket 의 `.airflowignore` 동작 검증
- [ ] DAG 100개+ 환경에서 parsing latency
- [ ] DAG 파일 변경 → Airflow UI 반영 시간 측정 (현재 ~1~2분)

## 다음 step

→ **검증 2: PyPI 자체 패키지 install** ([[03_custom_operator_pypi]])

## 관련

- [[README]] — PoC 인벤토리
- [[../2_Cloud Composer vs Self-managed 비교]] — DAG 배포 비교 행
- [[01_airflow3_compat_grep]] — 호환성 grep 결과
