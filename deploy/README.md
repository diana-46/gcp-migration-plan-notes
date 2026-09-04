# deploy — 컨텍스트

## 풀고자 하는 문제 / 의사결정

공통 Airflow Provider 패키지(`kakaoent/dp-airflow-provider` → `apache-airflow-providers-kakaoent-dataplatform`)를
**어떻게 빌드·업로드·소비**할 것인가. 그리고 통합 Airflow 환경을 쓰는 팀들에게 줄 사용 규약.

> 용어 주의: 이 파이프라인은 **Artifact Registry 업로드까지**다. 실제 "배포"는 소비 팀이 Composer에 반영하는 시점 (설계 노트 §0.1).

## 노트

- [[0_Airflow Provider 배포 파이프라인 설계]] — 설계 확정. wheel → GCP Artifact Registry, WIF 인증, dev/prod 분리, 버전 정책
- [[1_Airflow Provider 배포 셋업 런북]] — 워크플로우 커밋 전 GCP·GitHub 설정 절차. rc·main 전 경로 검증 완료
- [[2_공유 Airflow 사용 가이드]] — 통합 Airflow(`dev-data-airflow`)에 DAG·dbt를 올리는 팀용 규약. **위키 게시용 초안**

## Stack / 버전

- 인증: Workload Identity Federation (SA JSON key 미사용)
- 빌드 트리거: `rc/**` push → dev, `main` 머지 → prod
- 버전 단일 소스: `__init__.py`의 `__version__`, rc 형식 `X.Y.Z.dev<KST ts>`

## 관련 자료

- GitHub: `kakaoent/dp-airflow-provider`
- 관련 폴더: [[../스케줄러/7_3_공통 Custom Operator 제공 방안|스케줄러/7_3_공통 Custom Operator 제공 방안]]
