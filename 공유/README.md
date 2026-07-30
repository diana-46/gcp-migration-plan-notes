# 공유 — Neptune → dbt + 팀별 DAG 전환 제안

> 이 폴더는 DE / 매니저 / 플랫폼팀에 공유할 목적의 문서 세트. 각 파일은 하나의 관점을 다루며
> 독립적으로 읽어도 됨. 상세 근거는 `[[스케줄러/*]]`, `[[애슬론/*]]`, `[[dbt/*]]` 로 링크.

## 한 줄 요약

**BQ + Composer 이관을 계기로, SQL 은 dbt 로, DAG 은 팀별 저장소로.**
- SQL: git 안에, 로컬 초 단위 반복, lineage / 테스트 / 문서가 코드 옆
- DAG: 팀 자율 배포, 격리, ownership 명확, 비용·리소스 축소 축과 직결

## 파일 안내

| 문서 | 다루는 것 | 청중 |
|---|---|---|
| `1_배경과_결정.md` | 왜 지금, 두 결정 개요 | 전체 |
| `2_결정A_dbt로_왜_가는가.md` | Neptune SQL → dbt (SQL 축) | DE |
| `3_결정B_팀별_DAG_저장소.md` | Centralized → team split (orchestration 축) | DE + 매니저 |
| `4_Composer_조사_요약.md` | Composer 3 조사 결과 (Executor / 비용 / 권한 / 배포 등) | 매니저 + 플랫폼팀 |
| `5_3layer_배포_아키텍처.md` | Provider / dbt / DAG 3-layer 통합 | DE + 플랫폼팀 |
| `6_DE_관점_일상변화.md` | DE 일상 워크플로우 Before / After | DE |
| `7_Lineage와_관측성.md` | DataHub + Airflow Asset 통합 lineage | DE + 데이터 분석가 |
| `8_실행계획과_안전장치.md` | Phase 0~3 + 안전장치 + 요청사항 | 전체 |
| `9_Airflow3_Composer_이관_변경사항.md` | Airflow 2→3 breaking + Self-managed→Composer 변경 실전 | DE + 플랫폼팀 |
| `10_Sensor를_Triggerer와_Asset으로_대체하기.md` | Sensor → Deferrable / Asset 코드 실전 예시 | DE |
| `11_Cosmos란.md` | astronomer-cosmos 라이브러리 소개 (dbt ↔ Airflow 통합) | DE |

## 읽는 순서 추천

- **바쁘신 분**: `README.md` (여기) + `1_배경과_결정.md`
- **DE**: `2_결정A_dbt` → `3_결정B_팀별_DAG` → `6_DE_관점_일상변화` → `9_Airflow3_Composer_이관_변경사항` (실전 코드 변경)
- **매니저**: `1_배경과_결정` → `3_결정B_팀별_DAG` (비용 축) → `4_Composer_조사_요약`
- **플랫폼팀**: 전체 (특히 `4`, `5`, `9`)
