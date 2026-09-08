# 베리즈 데이터 이관 — 컨텍스트

## 풀고자 하는 문제

베리즈(berriz) 서비스 데이터의 GCP 이관. **진행 중인 워크스트림**으로, 날짜별 작업 로그 형식으로 기록한다.

## 노트 형식

- `YYMMDD.md` — 그날의 작업/이슈/미결 사항. 파편 메모는 당일 안에 문장으로 풀어서 남긴다.
- 결정이 쌓이면 숫자 prefix 노트(`1_개요.md` 등)로 승격.

## 관련 자료

- Terraform 첫 케이스: [[../스케줄러/16_Composer3 신규 환경 구축 (Terraform)|스케줄러/16_Composer3 신규 환경 구축 (Terraform)]] (`dev-berriz-airflow`)
- 공유 Airflow 규약: [[../deploy/2_공유 Airflow 사용 가이드|deploy/2_공유 Airflow 사용 가이드]]
