---
title: "Cloud Composer 2 vs Self-managed 비교"
status: draft
tags:
  - confluence
  - airflow
  - 스케줄러
created: 2026-05-11
updated: 2026-05-11
source: https://kakaoent.atlassian.net/wiki/spaces/DP/pages/5068260232/Airflow+Cloud+Composer+2+vs+Self-managed
confluence_id: 5068260232
space_id: 365101058
space_key: DP
version: 1
---

# Cloud Composer 2 vs Self-managed 비교

> GCP 이관 시 managed Airflow를 쓸지, 직접 GKE에 띄울지 결정하기 위한 비교.

## 같은 것

- Airflow 2.x (같은 버전)
- Airflow UI, DAG 작성 방식, Operator/Sensor/Hook
- Connection/Variable 개념
- Plugin 시스템

→ DAG 코드를 그대로 옮겨도 동작.

## 다른 것: 주변 인프라/운영

| 항목 | Cloud Composer 2 | Self-managed (GKE) |
| --- | --- | --- |
| Executor 선택 | CeleryKubernetes 거의 고정 | 자유 |
| Python 패키지 | PyPI 가능, 시스템 라이브러리 제약 | 자유 (Dockerfile 직접) |
| DAG 배포 | GCS bucket sync | git-sync sidecar / PV / 이미지 포함 |
| 메타스토어 DB | Cloud SQL 자동 | 직접 운영 |
| airflow.cfg | 일부 lock | 완전 자유 |
| 모니터링/로깅 | Cloud Logging/Monitoring 자동 | 직접 구성 |
| 네트워크/보안 | Private IP, IAP, Workload Identity 옵션 | 100% 자유 |
| 업그레이드 | GCP가 관리 | 직접 |
| 백업/복구 | Snapshot 자동 | 직접 구성 |
| Worker queue 분리 | ❌ 어려움 | ✅ 자유 |
| 비용 구조 | 환경 패키지 ($300~$3000+/월) | 노드+DB 비용 (실제 사용량) |

## Composer가 자동화한 것

- GKE 클러스터 생성/관리
- Airflow 설치/설정
- Cloud SQL 메타스토어
- Webserver/Scheduler/Worker 배포
- Network 설정
- Auto-scaling
- Cloud Logging/Monitoring 통합
- Secret Manager 연동
- Workload Identity 설정
- DAG 폴더 GCS sync
- 백업/복구
- 보안 패치

## Composer가 막아둔 것

- 시스템 패키지(apt-get) 자유 설치 어려움
- Dockerfile 완전 제어 불가
- Executor 자유 선택 불가
- Airflow 설정 100% 자유 X
- DB 직접 접근 제한 (SELECT는 가능)
- 네트워크 100% 자유 X
- 비표준 storage 사용 어려움
- **Celery worker queue별 분리 어려움** (단일 worker pool)

## Composer가 CeleryKubernetesExecutor를 쓰는 이유

빠른 task (대부분):
- Celery 워커가 즉시 실행
- Pod 생성 오버헤드 X
무거운 task (선택):
- queue를 'kubernetes'로 지정
- Pod로 분리 실행
→ best of both worlds
→ Redis(Memorystore) 필요

## 결정 기준

| 상황 | 추천 |
| --- | --- |
| 운영 인력 부족 | Cloud Composer |
| 패키지/Executor 자유 필요 | Self-managed |
| 빠른 마이그레이션 필요 | Cloud Composer |
| Worker queue 분리 필요 | Self-managed |
| GCP 다른 서비스 깊이 활용 | Cloud Composer |
| 비용 절감 1순위 (운영 인력 있음) | Self-managed |

## 관련 문서

- [[3_Executor 종류 및 비교]]
- [[4_Queue 라우팅과 Pod 스펙 설정]]
- [[5_Metadata DB 운영]]
