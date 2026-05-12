---
title: "Executor 종류 및 비교"
status: draft
tags:
  - confluence
  - airflow
  - 스케줄러
created: 2026-05-11
updated: 2026-05-11
source: https://kakaoent.atlassian.net/wiki/spaces/DP/pages/5068947598/Airflow+Executor
confluence_id: 5068947598
space_id: 365101058
space_key: DP
version: 5
---

# Executor 종류 및 비교

> Airflow에서 task가 실제로 어디서 어떻게 실행되는지 결정하는 컴포넌트. GCP 이관 시 Executor 선택의 출발점.

## Executor 4종

| Executor | task 실행 방식 | Message Queue | 워커 |
| --- | --- | --- | --- |
| **LocalExecutor** | Scheduler 머신에서 process로 | 불필요 | 없음 |
| **CeleryExecutor** | 상시 떠있는 Celery 워커가 실행 | Redis/RabbitMQ 필수 | 상시 떠있음 |
| **KubernetesExecutor** | task마다 Pod 생성 | 불필요 | task = pod |
| **CeleryKubernetesExecutor** | 기본은 Celery, 일부 task만 Pod | Redis 필수 | Celery 워커 + 가끔 Pod |

> 이름에 "Kubernetes"가 들어있어도 Celery 부분이 있으면 Redis 필요. KubernetesExecutor만(Celery 빠진) Redis 없이 동작.

## 비교 매트릭스

| 항목 | KubernetesExecutor | CeleryExecutor | CeleryKubernetesExecutor |
| --- | --- | --- | --- |
| Redis 필요 | ❌ | ✅ | ✅ |
| task = Pod | ✅ 매번 | ❌ 워커 재사용 | 선택적 |
| Pod 오버헤드 | 10~30초 | 없음 | 선택적 |
| 격리 | 완벽 | 워커 공유 | 선택적 |
| 빠른 task 적합 | ❌ | ✅ | ✅ |
| 무거운 task 적합 | ✅ | ⚠️ | ✅ |

## 사용 시나리오

KubernetesExecutor 단독:
task가 무겁고 격리 중요한 환경
CeleryExecutor 단독:
많은 짧은 task 처리
CeleryKubernetesExecutor:
task 종류 다양한 일반적 환경
짧은 건 Celery, 무거운 건 Pod
→ 대부분의 ETL 파이프라인에 적합

## Cloud Composer 2 기본값

Executor: CeleryKubernetesExecutor
Redis: Memorystore (GCP 관리)

→ Self-managed처럼 Executor 자유 선택은 안 됨.

## PoC 검증 포인트

1. task 종류별 실행 시간 측정
- 짧은 task 많음 → CeleryKubernetes
- 무거운 task만 → Kubernetes 단독
2. Pod 생성 오버헤드 (P50/P95)
3. 비용 비교
- Memorystore (Redis) 비용
- Celery 워커 vs Pod 비용
4. 격리 요구사항
- task끼리 영향 주면 안 되는 케이스
5. 커스텀 패키지/operator 호환성
6. 사내 네트워크 접근
7. Pod 스펙 차별화 활용도
- SMALL/MEDIUM/LARGE 프리셋 충분한가?

## 핵심 요약

| 질문 | 답 |
| --- | --- |
| Cloud Composer 2의 기본 Executor? | CeleryKubernetesExecutor |
| Redis 필요? | ✅ (Memorystore) |
| task마다 Pod만 원하면? | KubernetesExecutor 단독 → Redis 불필요 |
| Composer에서 Executor 자유 선택? | ❌ 거의 고정 |
| Celery vs Pod 어떻게 구분? | task의 `queue` 파라미터 |
| K8s queue 여러 개 지정? | ❌ 단일 이름 (`kubernetes_queue`) |
| K8s 안에서 Pod 스펙 차별화? | executor\_config로 차별화 |
| Pool은 어디서? | Airflow UI (Composer/Self 동일) |

## 관련 문서

- [[2_Cloud Composer 2 vs Self-managed 비교]]
- [[4_Queue 라우팅과 Pod 스펙 설정]]
- [[5_Metadata DB 운영]]
