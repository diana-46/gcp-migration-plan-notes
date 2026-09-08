---
title: "MySQL → Cloud SQL · Vault → Secret Manager"
status: draft
created: 2026-06-28
대상: userlake-worker / api / cdc-consumer / userlake-search-worker 의 MySQL + Vault 의존
용도: DB 이관 + 시크릿 관리 의사결정 / 영향 코드
부모: [[1_userlake-worker 인프라 이관]]
---

# MySQL → Cloud SQL · Vault → Secret Manager

> 상위 문서: [[1_userlake-worker 인프라 이관]] § 2-5

## 0. 결론

> **MySQL → Cloud SQL** 은 거의 URL/credential 교체 수준 (1~2일).
> **Vault → Secret Manager** 가 더 큰 결정 — `@VaultPropertySource` 기반 Spring 통합을 어떻게 풀지에 따라 1~3주.
> 추천: **Cloud SQL + Secret Manager 동시 이관** (일관성). 단 secret 정책 (`data_platform_common`) 재구성 부담 큼.

---

## 1. 현재 상태

### 1-1. MySQL 접근 패턴

```yaml
# userlake-worker/src/main/resources/application.yml
spring:
  datasource:
    url: jdbc:mysql://localhost:3306/athlon?autoReconnect=true&useUnicode=true&characterEncoding=UTF8&...
    driver-class-name: com.mysql.cj.jdbc.Driver
    # username / password 는 Vault 에서 주입 (아래)
```

### 1-2. Vault 통합

```yaml
vault:
  phase: ${VAULT_PHASE:local}
  uri: ${VAULT_URI:https://vault-beta.onkakao.net}
  token: hvs.CAESIJyQnUf3fLCgkvWA3DcLENS69rClfFN-1pJ_T9hdKqLpGh4KHGh2cy5...
  # vault-beta: data_platform_common policy
```

**Vault path 구조**:
```
secret/kakaopage/data_platform_common/athlon/mysql/<phase>
```

**Spring 통합** (api/src/main/kotlin/com/kakaopage/athlon/config/VaultDatabaseConfig.kt):
```kotlin
@Configuration
@ConditionalOnProperty("vault.uri")
@VaultPropertySource(
    value = ["secret/kakaopage/data_platform_common/athlon/mysql/\${vault.phase}"],
    propertyNamePrefix = "spring.datasource."
)
class VaultDatabaseConfig
```

→ Vault path 의 키들이 자동으로 `spring.datasource.*` 로 prefix 되어 Spring property 에 주입됨. 즉 `username`, `password` 등이 Vault → `spring.datasource.username` 으로.

### 1-3. 사용 모듈 (athlon 전사)

`VaultDatabaseConfig` 가 정의된 모듈:
- `api`
- `cdc-consumer`
- `userlake-search-worker`
- (userlake-worker 도 동일 패턴 사용 추정 — `vault.uri` 조건이라 활성화됨)

→ **athlon 전사 영향**.

---

## 2. MySQL → Cloud SQL 이관

### 2-1. 옵션

| 옵션 | 설명 | 권장 |
|---|---|---|
| **Cloud SQL for MySQL** | GCP 매니지드 MySQL | ✅ 일반적 선택 |
| AlloyDB for MySQL | PostgreSQL 호환만 있고 MySQL 호환 미정 | ❌ |
| 사내 MySQL 유지 | Cloud Interconnect 로 GCP → 사내 접근 | ❌ network latency, 의존 |

→ **Cloud SQL for MySQL** 확정.

### 2-2. 접속 모드

| 모드 | 설명 | userlake-worker 적합도 |
|---|---|---|
| **Public IP + Cloud SQL Auth Proxy** | proxy sidecar 통해 접속, IAM 인증 | ✅ Workload Identity 와 정합 |
| Private IP (VPC peering) | VPC 내부 IP 로 직접 | 가능. proxy 보다 빠름 |
| Public IP + SSL | proxy 없이 직접 | 권장 안 함 (관리 복잡) |

→ **Private IP** 권장 (latency 최적). proxy sidecar 옵션도 가능하지만 sidecar 추가 부담.

### 2-3. 접속 변경

```yaml
# Before
spring.datasource.url: jdbc:mysql://localhost:3306/athlon?autoReconnect=true&...

# After (Private IP)
spring.datasource.url: jdbc:mysql://<private-ip>:3306/athlon?...

# Or (Cloud SQL JDBC Socket Factory)
spring.datasource.url: jdbc:mysql:///athlon?cloudSqlInstance=<project>:<region>:<instance>&socketFactory=com.google.cloud.sql.mysql.SocketFactory&...
```

### 2-4. 데이터 마이그레이션

- **Database Migration Service** (DMS) — GCP 매니지드 dump+CDC 도구. downtime 최소
- 또는 mysqldump → import (downtime 허용 시)

---

## 3. Vault → Secret Manager 이관

### 3-1. 옵션 비교

| 옵션 | 설명 | 작업량 | 일관성 |
|---|---|---|---|
| **A. Secret Manager** | GCP 매니지드. Workload Identity 로 인증 | 중 (코드 변경) | 높음 |
| B. Vault 자체 호스팅 | GKE 에 Vault 띄움. 사내 정책 그대로 | 낮음 (host 만 교체) | 낮음 (GCP 와 이질) |
| C. 사내 Vault 유지 | Cloud Interconnect 로 접근 | 최저 | 최저 (network 의존) |

→ **A (Secret Manager)** 권장. 단 Vault 의 정책 / path 구조를 Secret Manager 의 secret/version 구조로 재구성 필요.

### 3-2. Spring 통합 — `@VaultPropertySource` 대체

| 옵션 | 설명 |
|---|---|
| **A1. Spring Cloud GCP Secret Manager** | `@PropertySource("sm://<secret-name>")` 또는 `spring.cloud.gcp.secretmanager.*` 자동 prefix |
| A2. 직접 SecretManagerServiceClient 호출 | `@Bean` 으로 secret 로드 후 dataSource 빌드 |

→ **A1** 이 `@VaultPropertySource` 패턴과 유사. 의존성: `spring-cloud-gcp-starter-secretmanager`.

코드 변경:
```kotlin
// Before
@VaultPropertySource(
    value = ["secret/kakaopage/data_platform_common/athlon/mysql/\${vault.phase}"],
    propertyNamePrefix = "spring.datasource."
)
class VaultDatabaseConfig

// After (Spring Cloud GCP 사용 시)
// application.yml 에서:
spring.datasource:
  url: ${sm://athlon-mysql-url}
  username: ${sm://athlon-mysql-username}
  password: ${sm://athlon-mysql-password}
```

→ `VaultDatabaseConfig` 클래스 폐기, application.yml 에서 직접 `sm://` 참조.

### 3-3. Secret 구조 매핑

| Vault path | Secret Manager 매핑 |
|---|---|
| `secret/kakaopage/data_platform_common/athlon/mysql/local` | secret `athlon-mysql-local-username`, `athlon-mysql-local-password`, ... |
| `secret/kakaopage/data_platform_common/athlon/mysql/prod` | secret `athlon-mysql-prod-*` |

→ Vault 의 키 단위 secret 으로 분리. **항목별 secret** vs **단일 JSON secret + 파싱** 결정 필요.

### 3-4. 인증

- Vault: `vault.token` 정적 토큰 (long-lived)
- Secret Manager: Workload Identity 로 IAM 자동 ([[7_Kerberos 제거 (인증 흐름 재설계)]] § 2-1 참조)

→ secret 노출 위험 감소.

### 3-5. 다른 secret 들

`data_platform_common/athlon/mysql/` 외에 `application.yml` 에서 Vault 가 채워주는 secret 있는지 확인 필요. 예: Slack token, API key 등.

---

## 4. 작업량 견적

### 4-1. MySQL → Cloud SQL

| 작업 | 소요 |
|---|---|
| Cloud SQL 인스턴스 프로비저닝 (HA, backup, monitoring) | 0.5일 |
| Private IP / VPC peering 설정 | 0.5일 |
| 데이터 마이그레이션 (DMS or mysqldump) | 1~3일 (사이즈 따라) |
| `application.yml` URL 변경 | 30분 |
| 정합성 검증 (worker 모듈별) | 1일 |
| **소계** | **3~5일** |

### 4-2. Vault → Secret Manager

| 작업 | 소요 |
|---|---|
| Secret Manager secret 생성 (모든 phase × 모든 모듈) | 1~2일 |
| `VaultDatabaseConfig` 폐기 + Spring Cloud GCP Secret Manager 통합 | 2~3일 |
| `application.yml` 의 vault.* 키 제거 | 30분 |
| 다른 Vault secret (slack token 등) 확인 + 이전 | 1~2일 |
| **athlon 전사** 의 모듈 (api / cdc-consumer / userlake-search-worker / userlake-worker) 업데이트 | 2~3일 |
| 검증 + 롤아웃 | 2~3일 |
| **소계** | **8~13일 (~2~3주)** |

→ 합계 **2~3주** (동시 진행 시).

---

## 5. 의사결정 분기

| # | 분기 | 옵션 | 영향 |
|---|---|---|---|
| 1 | **MySQL 접속 모드** | Private IP / Cloud SQL Proxy / Public IP+SSL | latency / sidecar 부담 |
| 2 | **Vault 이전 vs 유지** | Secret Manager / GKE 자체 호스팅 / 사내 Vault 유지 | 일관성 vs 작업량 |
| 3 | **Secret 구조** | 항목별 분리 secret / 단일 JSON secret | 관리 단위 |
| 4 | **이관 동시성** | Cloud SQL + Secret Manager 동시 / 순차 | 리스크 분산 |

---

## 6. PoC 검증 포인트

1. **Cloud SQL Private IP 접속** — Workload Identity / private network 통해
2. **DMS 통한 데이터 마이그레이션** — downtime / 데이터 손실 확인
3. **Spring Cloud GCP Secret Manager** 가 `@VaultPropertySource` 와 같은 의미로 동작 — `spring.datasource.*` 주입
4. **secret 회전 (rotation)** — Secret Manager 의 새 version → pod 재배포 없이 반영 가능한지
5. **athlon 다른 모듈** (api / cdc-consumer / userlake-search-worker) 의 영향 검증

---

## 7. 미해결 질문

- [ ] **Vault 에서 가져오는 secret 전체 목록** — mysql 외에 다른 게 있는지 (Slack token, API key 등)
- [ ] **MySQL 사이즈 / IOPS** — Cloud SQL tier 결정용
- [ ] **현재 MySQL 의 daily traffic** — connection pool / instance size 결정용
- [ ] **`data_platform_common` policy 의 정확한 권한 범위** — Secret Manager IAM 으로 매핑 위해
- [ ] **사내 Vault 에 의존하는 다른 사내 워크로드** — 사내 Vault 가 athlon 만 위한 건지 다른 데도 같이 쓰는지
- [ ] **secret 회전 정책** — 자동 회전인지 수동인지

---

## 8. 참고

- 상위 문서: [[1_userlake-worker 인프라 이관]] § 2-5
- 관련: [[7_Kerberos 제거 (인증 흐름 재설계)]] (Workload Identity)
- 파일 위치:
  - `userlake-worker/src/main/resources/application.yml` (spring.datasource, vault.*)
  - `api/src/main/kotlin/com/kakaopage/athlon/config/VaultDatabaseConfig.kt`
  - `cdc-consumer/src/main/kotlin/com/kakaopage/athlon/VaultDatabaseConfig.kt`
  - `userlake-search-worker/src/main/kotlin/com/kakaopage/athlon/config/VaultDatabaseConfig.kt`