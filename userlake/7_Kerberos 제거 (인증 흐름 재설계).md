---
title: "Kerberos 제거 — Workload Identity 로 인증 흐름 재설계"
status: draft
created: 2026-06-28
대상: userlake-worker / Spark Connect / Presto JDBC / HDFS / Vault 의 Kerberos 의존
용도: 인증 흐름 매핑 / 영향 받는 코드·인프라 / 제거 가능한 컴포넌트
부모: [[1_userlake-worker 인프라 이관]]
---

# Kerberos 제거 — Workload Identity 로 인증 흐름 재설계

> 상위 문서: [[1_userlake-worker 인프라 이관]] § 2-4

## 0. 결론

> Kerberos 는 athlon 의 사내 Hadoop / Presto / Hive 접근용 인증이라 **GCP 가면 자연 폐기**.
> 단순한 "configuration 키 제거" 가 아니라 **5개 컴포넌트에 걸친 인증 흐름 재설계**:
> 1. **userlake-worker pod 의 `kinit` sidecar** 제거 (gitops)
> 2. Presto JDBC URL 의 Kerberos 파라미터 4종 폐기
> 3. HDFS 접근의 UGI 설정 폐기 (`HadoopFile.getHdfsFileSystem()`)
> 4. Spark Connect 의 keytab mount + `spark.kerberos.*` 설정 폐기
> 5. Vault → Secret Manager 가는 경우 secret 인증도 변화
>
> 작업량 **3~5일** (자체 작업) + 다른 이관 (BQ / GCS / Spark Connect) 에 부분적으로 포함됨.

---

## 1. 현재 Kerberos 의존 인벤토리

### 1-1. application.yml 키 (userlake-worker)

```yaml
hadoop:
  kerberos:
    user: hadoop-kent-data
    config_path: /etc/krb5.conf
    keytab_path: /home/deploy/.kerberos/hadoop-kent-data.keytab
    realm: KAKAO.HADOOP
```

### 1-2. dp-gitops 의 Pod 인프라

`userlake-worker/base/deploy.yaml` 에:

```yaml
containers:
  - name: athlon-userlake-worker
    env:
      - {name: KRB5CCNAME, value: /dev/shm/ccache}
      - {name: HADOOP_CONF_DIR, value: /etc/hadoop/conf}
      - {name: SPARK_KRB_REALM, value: KAKAO.HADOOP}
      - {name: KRB_REALM, value: KAKAO.HADOOP}
    volumeMounts:
      - {name: hadoop-config, mountPath: /etc/hadoop/conf}
      - {name: kerberos-config, mountPath: /etc/krb5.conf, subPath: krb5.conf}
      - {name: kerberos-secret, mountPath: /home/deploy/.kerberos}
      - {name: ccache, mountPath: /dev/shm}

  # ⚠ Kerberos 갱신 사이드카
  - name: kinit
    image: idock.daumkakao.io/kakaoent-dp/kinit-sidecar:0.0.1
    env:
      - {name: KRB5_KTNAME, value: /krb5/hadoop-kent-data.keytab}
      - {name: OPTIONS, value: "hadoop-kent-data@KAKAO.HADOOP -k -t /krb5/..."}
    volumeMounts:
      - {name: kerberos-secret, mountPath: /krb5}
      - {name: ccache, mountPath: /dev/shm}      # ← 메인 컨테이너와 ccache 공유

volumes:
  - {name: kerberos-config, configMap: athlon-kerberos-config}
  - {name: kerberos-secret, secret: athlon-kerberos-secret}
  - {name: ccache, emptyDir: {medium: Memory}}    # tmpfs 로 credential cache 공유
```

→ 인증 매커니즘: **kinit sidecar 가 keytab 으로 ticket 갱신 → emptyDir(Memory) ccache 에 저장 → 메인 컨테이너가 KRB5CCNAME 로 사용**. 사실상 사내 표준 패턴.

### 1-3. 코드 의존처

| 위치 | 사용처 |
|---|---|
| `HadoopFile.getHdfsFileSystem()` (core) | 매 HDFS 접근마다 `HadoopConfig.setKerberosUGI()` 호출 |
| `HdfsFileReadWriter` (core) | HadoopFile 통해 간접 의존 |
| **Presto JDBC URL** (application.yml) | `KerberosRemoteServiceName=presto&KerberosPrincipal=...&KerberosConfigPath=...&KerberosKeytabPath=...` |
| **Spark Connect** (dp-gitops, spark-defaults.conf) | `spark.kerberos.principal hadoop-kent-data@KAKAO.HADOOP` / `spark.kerberos.keytab /opt/kerberos/keytab/hadoop-kent-data.keytab` |
| **`PrestoClusterConfig`** (distributed-query-engine) | Kerberos 옵션 구성 |

---

## 2. GCP 후 매핑

### 2-1. Workload Identity 매핑

```
GKE pod
  └─ Kubernetes ServiceAccount (KSA)
       └─ annotation: iam.gke.io/gcp-service-account=<GSA>@<project>.iam.gserviceaccount.com
            └─ Google IAM ServiceAccount (GSA)
                 └─ roles: BigQuery user/jobUser, GCS objectAdmin, etc.
```

→ pod 가 자동으로 GSA 자격증명 획득. **코드 / 환경변수 / sidecar 불필요**.

### 2-2. 컴포넌트별 인증 전환

| 컴포넌트 | 현재 (Kerberos) | GCP 후 |
|---|---|---|
| Presto / BigQuery | JDBC URL 에 keytab 경로 / principal | ADC (Workload Identity 자동) — JDBC 옵션 `OAuthType=3` 또는 BQ Java client 의 `BigQueryOptions.getDefaultInstance()` |
| HDFS / GCS | `setKerberosUGI()` 호출 | GCS Storage 클라이언트가 ADC 자동 사용 |
| Spark Connect | `spark.kerberos.*` 설정 + keytab mount | 제거. Dataproc Serverless 가 자체 SA 로 GCS 접근 |
| Vault → Secret Manager (만약 이전 시) | `vault.token` | Workload Identity 로 Secret Manager API 접근 |

→ **4개 컴포넌트 모두 한 GSA 로 통합 가능**. 권한 분리 원하면 컴포넌트별 다른 GSA 도 가능.

---

## 3. 작업 체크리스트

### 3-1. application.yml 정리

```yaml
# 제거
hadoop:
  basedir: ""
  kerberos:                         # 통째로 삭제
    user: ...
    config_path: ...
    keytab_path: ...
    realm: ...

# 변경
presto:                             # 통째로 삭제 (BigQuery 로)
  clusters: ...
  jdbc: ...
  auth.kerberos: ...
```

### 3-2. dp-gitops 의 deploy.yaml 정리

```yaml
# 제거 대상
- name: kinit                       # sidecar 통째로 삭제
- env: [KRB5CCNAME, HADOOP_CONF_DIR, SPARK_KRB_REALM, KRB_REALM]   # 모두 삭제
- volumes: [kerberos-config, kerberos-secret, ccache]              # 모두 삭제
- volumeMounts: 위 3개 mount 삭제

# 추가
spec:
  template:
    spec:
      serviceAccountName: athlon-userlake-worker-ksa   # KSA 매핑
```

### 3-3. 코드 정리

- `HadoopFile` 의 Kerberos preauth 호출 제거 (HdfsFileReadWriter 폐기와 같이)
- `PrestoClusterConfig` 폐기 (BigQuery 이관과 같이)
- `distributed-query-engine` 의 Kerberos 의존 라이브러리 제거

### 3-4. 사내 컴포넌트 (kinit-sidecar 이미지)

- `idock.daumkakao.io/kakaoent-dp/kinit-sidecar:0.0.1` 사용 안 함 — 다른 사내 워크로드도 이 이미지를 쓰면 사내 lib 그대로 유지

---

## 4. 다른 이관 작업과의 관계

Kerberos 제거는 **단독 작업이 아님**. 다른 이관 작업에 부분적으로 분산됨:

| 이관 작업 | Kerberos 영향 |
|---|---|
| [[4_BigQuery 이관 (Presto 쿼리 엔진 전환)]] § 4 | Presto JDBC 의 Kerberos 파라미터 4종 폐기. `PrestoClusterConfig` 폐기 |
| [[6_HDFS → GCS (FileSystemType 확장)]] § 1-4 | `HadoopFile` 의 UGI 설정 폐기 |
| [[2_Spark Connect → Dataproc Serverless 검토]] § 4-2 | Spark Connect 의 keytab mount + `spark.kerberos.*` 설정 폐기 |
| [[8_MySQL Cloud SQL · Vault Secret Manager]] (계획) | Vault → Secret Manager 시 인증 전환 |

→ 이 문서는 **공통 인증 흐름 매핑 가이드**. 각 이관 작업의 인증 부분을 일관성 있게 처리할 수 있도록 reference.

---

## 5. 작업량 견적

| 작업 | 소요 |
|---|---|
| GSA 생성 + IAM 권한 부여 (BigQuery / GCS / Pub/Sub / Secret Manager) | 0.5일 |
| KSA 생성 + Workload Identity 매핑 | 0.5일 |
| dp-gitops 의 deploy.yaml 정리 (kinit sidecar 제거, ConfigMap/Secret/Volume 제거) | 0.5일 |
| application.yml 의 `hadoop.kerberos.*`, `presto.*` 키 제거 | 0.5일 |
| 코드 정리 (HadoopFile, PrestoClusterConfig) — 다른 이관에서 처리됨 | (포함됨) |
| 권한 정합성 테스트 (각 컴포넌트 접근 검증) | 1~2일 |
| **합계** | **3~5일 (자체)** + 다른 이관에 분산 |

---

## 6. 의사결정 분기

| # | 분기 | 옵션 | 영향 |
|---|---|---|---|
| 1 | **GSA 분리 수준** | 컴포넌트별 다른 GSA (BQ 용, GCS 용, Pub/Sub 용) vs 단일 GSA | 권한 분리 vs 운영 단순화 |
| 2 | **사내 망 호출의 인증** | Cloud Interconnect + 사내 IP allow / API Gateway / mTLS 등 | 사내 서비스 호출 시 인증 방식 |

---

## 7. PoC 검증 포인트

1. **GKE pod 가 Workload Identity 로 BigQuery 쿼리** — keytab 없이 성공
2. **동일 pod 가 GCS read/write** — 같은 GSA 로
3. **Dataproc Serverless 의 Spark Connect 가 GCS 접근** — 동일 또는 별도 GSA
4. **사내 망 호출 (Slack, Loupe Kafka 등)** — Workload Identity 가 사내 망 접근에 인증 영향 주는지

---

## 8. 미해결 질문

- [ ] **사내 망 호출 인증** — Loupe Kafka 등 사내 서비스 호출 시 인증이 필요한지 (현재 사내망 내부라 무인증?)
- [ ] **`hadoop-kent-data` 외에 다른 Kerberos 식별자** — 다른 athlon 모듈에서 다른 principal 쓰는지
- [ ] **kinit-sidecar 이미지가 다른 athlon 모듈에서도 사용 중인지** — 공통 base image 인지

---

## 9. 참고

- 상위 문서: [[1_userlake-worker 인프라 이관]] § 2-4
- 관련 이관 작업:
  - [[4_BigQuery 이관 (Presto 쿼리 엔진 전환)]] § 4
  - [[6_HDFS → GCS (FileSystemType 확장)]] § 1-4
  - [[2_Spark Connect → Dataproc Serverless 검토]] § 4-2
- 파일 위치:
  - `userlake-worker/src/main/resources/application.yml` (hadoop.kerberos.*)
  - `dp-gitops/athlon/userlake-worker/base/deploy.yaml` (kinit sidecar)
  - `dp-gitops/athlon/spark-connect/prod/spark-defaults.conf` (spark.kerberos.*)
  - `core/src/main/kotlin/com/kakaopage/athlon/util/filerw/HadoopFile.kt`