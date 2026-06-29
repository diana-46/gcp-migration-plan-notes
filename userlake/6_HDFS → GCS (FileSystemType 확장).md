---
title: "HDFS → GCS — FileSystemType 확장"
status: draft
created: 2026-06-28
대상: core/util/filerw 패키지 + 모든 callers (userlake-worker stage 전체)
용도: GcsFileReadWriter 구현 / 의미론 차이 / atomic rename gotcha
부모: [[1_userlake-worker 인프라 이관]]
---

# HDFS → GCS — FileSystemType 확장

> 상위 문서: [[1_userlake-worker 인프라 이관]] § 2-3

## 0. 결론

> `core/util/filerw` 의 추상화에 **`GCS` 타입 1개 추가하는 것이 본질**.
> 작업량 **1~2주** (athlon 전체에서 가장 영향 범위 넓음 — 모든 stage 의 결과 파일 경로 의존).
> 가장 미묘한 3가지:
> 1. **GCS 는 atomic rename 없음** — `renameFile()` 의 atomic 가정에 의존하는 stage (Spark 결과 finalize 등) 가 깨질 수 있음
> 2. **GCS 는 mtime 없음** — `touchRecursively()` 의미가 사라짐 (callers 확인 필요)
> 3. **api/service 의 일부는 `FileReadWriter` 안 쓰고 raw Hadoop FS 직접 호출** — 같이 잡아야 함
>
> 영향 범위가 넓어서 **PoC 우선순위 1번 권장**. core 모듈 변경 → athlon 전사 재배포 필요.

---

## 1. 현재 추상화

### 1-1. `FileReadWriter` 인터페이스 (11개 메서드)

| 메서드 | 의미 | 자주 쓰임 |
|---|---|---|
| `bufferedReader / Writer / inputStream` | 스트림 획득 | 중간 |
| `readLines` (List) / `writeLines` (Collection) | 전체 메모리 적재 | 적음 |
| `writeSequence` (Lazy `Sequence<String>`) | **스트리밍 write, line count 반환** | ★ 가장 많이 씀 (Target/Extract stage 결과) |
| `findCsvFiles` / `findFiles` | extension 필터 + recursive | ★ Spark 결과 finalize 시 |
| `renameFile` | **HDFS atomic rename** / Local `File.renameTo` | ★ Spark 결과 finalize 시 |
| `copyFile` | 파일 복사 | CopyFileStageProcess |
| `deleteDirectory` | recursive 옵션 | cleanup |
| `touchRecursively` | mtime 업데이트 (tree walk) | 드물게 |
| `fileSystemType()` | enum 반환 (`pathWithScheme` 용) | scheme 부여 |

### 1-2. `FileSystemType` 현재

```kotlin
enum class FileSystemType {
    LOCAL,
    HDFS;

    fun pathWithScheme(path: String): String = when(this) {
        LOCAL -> "file://$path"
        HDFS  -> "hdfs://$path"
    }
}
```

→ `GCS` 추가 + `"gs://$path"` 추가.

### 1-3. Bean 선택 — `@ConditionalOnProperty`

```kotlin
@Component
@ConditionalOnProperty("userlake.filerw.type", havingValue = "hdfs")
class HdfsFileReadWriter ...

@Component
@ConditionalOnProperty("userlake.filerw.type", havingValue = "local", matchIfMissing = true)
class LocalFileReadWriter ...
```

→ 신규: `@ConditionalOnProperty("userlake.filerw.type", havingValue = "gcs")` 클래스 추가. application.yml `userlake.filerw.type: gcs`.

### 1-4. HDFS 의 Kerberos 의존

- `HadoopFile.getHdfsFileSystem()` 이 **매 호출마다** `HadoopConfig.setKerberosUGI()` → UGI 설정 → `FileSystem.get(conf)`
- 의존 properties:
  - `hadoop.kerberos.realm`
  - `hadoop.kerberos.user`
  - `hadoop.kerberos.config_path` (krb5.conf 경로)
  - `hadoop.kerberos.keytab_path` (keytab 경로)
- 인증 실패 시 `StageProcessException("failed to login kerberos")` 로 wrap

---

## 2. GcsFileReadWriter 구현 매핑

### 2-1. 메서드 1:1 매핑

| FileReadWriter 메서드 | GCS 구현 | 주의사항 |
|---|---|---|
| `bufferedReader(path)` | `Channels.newReader(storage.get(blobId).reader(), UTF_8)` | 자연스러움 |
| `bufferedWriter(path)` | `Channels.newWriter(storage.writer(blobInfo), UTF_8)` | 자연스러움 |
| `inputStream(path)` | `storage.get(blobId).reader().toInputStream()` | not-found → `AthlonFileNotFoundException` |
| `readLines / writeLines / writeSequence` | parent 의 helper 재사용 (BufferedReader/Writer 위) | 코드 그대로 |
| `findCsvFiles(dir)` / `findFiles(dir, ext, recursive)` | `storage.list(bucket, prefix(dir), ...)` + pagination | **pagination 처리 필수** (한 페이지 1000개) |
| `renameFile(from, to)` | **⚠ atomic 아님**. `storage.copy(from, to).result()` + `storage.delete(from)` | § 3-1 |
| `copyFile(from, to)` | `storage.copy(from, to)` | 자연스러움 |
| `deleteDirectory(path, recursive)` | list + batch delete loop | atomic recursive 없음. partial failure 시 retry 정책 필요 |
| `touchRecursively(path)` | **⚠ noop 또는 사용자 메타데이터에 timestamp 저장** | § 3-2 |
| `fileSystemType()` | `FileSystemType.GCS` | enum 추가 |

### 2-2. 인증

- GCS Storage 클라이언트는 **Application Default Credentials (ADC)** 기본 사용
- GKE 위에서 돌면 **Workload Identity** 가 자동으로 SA 매핑
- 코드: `StorageOptions.getDefaultInstance().service` (별도 설정 불필요)
- 명시적 SA 키 파일 쓸 거면 `GOOGLE_APPLICATION_CREDENTIALS` 환경변수
- **`hadoop.kerberos.*` 4개 프로퍼티 제거 가능**

### 2-3. 빌드 의존성

```kotlin
// core/build.gradle.kts 에 추가
implementation("com.google.cloud:google-cloud-storage:2.x.x")

// hadoop-client 는 유지 (HdfsFileReadWriter 가 hdfs 모드 코드 그대로 남아있고
// 다른 모듈도 직접 의존할 수 있음)
```

---

## 3. 미묘한 3가지 (가장 큰 리스크)

### 3-1. ⚠ Atomic rename 없음

HDFS 의 `FileSystem.rename(src, dst)` 는 **atomic** (같은 namenode 내부). 로컬 FS 의 `File.renameTo` 도 same-FS atomic.

GCS 의 "rename" 은 **copy + delete** 2-step. 중간 실패 시:
- copy 만 성공 → dst 에 새 파일 + src 에 원본 → **데이터 중복 가능성**
- delete 만 실패 → 동일하게 중복 가능성

**의존 코드**:
- `GateStageProcess` — Spark `coalesce(1).write()` 결과 디렉토리에서 `findCsvFiles` 로 찾은 part-file 을 최종 경로로 rename
- `SyncStageProcess.copy()` — 동일 패턴
- `SyncStageProcess.syncHive()` — 임시 디렉토리의 parquet 을 최종 파티션 디렉토리로 rename

이 경로들에서 rename 중간 실패 시 partial state 가 남을 가능성.

**대응 옵션**:
- (A) **GcsFileReadWriter 가 try-catch + 보상 정리** — copy 후 delete 실패 시 dst 삭제 시도. 단 leak 가능성 잔존
- (B) **결과 경로에 attempt id 포함** — 매번 새 경로 (`result_${attemptId}.csv`) 로 write. stage 가 idempotent 면 자연스러움 (worker 의 stage UUID 활용 가능)
- (C) **검증 후 rename** — copy 성공 → checksum 확인 → delete. 보수적이지만 비용 증가

**추천**: 단기 (A) + 장기 (B). worker 가 이미 UUID 기반 idempotent 처리하니 (B) 가 자연스러움.

### 3-2. ⚠ mtime 없음

`touchRecursively(path, mtime)` 가 GCS 에서 직접 불가.

**의존 코드 확인 필요** — grep 으로 `touchRecursively` 호출처 찾아보고 실제 의미 검토. 만약:
- 단순 디버깅 / 모니터링 용 → noop 으로 둬도 됨
- 다른 시스템 (Hive partition 갱신 등) 이 mtime 보는 경우 → custom metadata 로 저장 + 별도 처리

**대응**: GCS object 의 사용자 메타데이터 (`metadata = {"mtime": "..."}`) 에 timestamp 저장. 단 GCS 가 정렬에 안 씀, 단순 기록용.

### 3-3. ⚠ api/service 의 raw Hadoop FS 직접 호출

`api/service/ExtractRunResultIOService` 등은 **FileReadWriter 안 쓰고 `FileSystem.get(conf)` 직접 호출**:

```kotlin
val fs = FileSystem.get(conf)
val inputStream = fs.open(Path(path))
```

→ FileReadWriter 만 바꾼다고 끝나는 게 아님. **api 의 raw Hadoop 호출도 같이 GCS Storage API 로 교체** 필요.

대응:
- (A) api 도 `FileReadWriter` 사용하도록 리팩토링 (깔끔)
- (B) api 만 별도로 GCS Storage 직접 호출 (빠름, 일관성 ↓)

**추천**: (A) — 일관성 + 향후 유지보수.

---

## 4. 의존성 / Blast radius

### 4-1. userlake-worker 의 caller 패턴

| Stage / 컴포넌트 | 사용 메서드 | rename atomic 의존? |
|---|---|---|
| `TargetStageProcess` | `writeSequence` (CSV write) | ❌ |
| `ExtractStageProcess` | `writeSequence` | ❌ |
| `CopyFileStageProcess` | `copyFile` | ❌ |
| `GateStageProcess` | `findCsvFiles` + `renameFile` (Spark 결과 finalize) | ✅ **의존** |
| `SyncStageProcess.copy()` | `findCsvFiles` + `renameFile` + `deleteDirectory` | ✅ **의존** |
| `SyncStageProcess.syncHive()` | parquet `findFiles` + `renameFile` + `deleteDirectory` | ✅ **의존** |
| `SyncSender` 일부 | `findCsvFiles` + `renameFile` | ✅ **의존** |

→ **atomic rename 의존이 4곳**. § 3-1 의 대응 필요.

### 4-2. 다른 모듈

| 모듈 | FileReadWriter 사용? | raw Hadoop FS 직접? |
|---|---|---|
| **userlake-worker** | ✅ 주력 (70%+) | ❌ |
| **api** | 일부 | ✅ `ExtractRunResultIOService`, 어쩌면 `CsvUploadController`, `BatchController` |
| **core** | self | ❌ |
| 기타 athlon 모듈 | (확인 필요) | (확인 필요) |

→ **api 도 같이 잡아야 한다** (§ 3-3).

### 4-3. core 모듈 변경 → athlon 전사 재배포

`core` 모듈 변경 시 athlon 의 모든 의존 모듈 (api, worker, scheduler, etc.) 재배포 필요. 배포 절차에 영향.

---

## 5. application.yml 변경

### 5-1. userlake-worker (예시)

```yaml
userlake:
  filerw:
    type: gcs                    # local → gcs (worker 만 우선)
  basedir: gs://<bucket>/run/result/    # /team/athlon/... → GCS bucket path

# 제거 가능
hadoop:
  basedir: ""                    # 제거
  kerberos:                      # 통째로 제거
    user: ...
    config_path: ...
    keytab_path: ...
    realm: ...
```

### 5-2. GCS 인증

옵션 A — Workload Identity (권장):
```yaml
# 추가 설정 불필요. GKE pod 의 KSA → GSA 매핑이면 자동
```

옵션 B — SA 키 파일 (PoC 등 임시):
```yaml
# 환경변수 GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa-key.json
```

---

## 6. 작업량 견적

| 작업 | 소요 |
|---|---|
| `FileSystemType.GCS` enum 추가 + `pathWithScheme()` | 30분 |
| `GcsFileReadWriter` 구현 (11 메서드) | 4~5일 |
| pagination / batch delete / rename 보상 처리 | 2~3일 |
| `touchRecursively` 의존 검증 + noop / metadata 대안 | 0.5일 |
| Unit / integration 테스트 (testcontainers 의 fake-gcs-server 가능) | 3~4일 |
| **api 의 raw Hadoop FS 호출처** 식별 + `FileReadWriter` 로 마이그레이션 | 3~5일 |
| `application.yml` 정리 (hadoop.kerberos 제거, basedir 교체) | 0.5일 |
| Workload Identity 매핑 + GCS IAM | 0.5일 |
| **회귀 테스트** — Gate / Sync stage 의 atomic rename 의존 검증 | 2~3일 |
| build.gradle.kts 의존성 추가 | 0.5일 |
| **합계** | **16~22일 (3~4주)** |

> 이전 §1 메인 문서에서 "1~2주" 라고 했는데, api 의 raw Hadoop FS 마이그레이션 + atomic rename 회귀 검증 포함하면 **3~4주** 가 현실적.

---

## 7. 의사결정 분기

| # | 분기 | 옵션 | 영향 |
|---|---|---|---|
| 1 | **api 의 raw Hadoop FS 처리** | A: FileReadWriter 로 리팩토링 / B: 별도로 GCS Storage 직접 호출 | 일관성 vs 시간 |
| 2 | **atomic rename 대응** | A: copy+delete 보상 / B: attempt id 경로 + idempotent / C: 검증 후 rename | 단기 vs 장기 |
| 3 | **`touchRecursively` 처리** | A: noop / B: metadata 저장 | 현재 caller 가 진짜 mtime 보는지 따라 다름 |
| 4 | **GCS bucket 구조** | 단일 bucket + prefix vs 환경별 bucket | 운영 / 권한 / 비용 |
| 5 | **로컬 개발 환경** | LOCAL 유지 (testcontainers fake-gcs 만 테스트용) vs GCS 만 사용 | 개발자 부담 |

---

## 8. PoC 검증 포인트

1. **`writeSequence` 의 streaming** — 수천만 행 CSV 를 GCS write 시 메모리·latency 측정
2. **Pagination** — 큰 디렉토리 (수천 파일) 의 `findCsvFiles` 처리량
3. **`renameFile` 의 atomic-like 보상** — copy + delete 중간에 실패 주입했을 때 partial state 처리
4. **Workload Identity** — keytab 없이 GCS read/write 성공
5. **Cross-region** — bucket 이 다른 region 일 때 latency
6. **회귀** — Gate / Sync stage 의 결과 정합성 (rename 후 파일 단 1개, 누락 없음)

---

## 9. 미해결 질문

- [ ] **`touchRecursively` 실제 caller** — grep 결과 어디서 쓰이고, 실제 mtime 이 의미 있는지
- [ ] **api 의 raw Hadoop FS 직접 호출처 전체 목록** — `ExtractRunResultIOService` 외에 어디 더 있는지 (`CsvUploadController`, `BatchController` 등)
- [ ] **GCS bucket 구조 결정** — 단일 bucket + prefix (`gs://athlon-prod/userlake/...`) vs 모듈별 bucket
- [ ] **Spark Connect 의 GCS 인증** — Dataproc Serverless 의 Spark 가 GCS 읽을 때 동일 SA 사용 가능한지 ([[2_Spark Connect → Dataproc Serverless 검토]] § 4-2 와 연계)
- [ ] **userlake-worker 외 athlon 모듈** 의 FileReadWriter 사용 여부 (scheduler 등)
- [ ] **로컬 개발 환경** 에서 LOCAL 유지하면 GCS 와의 의미 차이 (atomic rename 등) 가 dev/prod 격차 만들 가능성

---

## 10. 참고

- 상위 문서: [[1_userlake-worker 인프라 이관]] § 2-3
- 관련: [[2_Spark Connect → Dataproc Serverless 검토]] § 4-2 (Spark 의 GCS 인증과 연계)
- 코드 위치:
  - `core/src/main/kotlin/com/kakaopage/athlon/util/filerw/`
  - `userlake-worker/src/main/kotlin/com/kakaopage/athlon/stage/` (모든 stage)
  - `api/src/main/kotlin/com/kakaopage/athlon/userlake/service/ExtractRunResultIOService.kt` 등 (raw Hadoop)