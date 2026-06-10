#!/usr/bin/env python3
"""
Obsidian Vault의 .md 파일을 Confluence 페이지로 동기화한다.

동작:
  1. main 브랜치 push 또는 workflow_dispatch로 실행
  2. 폴더 구조를 그대로 Confluence 부모/자식 페이지로 매핑
  3. 같은 제목의 페이지가 있으면 업데이트, 없으면 생성
  4. Obsidian 위키링크([[note]])는 일반 텍스트로 풀어둠 (안전한 변환)

필요한 GitHub Secrets:
  CONFLUENCE_BASE_URL      예) https://your-domain.atlassian.net
  CONFLUENCE_EMAIL         API Token을 발급받은 사용자 이메일
  CONFLUENCE_API_TOKEN     https://id.atlassian.com/manage-profile/security/api-tokens
  CONFLUENCE_SPACE_KEY     동기화 대상 Confluence Space Key (예: DP)
  CONFLUENCE_ROOT_PAGE_ID  (선택) 매핑에 없는 top-level 폴더의 기본 부모 ID

폴더-부모 매핑:
  .github/confluence-mapping.json 의 "folders" 키 참고
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Iterable

import markdown as md_lib
import requests
from requests.auth import HTTPBasicAuth


# ---------------------------------------------------------------------------
# 환경 변수
# ---------------------------------------------------------------------------
BASE_URL = os.environ["CONFLUENCE_BASE_URL"].rstrip("/")
EMAIL = os.environ["CONFLUENCE_EMAIL"]
API_TOKEN = os.environ["CONFLUENCE_API_TOKEN"]
SPACE_KEY = os.environ["CONFLUENCE_SPACE_KEY"]
ROOT_PAGE_ID = os.environ.get("CONFLUENCE_ROOT_PAGE_ID") or None
FULL_SYNC = os.environ.get("FULL_SYNC", "false").lower() == "true"
EVENT_NAME = os.environ.get("EVENT_NAME", "push")

AUTH = HTTPBasicAuth(EMAIL, API_TOKEN)
HEADERS = {"Accept": "application/json", "Content-Type": "application/json"}
API = f"{BASE_URL}/wiki/rest/api"

# 동기화 대상 외 경로
EXCLUDED_DIRS = {".git", ".github", ".obsidian", ".claude", ".claudian"}

# ---------------------------------------------------------------------------
# 폴더 매핑 로드
# ---------------------------------------------------------------------------
MAPPING_PATH = Path(".github/confluence-mapping.json")


def load_folder_mapping() -> dict[str, str]:
    """top-level 폴더명 → Confluence 부모 ID 매핑 로드."""
    if not MAPPING_PATH.exists():
        return {}
    data = json.loads(MAPPING_PATH.read_text(encoding="utf-8"))
    folders = data.get("folders", {}) or {}
    return {str(k): str(v) for k, v in folders.items()}


FOLDER_MAPPING = load_folder_mapping()


# ---------------------------------------------------------------------------
# 변경 파일 수집
# ---------------------------------------------------------------------------
def get_changed_md_files() -> list[Path]:
    """이번 push에서 변경된 .md 파일 목록을 반환한다."""
    try:
        result = subprocess.run(
            ["git", "diff", "--name-only", "--diff-filter=ACMR", "HEAD^", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
        )
    except subprocess.CalledProcessError:
        # 첫 커밋 등 HEAD^가 없는 경우 전체로 폴백
        return get_all_md_files()

    files = [Path(line.strip()) for line in result.stdout.splitlines() if line.strip()]
    return [f for f in files if f.suffix == ".md" and not _is_excluded(f)]


def get_all_md_files() -> list[Path]:
    """저장소 전체에서 .md 파일을 수집한다."""
    files: list[Path] = []
    for path in Path(".").rglob("*.md"):
        if not _is_excluded(path):
            files.append(path)
    return files


def _is_excluded(path: Path) -> bool:
    return any(part in EXCLUDED_DIRS for part in path.parts)


# ---------------------------------------------------------------------------
# Markdown → Confluence Storage Format 변환
# ---------------------------------------------------------------------------
WIKILINK_RE = re.compile(r"\[\[([^\]|]+)(?:\|([^\]]+))?\]\]")
EMBED_RE = re.compile(r"!\[\[([^\]]+)\]\]")


def convert_markdown(md_text: str) -> str:
    """Obsidian Markdown을 Confluence가 이해할 수 있는 XHTML로 변환한다."""
    # Obsidian 전용 문법 제거/치환
    md_text = EMBED_RE.sub(lambda m: f"_(embed: {m.group(1)})_", md_text)
    md_text = WIKILINK_RE.sub(
        lambda m: m.group(2) if m.group(2) else m.group(1), md_text
    )

    html = md_lib.markdown(
        md_text,
        extensions=["fenced_code", "tables", "toc", "sane_lists"],
    )
    # Confluence storage format은 self-closing 태그를 요구
    html = re.sub(r"<br\s*>", "<br/>", html)
    html = re.sub(r"<hr\s*>", "<hr/>", html)
    return html


# ---------------------------------------------------------------------------
# Confluence API
# ---------------------------------------------------------------------------
def find_page_by_title(title: str, parent_id: str | None) -> dict | None:
    """Space + 제목으로 페이지를 검색한다."""
    params = {
        "spaceKey": SPACE_KEY,
        "title": title,
        "expand": "version,ancestors",
        "limit": 25,
    }
    resp = requests.get(
        f"{API}/content", params=params, auth=AUTH, headers=HEADERS, timeout=30
    )
    resp.raise_for_status()
    results = resp.json().get("results", [])

    if not results:
        return None
    if parent_id is None:
        return results[0]
    # 부모 일치 항목 우선 매칭
    for page in results:
        ancestors = page.get("ancestors") or []
        if ancestors and str(ancestors[-1]["id"]) == str(parent_id):
            return page
    return results[0]


def create_page(title: str, body_html: str, parent_id: str | None) -> dict:
    payload = {
        "type": "page",
        "title": title,
        "space": {"key": SPACE_KEY},
        "body": {"storage": {"value": body_html, "representation": "storage"}},
    }
    if parent_id:
        payload["ancestors"] = [{"id": str(parent_id)}]

    resp = requests.post(
        f"{API}/content", json=payload, auth=AUTH, headers=HEADERS, timeout=30
    )
    resp.raise_for_status()
    return resp.json()


def update_page(page: dict, body_html: str, parent_id: str | None) -> dict:
    page_id = page["id"]
    new_version = page["version"]["number"] + 1
    payload = {
        "id": page_id,
        "type": "page",
        "title": page["title"],
        "space": {"key": SPACE_KEY},
        "body": {"storage": {"value": body_html, "representation": "storage"}},
        "version": {"number": new_version},
    }
    if parent_id:
        payload["ancestors"] = [{"id": str(parent_id)}]

    resp = requests.put(
        f"{API}/content/{page_id}",
        json=payload,
        auth=AUTH,
        headers=HEADERS,
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def upsert_page(title: str, body_html: str, parent_id: str | None) -> str:
    """제목 기준으로 페이지를 생성/업데이트하고 page id를 반환한다."""
    existing = find_page_by_title(title, parent_id)
    if existing:
        result = update_page(existing, body_html, parent_id)
        print(f"  ↻ updated  : {title} (id={result['id']})")
    else:
        result = create_page(title, body_html, parent_id)
        print(f"  + created  : {title} (id={result['id']})")
    return result["id"]


# ---------------------------------------------------------------------------
# 폴더 구조 → 부모 페이지 매핑
# ---------------------------------------------------------------------------
_folder_cache: dict[tuple[str, ...], str | None] = {}


def ensure_folder_page(parts: tuple[str, ...]) -> str | None:
    """경로의 디렉토리들에 해당하는 부모 페이지를 만들고 가장 안쪽 id를 반환한다.

    - parts[0] (vault top-level 폴더)가 FOLDER_MAPPING에 있으면 해당 Confluence ID를
      그대로 부모로 사용하고, 별도의 "폴더 페이지"는 생성하지 않는다.
    - 그 외 하위 폴더는 일반 페이지로 생성하여 트리를 구성한다.
    """
    if not parts:
        return ROOT_PAGE_ID
    if parts in _folder_cache:
        return _folder_cache[parts]

    # top-level 매핑에 해당하면 즉시 매핑된 ID 반환
    if len(parts) == 1 and parts[0] in FOLDER_MAPPING:
        mapped = FOLDER_MAPPING[parts[0]]
        print(f"  ↳ mapped top-level '{parts[0]}' → {mapped}")
        _folder_cache[parts] = mapped
        return mapped

    parent_id = ensure_folder_page(parts[:-1])
    title = parts[-1]
    # 폴더 페이지의 본문은 자식 페이지 목록을 표시하는 매크로로 채움
    body = (
        '<p>📁 이 페이지는 폴더입니다. 하위 문서는 아래 목록에서 확인하세요.</p>'
        '<ac:structured-macro ac:name="children" ac:schema-version="2">'
        '<ac:parameter ac:name="all">true</ac:parameter>'
        '</ac:structured-macro>'
    )
    page_id = upsert_page(title, body, parent_id)
    _folder_cache[parts] = page_id
    return page_id


# ---------------------------------------------------------------------------
# 메인
# ---------------------------------------------------------------------------
def derive_title(path: Path) -> str:
    """파일 경로에서 페이지 제목을 결정한다."""
    return path.stem


def sync_file(path: Path) -> None:
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        print(f"  ! skipped (not found): {path}")
        return

    title = derive_title(path)
    body = convert_markdown(text)

    # 페이지 하단에 소스 경로 표시
    body += (
        f'<hr/><p><em>Synced from GitHub: '
        f'<code>{path.as_posix()}</code></em></p>'
    )

    parent_parts = tuple(path.parent.parts) if str(path.parent) != "." else ()
    parent_id = ensure_folder_page(parent_parts)
    upsert_page(title, body, parent_id)


def main() -> int:
    print(f"[sync] folder mapping loaded: {FOLDER_MAPPING or '(empty)'}")
    if ROOT_PAGE_ID:
        print(f"[sync] default root page id: {ROOT_PAGE_ID}")

    if FULL_SYNC or EVENT_NAME == "workflow_dispatch":
        files = get_all_md_files()
        mode = "FULL"
    else:
        files = get_changed_md_files()
        mode = "INCREMENTAL"

    print(f"[sync] mode={mode}, target_files={len(files)}")
    if not files:
        print("[sync] no markdown changes — nothing to do.")
        return 0

    for path in sorted(files):
        print(f"\n→ {path}")
        try:
            sync_file(path)
        except requests.HTTPError as exc:
            body = exc.response.text if exc.response is not None else ""
            print(f"  ✗ HTTPError: {exc} :: {body[:400]}", file=sys.stderr)
            return 1
        except Exception as exc:  # noqa: BLE001
            print(f"  ✗ Failed: {exc}", file=sys.stderr)
            return 1

    print("\n[sync] done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
