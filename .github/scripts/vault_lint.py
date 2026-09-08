#!/usr/bin/env python3
"""Vault lint — wiki-manifest 계약 위반을 기계 검사한다.

검사 항목:
  L1  이미지 파일 유입 금지 (이미지 미사용 정책, 2026-09-08)
  L2  이미지 임베드(![[...png]]) 금지
  L3  frontmatter status 어휘: wip | draft | final | archived
  L4  신호 모순: wip/ 안의 노트가 status: final
  L5  깨진 wikilink (경로형·파일명형 모두 해석)

에러가 있으면 exit 1 — push마다 CI에서 실행된다. 로컬 실행: python3 .github/scripts/vault_lint.py
"""
import re
import sys
import pathlib

ROOT = pathlib.Path(__file__).resolve().parents[2]
ALLOWED_STATUS = {"wip", "draft", "final", "archived"}
IMG_EXT = {".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp", ".svg"}

errors = []

md_files = [p for p in ROOT.rglob("*.md") if ".git" not in p.parts]
stems = {}
for p in md_files:
    stems.setdefault(p.stem, []).append(p)

# L1 — 이미지 파일 유입
for p in ROOT.rglob("*"):
    if ".git" in p.parts or not p.is_file():
        continue
    if p.suffix.lower() in IMG_EXT:
        errors.append(f"L1 [이미지 파일] {p.relative_to(ROOT)} — 캡쳐 내용은 글로, 다이어그램은 mermaid 코드블록으로")

status_re = re.compile(r"^status:\s*([^\s#]+)", re.M)
embed_re = re.compile(r"!\[\[([^\]]+)\]\]")
wikilink_re = re.compile(r"\[\[([^\]\|#]+)")
codeblock_re = re.compile(r"```.*?```", re.S)
inline_code_re = re.compile(r"`[^`\n]*`")

for p in md_files:
    rel = p.relative_to(ROOT)
    text = p.read_text(encoding="utf-8")

    # L3/L4 — frontmatter status (파일 맨 앞 --- 블록만)
    if text.startswith("---"):
        parts = text.split("---", 2)
        fm = parts[1] if len(parts) >= 3 else ""
        m = status_re.search(fm)
        if m:
            val = m.group(1)
            if val not in ALLOWED_STATUS:
                errors.append(f"L3 [status 어휘] {rel}: '{val}' — wip|draft|final|archived 만 허용")
            if val == "final" and rel.parts[0] == "wip":
                errors.append(f"L4 [신호 모순] {rel}: wip/ 안의 노트가 status: final")

    body = codeblock_re.sub("", text)
    body = inline_code_re.sub("", body)

    # L2 — 이미지 임베드
    for emb in embed_re.findall(body):
        if pathlib.Path(emb.split("|")[0].strip()).suffix.lower() in IMG_EXT:
            errors.append(f"L2 [이미지 임베드] {rel}: ![[{emb}]]")

    # L5 — 깨진 wikilink
    for raw in wikilink_re.findall(body):
        t = raw.strip().rstrip("\\")  # 표 안 이스케이프된 alias 구분자([[경로\|표시]]) 잔여물 제거
        if not t or "://" in t:
            continue
        # 코드 경로 참조(.py:30 등) / md 외 확장자는 링크 검사 대상 아님
        if re.search(r"\.[A-Za-z]\w*(:\d+)?$", t) and not t.endswith(".md"):
            continue
        t = t.removesuffix(".md")
        if "/" in t:
            # 경로형: vault 루트 기준 또는 해당 노트 기준 상대 경로
            if not (ROOT / f"{t}.md").exists() and not (p.parent / f"{t}.md").resolve().exists():
                errors.append(f"L5 [깨진 링크] {rel}: [[{raw.strip()}]]")
        else:
            if t not in stems:
                errors.append(f"L5 [깨진 링크] {rel}: [[{raw.strip()}]]")

if errors:
    print(f"✗ vault lint 실패 — {len(errors)}건")
    for e in errors:
        print("  " + e)
    sys.exit(1)
print(f"✓ vault lint 통과 — 노트 {len(md_files)}개")
