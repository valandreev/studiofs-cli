#!/usr/bin/env python3
"""Render the landing page release badge from GitHub's latest-release API response."""

import json
import re
import sys
from pathlib import Path


MARKER = "<!-- RELEASE_BADGE -->"
RELEASE_TAG = re.compile(r"v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\Z")
SLOGAN = "Giving penguins the love they deserve."


def render_release_badge(metadata: object) -> str:
    if not isinstance(metadata, dict):
        raise ValueError("GitHub release metadata must be an object")
    tag = metadata.get("tag_name")
    if not isinstance(tag, str) or RELEASE_TAG.fullmatch(tag) is None:
        raise ValueError("latest release must have a stable vMAJOR.MINOR.PATCH tag")
    if metadata.get("draft") is not False or metadata.get("prerelease") is not False:
        raise ValueError("latest release must be published and stable")

    descriptor = SLOGAN if tag == "v0.2.1" else "Native builds for Windows, Linux &amp; macOS"
    url = f"https://github.com/valandreev/studiofs-cli/releases/tag/{tag}"
    return f'<a class="pill" href="{url}"><b>{tag}</b> {descriptor}</a>'


def render_page(metadata: object, source: str) -> str:
    if source.count(MARKER) != 1:
        raise ValueError("landing page must contain exactly one release badge marker")
    return source.replace(MARKER, render_release_badge(metadata))


def main() -> None:
    metadata_path, page_path = map(Path, sys.argv[1:])
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    page = page_path.read_text(encoding="utf-8")
    page_path.write_text(render_page(metadata, page), encoding="utf-8", newline="")


if __name__ == "__main__":
    main()
