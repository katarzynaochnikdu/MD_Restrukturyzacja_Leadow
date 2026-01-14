from __future__ import annotations

import collections
import re
from dataclasses import dataclass
from html.parser import HTMLParser
from typing import Dict, List, Tuple


PLACEHOLDER_PATTERN = re.compile(r"\{[^{}]*\}")


def extract_placeholders(text: str) -> List[str]:
    return PLACEHOLDER_PATTERN.findall(text or "")


def multiset(items: List[str]) -> Dict[str, int]:
    return collections.Counter(items)


class _TagCollector(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=False)
        self.tags: List[Tuple[str, Tuple[Tuple[str, str], ...]]] = []

    def handle_starttag(self, tag: str, attrs):
        self.tags.append((tag.lower(), tuple(sorted(((k.lower(), v or "") for k, v in attrs)))))

    def handle_startendtag(self, tag: str, attrs):
        self.tags.append((tag.lower(), tuple(sorted(((k.lower(), v or "") for k, v in attrs)))))

    def handle_endtag(self, tag: str):
        # Represent end tags to preserve count/order parity
        self.tags.append((f"/{tag.lower()}", tuple()))


def extract_html_signature(text: str) -> List[Tuple[str, Tuple[Tuple[str, str], ...]]]:
    parser = _TagCollector()
    try:
        parser.feed(text or "")
    except Exception:
        # We don't fix HTML; we just collect what we can
        pass
    return parser.tags


def compare_placeholders(en: str, pl: str) -> Tuple[bool, List[str]]:
    en_ph = extract_placeholders(en)
    pl_ph = extract_placeholders(pl)
    ok = multiset(en_ph) == multiset(pl_ph)
    issues: List[str] = []
    if not ok:
        issues.append("missing_placeholder")
    return ok, issues


def compare_html(en: str, pl: str) -> Tuple[bool, List[str]]:
    en_sig = extract_html_signature(en)
    pl_sig = extract_html_signature(pl)
    ok = en_sig == pl_sig
    issues: List[str] = []
    if not ok:
        issues.append("html_changed")
    return ok, issues

