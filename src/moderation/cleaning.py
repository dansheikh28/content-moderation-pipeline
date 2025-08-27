from __future__ import annotations

import re
from html import unescape

_URL_RE = re.compile(r"https?://\S+|www\.\S+", re.IGNORECASE)
_HTML_TAG_RE = re.compile(r"<[^>]+>")
_MULTI_WS_RE = re.compile(r"\s+")
_PUNCT_FIX_RE = re.compile(r"\s+([!?.,;:])")


def strip_html(text: str) -> str:
    return _HTML_TAG_RE.sub(" ", unescape(text))


def remove_urls(text: str) -> str:
    return _URL_RE.sub(" ", text)


def normalize_whitespace(text: str) -> str:
    return _MULTI_WS_RE.sub(" ", text).strip()


def basic_clean(text: str) -> str:
    """
    - remove HTML tags/entities
    - remove URLs
    - lowercase
    - collapse whitespace
    - fix spaces before punctuation
    """
    if text is None:
        return ""
    t = str(text)
    t = strip_html(t)
    t = remove_urls(t)
    t = t.lower()
    t = normalize_whitespace(t)
    t = _PUNCT_FIX_RE.sub(r"\1", t)
    return t
