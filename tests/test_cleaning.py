import re

from moderation.cleaning import basic_clean


def test_basic_clean_removes_html_urls_and_normalizes():
    s = "Hello <b>WORLD</b>! Visit https://example.com now.\n\n Thanks!"
    out = basic_clean(s)

    # Core behaviors
    assert out == out.lower()  # lowercased
    assert "<b>" not in out  # no html
    assert "http" not in out and "www." not in out  # no urls

    # Key tokens still present
    for tok in ["hello", "world", "visit", "now", "thanks"]:
        assert tok in out

    # Spacing/punctuation sanity
    assert not re.search(r"\s{2,}", out)  # no double spaces
    assert not re.search(r"\s[!?.,;:]", out)  # no space before punctuation
    assert re.search(r"thanks[!?.,;:]?$", out)  # ends with 'thanks' (punct optional)
