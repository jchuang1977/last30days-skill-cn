from datetime import date

import pytest

from web.service import SOURCES, validate_payload


def test_validate_payload_normalizes_defaults():
    result = validate_payload({"topic": "AI 工具", "sources": ["weibo"], "days": "7"})
    assert result["topic"] == "AI 工具"
    assert result["days"] == 7
    assert result["depth"] == "default"
    assert result["as_of"] == date.today().isoformat()


@pytest.mark.parametrize("payload", [
    {"topic": "", "sources": []},
    {"topic": "x" * 201, "sources": []},
    {"topic": "test", "days": 31, "sources": []},
    {"topic": "test", "sources": ["unknown"]},
    {"topic": "test", "as_of": "2030-01-01", "sources": []},
])
def test_validate_payload_rejects_invalid_input(payload):
    with pytest.raises(ValueError):
        validate_payload(payload)


def test_source_catalog_matches_public_contract():
    assert len(SOURCES) == 8
    assert "xiaohongshu" in SOURCES
