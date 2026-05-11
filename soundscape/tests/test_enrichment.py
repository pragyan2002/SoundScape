"""
Tests for enrichment pipeline — mocks Gemini API and S3.
"""
import json
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from soundscape.enrichment.enrich_tracks import (
    build_prompt,
    enrich_batch,
    BATCH_SIZE,
    PROMPT_TEMPLATE,
)


SAMPLE_TRACKS = [
    {
        "track_id": 1,
        "title": "Moonlight Drive",
        "artist_name": "The Doors",
        "genres": "['Rock']",
        "tags": "psychedelic",
    },
    {
        "track_id": 2,
        "title": "Blue in Green",
        "artist_name": "Miles Davis",
        "genres": "['Jazz']",
        "tags": "instrumental",
    },
]

MOCK_GEMINI_RESPONSE = [
    {
        "mood_tags": ["melancholic", "dreamy", "introspective"],
        "theme_tags": ["night", "journey", "longing"],
        "energy_level": "medium",
        "danceability_guess": "low",
        "content_summary": "A psychedelic rock track evoking a moonlit drive.",
    },
    {
        "mood_tags": ["peaceful", "contemplative", "tender"],
        "theme_tags": ["solitude", "jazz", "emotion"],
        "energy_level": "low",
        "danceability_guess": "low",
        "content_summary": "A slow jazz ballad with muted trumpet and sparse piano.",
    },
]


def test_build_prompt_contains_track_info():
    prompt = build_prompt(SAMPLE_TRACKS)
    assert "Moonlight Drive" in prompt
    assert "Miles Davis" in prompt
    assert "Rock" in prompt


def test_build_prompt_instructs_json_array():
    prompt = build_prompt(SAMPLE_TRACKS)
    assert "JSON array" in prompt
    assert "mood_tags" in prompt
    assert "energy_level" in prompt


def _make_mock_client(response_data: list) -> MagicMock:
    mock_response = MagicMock()
    mock_response.text = json.dumps(response_data)
    mock_client = MagicMock()
    mock_client.models.generate_content.return_value = mock_response
    return mock_client


def test_enrich_batch_returns_correct_shape():
    client = _make_mock_client(MOCK_GEMINI_RESPONSE)
    results = enrich_batch(client, SAMPLE_TRACKS)

    assert len(results) == len(SAMPLE_TRACKS)
    assert results[0]["track_id"] == 1
    assert results[1]["track_id"] == 2


def test_enrich_batch_fields_present():
    client = _make_mock_client(MOCK_GEMINI_RESPONSE)
    results = enrich_batch(client, SAMPLE_TRACKS)

    for r in results:
        assert "mood_tags" in r
        assert "theme_tags" in r
        assert "energy_level" in r
        assert "danceability_guess" in r
        assert "content_summary" in r
        assert isinstance(r["mood_tags"], list)
        assert isinstance(r["theme_tags"], list)


def test_enrich_batch_handles_short_response():
    """Model returns fewer items than batch size — should fill remaining with empty."""
    client = _make_mock_client(MOCK_GEMINI_RESPONSE[:1])  # only 1 result for 2 tracks
    results = enrich_batch(client, SAMPLE_TRACKS)

    assert len(results) == 2
    assert results[0]["mood_tags"] == MOCK_GEMINI_RESPONSE[0]["mood_tags"]
    assert results[1]["mood_tags"] == []  # filled with empty


def test_enrich_batch_energy_level_valid():
    client = _make_mock_client(MOCK_GEMINI_RESPONSE)
    results = enrich_batch(client, SAMPLE_TRACKS)

    for r in results:
        assert r["energy_level"] in ("low", "medium", "high", "")


def test_batch_size_constant():
    assert BATCH_SIZE == 10
