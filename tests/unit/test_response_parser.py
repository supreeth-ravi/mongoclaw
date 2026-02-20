"""Unit tests for response parser."""

from __future__ import annotations

import pytest

from mongoclaw.ai.response_parser import ResponseParser
from mongoclaw.core.types import AIResponse


class TestResponseParser:
    """Tests for ResponseParser."""

    @pytest.fixture
    def parser(self):
        """Create a response parser for testing."""
        return ResponseParser()

    @pytest.fixture
    def ai_response(self):
        """Create a sample AI response."""
        return AIResponse(
            content='{"category": "technical", "priority": "high"}',
            model="gpt-4o-mini",
            prompt_tokens=100,
            completion_tokens=50,
            total_tokens=150,
            latency_ms=500,
            cost_usd=0.001,
        )

    def test_parse_json_response(self, parser, ai_response):
        """Test parsing JSON response."""
        result = parser.parse(ai_response)
        assert result["category"] == "technical"
        assert result["priority"] == "high"

    def test_parse_json_in_markdown(self, parser):
        """Test parsing JSON wrapped in markdown code blocks."""
        response = AIResponse(
            content='```json\n{"result": "success"}\n```',
            model="gpt-4o-mini",
            prompt_tokens=100,
            completion_tokens=50,
            total_tokens=150,
            latency_ms=500,
            cost_usd=0.001,
        )
        result = parser.parse(response)
        assert result["result"] == "success"

    def test_parse_with_schema_validation(self, parser, ai_response):
        """Test parsing with schema validation."""
        schema = {
            "type": "object",
            "properties": {
                "category": {"type": "string"},
                "priority": {"type": "string"},
            },
            "required": ["category", "priority"],
        }
        result = parser.parse(ai_response, schema)
        assert "category" in result
        assert "priority" in result

    def test_parse_array_response(self, parser):
        """Test parsing array response."""
        response = AIResponse(
            content='["item1", "item2", "item3"]',
            model="gpt-4o-mini",
            prompt_tokens=100,
            completion_tokens=50,
            total_tokens=150,
            latency_ms=500,
            cost_usd=0.001,
        )
        result = parser.parse(response)
        assert isinstance(result, list)
        assert len(result) == 3

    def test_parse_nested_json(self, parser):
        """Test parsing nested JSON response."""
        response = AIResponse(
            content='{"user": {"name": "John", "age": 30}, "active": true}',
            model="gpt-4o-mini",
            prompt_tokens=100,
            completion_tokens=50,
            total_tokens=150,
            latency_ms=500,
            cost_usd=0.001,
        )
        result = parser.parse(response)
        assert result["user"]["name"] == "John"
        assert result["active"] is True

    def test_extract_json_from_text(self, parser):
        """Test extracting JSON from mixed text."""
        response = AIResponse(
            content='Here is the result: {"status": "ok"} Hope this helps!',
            model="gpt-4o-mini",
            prompt_tokens=100,
            completion_tokens=50,
            total_tokens=150,
            latency_ms=500,
            cost_usd=0.001,
        )
        result = parser.parse(response)
        assert result["status"] == "ok"

    def test_parse_invalid_json_strict(self):
        """Test parsing invalid JSON raises error in strict mode."""
        strict_parser = ResponseParser(strict=True)
        response = AIResponse(
            content="This is not JSON at all",
            model="gpt-4o-mini",
            prompt_tokens=100,
            completion_tokens=50,
            total_tokens=150,
            latency_ms=500,
            cost_usd=0.001,
        )
        with pytest.raises(Exception):
            strict_parser.parse(response)

    def test_parse_invalid_json_non_strict(self, parser):
        """Test parsing invalid JSON returns fallback in non-strict mode."""
        response = AIResponse(
            content="This is not JSON at all",
            model="gpt-4o-mini",
            prompt_tokens=100,
            completion_tokens=50,
            total_tokens=150,
            latency_ms=500,
            cost_usd=0.001,
        )
        result = parser.parse(response)
        assert "_raw" in result
        assert result["content"] == "This is not JSON at all"
