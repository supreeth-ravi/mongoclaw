"""Unit tests for prompt engine."""

from __future__ import annotations

import pytest

from mongoclaw.ai.prompt_engine import PromptEngine


class TestPromptEngine:
    """Tests for PromptEngine."""

    @pytest.fixture
    def engine(self):
        """Create a prompt engine for testing."""
        return PromptEngine()

    def test_simple_render(self, engine):
        """Test simple template rendering."""
        template = "Hello {{ name }}!"
        context = {"name": "World"}
        result = engine.render(template, context)
        assert result == "Hello World!"

    def test_render_with_document(self, engine):
        """Test rendering with document context."""
        template = "Process: {{ document.title }}"
        document = {"title": "Test Title", "content": "Test content"}
        context = engine.build_context(document=document)
        result = engine.render(template, context)
        assert result == "Process: Test Title"

    def test_nested_document_access(self, engine):
        """Test accessing nested document fields."""
        template = "User: {{ document.user.name }}"
        document = {"user": {"name": "John", "email": "john@test.com"}}
        context = engine.build_context(document=document)
        result = engine.render(template, context)
        assert result == "User: John"

    def test_truncate_filter(self, engine):
        """Test truncate filter."""
        template = "{{ text | truncate(10) }}"
        context = {"text": "This is a very long text"}
        result = engine.render(template, context)
        assert len(result) <= 13  # 10 chars + "..."

    def test_default_filter(self, engine):
        """Test default filter for missing values."""
        template = "Value: {{ missing | default('N/A') }}"
        context = {}
        result = engine.render(template, context)
        assert result == "Value: N/A"

    def test_json_filter(self, engine):
        """Test JSON filter."""
        template = "Data: {{ data | tojson }}"
        context = {"data": {"key": "value"}}
        result = engine.render(template, context)
        assert '"key"' in result
        assert '"value"' in result

    def test_get_required_variables(self, engine):
        """Test extracting required variables from template."""
        template = "{{ document.title }} by {{ document.author }}"
        variables = engine.get_required_variables(template)
        assert "document.title" in variables or "document" in variables

    def test_conditional_rendering(self, engine):
        """Test conditional template rendering."""
        template = "{% if document.get('priority') %}Priority: {{ document.priority }}{% endif %}"

        # With priority
        context = engine.build_context(document={"priority": "high"})
        result = engine.render(template, context)
        assert result == "Priority: high"

        # Without priority - use document.get() to avoid undefined error
        context = engine.build_context(document={})
        result = engine.render(template, context)
        assert result == ""

    def test_loop_rendering(self, engine):
        """Test loop rendering in templates."""
        template = "{% for item in items %}{{ item }},{% endfor %}"
        context = {"items": ["a", "b", "c"]}
        result = engine.render(template, context)
        assert result == "a,b,c,"

    def test_whitespace_handling(self, engine):
        """Test whitespace handling in templates."""
        template = """
        Title: {{ document.title }}
        Content: {{ document.content }}
        """
        document = {"title": "Test", "content": "Body"}
        context = engine.build_context(document=document)
        result = engine.render(template, context)
        assert "Title: Test" in result
        assert "Content: Body" in result
