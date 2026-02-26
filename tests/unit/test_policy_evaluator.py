"""Unit tests for policy condition evaluator."""

from __future__ import annotations

import pytest

from mongoclaw.policy.evaluator import PolicyEvaluationError, evaluate_policy_condition


def test_evaluate_simple_condition() -> None:
    context = {
        "document": {"amount": 5000},
        "result": {"risk_score": 0.95},
    }
    assert evaluate_policy_condition("result.risk_score > 0.8", context) is True
    assert evaluate_policy_condition("document.amount < 1000", context) is False


def test_evaluate_boolean_condition() -> None:
    context = {
        "document": {"status": "open"},
        "result": {"risk_score": 0.5},
    }
    assert evaluate_policy_condition("document.status == 'open' and result.risk_score <= 0.5", context)


def test_invalid_expression_raises() -> None:
    with pytest.raises(PolicyEvaluationError):
        evaluate_policy_condition("os.system('rm -rf /')", {"document": {}, "result": {}})
