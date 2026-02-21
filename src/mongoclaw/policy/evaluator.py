"""Safe evaluator for declarative policy conditions."""

from __future__ import annotations

import ast
from typing import Any


class PolicyEvaluationError(ValueError):
    """Raised when a policy expression is invalid or unsupported."""


def evaluate_policy_condition(condition: str, context: dict[str, Any]) -> bool:
    """
    Evaluate a policy condition expression against the provided context.

    Supported:
    - boolean ops: and, or, not
    - comparisons: ==, !=, >, >=, <, <=, in, not in
    - names/attributes: document.<field>, result.<field>
    - constants: strings, numbers, booleans, null
    """
    try:
        expr = ast.parse(condition, mode="eval")
    except SyntaxError as exc:
        raise PolicyEvaluationError(f"Invalid policy condition: {condition}") from exc

    return bool(_eval_node(expr.body, context))


def _eval_node(node: ast.AST, context: dict[str, Any]) -> Any:
    if isinstance(node, ast.BoolOp):
        values = [_eval_node(v, context) for v in node.values]
        if isinstance(node.op, ast.And):
            return all(values)
        if isinstance(node.op, ast.Or):
            return any(values)
        raise PolicyEvaluationError(f"Unsupported boolean operator: {type(node.op).__name__}")

    if isinstance(node, ast.UnaryOp):
        if isinstance(node.op, ast.Not):
            return not bool(_eval_node(node.operand, context))
        raise PolicyEvaluationError(f"Unsupported unary operator: {type(node.op).__name__}")

    if isinstance(node, ast.Compare):
        left = _eval_node(node.left, context)
        for op, comparator in zip(node.ops, node.comparators, strict=False):
            right = _eval_node(comparator, context)
            if not _eval_compare(op, left, right):
                return False
            left = right
        return True

    if isinstance(node, ast.Name):
        if node.id in context:
            return context[node.id]
        raise PolicyEvaluationError(f"Unknown symbol in policy condition: {node.id}")

    if isinstance(node, ast.Attribute):
        base = _eval_node(node.value, context)
        if isinstance(base, dict):
            return base.get(node.attr)
        return getattr(base, node.attr, None)

    if isinstance(node, ast.Constant):
        return node.value

    raise PolicyEvaluationError(f"Unsupported expression node: {type(node).__name__}")


def _eval_compare(op: ast.cmpop, left: Any, right: Any) -> bool:
    if isinstance(op, ast.Eq):
        return left == right
    if isinstance(op, ast.NotEq):
        return left != right
    if isinstance(op, ast.Gt):
        return left is not None and right is not None and left > right
    if isinstance(op, ast.GtE):
        return left is not None and right is not None and left >= right
    if isinstance(op, ast.Lt):
        return left is not None and right is not None and left < right
    if isinstance(op, ast.LtE):
        return left is not None and right is not None and left <= right
    if isinstance(op, ast.In):
        return left in right if right is not None else False
    if isinstance(op, ast.NotIn):
        return left not in right if right is not None else True
    raise PolicyEvaluationError(f"Unsupported comparison operator: {type(op).__name__}")
