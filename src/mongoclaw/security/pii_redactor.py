"""PII detection and redaction."""

from __future__ import annotations

import re
from typing import Any

from mongoclaw.observability.logging import get_logger

logger = get_logger(__name__)


class PIIType:
    """Common PII types."""

    EMAIL = "email"
    PHONE = "phone"
    SSN = "ssn"
    CREDIT_CARD = "credit_card"
    IP_ADDRESS = "ip_address"
    DATE_OF_BIRTH = "date_of_birth"
    ADDRESS = "address"
    NAME = "name"


class PIIRedactor:
    """
    Detects and redacts PII from text and documents.

    Supports:
    - Email addresses
    - Phone numbers
    - Social Security Numbers
    - Credit card numbers
    - IP addresses
    - Custom patterns
    """

    DEFAULT_PATTERNS: dict[str, str] = {
        PIIType.EMAIL: r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b",
        PIIType.PHONE: r"\b(?:\+?1[-.\s]?)?\(?[0-9]{3}\)?[-.\s]?[0-9]{3}[-.\s]?[0-9]{4}\b",
        PIIType.SSN: r"\b\d{3}-\d{2}-\d{4}\b",
        PIIType.CREDIT_CARD: r"\b(?:\d{4}[-\s]?){3}\d{4}\b",
        PIIType.IP_ADDRESS: r"\b(?:\d{1,3}\.){3}\d{1,3}\b",
    }

    def __init__(
        self,
        enabled: bool = True,
        redaction_char: str = "*",
        patterns: dict[str, str] | None = None,
        disabled_types: set[str] | None = None,
    ) -> None:
        """
        Initialize PII redactor.

        Args:
            enabled: Whether redaction is enabled.
            redaction_char: Character to use for redaction.
            patterns: Custom patterns to add/override.
            disabled_types: PII types to skip.
        """
        self._enabled = enabled
        self._redaction_char = redaction_char
        self._disabled_types = disabled_types or set()

        # Compile patterns
        self._patterns: dict[str, re.Pattern[str]] = {}
        all_patterns = {**self.DEFAULT_PATTERNS, **(patterns or {})}

        for pii_type, pattern in all_patterns.items():
            if pii_type not in self._disabled_types:
                self._patterns[pii_type] = re.compile(pattern, re.IGNORECASE)

    @property
    def enabled(self) -> bool:
        """Check if redaction is enabled."""
        return self._enabled

    def redact_text(
        self,
        text: str,
        pii_types: list[str] | None = None,
    ) -> tuple[str, list[dict[str, Any]]]:
        """
        Redact PII from text.

        Args:
            text: Text to redact.
            pii_types: Specific PII types to redact (all if None).

        Returns:
            Tuple of (redacted_text, list of detections).
        """
        if not self._enabled or not text:
            return text, []

        detections: list[dict[str, Any]] = []
        result = text

        patterns_to_check = (
            {k: v for k, v in self._patterns.items() if k in pii_types}
            if pii_types
            else self._patterns
        )

        for pii_type, pattern in patterns_to_check.items():
            for match in pattern.finditer(result):
                detections.append({
                    "type": pii_type,
                    "start": match.start(),
                    "end": match.end(),
                    "length": match.end() - match.start(),
                })

            # Redact matches
            result = pattern.sub(
                lambda m: self._redaction_char * len(m.group()),
                result,
            )

        if detections:
            logger.debug(
                "Redacted PII",
                detection_count=len(detections),
                types=list(set(d["type"] for d in detections)),
            )

        return result, detections

    def redact_document(
        self,
        document: dict[str, Any],
        fields: list[str] | None = None,
        recursive: bool = True,
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        """
        Redact PII from a document.

        Args:
            document: Document to redact.
            fields: Specific fields to check (all if None).
            recursive: Process nested documents.

        Returns:
            Tuple of (redacted_document, list of detections).
        """
        if not self._enabled:
            return document, []

        all_detections: list[dict[str, Any]] = []
        result = self._redact_value(
            document,
            fields=fields,
            recursive=recursive,
            path="",
            detections=all_detections,
        )

        return result, all_detections

    def _redact_value(
        self,
        value: Any,
        fields: list[str] | None,
        recursive: bool,
        path: str,
        detections: list[dict[str, Any]],
    ) -> Any:
        """Recursively redact values."""
        if isinstance(value, str):
            redacted, found = self.redact_text(value)
            for detection in found:
                detection["path"] = path
                detections.append(detection)
            return redacted

        elif isinstance(value, dict):
            result = {}
            for key, val in value.items():
                current_path = f"{path}.{key}" if path else key

                # Check if we should process this field
                should_process = (
                    fields is None
                    or key in fields
                    or any(f.startswith(current_path) for f in (fields or []))
                )

                if should_process and (recursive or not isinstance(val, dict)):
                    result[key] = self._redact_value(
                        val, fields, recursive, current_path, detections
                    )
                else:
                    result[key] = val

            return result

        elif isinstance(value, list):
            return [
                self._redact_value(
                    item, fields, recursive, f"{path}[{i}]", detections
                )
                for i, item in enumerate(value)
            ]

        else:
            return value

    def detect_only(self, text: str) -> list[dict[str, Any]]:
        """
        Detect PII without redacting.

        Args:
            text: Text to scan.

        Returns:
            List of detections.
        """
        if not text:
            return []

        detections = []

        for pii_type, pattern in self._patterns.items():
            for match in pattern.finditer(text):
                detections.append({
                    "type": pii_type,
                    "start": match.start(),
                    "end": match.end(),
                    "value": match.group(),
                })

        return detections

    def add_pattern(self, pii_type: str, pattern: str) -> None:
        """
        Add a custom PII pattern.

        Args:
            pii_type: Type identifier.
            pattern: Regex pattern.
        """
        self._patterns[pii_type] = re.compile(pattern, re.IGNORECASE)

    def remove_pattern(self, pii_type: str) -> None:
        """Remove a PII pattern."""
        self._patterns.pop(pii_type, None)

    def set_enabled(self, enabled: bool) -> None:
        """Enable or disable redaction."""
        self._enabled = enabled
