from __future__ import annotations

import json
from pathlib import Path

from mongoclaw.sdk import MongoClawClient


def main() -> None:
    fixture_path = Path(__file__).resolve().parents[1] / "fixtures" / "method_py_agent.json"
    config = json.loads(fixture_path.read_text())

    client = MongoClawClient(base_url="http://127.0.0.1:8000", api_key="test-key")
    created = client.create_agent(config)
    print(created.id)


if __name__ == "__main__":
    main()
