"""Configuration loading for the automation harness."""

from __future__ import annotations

import os
import platform
import uuid
from dataclasses import dataclass, field
from pathlib import Path


def _load_dotenv(path: Path) -> None:
    """Populate os.environ from a .env file without overriding existing values."""
    if not path.is_file():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


@dataclass(frozen=True)
class HarnessConfig:
    """Runtime settings shared by every deployment.

    Values come from constructor arguments, falling back to environment
    variables (optionally loaded from a .env file), then defaults.
    """

    data_dir: Path
    node_id: str = field(default="")

    def __post_init__(self) -> None:
        object.__setattr__(self, "data_dir", Path(self.data_dir))
        if not self.node_id:
            host = platform.node() or "node"
            object.__setattr__(self, "node_id", f"{host}-{uuid.uuid4().hex[:8]}")

    @property
    def database_path(self) -> Path:
        return self.data_dir / "harness.db"

    @property
    def lock_path(self) -> Path:
        return self.data_dir / "harness.lock"

    @property
    def status_path(self) -> Path:
        return self.data_dir / "status.json"

    @classmethod
    def from_env(cls, env_file: str | Path | None = ".env") -> HarnessConfig:
        if env_file is not None:
            _load_dotenv(Path(env_file))
        data_dir = os.environ.get("AUTOMATION_HARNESS_DATA_DIR", "data")
        node_id = os.environ.get("AUTOMATION_HARNESS_NODE_ID", "")
        return cls(data_dir=Path(data_dir), node_id=node_id)
