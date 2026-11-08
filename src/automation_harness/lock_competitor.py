"""Small process-boundary probe used by the instance-lease integration test."""

from __future__ import annotations

import sys
import time
import traceback
from pathlib import Path

from . import AlreadyRunningError, Harness, HarnessConfig


def main(argv: list[str] | None = None) -> int:
    args = argv or sys.argv[1:]
    url, name, ready_file = args
    already_ready = Path(ready_file).exists()
    harness = Harness(
        HarnessConfig(
            data_dir=Path(ready_file).parent, database_url=url, lock_lease_seconds=2
        ),
        name=name,
    )
    try:
        harness.start()
    except AlreadyRunningError:
        traceback.print_exc()
        harness.store.close()
        return 3
    Path(ready_file).write_text("ready")
    if not already_ready:
        time.sleep(30)
    harness.shutdown()
    return 0


if __name__ == "__main__":
    sys.exit(main())
