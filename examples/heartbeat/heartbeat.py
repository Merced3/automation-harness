"""Heartbeat example: minimal neutral automation on the harness.

Demonstrates lifecycle, persistent scheduling, restart recovery, job state,
and status reporting without any application-specific behavior.

Run:
    python examples/heartbeat/heartbeat.py

Then stop it (Ctrl+C or kill the process) and start it again: the schedule,
run history, and beat count persist in data/ and interrupted runs are marked.
Current status is always available in data/status.json.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from automation_harness import Harness, HarnessConfig, JobContext


def heartbeat(ctx: JobContext) -> None:
    beats = ctx.get_state("beats", 0) + 1
    ctx.set_state("beats", beats)
    print(f"heartbeat #{beats} (run_id={ctx.run_id})")


def main() -> None:
    harness = Harness(HarnessConfig(data_dir=Path(__file__).parent / "data"), name="heartbeat")
    harness.add_job("heartbeat", heartbeat, every_s=5, run_immediately=True)
    harness.run()


if __name__ == "__main__":
    main()
