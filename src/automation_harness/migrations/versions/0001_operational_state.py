"""Initial portable operational-state schema.

Revision ID: 0001
Revises:
Create Date: 2026-11-07
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "meta",
        sa.Column("key", sa.Text(), primary_key=True),
        sa.Column("value", sa.Text(), nullable=False),
    )
    op.create_table(
        "schedules",
        sa.Column("job_name", sa.Text(), primary_key=True),
        sa.Column("interval_s", sa.Double(), nullable=False),
        sa.Column("next_run_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_run_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.true()),
    )
    op.create_table(
        "runs",
        sa.Column("seq", sa.BigInteger(), sa.Identity(), primary_key=True),
        sa.Column("id", sa.Uuid(), nullable=False, unique=True),
        sa.Column("job_name", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("pid", sa.BigInteger(), nullable=False),
        sa.Column("node", sa.Text(), nullable=False),
    )
    op.create_index("idx_runs_job", "runs", ["job_name", "seq"])
    op.create_table(
        "kv_state",
        sa.Column("key", sa.Text(), primary_key=True),
        sa.Column("value", postgresql.JSONB(), nullable=False),
    )
    op.create_table(
        "instance_leases",
        sa.Column("harness_name", sa.Text(), primary_key=True),
        sa.Column("owner", sa.Uuid(), nullable=False, unique=True),
        sa.Column("acquired_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_table(
        "run_sources",
        sa.Column("source", sa.Text(), primary_key=True),
        sa.Column("source_run_id", sa.Text(), primary_key=True),
        sa.Column("run_id", sa.Uuid(), sa.ForeignKey("runs.id"), nullable=False),
    )


def downgrade() -> None:
    op.drop_table("run_sources")
    op.drop_table("instance_leases")
    op.drop_table("kv_state")
    op.drop_index("idx_runs_job", table_name="runs")
    op.drop_table("runs")
    op.drop_table("schedules")
    op.drop_table("meta")
