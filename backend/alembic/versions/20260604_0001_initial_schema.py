"""Initial schema for jobs, tasks, attempts, and records."""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260604_0001"
down_revision = None
branch_labels = None
depends_on = None


job_status = postgresql.ENUM(
    "pending",
    "running",
    "completed",
    "completed_with_failures",
    "failed",
    name="job_status",
    create_type=False,
)
task_status = postgresql.ENUM(
    "pending",
    "in_progress",
    "completed",
    "failed",
    name="task_status",
    create_type=False,
)
attempt_status = postgresql.ENUM(
    "running",
    "succeeded",
    "failed",
    name="attempt_status",
    create_type=False,
)
feed_type = postgresql.ENUM(
    "rss",
    "atom",
    "rdf",
    "unknown",
    name="feed_type",
    create_type=False,
)


def upgrade() -> None:
    bind = op.get_bind()
    job_status.create(bind, checkfirst=True)
    task_status.create(bind, checkfirst=True)
    attempt_status.create(bind, checkfirst=True)
    feed_type.create(bind, checkfirst=True)

    op.create_table(
        "jobs",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("idempotency_key", sa.Text(), nullable=False),
        sa.Column("temporal_run_id", sa.Text(), nullable=True),
        sa.Column("status", job_status, nullable=False, server_default="pending"),
        sa.Column("total_urls", sa.Integer(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.UniqueConstraint("idempotency_key", name="uq_jobs_idempotency_key"),
    )
    op.create_index("ix_jobs_created_at", "jobs", [sa.text("created_at DESC")], unique=False)

    op.create_table(
        "job_tasks",
        sa.Column("id", sa.BigInteger(), sa.Identity(always=True), primary_key=True, nullable=False),
        sa.Column("job_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False),
        sa.Column("url", sa.Text(), nullable=False),
        sa.Column("status", task_status, nullable=False, server_default="pending"),
        sa.Column("queue", sa.Text(), nullable=True),
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("last_error_type", sa.Text(), nullable=True),
        sa.Column("records_extracted", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("duration_ms", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    )
    op.create_index("ix_job_tasks_job_id_status", "job_tasks", ["job_id", "status"], unique=False)
    op.create_index("ix_job_tasks_job_id_id", "job_tasks", ["job_id", "id"], unique=False)

    op.create_table(
        "task_attempts",
        sa.Column("id", sa.BigInteger(), sa.Identity(always=True), primary_key=True, nullable=False),
        sa.Column("task_id", sa.BigInteger(), sa.ForeignKey("job_tasks.id", ondelete="CASCADE"), nullable=False),
        sa.Column("attempt_number", sa.Integer(), nullable=False),
        sa.Column("status", attempt_status, nullable=False, server_default="running"),
        sa.Column("error_type", sa.Text(), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("http_status", sa.Integer(), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("duration_ms", sa.Integer(), nullable=True),
        sa.UniqueConstraint("task_id", "attempt_number", name="uq_task_attempts_task_attempt"),
    )
    op.create_index("ix_task_attempts_task_id", "task_attempts", ["task_id", "attempt_number"], unique=False)

    op.create_table(
        "records",
        sa.Column("id", sa.BigInteger(), sa.Identity(always=True), primary_key=True, nullable=False),
        sa.Column("job_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False),
        sa.Column("task_id", sa.BigInteger(), sa.ForeignKey("job_tasks.id", ondelete="CASCADE"), nullable=False),
        sa.Column("title", sa.Text(), nullable=True),
        sa.Column("link", sa.Text(), nullable=True),
        sa.Column("author", sa.Text(), nullable=True),
        sa.Column("summary", sa.Text(), nullable=True),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("feed_type", feed_type, nullable=False, server_default="unknown"),
        sa.Column("dedupe_key", sa.Text(), nullable=False),
        sa.Column("extra", postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.UniqueConstraint("task_id", "dedupe_key", name="uq_records_task_dedupe"),
    )
    op.create_index("ix_records_task_id", "records", ["task_id", "id"], unique=False)
    op.create_index("ix_records_job_id", "records", ["job_id"], unique=False)
    op.create_index(
        "ix_records_published_at",
        "records",
        [sa.text("task_id"), sa.text("published_at DESC NULLS LAST")],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_records_published_at", table_name="records")
    op.drop_index("ix_records_job_id", table_name="records")
    op.drop_index("ix_records_task_id", table_name="records")
    op.drop_table("records")

    op.drop_index("ix_task_attempts_task_id", table_name="task_attempts")
    op.drop_table("task_attempts")

    op.drop_index("ix_job_tasks_job_id_id", table_name="job_tasks")
    op.drop_index("ix_job_tasks_job_id_status", table_name="job_tasks")
    op.drop_table("job_tasks")

    op.drop_index("ix_jobs_created_at", table_name="jobs")
    op.drop_table("jobs")

    bind = op.get_bind()
    feed_type.drop(bind, checkfirst=True)
    attempt_status.drop(bind, checkfirst=True)
    task_status.drop(bind, checkfirst=True)
    job_status.drop(bind, checkfirst=True)
