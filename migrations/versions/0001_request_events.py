"""Create the request event audit table."""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "0001_request_events"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if not inspector.has_table("request_events"):
        op.create_table(
            "request_events",
            sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
            sa.Column("event_id", sa.String(length=64), nullable=False),
            sa.Column("request_id", sa.String(length=128), nullable=False),
            sa.Column("trace_id", sa.String(length=32), nullable=False, server_default=""),
            sa.Column("provider", sa.String(length=1), nullable=False),
            sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("duration_ms", sa.Float(), nullable=False),
            sa.Column("gateway_status", sa.Integer(), nullable=False),
            sa.Column("upstream_status", sa.Integer(), nullable=True),
            sa.Column("request_payload", postgresql.JSONB(), nullable=False),
            sa.Column("response_body", postgresql.JSONB(), nullable=True),
            sa.Column("error", sa.String(length=256), nullable=True),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint("event_id"),
        )
    else:
        columns = {column["name"]: column for column in inspector.get_columns("request_events")}
        if not isinstance(columns.get("id", {}).get("type"), sa.BigInteger):
            op.alter_column(
                "request_events",
                "id",
                existing_type=sa.Integer(),
                type_=sa.BigInteger(),
                existing_nullable=False,
            )
        if "trace_id" not in columns:
            op.add_column(
                "request_events",
                sa.Column("trace_id", sa.String(length=32), nullable=False, server_default=""),
            )
        if "event_id" not in columns:
            op.add_column(
                "request_events",
                sa.Column("event_id", sa.String(length=64), nullable=True),
            )
            op.execute(
                "UPDATE request_events SET event_id = 'legacy-' || id::text "
                "WHERE event_id IS NULL"
            )
            op.alter_column(
                "request_events",
                "event_id",
                existing_type=sa.String(length=64),
                nullable=False,
            )
            op.create_unique_constraint(
                "uq_request_events_event_id", "request_events", ["event_id"]
            )

    existing_indexes = {
        index["name"] for index in sa.inspect(bind).get_indexes("request_events")
    }
    if "ix_request_events_request_id" not in existing_indexes:
        op.create_index("ix_request_events_request_id", "request_events", ["request_id"])
    if "ix_request_events_started_at" not in existing_indexes:
        op.create_index("ix_request_events_started_at", "request_events", ["started_at"])


def downgrade() -> None:
    op.drop_index("ix_request_events_started_at", table_name="request_events")
    op.drop_index("ix_request_events_request_id", table_name="request_events")
    op.drop_table("request_events")
