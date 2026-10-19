"""Keep the request event ID sequence aligned with the BIGINT column."""

from alembic import op
import sqlalchemy as sa


revision = "0002_bigint_event_sequence"
down_revision = "0001_request_events"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    sequence_name = bind.execute(
        sa.text("SELECT pg_get_serial_sequence('request_events', 'id')")
    ).scalar_one_or_none()
    if sequence_name:
        preparer = bind.dialect.identifier_preparer
        quoted_sequence = ".".join(
            preparer.quote(part.strip('"'))
            for part in sequence_name.split(".")
        )
        op.execute(f"ALTER SEQUENCE {quoted_sequence} AS BIGINT")


def downgrade() -> None:
    # Keep the widened sequence to avoid truncating IDs or breaking writes.
    pass
