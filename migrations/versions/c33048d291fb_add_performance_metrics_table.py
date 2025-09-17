"""Add performance_metrics table

Revision ID: c33048d291fb
Revises: 6fb0f9a4afc4
Create Date: 2025-09-12 12:52:06.017718

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'c33048d291fb'
down_revision: Union[str, Sequence[str], None] = '6fb0f9a4afc4'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'performance_metrics',
        sa.Column('id', sa.Integer(), primary_key=True, autoincrement=True, nullable=False),
        sa.Column('timestamp', sa.DateTime(), nullable=True, server_default=sa.text('CURRENT_TIMESTAMP')),
        sa.Column('operation', sa.String(length=100)),
        sa.Column('symbol', sa.String(length=20)),
        sa.Column('duration_ms', sa.Float()),
        sa.Column('records_affected', sa.Integer()),
        sa.Column('success', sa.Boolean()),
        sa.Column('memory_usage_mb', sa.Float()),
        sa.Column('cache_hit', sa.Boolean()),
    )


def downgrade() -> None:
    """Downgrade schema."""
    pass
