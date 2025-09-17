"""Add ohlcv_data table

Revision ID: 4218bfb197f8
Revises: e2de9e0c21ef
Create Date: 2025-09-12 12:08:01.020202

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '4218bfb197f8'
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table('ohlcv_data',
        sa.Column('timestamp', sa.DateTime(), nullable=False),
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('symbol', sa.String(length=20), nullable=False),
        sa.Column('open', sa.Float(), nullable=False),
        sa.Column('high', sa.Float(), nullable=False),
        sa.Column('low', sa.Float(), nullable=False),
        sa.Column('close', sa.Float(), nullable=False),
        sa.Column('volume', sa.Integer(), nullable=False),
        sa.Column('data_source', sa.String(length=50), nullable=True),
        sa.Column('quality_score', sa.Float(), nullable=True),
        sa.Column('sector', sa.String(length=50), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('timestamp', 'symbol', name='uix_timestamp_symbol')
    )


def downgrade() -> None:
    """Downgrade schema."""
    pass
