"""Add live_data_cache table

Revision ID: 8969f02cdfd5
Revises: efea93bfdb4e
Create Date: 2025-09-12 12:41:13.802270

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '8969f02cdfd5'
down_revision: Union[str, Sequence[str], None] = 'efea93bfdb4e'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table('live_data_cache',
        sa.Column('symbol', sa.String(length=20), nullable=False),
        sa.Column('timestamp', sa.DateTime(), nullable=False),
        sa.Column('open', sa.Float(), nullable=False),
        sa.Column('high', sa.Float(), nullable=False),
        sa.Column('low', sa.Float(), nullable=False),
        sa.Column('close', sa.Float(), nullable=False),
        sa.Column('volume', sa.Integer(), nullable=False),
        sa.Column('last_updated', sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint('symbol')
    )


def downgrade() -> None:
    """Downgrade schema."""
    pass
