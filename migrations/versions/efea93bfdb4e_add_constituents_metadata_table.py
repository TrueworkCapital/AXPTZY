"""Add constituents_metadata table

Revision ID: efea93bfdb4e
Revises: 4218bfb197f8
Create Date: 2025-09-12 12:28:29.457819

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'efea93bfdb4e'
down_revision: Union[str, Sequence[str], None] = '4218bfb197f8'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table('constituents_metadata',
        sa.Column('symbol', sa.String(length=20), nullable=False),
        sa.Column('company_name', sa.String(length=200), nullable=False),
        sa.Column('sector', sa.String(length=100), nullable=False),
        sa.Column('market_cap_category', sa.String(length=20), nullable=True),
        sa.Column('is_active', sa.Boolean(), nullable=True),
        sa.Column('weightage', sa.Float(), nullable=True),
        sa.Column('added_date', sa.Date(), nullable=True),
        sa.Column('last_updated', sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint('symbol')
    )


def downgrade() -> None:
    """Downgrade schema."""
    pass
