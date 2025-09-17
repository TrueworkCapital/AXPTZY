"""Add export_history table

Revision ID: 9dc3f26ec953
Revises: c33048d291fb
Create Date: 2025-09-12 12:57:40.786789

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '9dc3f26ec953'
down_revision: Union[str, Sequence[str], None] = 'c33048d291fb'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'export_history',
        sa.Column('id', sa.Integer(), primary_key=True, autoincrement=True, nullable=False),
        sa.Column('export_type', sa.String(length=50), nullable=True),
        sa.Column('symbols', sa.Text(), nullable=True),
        sa.Column('date_range_start', sa.Date(), nullable=True),
        sa.Column('date_range_end', sa.Date(), nullable=True),
        sa.Column('format', sa.String(length=20), nullable=True),
        sa.Column('file_path', sa.Text(), nullable=True),
        sa.Column('file_size_mb', sa.Float(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=True, server_default=sa.text('CURRENT_TIMESTAMP'))
    )


def downgrade() -> None:
    """Downgrade schema."""
    pass
