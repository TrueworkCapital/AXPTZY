"""Add data_quality_log table

Revision ID: 6fb0f9a4afc4
Revises: 8969f02cdfd5
Create Date: 2025-09-12 12:43:00.057347

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '6fb0f9a4afc4'
down_revision: Union[str, Sequence[str], None] = '8969f02cdfd5'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'data_quality_log',
        sa.Column('id', sa.Integer(), primary_key=True, autoincrement=True, nullable=False),
        sa.Column('symbol', sa.String(length=20), nullable=True),
        sa.Column('timestamp', sa.DateTime(), nullable=True, server_default=sa.text('CURRENT_TIMESTAMP')),
        sa.Column('quality_score', sa.Float(), nullable=True),
        sa.Column('issues_found', sa.Text(), nullable=True),
        sa.Column('severity', sa.Integer(), nullable=True, server_default=sa.text('1')),
        sa.Column('resolved', sa.Boolean(), nullable=True, server_default=sa.text('FALSE'))
    )

def downgrade() -> None:
    """Downgrade schema."""
    pass
