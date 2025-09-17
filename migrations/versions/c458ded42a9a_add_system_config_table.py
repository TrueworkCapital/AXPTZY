"""Add system_config table

Revision ID: c458ded42a9a
Revises: 9dc3f26ec953
Create Date: 2025-09-12 12:58:47.204244

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'c458ded42a9a'
down_revision: Union[str, Sequence[str], None] = '9dc3f26ec953'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'system_config',
        sa.Column('key', sa.String(100), primary_key=True),
        sa.Column('value', sa.Text),
        sa.Column('category', sa.String(50)),
        sa.Column('description', sa.Text),
        sa.Column('updated_at', sa.DateTime, server_default=sa.text('CURRENT_TIMESTAMP'))
    )


def downgrade() -> None:
    """Downgrade schema."""
    pass
