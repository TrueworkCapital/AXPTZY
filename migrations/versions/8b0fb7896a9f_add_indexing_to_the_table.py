"""Add indexing_to_the_table

Revision ID: 8b0fb7896a9f
Revises: c458ded42a9a
Create Date: 2025-09-12 13:00:42.075428

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '8b0fb7896a9f'
down_revision: Union[str, Sequence[str], None] = 'c458ded42a9a'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    production_indexes = [
        # Primary performance indexes
        "CREATE INDEX IF NOT EXISTS idx_ohlcv_symbol_timestamp ON ohlcv_data(symbol, timestamp DESC)",
        "CREATE INDEX IF NOT EXISTS idx_ohlcv_timestamp ON ohlcv_data(timestamp DESC)",
        "CREATE INDEX IF NOT EXISTS idx_ohlcv_symbol ON ohlcv_data(symbol)",
        "CREATE INDEX IF NOT EXISTS idx_ohlcv_sector_timestamp ON ohlcv_data(sector, timestamp DESC)",

        # Quality and monitoring indexes
        "CREATE INDEX IF NOT EXISTS idx_quality_symbol_timestamp ON data_quality_log(symbol, timestamp DESC)",
        "CREATE INDEX IF NOT EXISTS idx_performance_operation ON performance_metrics(operation, timestamp DESC)",
        "CREATE INDEX IF NOT EXISTS idx_live_data_timestamp ON live_data_cache(timestamp DESC)",

        # Metadata indexes
        "CREATE INDEX IF NOT EXISTS idx_constituents_sector ON constituents_metadata(sector)",
        "CREATE INDEX IF NOT EXISTS idx_constituents_active ON constituents_metadata(is_active)",

        # Export tracking
        "CREATE INDEX IF NOT EXISTS idx_export_timestamp ON export_history(created_at DESC)"
    ]
    for stmt in production_indexes:
        op.execute(stmt)


def downgrade() -> None:
    """Downgrade schema."""
    pass
