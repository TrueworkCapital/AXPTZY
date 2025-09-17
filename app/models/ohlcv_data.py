from sqlalchemy import Column, Integer, String, Float, DateTime, UniqueConstraint
from app.database import Base
from sqlalchemy.sql import func

class OHLCV(Base):
    __tablename__ = 'ohlcv_data'
    
    id = Column(Integer, primary_key=True, index=True)
    timestamp = Column(DateTime, nullable=False)
    symbol = Column(String(20), nullable=False)
    open = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    volume = Column(Integer, nullable=False)
    data_source = Column(String(50), default='zerodha_kite')
    quality_score = Column(Float, default=1.0)
    sector = Column(String(50))
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    __table_args__ = (UniqueConstraint('timestamp', 'symbol', name='uix_timestamp_symbol'),)