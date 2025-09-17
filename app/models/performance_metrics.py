from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean
from app.database import Base
from sqlalchemy.sql import func

class PerformanceMetrics(Base):
    __tablename__ = 'performance_metrics'
    
    id = Column(Integer, primary_key=True, autoincrement=True,nullable=False)
    timestamp = Column(DateTime)
    operation = Column(String(100))
    symbol = Column(String(20))
    duration_ms = Column(Float)
    records_affected = Column(Integer)
    success = Column(Boolean)
    memory_usage_mb = Column(Float)
    cache_hit = Column(Boolean)
    