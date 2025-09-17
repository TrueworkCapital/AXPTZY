from sqlalchemy import Column, Integer, String, Float, DateTime, Text, UniqueConstraint, Boolean, Date
from app.database import Base

class DataQualityLog(Base):
    __tablename__ = 'data_quality_log'
    
    id = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    symbol = Column(String(20))
    timestamp = Column(DateTime, default=None)
    quality_score = Column(Float)
    issues_found = Column(Text)
    severity = Column(Integer, default=1)
    resolved = Column(Boolean, default=False)
