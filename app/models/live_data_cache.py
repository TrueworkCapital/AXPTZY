from sqlalchemy import Column, Integer, String, Float, DateTime
from app.database import Base       
from sqlalchemy.sql import func

class LiveDataCache(Base):
    __tablename__ = 'live_data_cache'
    
    symbol = Column(String(20), primary_key=True)
    timestamp = Column(DateTime, nullable=False)
    open = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    volume = Column(Integer, nullable=False)
    last_updated = Column(DateTime, server_default=func.now())