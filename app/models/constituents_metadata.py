from sqlalchemy import Column, String, Float, DateTime, Boolean, Date
from app.database import Base       
from sqlalchemy.sql import func

class ConstituentsMetadata(Base):
    __tablename__ = 'constituents_metadata'
    
    symbol = Column(String(20), primary_key=True)
    company_name = Column(String(200), nullable=False)
    sector = Column(String(100), nullable=False)
    market_cap_category = Column(String(20), default='Large')
    is_active = Column(Boolean, default=True)
    weightage = Column(Float, default=2.0)
    added_date = Column(Date, default='2025-01-01')
    last_updated = Column(DateTime)