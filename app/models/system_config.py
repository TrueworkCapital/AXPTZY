from sqlalchemy import Column, Integer, String, Float, DateTime, Text, UniqueConstraint, Boolean, Date
from app.database import Base
from sqlalchemy.sql import func

class SystemConfig(Base):
    __tablename__ = "system_config"

    key = Column(String(100), primary_key=True, nullable=False)
    value = Column(Text, nullable=False)
    category = Column(String(50))
    description = Column(Text)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())