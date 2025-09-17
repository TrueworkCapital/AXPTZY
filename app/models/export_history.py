from sqlalchemy import Column, Integer, String, Float, DateTime, Text, UniqueConstraint, Boolean, Date
from app.database import Base
from sqlalchemy.sql import func

class ExportHistory(Base):
    __tablename__ = "export_history"

    id = Column(Integer, primary_key=True, autoincrement=True)
    export_type = Column(String(50))
    symbols = Column(String)
    date_range_start = Column(Date)
    date_range_end = Column(Date)
    format = Column(String(20))
    file_path = Column(Text)
    file_size_mb = Column(Float)
    created_at = Column(DateTime)
