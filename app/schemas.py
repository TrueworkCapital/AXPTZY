from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class OHLCVIn(BaseModel):
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int


class OHLCVOut(OHLCVIn):
    symbol: str
    data_source: Optional[str] = Field(default='zerodha_kite')
    quality_score: Optional[float] = None
    sector: Optional[str] = None

