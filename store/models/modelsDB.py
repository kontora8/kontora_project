# Database model
from datetime import datetime
from pydantic import BaseModel


class ProcessedAgentDataInDB(BaseModel):
    id: int
    road_state: str
    air: float
    noise: float
    x: float
    y: float
    z: float
    latitude: float
    longitude: float
    timestamp: datetime
