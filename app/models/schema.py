from pydantic import BaseModel, ConfigDict
from typing import Optional, Dict, Any
from datetime import datetime

class CommonData(BaseModel):
    id: str
    source: str
    title: str
    description: Optional[str] = None
    created_at: datetime
    raw_data: Dict[str, Any]

    model_config = ConfigDict(extra="ignore")
