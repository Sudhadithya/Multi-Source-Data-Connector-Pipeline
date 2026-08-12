from datetime import datetime
from typing import Any, Dict, Optional

from pydantic import BaseModel, ConfigDict


class CommonData(BaseModel):
    id: str
    source: str
    title: str
    description: Optional[str] = None
    created_at: datetime
    raw_data: Dict[str, Any]

    model_config = ConfigDict(extra="ignore")
