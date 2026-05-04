from typing import Dict, Any, List
from datetime import datetime, timezone
import uuid
from app.models.schema import CommonData
from app.utils.logger import get_logger

logger = get_logger("transformer")

class Transformer:
    @staticmethod
    def transform(source: str, raw_records: List[Dict[str, Any]]) -> List[CommonData]:
        transformed = []
        for raw in raw_records:
            try:
                if source == "github":
                    data = CommonData(
                        id=str(raw.get("id", uuid.uuid4())),
                        source=source,
                        title=raw.get("name") or raw.get("title") or "Untitled",
                        description=raw.get("description"),
                        created_at=raw.get("created_at") or datetime.now(timezone.utc).isoformat(),
                        raw_data=raw
                    )
                elif source == "basic_auth_api":
                    data = CommonData(
                        id=str(raw.get("id", raw.get("uuid", str(uuid.uuid4())))),
                        source=source,
                        title=raw.get("name", "Basic Auth Resource"),
                        description=raw.get("details", ""),
                        created_at=datetime.now(timezone.utc).isoformat(),
                        raw_data=raw
                    )
                else:
                    logger.warning(f"Unknown source: {source}")
                    continue
                transformed.append(data)
            except Exception as e:
                error_msg = f"Error transforming record: {str(e)}"
                logger.error(f"{error_msg} for source {source}")
                from app.loader.postgres import insert_failed_record
                insert_failed_record(source, raw, error_msg)
        
        logger.info(f"Transformed {len(transformed)} records for source: {source}")
        return transformed
