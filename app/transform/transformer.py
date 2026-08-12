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
                elif source == "stripe":
                    data = CommonData(
                        id=str(raw.get("id", uuid.uuid4())),
                        source=source,
                        title=raw.get("name") or raw.get("email") or raw.get("description") or "Stripe Object",
                        description=str(raw.get("amount", "")) or raw.get("description", ""),
                        created_at=datetime.fromtimestamp(raw.get("created"), tz=timezone.utc).isoformat() if raw.get("created") else datetime.now(timezone.utc).isoformat(),
                        raw_data=raw
                    )
                elif source == "newsapi":
                    data = CommonData(
                        id=str(raw.get("url") or uuid.uuid4()),
                        source=source,
                        title=raw.get("title") or "News Article",
                        description=raw.get("description", ""),
                        created_at=raw.get("publishedAt") or datetime.now(timezone.utc).isoformat(),
                        raw_data=raw
                    )
                elif source == "openweathermap":
                    # OpenWeatherMap data structure
                    weather_array = raw.get("weather", [])
                    weather_desc = weather_array[0].get("description", "") if weather_array else ""
                    main_data = raw.get("main", {})
                    temp = main_data.get("temp", "")
                    
                    data = CommonData(
                        id=str(raw.get("id") or uuid.uuid4()),
                        source=source,
                        title=f"Weather in {raw.get('name', 'Unknown City')}",
                        description=f"{weather_desc}, {temp}°C",
                        created_at=datetime.fromtimestamp(raw.get("dt"), tz=timezone.utc).isoformat() if raw.get("dt") else datetime.now(timezone.utc).isoformat(),
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
