from sqlalchemy import create_engine, Column, String, DateTime, JSON
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.dialects.postgresql import insert
from datetime import datetime

from app.config import settings
from app.utils.logger import get_logger

logger = get_logger("loader")

Base = declarative_base()

class NormalizedData(Base):
    __tablename__ = "normalized_data"

    id = Column(String, primary_key=True)
    source = Column(String, primary_key=True)
    title = Column(String)
    description = Column(String, nullable=True)
    created_at = Column(DateTime)
    raw_data = Column(JSON)

class FailedRecord(Base):
    __tablename__ = "failed_records"

    id = Column(String, primary_key=True)
    source = Column(String, primary_key=True)
    raw_data = Column(JSON)
    error_reason = Column(String)
    failed_at = Column(DateTime, default=datetime.utcnow)

engine = create_engine(settings.database_url)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def init_db():
    logger.info("Initializing database schema...")
    Base.metadata.create_all(bind=engine)

def insert_failed_record(source: str, raw_data: dict, error_reason: str):
    db = SessionLocal()
    try:
        record_id = raw_data.get("id") or raw_data.get("uuid") or str(datetime.utcnow().timestamp())
        stmt = insert(FailedRecord).values(
            id=str(record_id),
            source=source,
            raw_data=raw_data,
            error_reason=error_reason,
            failed_at=datetime.utcnow()
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=["id", "source"],
            set_={
                "raw_data": stmt.excluded.raw_data,
                "error_reason": stmt.excluded.error_reason,
                "failed_at": stmt.excluded.failed_at
            }
        )
        db.execute(stmt)
        db.commit()
    except Exception as e:
        db.rollback()
        logger.error(f"Failed to insert dead letter record: {e}")
    finally:
        db.close()

def upsert_data(records):
    """
    Upsert data to PostgreSQL.
    """
    if not records:
        return
        
    db = SessionLocal()
    try:
        stmt = insert(NormalizedData).values([{
            "id": r.id,
            "source": r.source,
            "title": r.title,
            "description": r.description,
            "created_at": r.created_at,
            "raw_data": r.raw_data
        } for r in records])

        # On conflict on (id, source), do update
        update_dict = {
            "title": stmt.excluded.title,
            "description": stmt.excluded.description,
            "created_at": stmt.excluded.created_at,
            "raw_data": stmt.excluded.raw_data
        }

        stmt = stmt.on_conflict_do_update(
            index_elements=["id", "source"],
            set_=update_dict
        )

        db.execute(stmt)
        db.commit()
        logger.info(f"Successfully upserted {len(records)} records into PostgreSQL.")
    except Exception as e:
        db.rollback()
        logger.error(f"Error during upsert: {e}")
        raise
    finally:
        db.close()
