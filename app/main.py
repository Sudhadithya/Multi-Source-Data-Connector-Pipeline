from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel

from app.connectors.factory import ConnectorFactory
from app.transform.transformer import Transformer
from app.loader.postgres import init_db, upsert_data
from app.utils.logger import get_logger

logger = get_logger("main")

app = FastAPI(title="Multi-Source Data Connector Pipeline")

@app.get("/")
def read_root():
    return {"message": "Multi-Source Data Pipeline is running. Go to /docs for API documentation."}

@app.get("/health")
def health_check():
    from sqlalchemy import text
    from app.loader.postgres import SessionLocal
    db = SessionLocal()
    try:
        db.execute(text("SELECT 1"))
        return {"status": "ok", "db": "connected"}
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {"status": "error", "db": "disconnected", "detail": str(e)}
    finally:
        db.close()

@app.get("/data/{source_name}")
def view_data(source_name: str):
    from sqlalchemy import select
    from app.loader.postgres import SessionLocal, get_dynamic_table
    db = SessionLocal()
    try:
        table = get_dynamic_table(source_name)
        stmt = select(table).limit(100)
        data = db.execute(stmt).fetchall()
        return [
            {
                "id": d.id,
                "source": d.source,
                "title": d.title,
                "created_at": d.created_at
            } for d in data
        ]
    except Exception as e:
        logger.error(f"Error fetching data for {source_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()

@app.on_event("startup")
def startup_event():
    init_db()

class SyncResponse(BaseModel):
    status: str
    message: str

def run_sync_pipeline(connector_name: str):
    logger.info(f"Starting sync pipeline for connector: {connector_name}")
    try:
        # 1. Connector Layer
        connector = ConnectorFactory.get_connector(connector_name)
        raw_data = connector.fetch_data()
        
        if not raw_data:
            logger.info(f"No data fetched for {connector_name}")
            return
            
        # 2. Transform Layer
        transformer = Transformer()
        transformed_data = transformer.transform(connector_name, raw_data)
        
        # 3. Loader Layer
        upsert_data(transformed_data, connector_name)
        logger.info(f"Completed sync pipeline for connector: {connector_name}")
        
    except Exception as e:
        logger.error(f"Sync pipeline failed for {connector_name}: {e}")

@app.post("/sync/{connector_name}", response_model=SyncResponse)
def trigger_sync(connector_name: str, background_tasks: BackgroundTasks):
    try:
        # Validate connector exists
        ConnectorFactory.get_connector(connector_name)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
        
    # Run sync in background so we don't block the API
    background_tasks.add_task(run_sync_pipeline, connector_name)
    
    return SyncResponse(
        status="success",
        message=f"Sync process for '{connector_name}' triggered in the background."
    )
