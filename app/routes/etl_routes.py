from fastapi import APIRouter, Depends, BackgroundTasks
from sqlalchemy.orm import Session
from datetime import datetime
import uuid

from ..database import get_db
from ..models import ETLJob
from ..schemas import ETLJobSchema
from ..etl_pipeline import run_etl

router = APIRouter(prefix="/etl", tags=["ETL"])

@router.post("/run")
def trigger_etl_run(
    background_tasks: BackgroundTasks, 
    db: Session = Depends(get_db),
    use_mock: bool = False  # Allows you to toggle mock data in Swagger UI
):
    """
    POST /etl/run: Creates a job record and runs the pipeline. [cite: 156]
    """
    job_id = uuid.uuid4() # Requirement: job_id UUID [cite: 132]
    
    new_job = ETLJob(
        job_id=job_id,
        status="running",
        started_at=datetime.utcnow()
    )
    db.add(new_job)
    db.commit()

    def etl_task():
        from ..database import SessionLocal
        inner_db = SessionLocal()
        try:
            # Pass the use_mock flag down to the pipeline logic
            records, error = run_etl(inner_db, str(job_id), use_mock=use_mock)
            
            job = inner_db.query(ETLJob).filter(ETLJob.job_id == job_id).first()
            if error:
                job.status = "failed"
                job.error_message = error
            else:
                job.status = "success"
                job.records_processed = records
            
            job.finished_at = datetime.utcnow()
            inner_db.commit()
        finally:
            inner_db.close()

    background_tasks.add_task(etl_task)

    return {"job_id": str(job_id), "status": "running"}
  
     

@router.get("/jobs", response_model=list[ETLJobSchema])
def get_etl_jobs(db: Session = Depends(get_db)):
    """
    GET /etl/jobs: Returns history of ETL runs.
    Requirement: 5. Job Status API
    """
    jobs = db.query(ETLJob).order_by(ETLJob.started_at.desc()).all()
    return jobs