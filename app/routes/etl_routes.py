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
def trigger_etl_run(background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    """
    POST /etl/run: Creates a job record and runs the pipeline.
    Requirement: 5. Job Status API + Run ETL Job
    """
    # 1. Create a unique Job ID (Requirement: job_id UUID)
    job_id = uuid.uuid4()
    
    # 2. Create the initial ETL job record (Requirement: Track each ETL run)
    new_job = ETLJob(
        job_id=job_id,
        status="running",
        started_at=datetime.utcnow()
    )
    db.add(new_job)
    db.commit()

    # 3. Define the actual work to be done
    def etl_task():
        # Create a new session for the background thread
        from ..database import SessionLocal
        inner_db = SessionLocal()
        try:
            records, error = run_etl(inner_db, str(job_id))
            
            # Update the job record upon completion (Requirement: Update job status)
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

    # 4. Run the ETL in the background so the API doesn't hang (Best Practice)
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