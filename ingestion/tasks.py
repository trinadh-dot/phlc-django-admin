"""
Celery tasks for ingestion app.
"""
import io
import sys
from pathlib import Path
from celery import shared_task
from django.conf import settings
from django.db import transaction
import logging

logger = logging.getLogger('ingestion')

# Add parent directory to path to import from original services
BASE_DIR = Path(__file__).resolve().parent.parent.parent
if str(BASE_DIR.parent) not in sys.path:
    sys.path.insert(0, str(BASE_DIR.parent))

# Import services from original app/services.py
try:
    from app.services import (
        process_uploaded_file as original_process_uploaded_file,
        process_s3_upload as original_process_s3_upload,
        process_s3_directory_upload as original_process_s3_directory_upload,
        build_ta_analytics_tables as original_build_ta_analytics_tables,
    )
except ImportError:
    # If original services not available, we'll need to port the logic
    logger.warning("Could not import from app.services. Functions need to be ported.")
    original_process_uploaded_file = None
    original_process_s3_upload = None
    original_process_s3_directory_upload = None
    original_build_ta_analytics_tables = None


@shared_task(bind=True, max_retries=5, default_retry_delay=60)
def process_uploaded_file_task(self, job_id: str, file_bytes: bytes, filename: str):
    """
    Celery task to process uploaded file.
    
    Args:
        job_id: UUID string of the job
        file_bytes: File contents as bytes
        filename: Original filename
    """
    from .models import Job
    
    try:
        job = Job.objects.get(id=job_id)
        
        # Convert bytes back to BytesIO for processing
        bio = io.BytesIO(file_bytes)
        
        # Call original processing function
        if original_process_uploaded_file:
            # We need to adapt the original function to work with Django models
            # The original function uses SQLAlchemy, so we'll need to wrap it
            original_process_uploaded_file(job_id, bio, filename)
        else:
            # Fallback: mark as failed if services not available
            job.mark_failed(message="Processing service not available. Please port services.py logic.")
            logger.error(f"Processing service not available for job {job_id}")
            return
        
        # Refresh job from database
        job.refresh_from_db()
        logger.info(f"Job {job_id} processing completed with status {job.status}")
        
    except Job.DoesNotExist:
        logger.error(f"Job {job_id} not found")
        raise
    except Exception as exc:
        logger.error(f"Error processing job {job_id}: {exc}", exc_info=True)
        
        # Update job status
        try:
            job = Job.objects.get(id=job_id)
            current_retry = job.retry_count
            
            if current_retry < 5:
                # Retry the job
                new_retry_count = current_retry + 1
                job.increment_retry()
                job.status = 'running'
                job.message = f"Retry {new_retry_count}/5 - Previous error: {str(exc)[:200]}"
                job.save(update_fields=['status', 'message', 'retry_count', 'updated_at'])
                
                # Retry the task
                raise self.retry(exc=exc, countdown=60 * new_retry_count)
            else:
                # Max retries reached
                job.mark_failed(message=f"Failed after 5 retries. Last error: {str(exc)[:500]}")
                logger.error(f"Job {job_id} failed after 5 retries")
        except Job.DoesNotExist:
            logger.error(f"Job {job_id} not found when trying to update status")
        
        raise


@shared_task(bind=True, max_retries=5, default_retry_delay=60)
def process_s3_upload_task(
    self,
    job_id: str,
    contents: bytes,
    filename: str,
    upload_type: str,
    content_type: str,
    preserve_filename: bool
):
    """
    Celery task to process S3 upload.
    
    Args:
        job_id: UUID string of the job
        contents: File contents as bytes
        filename: Original filename
        upload_type: 'file' or 'directory'
        content_type: MIME type
        preserve_filename: Whether to preserve original filename
    """
    from .models import Job
    
    try:
        job = Job.objects.get(id=job_id)
        
        # Call original processing function
        if original_process_s3_upload:
            original_process_s3_upload(
                job_id, contents, filename, upload_type, content_type, preserve_filename
            )
        else:
            job.mark_failed(message="S3 upload service not available. Please port services.py logic.")
            logger.error(f"S3 upload service not available for job {job_id}")
            return
        
        # Refresh job from database
        job.refresh_from_db()
        logger.info(f"S3 upload job {job_id} completed with status {job.status}")
        
    except Job.DoesNotExist:
        logger.error(f"Job {job_id} not found")
        raise
    except Exception as exc:
        logger.error(f"Error processing S3 upload job {job_id}: {exc}", exc_info=True)
        
        try:
            job = Job.objects.get(id=job_id)
            job.mark_failed(message=f"S3 upload failed: {str(exc)[:500]}")
        except Job.DoesNotExist:
            pass
        
        raise


@shared_task(bind=True, max_retries=5, default_retry_delay=60)
def process_s3_directory_upload_task(
    self,
    job_id: str,
    entries: list,
    preserve_filename: bool
):
    """
    Celery task to process S3 directory upload.
    
    Args:
        job_id: UUID string of the job
        entries: List of dicts with 'path', 'content', 'content_type'
        preserve_filename: Whether to preserve original filenames
    """
    from .models import Job
    
    try:
        job = Job.objects.get(id=job_id)
        
        # Convert entries to proper format if needed
        # entries should be list of dicts: [{'path': str, 'content': bytes, 'content_type': str}]
        
        # Call original processing function
        if original_process_s3_directory_upload:
            original_process_s3_directory_upload(job_id, entries, preserve_filename)
        else:
            job.mark_failed(message="S3 directory upload service not available. Please port services.py logic.")
            logger.error(f"S3 directory upload service not available for job {job_id}")
            return
        
        # Refresh job from database
        job.refresh_from_db()
        logger.info(f"S3 directory upload job {job_id} completed with status {job.status}")
        
    except Job.DoesNotExist:
        logger.error(f"Job {job_id} not found")
        raise
    except Exception as exc:
        logger.error(f"Error processing S3 directory upload job {job_id}: {exc}", exc_info=True)
        
        try:
            job = Job.objects.get(id=job_id)
            job.mark_failed(message=f"S3 directory upload failed: {str(exc)[:500]}")
        except Job.DoesNotExist:
            pass
        
        raise


@shared_task(bind=True, max_retries=3, default_retry_delay=120)
def build_ta_analytics_tables_task(self):
    """
    Celery task to build TA Analytics tables.
    """
    from .models import Job
    
    try:
        # Call original function
        if original_build_ta_analytics_tables:
            result = original_build_ta_analytics_tables()
            logger.info(f"TA Analytics tables built successfully: {result}")
            return result
        else:
            logger.error("TA Analytics build service not available. Please port services.py logic.")
            raise RuntimeError("TA Analytics build service not available")
            
    except Exception as exc:
        logger.error(f"Error building TA Analytics tables: {exc}", exc_info=True)
        raise self.retry(exc=exc, countdown=120)

