"""
Services layer for ingestion app.
This module contains business logic ported from the FastAPI services.py
"""
import os
import sys
from pathlib import Path

# Add parent directory to path to import from original services
BASE_DIR = Path(__file__).resolve().parent.parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

# Import all functions from original services.py
# Note: In production, you should copy the actual functions here
# For now, we'll import from the original location
try:
    # Try to import from original app/services.py
    sys.path.insert(0, str(BASE_DIR.parent))
    from app.services import (
        compute_sha256_bytes,
        has_successful_job as _has_successful_job,
        download_file_from_s3,
        validate_upload_payload,
        normalize_relative_path,
        compute_directory_hash,
        process_uploaded_file as _process_uploaded_file,
        process_s3_upload as _process_s3_upload,
        process_s3_directory_upload as _process_s3_directory_upload,
        build_ta_analytics_tables as _build_ta_analytics_tables,
    )
except ImportError:
    # If original services not available, define minimal versions
    import hashlib
    import io
    import zipfile
    from typing import List, Dict
    
    def compute_sha256_bytes(b: bytes) -> str:
        """Compute SHA256 hash of bytes."""
        h = hashlib.sha256()
        h.update(b)
        return h.hexdigest()
    
    def _has_successful_job(file_hash: str, ingestion_type: str = 'Postgres') -> bool:
        """Check if there's a successful job for the given hash."""
        from .models import Job
        return Job.objects.filter(
            file_hash=file_hash,
            ingestion_type=ingestion_type,
            status='completed'
        ).exists()
    
    def download_file_from_s3(s3_key: str) -> bytes:
        """Download file from S3."""
        import boto3
        from django.conf import settings
        from botocore.exceptions import ClientError
        
        s3_client = boto3.client(
            's3',
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
            region_name=settings.AWS_REGION
        )
        try:
            response = s3_client.get_object(Bucket=settings.S3_BUCKET, Key=s3_key)
            return response['Body'].read()
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code == 'NoSuchKey':
                raise ValueError(f"File '{s3_key}' not found in S3 bucket '{settings.S3_BUCKET}'")
            elif error_code == 'NoSuchBucket':
                raise ValueError(f"S3 bucket '{settings.S3_BUCKET}' not found")
            else:
                raise RuntimeError(f"Error downloading file from S3: {e}") from e
    
    def validate_upload_payload(contents: bytes, upload_type: str):
        """Validate upload payload."""
        upload_type = (upload_type or 'file').lower()
        if upload_type not in {'file', 'directory'}:
            raise ValueError("upload_type must be 'file' or 'directory'")
        if upload_type == 'directory':
            bio = io.BytesIO(contents)
            if not zipfile.is_zipfile(bio):
                raise ValueError("Directory uploads must be provided as a .zip archive")
        else:
            if not contents:
                raise ValueError("Uploaded file is empty")
        return upload_type
    
    def normalize_relative_path(filename: str) -> str:
        """Normalize relative path."""
        path = (filename or '').replace('\\', '/').strip()
        if not path:
            raise ValueError("Each file must include a relative path or filename")
        if path.startswith('/'):
            path = path.lstrip('/')
        segments = []
        for segment in path.split('/'):
            if segment in ('', '.'):
                continue
            if segment == '..':
                raise ValueError("Directory traversal sequences ('..') are not allowed in file paths")
            segments.append(segment)
        normalized = '/'.join(segments)
        if not normalized:
            raise ValueError("Invalid relative path provided")
        return normalized
    
    def compute_directory_hash(entries: List[Dict[str, bytes]]) -> str:
        """Compute hash for directory entries."""
        if not entries:
            raise ValueError("No files provided to compute directory hash")
        h = hashlib.sha256()
        for entry in sorted(entries, key=lambda e: e['path']):
            h.update(entry['path'].encode('utf-8'))
            content = entry['content']
            h.update(len(content).to_bytes(8, 'big'))
            h.update(content)
        return h.hexdigest()
    
    def _process_uploaded_file(job_id, file_bytes, filename):
        """Process uploaded file - placeholder."""
        raise NotImplementedError("This should be implemented in tasks.py")
    
    def _process_s3_upload(job_id, contents, filename, upload_type, content_type, preserve_filename):
        """Process S3 upload - placeholder."""
        raise NotImplementedError("This should be implemented in tasks.py")
    
    def _process_s3_directory_upload(job_id, entries, preserve_filename):
        """Process S3 directory upload - placeholder."""
        raise NotImplementedError("This should be implemented in tasks.py")
    
    def _build_ta_analytics_tables():
        """Build TA Analytics tables - placeholder."""
        raise NotImplementedError("This should be implemented in tasks.py")


# Wrapper functions that work with Django models
def has_successful_job(file_hash: str, ingestion_type: str = 'Postgres') -> bool:
    """Check if there's a successful job for the given hash and ingestion type."""
    from .models import Job
    return Job.objects.filter(
        file_hash=file_hash,
        ingestion_type=ingestion_type,
        status='completed'
    ).exists()

# Export all functions
__all__ = [
    'compute_sha256_bytes',
    'has_successful_job',
    'download_file_from_s3',
    'validate_upload_payload',
    'normalize_relative_path',
    'compute_directory_hash',
]

