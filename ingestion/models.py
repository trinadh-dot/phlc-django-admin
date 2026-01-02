"""
Django models for the ingestion app.
"""
import uuid
from django.db import models
from django.utils import timezone


class JobManager(models.Manager):
    """Custom manager for Job model with status filtering."""
    
    def completed(self):
        """Return completed jobs."""
        return self.filter(status='completed')
    
    def failed(self):
        """Return failed jobs."""
        return self.filter(status='failed')
    
    def running(self):
        """Return running jobs."""
        return self.filter(status='running')
    
    def queued(self):
        """Return queued jobs."""
        return self.filter(status='queued')
    
    def by_ingestion_type(self, ingestion_type):
        """Filter by ingestion type."""
        return self.filter(ingestion_type=ingestion_type)
    
    def by_file_hash(self, file_hash):
        """Filter by file hash."""
        return self.filter(file_hash=file_hash)


class Job(models.Model):
    """
    Job model for tracking file ingestion and processing tasks.
    """
    STATUS_CHOICES = [
        ('queued', 'Queued'),
        ('running', 'Running'),
        ('completed', 'Completed'),
        ('failed', 'Failed'),
    ]
    
    INGESTION_TYPE_CHOICES = [
        ('Postgres', 'PostgreSQL'),
        ('S3', 'S3 Upload'),
    ]
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    file_hash = models.CharField(max_length=128, db_index=True, help_text='SHA256 hash of the file')
    ingestion_type = models.CharField(
        max_length=32,
        choices=INGESTION_TYPE_CHOICES,
        default='Postgres',
        db_index=True,
        help_text='Type of ingestion (Postgres or S3)'
    )
    status = models.CharField(
        max_length=32,
        choices=STATUS_CHOICES,
        default='queued',
        db_index=True,
        help_text='Current status of the job'
    )
    table_name = models.CharField(
        max_length=128,
        null=True,
        blank=True,
        help_text='Name of the database table created/updated'
    )
    inserted_count = models.IntegerField(
        null=True,
        blank=True,
        help_text='Number of rows inserted or file size in bytes'
    )
    file_names = models.TextField(
        null=True,
        blank=True,
        help_text='JSON string of file names (for multiple files) or single filename'
    )
    file_count = models.IntegerField(
        null=True,
        blank=True,
        help_text='Number of files processed (for directory uploads)'
    )
    message = models.TextField(
        null=True,
        blank=True,
        help_text='Status message or error description'
    )
    retry_count = models.IntegerField(
        default=0,
        help_text='Number of retry attempts'
    )
    created_at = models.DateTimeField(
        auto_now_add=True,
        db_index=True,
        help_text='Job creation timestamp'
    )
    updated_at = models.DateTimeField(
        auto_now=True,
        help_text='Last update timestamp'
    )
    
    objects = JobManager()
    
    class Meta:
        db_table = 'jobs'
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['file_hash', 'ingestion_type']),
            models.Index(fields=['status', 'created_at']),
            models.Index(fields=['ingestion_type', 'status']),
        ]
        verbose_name = 'Job'
        verbose_name_plural = 'Jobs'
    
    def __str__(self):
        return f"{self.ingestion_type} - {self.status} - {self.id}"
    
    @property
    def is_completed(self):
        """Check if job is completed."""
        return self.status == 'completed'
    
    @property
    def is_failed(self):
        """Check if job is failed."""
        return self.status == 'failed'
    
    @property
    def is_running(self):
        """Check if job is running."""
        return self.status == 'running'
    
    def mark_completed(self, **kwargs):
        """Mark job as completed with optional fields."""
        self.status = 'completed'
        for key, value in kwargs.items():
            if hasattr(self, key):
                setattr(self, key, value)
        self.save(update_fields=['status', 'updated_at'] + list(kwargs.keys()))
    
    def mark_failed(self, message=None, **kwargs):
        """Mark job as failed with optional message."""
        self.status = 'failed'
        if message:
            self.message = message
        for key, value in kwargs.items():
            if hasattr(self, key):
                setattr(self, key, value)
        self.save(update_fields=['status', 'message', 'updated_at'] + list(kwargs.keys()))
    
    def increment_retry(self):
        """Increment retry count."""
        self.retry_count += 1
        self.save(update_fields=['retry_count', 'updated_at'])

