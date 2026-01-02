"""
DRF serializers for ingestion app.
"""
from typing import Optional, List, Union
from rest_framework import serializers
from drf_spectacular.utils import extend_schema_field
from drf_spectacular.types import OpenApiTypes
from .models import Job
import json


class IngestResponseSerializer(serializers.Serializer):
    """Serializer for ingestion response."""
    job_id = serializers.UUIDField()
    message = serializers.CharField()
    file_hash = serializers.CharField(required=False, allow_null=True)
    status = serializers.CharField(required=False, allow_null=True)
    ingestion_type = serializers.CharField(required=False, allow_null=True)
    is_duplicate = serializers.BooleanField(default=False)


class StatusResponseSerializer(serializers.ModelSerializer):
    """Serializer for job status response."""
    job_id = serializers.SerializerMethodField()
    file_size = serializers.SerializerMethodField()
    file_name = serializers.SerializerMethodField()
    file_names = serializers.SerializerMethodField()
    
    class Meta:
        model = Job
        fields = [
            'job_id', 'status', 'ingestion_type', 'file_size',
            'file_count', 'file_name', 'file_names', 'message'
        ]
        read_only_fields = ['status', 'ingestion_type', 'file_count', 'message']
    
    @extend_schema_field(OpenApiTypes.UUID)
    def get_job_id(self, obj: Job) -> str:
        """Return job ID."""
        return str(obj.id)
    
    @extend_schema_field(OpenApiTypes.STR)
    def get_file_size(self, obj: Job) -> Optional[str]:
        """Format file size if inserted_count represents bytes."""
        if obj.inserted_count:
            size_bytes = obj.inserted_count
            if size_bytes < 1024:
                return f"{size_bytes} B"
            elif size_bytes < 1024 * 1024:
                return f"{size_bytes / 1024:.1f} KB"
            else:
                return f"{size_bytes / (1024 * 1024):.1f} MB"
        return None
    
    @extend_schema_field(OpenApiTypes.STR)
    def get_file_name(self, obj: Job) -> Optional[str]:
        """Get single file name if file_names is not JSON."""
        if obj.file_names:
            try:
                # Try to parse as JSON (for multiple files)
                names = json.loads(obj.file_names)
                return None  # If JSON, use file_names instead
            except (json.JSONDecodeError, TypeError):
                # If not JSON, treat as single filename
                return obj.file_names
        return None
    
    @extend_schema_field(OpenApiTypes.OBJECT)
    def get_file_names(self, obj: Job) -> Optional[List[str]]:
        """Get file names as list if JSON, otherwise None."""
        if obj.file_names:
            try:
                # Try to parse as JSON (for multiple files)
                return json.loads(obj.file_names)
            except (json.JSONDecodeError, TypeError):
                # If not JSON, return None (use file_name instead)
                return None
        return None


class S3IngestRequestSerializer(serializers.Serializer):
    """Serializer for S3 ingestion request."""
    s3_key = serializers.CharField(
        required=True,
        help_text='The S3 key (filename/path) of the file in the S3 bucket'
    )


class JobSerializer(serializers.ModelSerializer):
    """Serializer for Job model."""
    
    class Meta:
        model = Job
        fields = [
            'id', 'file_hash', 'ingestion_type', 'status', 'table_name',
            'inserted_count', 'file_names', 'file_count', 'message',
            'retry_count', 'created_at', 'updated_at'
        ]
        read_only_fields = ['id', 'created_at', 'updated_at']


class TableListSerializer(serializers.Serializer):
    """Serializer for table list response."""
    table_name = serializers.CharField()
    schema_name = serializers.CharField()


class TableDataSerializer(serializers.Serializer):
    """Serializer for table data response."""
    data = serializers.ListField(
        child=serializers.DictField(),
        help_text='Array of table rows'
    )
    total_rows = serializers.IntegerField(
        help_text='Total number of rows in the table'
    )
    columns = serializers.ListField(
        child=serializers.CharField(),
        help_text='List of column names'
    )


class BuildTAAnalyticsResponseSerializer(serializers.Serializer):
    """Serializer for TA Analytics build response."""
    message = serializers.CharField()
    status = serializers.CharField()
    ta_combined_rows = serializers.IntegerField(required=False, allow_null=True)
    ta_hours_rows = serializers.IntegerField(required=False, allow_null=True)
    tables_processed = serializers.IntegerField(required=False, allow_null=True)

