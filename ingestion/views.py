"""
DRF views for ingestion app.
"""
import io
import json
import re
from django.db import connection
from django.http import Http404
from django.db.models import Q
from rest_framework import viewsets, status, views
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.parsers import MultiPartParser, FormParser, JSONParser
from rest_framework.permissions import AllowAny
from drf_spectacular.utils import extend_schema, OpenApiParameter, OpenApiExample
from drf_spectacular.types import OpenApiTypes
from .models import Job
from .serializers import (
    IngestResponseSerializer, StatusResponseSerializer,
    S3IngestRequestSerializer, JobSerializer,
    TableListSerializer, TableDataSerializer,
    BuildTAAnalyticsResponseSerializer
)
from .services import (
    compute_sha256_bytes, has_successful_job,
    download_file_from_s3, validate_upload_payload,
    normalize_relative_path, compute_directory_hash
)
from .tasks import (
    process_uploaded_file_task, process_s3_upload_task,
    process_s3_directory_upload_task, build_ta_analytics_tables_task
)


class JobViewSet(viewsets.ModelViewSet):
    """ViewSet for Job model."""
    queryset = Job.objects.all()
    serializer_class = JobSerializer
    permission_classes = [AllowAny]  # Adjust based on your auth requirements
    filterset_fields = ['status', 'ingestion_type', 'file_hash']
    search_fields = ['file_hash', 'table_name', 'message']
    ordering_fields = ['created_at', 'updated_at', 'status']
    ordering = ['-created_at']
    
    @extend_schema(
        summary='Delete a job',
        description='Delete a job by job_id from the jobs table.',
        responses={200: JobSerializer}
    )
    def destroy(self, request, *args, **kwargs):
        """Delete a job."""
        try:
            instance = self.get_object()
            job_id = instance.id
            self.perform_destroy(instance)
            return Response({
                'message': f'Job {job_id} deleted successfully',
                'job_id': str(job_id),
                'status': 'deleted'
            })
        except Http404:
            return Response(
                {'detail': 'Job not found'},
                status=status.HTTP_404_NOT_FOUND
            )


class IngestViewSet(viewsets.ViewSet):
    """ViewSet for file ingestion endpoints."""
    permission_classes = [AllowAny]
    parser_classes = [MultiPartParser, FormParser]
    
    @extend_schema(
        summary='Ingest Excel file into PostgreSQL',
        description='Ingest Excel file into PostgreSQL database.',
        request={
            'multipart/form-data': {
                'type': 'object',
                'properties': {
                    'file': {
                        'type': 'string',
                        'format': 'binary',
                        'description': 'Excel file to ingest'
                    }
                }
            }
        },
        responses={200: IngestResponseSerializer},
        examples=[
            OpenApiExample(
                'Success Response',
                value={
                    'job_id': '123e4567-e89b-12d3-a456-426614174000',
                    'message': 'PostgreSQL ingestion started',
                    'file_hash': 'abc123...',
                    'status': 'running',
                    'ingestion_type': 'Postgres',
                    'is_duplicate': False
                }
            )
        ]
    )
    @action(detail=False, methods=['post'], url_path='ingest/postgres')
    def ingest_postgres(self, request):
        """Ingest Excel file into PostgreSQL."""
        if 'file' not in request.FILES:
            return Response(
                {'detail': 'No file provided'},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        file = request.FILES['file']
        contents = file.read()
        file_hash = compute_sha256_bytes(contents)
        ingestion_type = 'Postgres'
        
        # Check for duplicate
        if has_successful_job(file_hash, ingestion_type=ingestion_type):
            completed_job = Job.objects.filter(
                file_hash=file_hash,
                ingestion_type=ingestion_type,
                status='completed'
            ).first()
            if completed_job:
                serializer = IngestResponseSerializer({
                    'job_id': completed_job.id,
                    'message': f'File already successfully ingested to PostgreSQL. Status: {completed_job.status}',
                    'file_hash': completed_job.file_hash,
                    'status': completed_job.status,
                    'ingestion_type': completed_job.ingestion_type,
                    'is_duplicate': True
                })
                return Response(serializer.data)
        
        # Create job
        job = Job.objects.create(
            file_hash=file_hash,
            ingestion_type=ingestion_type,
            status='running'
        )
        
        # Schedule background processing
        bio = io.BytesIO(contents)
        process_uploaded_file_task.delay(str(job.id), bio.read(), file.name)
        
        serializer = IngestResponseSerializer({
            'job_id': job.id,
            'message': 'PostgreSQL ingestion started',
            'file_hash': job.file_hash,
            'status': job.status,
            'ingestion_type': job.ingestion_type,
            'is_duplicate': False
        })
        return Response(serializer.data, status=status.HTTP_201_CREATED)
    
    @extend_schema(
        summary='Ingest file from S3 into PostgreSQL',
        description='Ingest file from S3 into PostgreSQL database. The file must already be uploaded to S3.',
        request=S3IngestRequestSerializer,
        responses={200: IngestResponseSerializer, 404: {'detail': 'File not found in S3'}}
    )
    @action(detail=False, methods=['post'], url_path='ingest/postgres/from-s3')
    def ingest_postgres_from_s3(self, request):
        """Ingest file from S3 into PostgreSQL."""
        serializer = S3IngestRequestSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        s3_key = serializer.validated_data['s3_key']
        ingestion_type = 'Postgres'
        
        try:
            contents = download_file_from_s3(s3_key)
            file_hash = compute_sha256_bytes(contents)
            
            # Check for duplicate
            if has_successful_job(file_hash, ingestion_type=ingestion_type):
                completed_job = Job.objects.filter(
                    file_hash=file_hash,
                    ingestion_type=ingestion_type,
                    status='completed'
                ).first()
                if completed_job:
                    serializer = IngestResponseSerializer({
                        'job_id': completed_job.id,
                        'message': f'File already successfully ingested to PostgreSQL. Status: {completed_job.status}',
                        'file_hash': completed_job.file_hash,
                        'status': completed_job.status,
                        'ingestion_type': completed_job.ingestion_type,
                        'is_duplicate': True
                    })
                    return Response(serializer.data)
            
            # Create job
            job = Job.objects.create(
                file_hash=file_hash,
                ingestion_type=ingestion_type,
                status='running'
            )
            filename = s3_key.split('/')[-1]
            bio = io.BytesIO(contents)
            process_uploaded_file_task.delay(str(job.id), bio.read(), filename)
            
            serializer = IngestResponseSerializer({
                'job_id': job.id,
                'message': f'PostgreSQL ingestion started from S3: {s3_key}',
                'file_hash': job.file_hash,
                'status': job.status,
                'ingestion_type': job.ingestion_type,
                'is_duplicate': False
            })
            return Response(serializer.data, status=status.HTTP_201_CREATED)
        except ValueError as e:
            return Response(
                {'detail': str(e)},
                status=status.HTTP_404_NOT_FOUND
            )
        except RuntimeError as e:
            return Response(
                {'detail': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class S3UploadViewSet(viewsets.ViewSet):
    """ViewSet for S3 upload endpoints."""
    permission_classes = [AllowAny]
    parser_classes = [MultiPartParser, FormParser]
    
    @extend_schema(
        summary='Upload files to S3',
        description='Upload single file, multiple files, or a folder (as multiple files) to S3.',
        request={
            'multipart/form-data': {
                'type': 'object',
                'properties': {
                    'files': {
                        'type': 'array',
                        'items': {'type': 'string', 'format': 'binary'},
                        'description': 'Files to upload'
                    },
                    'preserve_filename': {
                        'type': 'boolean',
                        'description': 'If True, keep original filenames/paths in S3 (may overwrite). If False, use UUID prefixes for uniqueness.'
                    }
                }
            }
        },
        responses={200: IngestResponseSerializer}
    )
    @action(detail=False, methods=['post'], url_path='upload/s3')
    def upload_to_s3(self, request):
        """Upload files to S3."""
        files = request.FILES.getlist('files')
        preserve_filename = request.data.get('preserve_filename', 'true').lower() == 'true'
        
        if not files:
            return Response(
                {'detail': 'At least one file must be provided'},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        ingestion_type = 'S3'
        payload_kind = None
        file_hash = None
        contents = None
        raw_directory_entries = None
        
        # Case 1: Single file uploaded
        if len(files) == 1:
            file = files[0]
            contents = file.read()
            
            # Check if it's a zip file
            if file.name.lower().endswith('.zip'):
                try:
                    validate_upload_payload(contents, 'directory')
                except ValueError as exc:
                    return Response(
                        {'detail': str(exc)},
                        status=status.HTTP_400_BAD_REQUEST
                    )
                file_hash = compute_sha256_bytes(contents)
                payload_kind = 'directory_archive'
            else:
                try:
                    validate_upload_payload(contents, 'file')
                except ValueError as exc:
                    return Response(
                        {'detail': str(exc)},
                        status=status.HTTP_400_BAD_REQUEST
                    )
                file_hash = compute_sha256_bytes(contents)
                payload_kind = 'single'
        
        # Case 2: Multiple files uploaded
        else:
            raw_directory_entries = []
            for entry in files:
                data = entry.read()
                try:
                    rel_path = normalize_relative_path(entry.name)
                except ValueError as exc:
                    return Response(
                        {'detail': str(exc)},
                        status=status.HTTP_400_BAD_REQUEST
                    )
                raw_directory_entries.append({
                    'path': rel_path,
                    'content': data,
                    'content_type': entry.content_type
                })
            try:
                file_hash = compute_directory_hash(raw_directory_entries)
            except ValueError as exc:
                return Response(
                    {'detail': str(exc)},
                    status=status.HTTP_400_BAD_REQUEST
                )
            payload_kind = 'raw_directory'
        
        # Check for existing job
        existing = Job.objects.filter(
            file_hash=file_hash,
            ingestion_type=ingestion_type
        ).first()
        if existing:
            serializer = IngestResponseSerializer({
                'job_id': existing.id,
                'message': f'File already uploaded to S3. Current status: {existing.status}',
                'file_hash': existing.file_hash,
                'status': existing.status,
                'ingestion_type': existing.ingestion_type,
                'is_duplicate': True
            })
            return Response(serializer.data)
        
        # Create job
        job = Job.objects.create(
            file_hash=file_hash,
            ingestion_type=ingestion_type,
            status='running'
        )
        
        # Process based on payload kind
        if payload_kind == 'raw_directory':
            process_s3_directory_upload_task.delay(
                str(job.id),
                raw_directory_entries,
                preserve_filename
            )
        else:
            process_s3_upload_task.delay(
                str(job.id),
                contents,
                files[0].name,
                'directory' if payload_kind == 'directory_archive' else 'file',
                files[0].content_type,
                preserve_filename
            )
        
        serializer = IngestResponseSerializer({
            'job_id': job.id,
            'message': 'S3 upload started',
            'file_hash': job.file_hash,
            'status': job.status,
            'ingestion_type': job.ingestion_type,
            'is_duplicate': False
        })
        return Response(serializer.data, status=status.HTTP_201_CREATED)


class StatusView(views.APIView):
    """View for checking job status."""
    permission_classes = [AllowAny]
    
    @extend_schema(
        summary='Get job status',
        description='Check the status of an ingestion job.',
        responses={200: StatusResponseSerializer, 404: {'detail': 'Job not found'}}
    )
    def get(self, request, job_id):
        """Get job status."""
        try:
            job = Job.objects.get(id=job_id)
            serializer = StatusResponseSerializer(job)
            return Response(serializer.data)
        except Job.DoesNotExist:
            return Response(
                {'detail': 'Job not found'},
                status=status.HTTP_404_NOT_FOUND
            )


class TableListView(views.APIView):
    """View for listing database tables."""
    permission_classes = [AllowAny]
    
    @extend_schema(
        summary='List all tables',
        description='List all tables in the public schema.',
        responses={200: TableListSerializer(many=True)}
    )
    def get(self, request):
        """List all tables."""
        try:
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT table_name, table_schema AS schema_name
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                      AND table_type = 'BASE TABLE'
                    ORDER BY table_name;
                """)
                columns = [col[0] for col in cursor.description]
                rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
            return Response(rows)
        except Exception as exc:
            return Response(
                {'detail': f'Failed to fetch tables: {str(exc)}'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class TableDataView(views.APIView):
    """View for getting table data."""
    permission_classes = [AllowAny]
    
    _NAME_RE = re.compile(r'^[A-Za-z0-9_\-\s]+$')
    
    def _validate_name(self, name: str) -> bool:
        """Validate table/column name."""
        return bool(self._NAME_RE.match(name))
    
    @extend_schema(
        summary='Get table data',
        description='Get data from a specific table with optional filtering.',
        parameters=[
            OpenApiParameter(
                'limit',
                OpenApiTypes.INT,
                description='Number of rows to return (default 100)',
                required=False
            ),
            OpenApiParameter(
                'offset',
                OpenApiTypes.INT,
                description='Number of rows to skip (default 0)',
                required=False
            ),
            OpenApiParameter(
                'filter_<column_name>',
                OpenApiTypes.STR,
                description='Filter by column value (multiple filters supported)',
                required=False
            ),
        ],
        responses={200: TableDataSerializer}
    )
    def get(self, request, table_name):
        """Get table data."""
        # Validate table name
        if not self._validate_name(table_name):
            return Response(
                {'detail': 'Invalid table name'},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        limit = int(request.query_params.get('limit', 100))
        offset = int(request.query_params.get('offset', 0))
        limit = max(1, min(limit, 5000))  # Clamp between 1 and 5000
        offset = max(0, offset)
        
        try:
            with connection.cursor() as cursor:
                # Get available columns
                cursor.execute("""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                      AND table_name ILIKE %s
                    ORDER BY ordinal_position
                """, [table_name])
                columns = [row[0] for row in cursor.fetchall()]
                
                if not columns:
                    return Response(
                        {'detail': 'Table not found or has no columns'},
                        status=status.HTTP_404_NOT_FOUND
                    )
                
                # Build WHERE clauses from query parameters
                where_clauses = []
                params = []
                
                for key, value in request.query_params.items():
                    if not key.startswith('filter_'):
                        continue
                    
                    col = key[len('filter_'):]
                    
                    if not self._validate_name(col):
                        return Response(
                            {'detail': f'Invalid filter column: {col}'},
                            status=status.HTTP_400_BAD_REQUEST
                        )
                    
                    if col not in columns:
                        return Response(
                            {'detail': f'Filter column does not exist on table: {col}'},
                            status=status.HTTP_400_BAD_REQUEST
                        )
                    
                    where_clauses.append(f'"{col}"::text ILIKE %s')
                    params.append(f'%{value}%')
                
                where_clause_sql = ''
                if where_clauses:
                    where_clause_sql = 'WHERE ' + ' AND '.join(where_clauses)
                
                # Get total count
                quoted_table = f'"{table_name}"'
                count_sql = f'SELECT COUNT(*) as total FROM {quoted_table} {where_clause_sql}'
                cursor.execute(count_sql, params)
                total_rows = cursor.fetchone()[0]
                
                # Fetch data rows
                params_with_pagination = params + [limit, offset]
                data_sql = f'SELECT * FROM {quoted_table} {where_clause_sql} ORDER BY 1 LIMIT %s OFFSET %s'
                cursor.execute(data_sql, params_with_pagination)
                
                columns_data = [col[0] for col in cursor.description]
                data_rows = [dict(zip(columns_data, row)) for row in cursor.fetchall()]
                
                serializer = TableDataSerializer({
                    'data': data_rows,
                    'total_rows': total_rows,
                    'columns': columns
                })
                return Response(serializer.data)
        except Exception as exc:
            return Response(
                {'detail': f'Failed to fetch table data: {str(exc)}'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class BuildTAAnalyticsView(views.APIView):
    """View for building TA Analytics tables."""
    permission_classes = [AllowAny]
    
    @extend_schema(
        summary='Build TA Analytics tables',
        description='Build TA_Combined and TA_Hours tables from source tables.',
        request=None,
        responses={200: BuildTAAnalyticsResponseSerializer}
    )
    def post(self, request):
        """Build TA Analytics tables."""
        try:
            # Run in background task
            task = build_ta_analytics_tables_task.delay()
            return Response({
                'message': 'TA analytics tables build started in background',
                'status': 'running',
                'task_id': str(task.id)
            })
        except Exception as exc:
            return Response(
                {'detail': f'Failed to build TA analytics tables: {str(exc)}'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

