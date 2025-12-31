"""
Django Admin configuration for ingestion app.
"""
import csv
from django.contrib import admin
from django.http import HttpResponse
from django.utils.html import format_html
from django.urls import reverse, path
from django.utils import timezone
from django.shortcuts import render, get_object_or_404
from django.db import connection
from django.contrib.admin.views.decorators import staff_member_required
from .models import Job


@admin.action(description='Retry selected failed jobs')
def retry_failed_jobs(modeladmin, request, queryset):
    """Retry failed jobs by resetting status to queued."""
    failed_jobs = queryset.filter(status='failed')
    count = failed_jobs.count()
    failed_jobs.update(status='queued', message='Retried from admin')
    modeladmin.message_user(
        request,
        f'{count} job(s) marked for retry.',
        level='success'
    )


@admin.action(description='Delete selected jobs')
def bulk_delete_jobs(modeladmin, request, queryset):
    """Bulk delete jobs."""
    count = queryset.count()
    queryset.delete()
    modeladmin.message_user(
        request,
        f'{count} job(s) deleted successfully.',
        level='success'
    )


@admin.action(description='Export selected jobs to CSV')
def export_to_csv(modeladmin, request, queryset):
    """Export selected jobs to CSV."""
    response = HttpResponse(content_type='text/csv')
    response['Content-Disposition'] = 'attachment; filename="jobs_export.csv"'
    
    writer = csv.writer(response)
    writer.writerow([
        'ID', 'File Hash', 'Ingestion Type', 'Status', 'Table Name',
        'Inserted Count', 'File Count', 'Retry Count', 'Message',
        'Created At', 'Updated At'
    ])
    
    for job in queryset:
        writer.writerow([
            str(job.id),
            job.file_hash,
            job.ingestion_type,
            job.status,
            job.table_name or '',
            job.inserted_count or '',
            job.file_count or '',
            job.retry_count,
            job.message or '',
            job.created_at.isoformat(),
            job.updated_at.isoformat(),
        ])
    
    return response


@admin.register(Job)
class JobAdmin(admin.ModelAdmin):
    """Admin interface for Job model."""
    
    list_display = [
        'id_link', 'file_hash_short', 'ingestion_type', 'status_badge',
        'table_name', 'inserted_count', 'file_count', 'retry_count',
        'created_at', 'duration'
    ]
    list_filter = [
        'status', 'ingestion_type', 'created_at', 'retry_count'
    ]
    search_fields = [
        'file_hash', 'table_name', 'message', 'id'
    ]
    readonly_fields = [
        'id', 'file_hash', 'created_at', 'updated_at', 'status_badge_display'
    ]
    date_hierarchy = 'created_at'
    ordering = ['-created_at']
    actions = [retry_failed_jobs, bulk_delete_jobs, export_to_csv]
    
    fieldsets = (
        ('Basic Information', {
            'fields': ('id', 'file_hash', 'ingestion_type', 'status_badge_display')
        }),
        ('Processing Details', {
            'fields': ('table_name', 'inserted_count', 'file_count', 'file_names')
        }),
        ('Status Information', {
            'fields': ('message', 'retry_count')
        }),
        ('Timestamps', {
            'fields': ('created_at', 'updated_at'),
            'classes': ('collapse',)
        }),
    )
    
    def get_queryset(self, request):
        """Optimize queryset with select_related and prefetch_related."""
        qs = super().get_queryset(request)
        return qs.select_related().prefetch_related()
    
    def id_link(self, obj):
        """Display job ID as a link to detail page."""
        url = reverse('admin:ingestion_job_change', args=[obj.pk])
        return format_html('<a href="{}">{}</a>', url, str(obj.id)[:8])
    id_link.short_description = 'Job ID'
    id_link.admin_order_field = 'id'
    
    def file_hash_short(self, obj):
        """Display shortened file hash."""
        return obj.file_hash[:16] + '...' if len(obj.file_hash) > 16 else obj.file_hash
    file_hash_short.short_description = 'File Hash'
    file_hash_short.admin_order_field = 'file_hash'
    
    def status_badge(self, obj):
        """Display status with color badge."""
        colors = {
            'completed': 'green',
            'failed': 'red',
            'running': 'orange',
            'queued': 'gray',
        }
        color = colors.get(obj.status, 'gray')
        return format_html(
            '<span style="background-color: {}; color: white; padding: 3px 8px; '
            'border-radius: 3px; font-weight: bold;">{}</span>',
            color,
            obj.get_status_display()
        )
    status_badge.short_description = 'Status'
    status_badge.admin_order_field = 'status'
    
    def status_badge_display(self, obj):
        """Display status badge in detail view."""
        return self.status_badge(obj)
    status_badge_display.short_description = 'Status'
    
    def duration(self, obj):
        """Calculate and display job duration."""
        if obj.status == 'running':
            delta = timezone.now() - obj.created_at
        elif obj.updated_at:
            delta = obj.updated_at - obj.created_at
        else:
            return '-'
        
        total_seconds = int(delta.total_seconds())
        hours, remainder = divmod(total_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        
        if hours > 0:
            return f'{hours}h {minutes}m {seconds}s'
        elif minutes > 0:
            return f'{minutes}m {seconds}s'
        else:
            return f'{seconds}s'
    duration.short_description = 'Duration'
    
    def get_list_display_links(self, request, list_display):
        """Make ID column clickable."""
        return ('id_link',)
    
    def has_delete_permission(self, request, obj=None):
        """Allow deletion."""
        return True
    
    def has_add_permission(self, request):
        """Jobs are created via API, not admin."""
        return False
    
    class Media:
        css = {
            'all': ('admin/css/job_admin.css',)
        }


# Custom admin views for database tables
def database_tables_view(request):
    """View to list all database tables and views."""
    try:
        with connection.cursor() as cursor:
            # Get all tables
            cursor.execute("""
                SELECT table_name, table_schema AS schema_name, 'table' AS type
                FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_type = 'BASE TABLE'
                ORDER BY table_name;
            """)
            tables = [dict(zip([col[0] for col in cursor.description], row)) for row in cursor.fetchall()]
            
            # Get all views
            cursor.execute("""
                SELECT table_name, table_schema AS schema_name, 'view' AS type
                FROM information_schema.views
                WHERE table_schema = 'public'
                ORDER BY table_name;
            """)
            views = [dict(zip([col[0] for col in cursor.description], row)) for row in cursor.fetchall()]
            
            all_objects = tables + views
            
    except Exception as exc:
        all_objects = []
        error_message = str(exc)
    else:
        error_message = None
    
    context = {
        **admin.site.each_context(request),
        'title': 'Database Tables and Views',
        'tables': [obj for obj in all_objects if obj['type'] == 'table'],
        'views': [obj for obj in all_objects if obj['type'] == 'view'],
        'error_message': error_message,
    }
    return render(request, 'admin/ingestion/database_tables.html', context)


def table_data_view(request, table_name):
    """View to display data from a specific table."""
    limit = int(request.GET.get('limit', 100))
    offset = int(request.GET.get('offset', 0))
    limit = max(1, min(limit, 5000))
    offset = max(0, offset)
    
    try:
        with connection.cursor() as cursor:
            # Get columns
            cursor.execute("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = %s
                ORDER BY ordinal_position
            """, [table_name])
            columns = [{'name': row[0], 'type': row[1]} for row in cursor.fetchall()]
            
            if not columns:
                error_message = f'Table "{table_name}" not found or has no columns'
                context = {
                    **admin.site.each_context(request),
                    'title': f'Table: {table_name}',
                    'table_name': table_name,
                    'error_message': error_message,
                }
                return render(request, 'admin/ingestion/table_data.html', context)
            
            # Get primary key columns
            cursor.execute("""
                SELECT a.attname
                FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                WHERE i.indrelid = %s::regclass
                  AND i.indisprimary
                ORDER BY a.attnum
            """, [table_name])
            primary_keys = [row[0] for row in cursor.fetchall()]
            
            # If no primary key, use first column as identifier
            if not primary_keys and columns:
                primary_keys = [columns[0]['name']]
            
            # Get total count
            cursor.execute(f'SELECT COUNT(*) FROM "{table_name}"')
            total_rows = cursor.fetchone()[0]
            
            # Get data
            column_names = [col['name'] for col in columns]
            quoted_columns = ', '.join([f'"{col}"' for col in column_names])
            cursor.execute(
                f'SELECT {quoted_columns} FROM "{table_name}" ORDER BY 1 LIMIT %s OFFSET %s',
                [limit, offset]
            )
            rows = cursor.fetchall()
            data = [dict(zip(column_names, row)) for row in rows]
            
    except Exception as exc:
        error_message = str(exc)
        columns = []
        data = []
        total_rows = 0
        primary_keys = []
    else:
        error_message = None
    
    context = {
        **admin.site.each_context(request),
        'title': f'Table: {table_name}',
        'table_name': table_name,
        'columns': columns,
        'data': data,
        'total_rows': total_rows,
        'limit': limit,
        'offset': offset,
        'has_next': (offset + limit) < total_rows,
        'has_previous': offset > 0,
        'next_offset': offset + limit if (offset + limit) < total_rows else None,
        'prev_offset': max(0, offset - limit),
        'error_message': error_message,
        'primary_keys': primary_keys,
    }
    return render(request, 'admin/ingestion/table_data.html', context)


@staff_member_required
def table_row_view(request, table_name):
    """View a single row from a table."""
    try:
        with connection.cursor() as cursor:
            # Get primary key columns
            cursor.execute("""
                SELECT a.attname
                FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                WHERE i.indrelid = %s::regclass
                  AND i.indisprimary
                ORDER BY a.attnum
            """, [table_name])
            primary_keys = [row[0] for row in cursor.fetchall()]
            
            # Get all columns
            cursor.execute("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = %s
                ORDER BY ordinal_position
            """, [table_name])
            columns = [{'name': row[0], 'type': row[1]} for row in cursor.fetchall()]
            
            if not primary_keys and columns:
                primary_keys = [columns[0]['name']]
            
            # Build WHERE clause from primary keys
            where_clauses = []
            params = []
            for pk in primary_keys:
                value = request.GET.get(pk)
                if not value:
                    error_message = f'Missing primary key value: {pk}'
                    context = {
                        **admin.site.each_context(request),
                        'title': f'View Row: {table_name}',
                        'table_name': table_name,
                        'error_message': error_message,
                    }
                    return render(request, 'admin/ingestion/table_row_view.html', context)
                where_clauses.append(f'"{pk}" = %s')
                params.append(value)
            
            where_clause = ' AND '.join(where_clauses)
            column_names = [col['name'] for col in columns]
            quoted_columns = ', '.join([f'"{col}"' for col in column_names])
            
            cursor.execute(
                f'SELECT {quoted_columns} FROM "{table_name}" WHERE {where_clause}',
                params
            )
            row = cursor.fetchone()
            
            if not row:
                error_message = 'Row not found'
                context = {
                    **admin.site.each_context(request),
                    'title': f'View Row: {table_name}',
                    'table_name': table_name,
                    'error_message': error_message,
                }
                return render(request, 'admin/ingestion/table_row_view.html', context)
            
            data = dict(zip(column_names, row))
            
    except Exception as exc:
        error_message = str(exc)
        columns = []
        data = {}
        primary_keys = []
    else:
        error_message = None
    
    context = {
        **admin.site.each_context(request),
        'title': f'View Row: {table_name}',
        'table_name': table_name,
        'columns': columns,
        'data': data,
        'primary_keys': primary_keys,
        'error_message': error_message,
    }
    return render(request, 'admin/ingestion/table_row_view.html', context)


@staff_member_required
def table_row_edit(request, table_name):
    """Edit a single row from a table."""
    if request.method == 'POST':
        try:
            with connection.cursor() as cursor:
                # Get primary key columns
                cursor.execute("""
                    SELECT a.attname
                    FROM pg_index i
                    JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                    WHERE i.indrelid = %s::regclass
                      AND i.indisprimary
                    ORDER BY a.attnum
                """, [table_name])
                primary_keys = [row[0] for row in cursor.fetchall()]
                
                # Get all columns
                cursor.execute("""
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                      AND table_name = %s
                    ORDER BY ordinal_position
                """, [table_name])
                columns = [{'name': row[0], 'type': row[1]} for row in cursor.fetchall()]
                
                if not primary_keys and columns:
                    primary_keys = [columns[0]['name']]
                
                # Build WHERE clause from primary keys
                where_clauses = []
                where_params = []
                for pk in primary_keys:
                    value = request.POST.get(f'pk_{pk}')
                    if not value:
                        error_message = f'Missing primary key value: {pk}'
                        context = {
                            **admin.site.each_context(request),
                            'title': f'Edit Row: {table_name}',
                            'table_name': table_name,
                            'error_message': error_message,
                        }
                        return render(request, 'admin/ingestion/table_row_edit.html', context)
                    where_clauses.append(f'"{pk}" = %s')
                    where_params.append(value)
                
                # Build SET clause for non-primary key columns
                set_clauses = []
                set_params = []
                for col in columns:
                    if col['name'] not in primary_keys:
                        value = request.POST.get(col['name'], '')
                        set_clauses.append(f'"{col["name"]}" = %s')
                        set_params.append(value if value else None)
                
                where_clause = ' AND '.join(where_clauses)
                set_clause = ', '.join(set_clauses)
                
                cursor.execute(
                    f'UPDATE "{table_name}" SET {set_clause} WHERE {where_clause}',
                    set_params + where_params
                )
                
                # Redirect back to table view
                from django.shortcuts import redirect
                return redirect('admin:table_data', table_name=table_name)
                
        except Exception as exc:
            error_message = str(exc)
            # Fall through to show edit form with error
        else:
            error_message = None
    
    # GET request or error - show edit form
    try:
        with connection.cursor() as cursor:
            # Get primary key columns
            cursor.execute("""
                SELECT a.attname
                FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                WHERE i.indrelid = %s::regclass
                  AND i.indisprimary
                ORDER BY a.attnum
            """, [table_name])
            primary_keys = [row[0] for row in cursor.fetchall()]
            
            # Get all columns
            cursor.execute("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = %s
                ORDER BY ordinal_position
            """, [table_name])
            columns = [{'name': row[0], 'type': row[1]} for row in cursor.fetchall()]
            
            if not primary_keys and columns:
                primary_keys = [columns[0]['name']]
            
            # Build WHERE clause from primary keys
            where_clauses = []
            params = []
            for pk in primary_keys:
                value = request.GET.get(pk)
                if not value:
                    error_message = f'Missing primary key value: {pk}'
                    context = {
                        **admin.site.each_context(request),
                        'title': f'Edit Row: {table_name}',
                        'table_name': table_name,
                        'error_message': error_message,
                    }
                    return render(request, 'admin/ingestion/table_row_edit.html', context)
                where_clauses.append(f'"{pk}" = %s')
                params.append(value)
            
            where_clause = ' AND '.join(where_clauses)
            column_names = [col['name'] for col in columns]
            quoted_columns = ', '.join([f'"{col}"' for col in column_names])
            
            cursor.execute(
                f'SELECT {quoted_columns} FROM "{table_name}" WHERE {where_clause}',
                params
            )
            row = cursor.fetchone()
            
            if not row:
                error_message = 'Row not found'
                context = {
                    **admin.site.each_context(request),
                    'title': f'Edit Row: {table_name}',
                    'table_name': table_name,
                    'error_message': error_message,
                }
                return render(request, 'admin/ingestion/table_row_edit.html', context)
            
            data = dict(zip(column_names, row))
            
    except Exception as exc:
        error_message = str(exc)
        columns = []
        data = {}
        primary_keys = []
    else:
        error_message = None
    
    context = {
        **admin.site.each_context(request),
        'title': f'Edit Row: {table_name}',
        'table_name': table_name,
        'columns': columns,
        'data': data,
        'primary_keys': primary_keys,
        'error_message': error_message,
    }
    return render(request, 'admin/ingestion/table_row_edit.html', context)


@staff_member_required
def table_row_delete(request, table_name):
    """Delete a single row from a table."""
    if request.method == 'POST':
        try:
            with connection.cursor() as cursor:
                # Get primary key columns
                cursor.execute("""
                    SELECT a.attname
                    FROM pg_index i
                    JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                    WHERE i.indrelid = %s::regclass
                      AND i.indisprimary
                    ORDER BY a.attnum
                """, [table_name])
                primary_keys = [row[0] for row in cursor.fetchall()]
                
                # Get all columns
                cursor.execute("""
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                      AND table_name = %s
                    ORDER BY ordinal_position
                """, [table_name])
                columns = [{'name': row[0], 'type': row[1]} for row in cursor.fetchall()]
                
                if not primary_keys and columns:
                    primary_keys = [columns[0]['name']]
                
                # Build WHERE clause from primary keys
                where_clauses = []
                params = []
                for pk in primary_keys:
                    value = request.POST.get(f'pk_{pk}')
                    if not value:
                        error_message = f'Missing primary key value: {pk}'
                        context = {
                            **admin.site.each_context(request),
                            'title': f'Delete Row: {table_name}',
                            'table_name': table_name,
                            'error_message': error_message,
                        }
                        return render(request, 'admin/ingestion/table_row_delete.html', context)
                    where_clauses.append(f'"{pk}" = %s')
                    params.append(value)
                
                where_clause = ' AND '.join(where_clauses)
                
                cursor.execute(
                    f'DELETE FROM "{table_name}" WHERE {where_clause}',
                    params
                )
                
                # Redirect back to table view
                from django.shortcuts import redirect
                return redirect('admin:table_data', table_name=table_name)
                
        except Exception as exc:
            error_message = str(exc)
            # Fall through to show delete confirmation
        else:
            error_message = None
    
    # GET request or error - show delete confirmation
    try:
        with connection.cursor() as cursor:
            # Get primary key columns
            cursor.execute("""
                SELECT a.attname
                FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                WHERE i.indrelid = %s::regclass
                  AND i.indisprimary
                ORDER BY a.attnum
            """, [table_name])
            primary_keys = [row[0] for row in cursor.fetchall()]
            
            # Get all columns
            cursor.execute("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = %s
                ORDER BY ordinal_position
            """, [table_name])
            columns = [{'name': row[0], 'type': row[1]} for row in cursor.fetchall()]
            
            if not primary_keys and columns:
                primary_keys = [columns[0]['name']]
            
            # Build WHERE clause from primary keys
            where_clauses = []
            params = []
            for pk in primary_keys:
                value = request.GET.get(pk)
                if not value:
                    error_message = f'Missing primary key value: {pk}'
                    context = {
                        **admin.site.each_context(request),
                        'title': f'Delete Row: {table_name}',
                        'table_name': table_name,
                        'error_message': error_message,
                    }
                    return render(request, 'admin/ingestion/table_row_delete.html', context)
                where_clauses.append(f'"{pk}" = %s')
                params.append(value)
            
            where_clause = ' AND '.join(where_clauses)
            column_names = [col['name'] for col in columns]
            quoted_columns = ', '.join([f'"{col}"' for col in column_names])
            
            cursor.execute(
                f'SELECT {quoted_columns} FROM "{table_name}" WHERE {where_clause}',
                params
            )
            row = cursor.fetchone()
            
            if not row:
                error_message = 'Row not found'
                context = {
                    **admin.site.each_context(request),
                    'title': f'Delete Row: {table_name}',
                    'table_name': table_name,
                    'error_message': error_message,
                }
                return render(request, 'admin/ingestion/table_row_delete.html', context)
            
            data = dict(zip(column_names, row))
            
    except Exception as exc:
        error_message = str(exc)
        columns = []
        data = {}
        primary_keys = []
    else:
        error_message = None
    
    context = {
        **admin.site.each_context(request),
        'title': f'Delete Row: {table_name}',
        'table_name': table_name,
        'columns': columns,
        'data': data,
        'primary_keys': primary_keys,
        'error_message': error_message,
    }
    return render(request, 'admin/ingestion/table_row_delete.html', context)

