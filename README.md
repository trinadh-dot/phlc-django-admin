# PHLC Django Ingestion API

Django-based ingestion API migrated from FastAPI. This application provides file ingestion capabilities for PostgreSQL and S3, with comprehensive Django Admin interface and REST API.

## Features

- ✅ **Django REST Framework API** - All endpoints from FastAPI converted to DRF
- ✅ **Django Admin** - Full CRUD interface with advanced filtering, search, and export
- ✅ **Celery Background Tasks** - Asynchronous file processing with retry logic
- ✅ **S3 Integration** - File upload with post-copy validation
- ✅ **PostgreSQL Ingestion** - Excel/CSV file processing with special handlers
- ✅ **TA Analytics** - Automated table building from source data
- ✅ **API Documentation** - Swagger/OpenAPI documentation with drf-spectacular
- ✅ **Job Tracking** - Comprehensive job status tracking and monitoring

## Project Structure

```
phlc_django/
├── manage.py
├── requirements.txt
├── README.md
├── phlc/
│   ├── __init__.py
│   ├── settings.py
│   ├── urls.py
│   ├── celery.py
│   ├── wsgi.py
│   └── asgi.py
└── ingestion/
    ├── __init__.py
    ├── apps.py
    ├── models.py
    ├── admin.py
    ├── views.py
    ├── serializers.py
    ├── services.py
    ├── tasks.py
    ├── signals.py
    ├── urls.py
    ├── management/
    │   └── commands/
    │       ├── init_db.py
    │       ├── build_ta_analytics.py
    │       └── migrate_data.py
    └── migrations/
```

## Installation

### 1. Prerequisites

- Python 3.10+
- PostgreSQL 12+
- Redis (for Celery)
- AWS credentials (for S3)

### 2. Setup

```bash
# Clone or navigate to project directory
cd phlc_django

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Create .env file (copy from .env.example)
cp .env.example .env
# Edit .env with your configuration
```

### 3. Environment Variables

Create a `.env` file with the following variables:

```env
# Django
SECRET_KEY=your-secret-key-here
DEBUG=True
ALLOWED_HOSTS=localhost,127.0.0.1
TIMEZONE=UTC

# Database
DATABASE_URL=postgresql://user:password@localhost:5432/phlc_db

# Celery
CELERY_BROKER_URL=redis://localhost:6379/0
CELERY_RESULT_BACKEND=redis://localhost:6379/0

# AWS S3
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key
AWS_REGION=us-east-1
S3_BUCKET=phlc

# File Upload
UPLOAD_FOLDER=/tmp/phlc_uploads

# Google Drive (optional, for scheduler)
DRIVE_FOLDER_ID=your-folder-id
GOOGLE_SERVICE_ACCOUNT_FILE=path/to/service-account.json
CRON_HOUR=0
CRON_MINUTE=0
```

### 4. Database Setup

```bash
# Run migrations
python manage.py migrate

# Initialize database (creates tables if needed)
python manage.py init_db

# Create superuser for admin
python manage.py createsuperuser
```

### 5. Start Services

```bash
# Terminal 1: Django development server
python manage.py runserver

# Terminal 2: Celery worker
celery -A phlc worker --loglevel=info

# Terminal 3: Celery beat (if using scheduled tasks)
celery -A phlc beat --loglevel=info
```

## API Endpoints

### File Ingestion

- `POST /api/ingest/postgres` - Ingest Excel/CSV file into PostgreSQL
- `POST /api/ingest/postgres/from-s3` - Ingest file from S3 into PostgreSQL
- `POST /api/upload/s3` - Upload files to S3

### Job Management

- `GET /api/status/{job_id}` - Get job status
- `GET /api/jobs/` - List all jobs
- `GET /api/jobs/{id}/` - Get job details
- `DELETE /api/jobs/{id}/` - Delete job

### Database Operations

- `GET /api/list_tables/` - List all database tables
- `GET /api/table_data/{table_name}/` - Get table data with filters
- `POST /api/build-ta-analytics/` - Build TA Analytics tables

### API Documentation

- `GET /api/schema/` - OpenAPI schema
- `GET /api/docs/` - Swagger UI
- `GET /api/redoc/` - ReDoc

## Django Admin

Access the admin interface at `http://localhost:8000/admin/`

### Features

- **List View**: Filterable and searchable job list
- **Detail View**: Complete job information with status badges
- **Bulk Actions**: Retry failed jobs, delete jobs, export to CSV
- **Filters**: Status, ingestion type, date range
- **Search**: File hash, table name, message
- **Date Hierarchy**: Navigate by creation date

## Migration from FastAPI

### Step 1: Database Migration

The Django app uses the same database schema. Run migrations to ensure tables are created:

```bash
python manage.py migrate
```

### Step 2: Port Services Logic

The `services.py` file currently imports from the original FastAPI `app/services.py`. For production, you should:

1. Copy all functions from `app/services.py` to `ingestion/services.py`
2. Replace SQLAlchemy code with Django ORM
3. Update database connections to use Django's database connection

### Step 3: Update Celery Tasks

The `tasks.py` file imports from original services. Update tasks to use Django services:

1. Replace `original_process_uploaded_file` calls with Django-compatible versions
2. Update database operations to use Django ORM
3. Test all background tasks

### Step 4: Test Endpoints

```bash
# Test file ingestion
curl -X POST http://localhost:8000/api/ingest/postgres \
  -F "file=@test.xlsx"

# Check job status
curl http://localhost:8000/api/status/{job_id}/

# List tables
curl http://localhost:8000/api/list_tables/
```

## Management Commands

### Initialize Database

```bash
python manage.py init_db
```

### Build TA Analytics Tables

```bash
# Synchronous
python manage.py build_ta_analytics

# Asynchronous (using Celery)
python manage.py build_ta_analytics --async
```

### Migrate Data

```bash
# Dry run
python manage.py migrate_data --dry-run

# Actual migration
python manage.py migrate_data
```

## Development

### Running Tests

```bash
# Install test dependencies
pip install pytest pytest-django pytest-cov

# Run tests
pytest

# With coverage
pytest --cov=ingestion --cov-report=html
```

### Code Style

```bash
# Install black and flake8
pip install black flake8

# Format code
black .

# Check style
flake8 .
```

## Production Deployment

### 1. Update Settings

Set `DEBUG=False` and configure production settings in `settings.py`:

```python
DEBUG = False
ALLOWED_HOSTS = ['your-domain.com']
SECURE_SSL_REDIRECT = True
```

### 2. Static Files

```bash
python manage.py collectstatic
```

### 3. Gunicorn

```bash
gunicorn phlc.wsgi:application --bind 0.0.0.0:8000
```

### 4. Celery Worker

```bash
celery -A phlc worker --loglevel=info --concurrency=4
```

### 5. Nginx Configuration

Example Nginx configuration:

```nginx
server {
    listen 80;
    server_name your-domain.com;

    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }

    location /static/ {
        alias /path/to/staticfiles/;
    }

    location /media/ {
        alias /path/to/media/;
    }
}
```

## Troubleshooting

### Database Connection Issues

```bash
# Test database connection
python manage.py dbshell

# Check migrations
python manage.py showmigrations
```

### Celery Not Working

```bash
# Check Celery status
celery -A phlc inspect active

# Check Redis connection
redis-cli ping
```

### Import Errors

If you see import errors from `app.services`, you need to:

1. Copy `app/services.py` to `ingestion/services.py`
2. Update imports to use Django models
3. Replace SQLAlchemy with Django ORM

## Support

For issues or questions, please refer to the original FastAPI codebase or Django documentation.

## License

[Your License Here]

