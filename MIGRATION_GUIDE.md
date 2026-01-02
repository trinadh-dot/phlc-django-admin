# Migration Guide: FastAPI to Django

This guide will help you complete the migration from FastAPI to Django.

## Current Status

✅ **Completed:**
- Django project structure
- Models (Job model with all fields)
- Django Admin with full features
- DRF serializers and views
- URL routing
- Celery task structure
- Management commands
- Settings configuration

⚠️ **Needs Completion:**
- Port `app/services.py` logic to Django services
- Update Celery tasks to use Django ORM
- Create initial migrations
- Test all endpoints

## Step-by-Step Migration

### 1. Create Initial Migrations

```bash
cd phlc_django
python manage.py makemigrations
python manage.py migrate
```

### 2. Port Services Logic

The `ingestion/services.py` currently imports from the original `app/services.py`. You need to:

1. **Copy functions from `app/services.py`** to `ingestion/services.py`
2. **Replace SQLAlchemy with Django ORM:**
   - `SessionLocal()` → Django model queries
   - `db.query(Model)` → `Model.objects.filter()`
   - `db.add()` / `db.commit()` → `model.save()`
   - `engine.begin()` → `transaction.atomic()`

3. **Update database connections:**
   ```python
   # Old (SQLAlchemy)
   from app.db import engine, SessionLocal
   with engine.begin() as conn:
       # ...
   
   # New (Django)
   from django.db import connection, transaction
   with transaction.atomic():
       with connection.cursor() as cursor:
           # ...
   ```

### 3. Update Celery Tasks

The `ingestion/tasks.py` imports from original services. Update to:

1. **Use Django models directly:**
   ```python
   from ingestion.models import Job
   from ingestion.services import process_uploaded_file
   
   @shared_task
   def process_uploaded_file_task(job_id, file_bytes, filename):
       job = Job.objects.get(id=job_id)
       # Use Django services
       process_uploaded_file(job, file_bytes, filename)
   ```

2. **Replace SQLAlchemy operations:**
   - All database operations should use Django ORM
   - Use `transaction.atomic()` for transactions
   - Use `connection.cursor()` for raw SQL

### 4. Key Function Mappings

#### Database Operations

| FastAPI (SQLAlchemy) | Django |
|---------------------|--------|
| `SessionLocal()` | `Model.objects` |
| `db.query(Model).filter()` | `Model.objects.filter()` |
| `db.add(obj)` | `obj.save()` |
| `db.commit()` | `obj.save()` (automatic) |
| `engine.begin()` | `transaction.atomic()` |
| `conn.execute(text(sql))` | `cursor.execute(sql)` |

#### CRUD Operations

| FastAPI | Django |
|---------|--------|
| `crud.create_job(db, ...)` | `Job.objects.create(...)` |
| `crud.get_job(db, job_id)` | `Job.objects.get(id=job_id)` |
| `crud.update_job_status(db, job, ...)` | `job.mark_completed(...)` or `job.save()` |
| `crud.has_successful_job(db, ...)` | `Job.objects.filter(...).exists()` |

### 5. Update Specific Functions

#### `process_uploaded_file`

```python
# Old
def process_uploaded_file(job_id, file_bytes, filename):
    db = SessionLocal()
    job = crud.get_job(db, job_id)
    # ... processing ...
    update_job_status(db, job, status='completed')
    db.close()

# New
def process_uploaded_file(job_id, file_bytes, filename):
    from ingestion.models import Job
    job = Job.objects.get(id=job_id)
    # ... processing ...
    job.mark_completed(inserted_count=count, table_name=table_name)
```

#### `build_ta_analytics_tables`

```python
# Old
def build_ta_analytics_tables():
    with engine.begin() as conn:
        # ... SQL operations ...
        conn.execute(text(sql))

# New
def build_ta_analytics_tables():
    from django.db import connection, transaction
    with transaction.atomic():
        with connection.cursor() as cursor:
            # ... SQL operations ...
            cursor.execute(sql)
```

### 6. Testing Checklist

- [ ] All API endpoints work
- [ ] File uploads process correctly
- [ ] S3 uploads work with validation
- [ ] Background tasks complete successfully
- [ ] Job status updates correctly
- [ ] Django Admin displays jobs
- [ ] TA Analytics tables build correctly
- [ ] Database queries are efficient
- [ ] Error handling works properly

### 7. Performance Considerations

1. **Database Queries:**
   - Use `select_related()` and `prefetch_related()` for joins
   - Use `only()` and `defer()` to limit fields
   - Use `exists()` instead of `count()` when checking existence

2. **Background Tasks:**
   - Set appropriate `CELERY_TASK_TIME_LIMIT`
   - Use task result backend for long-running tasks
   - Monitor Celery worker logs

3. **File Processing:**
   - Use streaming for large files
   - Clean up temporary files
   - Monitor memory usage

### 8. Common Issues and Solutions

#### Issue: Import errors from `app.services`

**Solution:** Copy all functions to `ingestion/services.py` and update imports.

#### Issue: Database connection errors

**Solution:** Ensure `DATABASE_URL` is set correctly in `.env`.

#### Issue: Celery tasks not running

**Solution:** 
- Check Redis is running: `redis-cli ping`
- Check Celery worker: `celery -A phlc inspect active`
- Check task logs for errors

#### Issue: File uploads failing

**Solution:**
- Check `UPLOAD_FOLDER` exists and is writable
- Check file size limits in settings
- Check Celery worker has access to files

### 9. Deployment Checklist

- [ ] Set `DEBUG=False` in production
- [ ] Configure `ALLOWED_HOSTS`
- [ ] Set secure `SECRET_KEY`
- [ ] Configure SSL/HTTPS
- [ ] Set up static file serving
- [ ] Configure Celery workers
- [ ] Set up monitoring/logging
- [ ] Test all endpoints in production
- [ ] Set up database backups
- [ ] Configure CORS if needed

### 10. Next Steps

1. **Complete services porting** - This is the most critical step
2. **Test thoroughly** - Test all endpoints and background tasks
3. **Optimize queries** - Use Django ORM best practices
4. **Add error handling** - Comprehensive error handling and logging
5. **Documentation** - Update API documentation
6. **Monitoring** - Set up monitoring for production

## Support

If you encounter issues during migration:

1. Check Django logs: `python manage.py runserver --verbosity=2`
2. Check Celery logs: `celery -A phlc worker --loglevel=debug`
3. Check database: `python manage.py dbshell`
4. Review original FastAPI code for reference

## Resources

- [Django ORM Documentation](https://docs.djangoproject.com/en/4.2/topics/db/)
- [Django REST Framework](https://www.django-rest-framework.org/)
- [Celery Documentation](https://docs.celeryq.dev/)
- [Django Admin Documentation](https://docs.djangoproject.com/en/4.2/ref/contrib/admin/)

