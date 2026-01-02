"""
URL configuration for ingestion app.
"""
from django.urls import path, include
from rest_framework.routers import DefaultRouter
from .views import (
    JobViewSet,
    IngestViewSet,
    S3UploadViewSet,
    StatusView,
    TableListView,
    TableDataView,
    BuildTAAnalyticsView,
)

router = DefaultRouter()
router.register(r'jobs', JobViewSet, basename='job')
router.register(r'ingest', IngestViewSet, basename='ingest')
router.register(r'upload', S3UploadViewSet, basename='upload')

urlpatterns = [
    # Router URLs
    path('', include(router.urls)),
    
    # Custom endpoints
    path('status/<uuid:job_id>/', StatusView.as_view(), name='job-status'),
    path('list_tables/', TableListView.as_view(), name='list-tables'),
    path('table_data/<str:table_name>/', TableDataView.as_view(), name='table-data'),
    path('build-ta-analytics/', BuildTAAnalyticsView.as_view(), name='build-ta-analytics'),
]

