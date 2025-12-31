"""
URL configuration for phlc project.
"""
from django.contrib import admin
from django.urls import path, include
from django.conf import settings
from django.conf.urls.static import static
from drf_spectacular.views import (
    SpectacularAPIView,
    SpectacularRedocView,
    SpectacularSwaggerView,
)
from ingestion.admin import (
    database_tables_view, 
    table_data_view, 
    table_row_view, 
    table_row_edit, 
    table_row_delete
)

# Override admin site's get_urls to add custom URLs
original_get_urls = admin.site.get_urls

def custom_get_urls():
    """Add custom URLs to admin site."""
    from django.urls import path
    urls = original_get_urls()
    # Insert custom URLs before the catch-all pattern
    custom_urls = [
        path('database-tables/', admin.site.admin_view(database_tables_view), name='database_tables'),
        path('database-tables/<str:table_name>/', admin.site.admin_view(table_data_view), name='table_data'),
        path('database-tables/<str:table_name>/view/', admin.site.admin_view(table_row_view), name='table_row_view'),
        path('database-tables/<str:table_name>/edit/', admin.site.admin_view(table_row_edit), name='table_row_edit'),
        path('database-tables/<str:table_name>/delete/', admin.site.admin_view(table_row_delete), name='table_row_delete'),
    ]
    # Insert before the last item (catch-all pattern)
    urls = urls[:-1] + custom_urls + urls[-1:]
    return urls

admin.site.get_urls = custom_get_urls

urlpatterns = [
    # Admin
    path('admin/', admin.site.urls),
    
    # API Documentation
    path('api/schema/', SpectacularAPIView.as_view(), name='schema'),
    path('api/docs/', SpectacularSwaggerView.as_view(url_name='schema'), name='swagger-ui'),
    path('api/redoc/', SpectacularRedocView.as_view(url_name='schema'), name='redoc'),
    
    # API endpoints
    path('api/', include('ingestion.urls')),
]

# Serve media files in development
if settings.DEBUG:
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
    urlpatterns += static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)

# Admin site customization
admin.site.site_header = 'PHLC Ingestion Admin'
admin.site.site_title = 'PHLC Admin'
admin.site.index_title = 'Welcome to PHLC Ingestion Administration'

