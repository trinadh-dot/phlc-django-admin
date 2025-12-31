from django.apps import AppConfig


class IngestionConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'ingestion'
    verbose_name = 'Data Ingestion'

    def ready(self):
        import ingestion.signals  # noqa

