"""
Management command to build TA Analytics tables.
"""
from django.core.management.base import BaseCommand
from ingestion.tasks import build_ta_analytics_tables_task


class Command(BaseCommand):
    help = 'Build TA_Combined and TA_Hours tables from source tables'
    
    def add_arguments(self, parser):
        parser.add_argument(
            '--async',
            action='store_true',
            help='Run the build asynchronously using Celery',
        )
    
    def handle(self, *args, **options):
        """Build TA Analytics tables."""
        if options['async']:
            self.stdout.write('🚀 Starting TA Analytics build in background...')
            task = build_ta_analytics_tables_task.delay()
            self.stdout.write(self.style.SUCCESS(f'✅ Task started with ID: {task.id}'))
            self.stdout.write('   Use Celery to monitor task progress.')
        else:
            self.stdout.write('📊 Building TA Analytics tables synchronously...')
            try:
                result = build_ta_analytics_tables_task()
                self.stdout.write(self.style.SUCCESS('✅ TA Analytics tables built successfully'))
                if result:
                    self.stdout.write(f'   TA_Combined: {result.get("ta_combined_rows", 0)} rows')
                    self.stdout.write(f'   TA_Hours: {result.get("ta_hours_rows", 0)} rows')
                    self.stdout.write(f'   Tables processed: {result.get("tables_processed", 0)}')
            except Exception as e:
                self.stdout.write(self.style.ERROR(f'❌ Error building TA Analytics tables: {e}'))

