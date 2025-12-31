"""
Management command to initialize database tables.
"""
from django.core.management.base import BaseCommand
from django.db import connection
from ingestion.models import Job


class Command(BaseCommand):
    help = 'Initialize database tables (create jobs table if it does not exist)'
    
    def handle(self, *args, **options):
        """Create database tables."""
        self.stdout.write('📝 Checking database tables...')
        
        with connection.cursor() as cursor:
            # Check if jobs table exists
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = 'jobs'
                );
            """)
            table_exists = cursor.fetchone()[0]
            
            if table_exists:
                self.stdout.write(self.style.SUCCESS('✅ Database tables already exist'))
            else:
                self.stdout.write('📝 Creating database tables...')
                # Django will create tables via migrations
                from django.core.management import call_command
                call_command('migrate', verbosity=0)
                self.stdout.write(self.style.SUCCESS('✅ Database tables created successfully'))
        
        # Verify Job model works
        try:
            count = Job.objects.count()
            self.stdout.write(self.style.SUCCESS(f'✅ Job model is working. Current job count: {count}'))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'❌ Error accessing Job model: {e}'))

