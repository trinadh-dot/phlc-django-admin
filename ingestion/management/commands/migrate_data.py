"""
Management command to migrate data from old FastAPI system.
"""
from django.core.management.base import BaseCommand
from django.db import connection
from ingestion.models import Job


class Command(BaseCommand):
    help = 'Migrate data from old FastAPI system (if needed)'
    
    def add_arguments(self, parser):
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Show what would be migrated without actually migrating',
        )
    
    def handle(self, *args, **options):
        """Migrate data from old system."""
        dry_run = options['dry_run']
        
        if dry_run:
            self.stdout.write(self.style.WARNING('🔍 DRY RUN MODE - No changes will be made'))
        
        self.stdout.write('📊 Checking for existing data...')
        
        # Check if jobs table has data
        job_count = Job.objects.count()
        self.stdout.write(f'   Current jobs in database: {job_count}')
        
        if job_count > 0:
            self.stdout.write(self.style.SUCCESS('✅ Jobs table already has data'))
        else:
            self.stdout.write('ℹ️  Jobs table is empty')
            self.stdout.write('   If you need to migrate from the old system, ensure:')
            self.stdout.write('   1. Old database is accessible')
            self.stdout.write('   2. DATABASE_URL points to the correct database')
            self.stdout.write('   3. Run migrations: python manage.py migrate')
        
        # Check for other tables
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_type = 'BASE TABLE'
                AND table_name != 'jobs'
                ORDER BY table_name;
            """)
            tables = [row[0] for row in cursor.fetchall()]
            
            if tables:
                self.stdout.write(f'\n📋 Found {len(tables)} other tables in database:')
                for table in tables[:10]:  # Show first 10
                    self.stdout.write(f'   - {table}')
                if len(tables) > 10:
                    self.stdout.write(f'   ... and {len(tables) - 10} more')
            else:
                self.stdout.write('\nℹ️  No other tables found in database')

