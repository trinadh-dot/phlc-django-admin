"""
Tests for ingestion app.
"""
from django.test import TestCase
from django.urls import reverse
from rest_framework.test import APIClient
from rest_framework import status
from .models import Job
import uuid


class JobModelTest(TestCase):
    """Test Job model."""
    
    def setUp(self):
        """Set up test data."""
        self.job = Job.objects.create(
            file_hash='test_hash_123',
            ingestion_type='Postgres',
            status='running'
        )
    
    def test_job_creation(self):
        """Test job creation."""
        self.assertEqual(self.job.status, 'running')
        self.assertEqual(self.job.ingestion_type, 'Postgres')
        self.assertIsNotNone(self.job.id)
    
    def test_job_mark_completed(self):
        """Test marking job as completed."""
        self.job.mark_completed(
            inserted_count=100,
            table_name='test_table',
            message='Test completed'
        )
        self.assertEqual(self.job.status, 'completed')
        self.assertEqual(self.job.inserted_count, 100)
    
    def test_job_mark_failed(self):
        """Test marking job as failed."""
        self.job.mark_failed(message='Test error')
        self.assertEqual(self.job.status, 'failed')
        self.assertEqual(self.job.message, 'Test error')
    
    def test_job_increment_retry(self):
        """Test incrementing retry count."""
        initial_count = self.job.retry_count
        self.job.increment_retry()
        self.assertEqual(self.job.retry_count, initial_count + 1)


class JobAPITest(TestCase):
    """Test Job API endpoints."""
    
    def setUp(self):
        """Set up test client and data."""
        self.client = APIClient()
        self.job = Job.objects.create(
            file_hash='test_hash_123',
            ingestion_type='Postgres',
            status='completed',
            inserted_count=100,
            table_name='test_table'
        )
    
    def test_list_jobs(self):
        """Test listing jobs."""
        url = reverse('job-list')
        response = self.client.get(url)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertGreaterEqual(len(response.data['results']), 1)
    
    def test_get_job_detail(self):
        """Test getting job details."""
        url = reverse('job-detail', args=[self.job.id])
        response = self.client.get(url)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data['id'], str(self.job.id))
    
    def test_delete_job(self):
        """Test deleting a job."""
        url = reverse('job-detail', args=[self.job.id])
        response = self.client.delete(url)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertFalse(Job.objects.filter(id=self.job.id).exists())


class StatusAPITest(TestCase):
    """Test status endpoint."""
    
    def setUp(self):
        """Set up test client and data."""
        self.client = APIClient()
        self.job = Job.objects.create(
            file_hash='test_hash_123',
            ingestion_type='Postgres',
            status='running',
            inserted_count=100
        )
    
    def test_get_status(self):
        """Test getting job status."""
        url = reverse('job-status', args=[self.job.id])
        response = self.client.get(url)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data['status'], 'running')
        self.assertEqual(response.data['job_id'], str(self.job.id))
    
    def test_get_status_not_found(self):
        """Test getting status for non-existent job."""
        fake_id = uuid.uuid4()
        url = reverse('job-status', args=[fake_id])
        response = self.client.get(url)
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

