"""
Django signals for ingestion app.
"""
from django.db.models.signals import post_save, pre_save
from django.dispatch import receiver
from .models import Job
import logging

logger = logging.getLogger('ingestion')


@receiver(post_save, sender=Job)
def job_status_changed(sender, instance, created, **kwargs):
    """Signal handler for job status changes."""
    if created:
        logger.info(f'Job {instance.id} created with status {instance.status}')
    else:
        # Check if status changed
        if instance.status != instance._state.adding:
            logger.info(f'Job {instance.id} status changed to {instance.status}')


@receiver(pre_save, sender=Job)
def job_pre_save(sender, instance, **kwargs):
    """Signal handler before job save."""
    # Store original status if updating
    if instance.pk:
        try:
            original = Job.objects.get(pk=instance.pk)
            instance._original_status = original.status
        except Job.DoesNotExist:
            instance._original_status = None

