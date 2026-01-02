#!/usr/bin/env python
"""
Script to change Django admin username.
Usage: python manage.py shell < change_admin_username.py
Or run: python manage.py shell and paste the commands.
"""
import os
import django

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'phlc.settings')
django.setup()

from django.contrib.auth.models import User

# Get current admin user
old_username = input("Enter current admin username: ")
new_username = input("Enter new username: ")

try:
    user = User.objects.get(username=old_username, is_superuser=True)
    user.username = new_username
    user.save()
    print(f"✅ Successfully changed username from '{old_username}' to '{new_username}'")
except User.DoesNotExist:
    print(f"❌ Error: User '{old_username}' not found or is not a superuser")
except Exception as e:
    print(f"❌ Error: {e}")

