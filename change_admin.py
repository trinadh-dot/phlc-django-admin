#!/usr/bin/env python
"""
Script to change Django admin username.
Run this from WSL: python3 change_admin.py
"""
import os
import sys
import django

# Setup Django
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'phlc.settings')
django.setup()

from django.contrib.auth.models import User

# List all admin users
print("\n📋 Current admin users:")
admins = User.objects.filter(is_superuser=True)
if not admins.exists():
    print("  No admin users found!")
    sys.exit(1)

for admin in admins:
    print(f"  - Username: {admin.username}")
    print(f"    Email: {admin.email}")
    print()

# Get input
old_username = input("Enter current username to change: ").strip()
new_username = input("Enter new username: ").strip()

if not old_username or not new_username:
    print("❌ Error: Username cannot be empty")
    sys.exit(1)

# Check if new username already exists
if User.objects.filter(username=new_username).exists():
    print(f"❌ Error: Username '{new_username}' already exists!")
    sys.exit(1)

try:
    user = User.objects.get(username=old_username, is_superuser=True)
    user.username = new_username
    user.save()
    print(f"\n✅ Successfully changed username from '{old_username}' to '{new_username}'")
except User.DoesNotExist:
    print(f"❌ Error: User '{old_username}' not found or is not a superuser")
    sys.exit(1)
except Exception as e:
    print(f"❌ Error: {e}")
    sys.exit(1)

