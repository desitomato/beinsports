# -*- coding: utf-8 -*-
# Generated by Django 1.11 on 2017-11-16 08:19
from __future__ import unicode_literals

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('middleware', '0004_auto_20171114_1656'),
    ]

    operations = [
        migrations.RenameField(
            model_name='metadataattributes',
            old_name='original_filed_name',
            new_name='original_field_name',
        ),
    ]
