# -*- coding: utf-8 -*-
# Generated by Django 1.11 on 2017-11-20 18:54
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('middleware', '0006_auto_20171116_1728'),
    ]

    operations = [
        migrations.AlterField(
            model_name='basicxml',
            name='status',
            field=models.CharField(blank=True, choices=[('converted', b'Converted'), ('error', b'Error'), ('ingested', b'Ingested'), ('pending', b'Pending')], default='pending', max_length=20),
        ),
    ]
