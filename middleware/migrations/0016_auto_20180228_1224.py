# -*- coding: utf-8 -*-
# Generated by Django 1.11 on 2018-02-28 06:54
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('middleware', '0015_auto_20180224_1400'),
    ]

    operations = [
        migrations.AlterField(
            model_name='optaxml',
            name='opta_id',
            field=models.CharField(blank=True, default='', max_length=50),
        ),
    ]