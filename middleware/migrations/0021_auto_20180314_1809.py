# -*- coding: utf-8 -*-
# Generated by Django 1.11 on 2018-03-14 12:39
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('middleware', '0020_auto_20180314_1600'),
    ]

    operations = [
        migrations.AlterField(
            model_name='optaxml',
            name='last_update_timestamp',
            field=models.DateTimeField(null=True),
        ),
    ]