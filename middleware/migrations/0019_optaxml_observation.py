# -*- coding: utf-8 -*-
# Generated by Django 1.11 on 2018-03-14 08:19
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('middleware', '0018_basicxml_som'),
    ]

    operations = [
        migrations.AddField(
            model_name='optaxml',
            name='observation',
            field=models.PositiveSmallIntegerField(default=0),
        ),
    ]
