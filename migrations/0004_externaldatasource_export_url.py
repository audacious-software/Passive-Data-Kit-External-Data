# pylint: skip-file
# -*- coding: utf-8 -*-
# Generated by Django 1.11.23 on 2019-11-14 17:57
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('passive_data_kit_external_data', '0003_externaldatasource_priority'),
    ]

    operations = [
        migrations.AddField(
            model_name='externaldatasource',
            name='export_url',
            field=models.URLField(blank=True, null=True),
        ),
    ]
