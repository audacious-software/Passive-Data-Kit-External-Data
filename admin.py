# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.contrib.gis import admin

from .models import ExternalDataSource, ExternalDataRequest, ExternalDataRequestFile

@admin.register(ExternalDataSource)
class ExternalDataSourceAdmin(admin.OSMGeoAdmin):
    list_display = ('name', 'identifier', 'priority',)
    list_filter = ('name', 'identifier',)

@admin.register(ExternalDataRequest)
class ExternalDataRequestAdmin(admin.OSMGeoAdmin):
    list_display = ('requested', 'identifier', 'email', 'can_email', 'last_emailed', 'completed',)
    list_filter = ('requested', 'sources', 'can_email',)

@admin.register(ExternalDataRequestFile)
class ExternalDataRequestFileAdmin(admin.OSMGeoAdmin):
    list_display = ('request', 'source', 'processed', 'skipped',)
    list_filter = ('processed', 'source',)
