# pylint: disable=line-too-long
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
    list_display = ('pk', 'requested', 'identifier', 'email', 'can_email', 'last_emailed', 'completed',)
    list_filter = ('requested', 'sources', 'can_email',)
    search_fields = ('identifier', 'token', 'extras',)

@admin.register(ExternalDataRequestFile)
class ExternalDataRequestFileAdmin(admin.OSMGeoAdmin):
    list_display = ('request', 'id', 'source', 'uploaded', 'processed', 'skipped', 'encrypted',)
    list_filter = ('processed', 'skipped', 'uploaded', 'source', 'request')
