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
    list_display = ('requested', 'identifier', 'email',)
    list_filter = ('requested', 'sources',)

@admin.register(ExternalDataRequestFile)
class ExternalDataRequestFileAdmin(admin.OSMGeoAdmin):
    list_display = ('request', 'source', 'processed',)
    list_filter = ('processed', 'source',)
