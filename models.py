# pylint: disable=line-too-long, no-member
# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import importlib
import random

from django.conf import settings
from django.contrib.gis.db import models
from django.template.loader import render_to_string
from django.utils import timezone

class ExternalDataSource(models.Model):
    name = models.CharField(max_length=1024)
    identifier = models.SlugField(max_length=1024, unique=True)
    priority = models.IntegerField(default=0)

    export_url = models.URLField(null=True, blank=True)

    def instruction_content(self):
        return render_to_string('sources/pdk_export_instructions_' + self.identifier + '.html')

    def __unicode__(self):
        return self.name


class ExternalDataRequest(models.Model):
    identifier = models.CharField(max_length=1024)
    email = models.CharField(max_length=1024)

    token = models.CharField(max_length=1024, null=True, blank=True)

    requested = models.DateTimeField()

    sources = models.ManyToManyField(ExternalDataSource, related_name='requests')

    def __unicode__(self):
        return self.identifier

    def request_files(self):
        files = []

        for source in self.sources.all().order_by('priority'):
            file_dict = {
                'identifier': source.identifier,
                'name': source.name
            }

            uploaded = self.data_files.filter(source=source).order_by('-uploaded').first()

            file_dict['file'] = uploaded

            files.append(file_dict)

        return files

    def generate_content_token(self):
        valid_characters = '9876543210abcdef'

        token_exists = True

        token = None

        while token_exists:
            token = ''.join(random.SystemRandom().choice(valid_characters) for _ in range(256))

            token_exists = (ExternalDataRequest.objects.filter(token=token).count() > 0)

        self.token = token
        self.save()


class ExternalDataRequestFile(models.Model):
    request = models.ForeignKey(ExternalDataRequest, related_name='data_files')
    source = models.ForeignKey(ExternalDataSource, related_name='data_files')

    data_file = models.FileField(upload_to='pdk_external_files/')

    uploaded = models.DateTimeField()
    processed = models.DateTimeField(null=True, blank=True)

    def process(self):
        if self.processed is not None:
            print 'Already processed: ' + self.data_file.path

            return False

        file_processed = False

        for app in settings.INSTALLED_APPS:
            if file_processed is False:
                try:
                    pdk_api = importlib.import_module(app + '.pdk_api')

                    file_processed = pdk_api.import_external_data(self.source.identifier, self.request.identifier, self.data_file.path)
                except ImportError:
                    pass
                except AttributeError:
                    pass

        if file_processed:
            self.processed = timezone.now()
            self.save()

        return file_processed
