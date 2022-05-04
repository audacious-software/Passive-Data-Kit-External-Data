# pylint: disable=line-too-long, no-member, no-else-return
# -*- coding: utf-8 -*-

from __future__ import print_function
from __future__ import unicode_literals

import importlib
import json
import random
import traceback

from six import python_2_unicode_compatible

from django.conf import settings
from django.contrib.gis.db import models
from django.core.checks import Warning, register # pylint: disable=redefined-builtin
from django.core.mail import send_mail
from django.template.loader import render_to_string
from django.urls import reverse
from django.utils import timezone


@register()
def check_twilio_settings_defined(app_configs, **kwargs): # pylint: disable=unused-argument
    errors = []

    if hasattr(settings, 'PDK_EXTERNAL_CONTENT_PUBLIC_KEY') is False:
        warning = Warning('PDK_EXTERNAL_CONTENT_PUBLIC_KEY parameter not defined', hint='Update configuration to include PDK_EXTERNAL_CONTENT_PUBLIC_KEY (hint: pdk_generate_key_pair).', obj=None, id='passive_data_kit_external_data.W001')
        errors.append(warning)

    return errors

def annotate_field(container, field_name=None, field_value=None):
    for app in settings.INSTALLED_APPS:
        try:
            pdk_external_api = importlib.import_module(app + '.pdk_external_api')

            pdk_external_api.pdk_annotate_field(container, field_name, field_value)
        except ImportError:
            pass
        except AttributeError:
            pass


def fetch_annotation_fields():
    fields = []

    for app in settings.INSTALLED_APPS:
        try:
            pdk_external_api = importlib.import_module(app + '.pdk_external_api')

            fields.extend(pdk_external_api.fetch_annotation_fields())
        except ImportError:
            pass
        except AttributeError:
            pass

    return fields


def fetch_annotations(properties):
    annotations = {}

    for app in settings.INSTALLED_APPS:
        try:
            pdk_external_api = importlib.import_module(app + '.pdk_external_api')

            annotations.update(pdk_external_api.fetch_annotations(properties))
        except ImportError:
            pass
        except AttributeError:
            pass

    return annotations


@python_2_unicode_compatible
class ExternalDataSource(models.Model):
    name = models.CharField(max_length=1024)
    identifier = models.SlugField(max_length=1024, unique=True)
    priority = models.IntegerField(default=0)

    configuration = models.TextField(max_length=(1024*1024), default='{}')

    export_url = models.URLField(null=True, blank=True)

    upload_extension = models.CharField(max_length=64, default='zip', null=True)

    def instruction_content(self):
        context = {'source': self}

        return render_to_string('sources/pdk_export_instructions_' + self.identifier + '.html', context=context)

    def __str__(self): # pylint: disable=invalid-str-returned
        return self.name

    def fetch_configuration(self):
        return json.loads(self.configuration)


@python_2_unicode_compatible
class ExternalDataRequest(models.Model):
    identifier = models.CharField(max_length=1024)
    email = models.CharField(max_length=1024)

    token = models.CharField(max_length=1024, null=True, blank=True)

    requested = models.DateTimeField()

    sources = models.ManyToManyField(ExternalDataSource, related_name='requests')

    extras = models.TextField(max_length=1048576, default='{}')

    last_emailed = models.DateTimeField(null=True, blank=True)
    can_email = models.BooleanField(default=True)

    def merge(self, request):
        if request.pk > self.pk:
            self.email = request.email
            self.token = request.token
            self.extras = request.extras

            self.can_email = request.can_email

            if self.last_emailed is None or (request.last_emailed is not None and self.last_emailed < request.last_emailed):
                self.last_emailed = request.last_emailed

            self.save()

        for source in request.sources.all():
            self.sources.add(source)

        for request_file in request.data_files.all():
            request_file.request = self
            request_file.save()

    def completed(self):
        if self.incomplete_sources():
            return False

        return True

    def incomplete_sources(self):
        incomplete = []

        for source in self.sources.all():
            if self.data_files.filter(source=source).count() == 0:
                incomplete.append(source)

        return incomplete

    def email_reminder(self):
        if self.can_email is False:
            return

        if self.completed():
            return

        if self.last_emailed is None or (self.last_emailed + settings.PDK_EMAIL_REMINDER_DURATION) < timezone.now():
            mail_context = {
                'upload_link': settings.SITE_URL + self.get_absolute_url(),
                'opt_out_link': settings.SITE_URL + self.get_opt_out_url(),
                'request': self
            }

            request_email_subject = render_to_string('email/pdk_external_request_data_reminder_email_subject.txt', context=mail_context)
            request_email = render_to_string('email/pdk_external_request_data_reminder_email.txt', context=mail_context)

            send_mail(request_email_subject, request_email, settings.AUTOMATED_EMAIL_FROM_ADDRESS, [self.email], fail_silently=False)

            self.last_emailed = timezone.now()
            self.save()

    def __str__(self): # pylint: disable=invalid-str-returned
        return self.identifier

    def request_files(self):
        files = []

        for source in self.sources.all().order_by('priority'):
            file_dict = {
                'identifier': source.identifier,
                'name': source.name,
                'extension': source.upload_extension
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

    def get_absolute_url(self):
        return reverse('pdk_external_upload_data', args=[self.token])

    def get_opt_out_url(self):
        return reverse('pdk_external_email_opt_out', args=[self.token])


class ExternalDataRequestFile(models.Model):
    request = models.ForeignKey(ExternalDataRequest, related_name='data_files', on_delete=models.CASCADE)
    source = models.ForeignKey(ExternalDataSource, related_name='data_files', on_delete=models.CASCADE)

    data_file = models.FileField(upload_to='pdk_external_files/')

    uploaded = models.DateTimeField()

    processed = models.DateTimeField(null=True, blank=True)
    skipped = models.DateTimeField(null=True, blank=True)

    def extension(self):
        return self.source.upload_extension

    def encrypted(self):
        if self.data_file is not None:
            if self.data_file.path.endswith('.encrypted'):
                return True
            elif self.data_file.path.endswith('.aes'):
                return True

        return None

    def process(self):
        if self.processed is not None:
            print('Already processed: ' + self.data_file.path)

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
                except: #pylint: disable=bare-except
                    traceback.print_exc()
                    file_processed = False

        if file_processed:
            self.processed = timezone.now()
            self.save()

        return file_processed
