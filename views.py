# pylint: disable=no-member, line-too-long
# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import base64
import io
import json
import os
import random
import urllib

from django.core.mail import send_mail
from django.conf import settings
from django.contrib.admin.views.decorators import staff_member_required
from django.http import HttpResponse, FileResponse
from django.shortcuts import render, redirect, get_object_or_404
from django.template.loader import render_to_string
from django.urls import reverse
from django.utils import timezone

from passive_data_kit.models import DataSource

from .models import ExternalDataSource, ExternalDataRequest, ExternalDataRequestFile
from .utils import secret_encrypt_content, secret_decrypt_content

def pdk_external_generate_identifier(request): # pylint: disable=invalid-name, unused-argument
    identifier = None

    try:
        settings.PDK_EXTERNAL_DATA_GENERATE_IDENTIFIER()
    except AttributeError:
        pass

    if identifier is None:
        valid_characters = '987654321'

        while identifier is None or DataSource.objects.filter(identifier=identifier).count() > 0:
            identifier = ''.join(random.SystemRandom().choice(valid_characters) for _ in range(8))

    response = {
        'identifier': identifier
    }

    DataSource(name=identifier, identifier=identifier).save()

    response = {
        'identifier': identifier
    }

    return HttpResponse(json.dumps(response, indent=2), content_type='application/json', status=200)

def pdk_external_request_data_help(request, source): # pylint: disable=too-many-branches
    context = {}

    context['source'] = ExternalDataSource.objects.get(identifier=source)

    token = request.GET.get('token', None)

    if token is not None:
        context['token'] = token

    return render(request, 'pdk_external_request_data_help.html', context=context)

def pdk_external_request_data(request, token=None): # pylint: disable=too-many-branches, too-many-statements, too-many-locals
    context = {}

    context['sources'] = ExternalDataSource.objects.all().order_by('priority')

    if request.method == 'GET':
        auto_create = request.GET.get('auto', None)

        if auto_create is None:
            if 'pending_sites' in request.session:
                del request.session['pending_sites']

            if 'identifier' in request.session:
                del request.session['identifier']

            if 'email' in request.session:
                del request.session['email']

            if 'extras' in request.session:
                del request.session['extras']

            if token is not None:
                cleartext = secret_decrypt_content(urllib.parse.unquote(token)).decode('utf-8')

                tokens = cleartext.split(':')

                request.session['identifier'] = tokens[0]
                request.session['email'] = tokens[1]

                if len(tokens) > 2:
                    request.session['extras'] = json.loads(base64.b64decode(tokens[2]).decode('utf-8'))

            return render(request, 'pdk_external_request_data_start.html', context=context)

        create_elements = secret_decrypt_content(auto_create)

        tokens = create_elements.decode().split(':', 3)

        if len(tokens) >= 3:
            email = tokens[0]
            identifier = tokens[1]
            sites = tokens[2]

            request.session['identifier'] = identifier
            request.session['email'] = email

            if len(tokens) >= 4:
                extras_str = tokens[3]

                try:
                    request.session['extras'] = json.loads(extras_str)
                except json.JSONDecodeError:
                    request.session['extras'] = {
                        'extras': extras_str
                    }

            data_request = ExternalDataRequest.objects.filter(identifier=identifier).first()

            if data_request is None:
                data_request = ExternalDataRequest(identifier=identifier, requested=timezone.now())

            data_request.email = email

            data_request.save()

            site_ids = sites.split(',')

            pending_sites = []
            pending_index = 0

            for site_id in site_ids:
                for source in context['sources']:
                    if source.identifier == site_id:
                        pending_sites.append({
                            'identifier': source.identifier,
                            'name': source.name,
                            'index': pending_index,
                        })

                        data_request.sources.add(source)

                        pending_index += 1

            request.session['pending_sites'] = pending_sites
            request.session['pending_index'] = 0

            if request.session['pending_index'] < len(pending_sites):
                for source in context['sources']:
                    if source.identifier == pending_sites[request.session['pending_index']]['identifier']:
                        request.session['pending_index'] += 1

                        context['source'] = source

                        return render(request, 'pdk_external_request_data_source.html', context=context)

            request.session['identifier'] = identifier
            request.session['email'] = email
    elif request.method == 'POST':
        data_request = None

        if 'identifier' in request.session:
            data_request = ExternalDataRequest.objects.filter(identifier=request.session['identifier']).first()

        identifier = request.session.get('identifier', None)

        if identifier is None:
            identifier = request.POST.get('identifier', 'missing-id')

        if data_request is None:
            data_request = ExternalDataRequest(identifier=identifier, requested=timezone.now())

        if 'extras' in request.session:
            for key in request.session['extras']:
                if key in request.POST:
                    request.session['extras'][key] = request.POST[key]
        else:
            request.session['extras'] = {}

            for key in request.POST:
                request.session['extras'][key] = request.POST[key]

            if 'step' in request.session['extras']:
                del request.session['extras']['step']

            if 'csrfmiddlewaretoken' in request.session['extras']:
                del request.session['extras']['csrfmiddlewaretoken']

            if 'identifier' in request.session['extras']:
                del request.session['extras']['identifier']

            if 'email' in request.session['extras']:
                del request.session['extras']['email']

            for source in context['sources']:
                if source.identifier in request.session['extras']:
                    del request.session['extras'][source.identifier]

        data_request.extras = json.dumps(request.session['extras'], indent=2)

        if ('pending_sites' in request.session) is False:
            request.session['identifier'] = request.POST['identifier']
            request.session['email'] = request.POST['email']

            pending_sites = []

            pending_index = 0

            for source in context['sources']:
                if source.identifier in request.POST:
                    pending_sites.append({
                        'identifier': source.identifier,
                        'name': source.name,
                        'index': pending_index,
                    })

                    pending_index += 1

            if len(pending_sites) == 0: # pylint: disable=len-as-condition
                return render(request, 'pdk_external_request_data_start.html', context=context)

            request.session['pending_sites'] = pending_sites
            request.session['pending_index'] = 0

        pending_sites = request.session['pending_sites']

        if request.session['pending_index'] < len(pending_sites):
            for source in context['sources']:
                if source.identifier == pending_sites[request.session['pending_index']]['identifier']:
                    request.session['pending_index'] += 1

                    context['source'] = source

                    return render(request, 'pdk_external_request_data_source.html', context=context)

        if data_request is None:
            data_request = ExternalDataRequest(identifier=request.session['identifier'], requested=timezone.now())
            data_request.email = request.session['email']

        try:
            data_request.can_email = settings.PDK_EXTERNAL_CAN_EMAIL_DEFAULT
        except AttributeError:
            pass

        data_request.save()

        data_request.generate_content_token()

        for pending_site in pending_sites:
            for source in context['sources']:
                if source.identifier == pending_site['identifier']:
                    data_request.sources.add(source)

        data_request.save()

        return redirect('pdk_external_upload_data', token=data_request.token)

    return render(request, 'pdk_external_request_data_start.html', context=context)


def pdk_external_upload_data(request, token):
    context = {
        'needs_upload': False
    }

    data_request = ExternalDataRequest.objects.filter(token=token).first()

    if data_request is not None: # pylint: disable=no-else-return
        if data_request.data_files.count() < data_request.sources.count():
            context['needs_upload'] = True

        context['data_request'] = data_request
        context['now_time'] = timezone.now()

        if request.method == 'POST':
            context['message'] = 'Unable to upload file successfully. Our staff is investigating the issue.'
            context['message_type'] = 'danger'

            for source in data_request.sources.all():
                if source.identifier in request.FILES:
                    for file_item in request.FILES.getlist(source.identifier):
                        request_file = ExternalDataRequestFile(request=data_request, source=source, uploaded=timezone.now())
                        request_file.data_file = file_item

                        request_file.save()

                        context['message'] = 'File uploaded and saved successfully.'
                        context['message_type'] = 'success'

        return render(request, 'pdk_external_request_data_upload.html', context=context)
    else:
        return redirect('pdk_external_request_data')

def pdk_external_email_opt_out(request, token):
    context = {}

    data_request = ExternalDataRequest.objects.filter(token=token).first()

    if data_request is not None:
        context['data_request'] = data_request

        if request.method == 'POST':
            if request.POST['token'] == token:
                data_request.can_email = False
                data_request.save()

    return render(request, 'pdk_external_request_data_opt_out_confirm.html', context=context)

@staff_member_required
def pdk_external_request(request):
    context = {}

    if request.method == 'POST':
        request.session['identifier'] = request.POST['identifier']
        request.session['email'] = request.POST['email']

        extras = dict(request.POST)
        del extras['identifier']
        del extras['email']
        del extras['csrfmiddlewaretoken']

        b64_extras = base64.b64encode(json.dumps(extras).encode('utf-8')).decode('ascii')

        token = secret_encrypt_content((request.POST['identifier'] + ':' + request.POST['email'] + ':' + b64_extras).encode('utf-8'))

        can_send = True

        try:
            can_send = settings.PDK_EXTERNAL_CAN_EMAIL_DEFAULT
        except AttributeError:
            pass

        if can_send:
            mail_context = {
                'requester_name': request.user.get_full_name(),
                'request_link': settings.SITE_URL + reverse('pdk_external_request_data_with_params', kwargs={'token': urllib.parse.quote(token)}),
                'request_extras': extras
            }

            context['request_email_subject'] = render_to_string('email/pdk_external_request_data_request_email_subject.txt', context=mail_context)
            context['request_email'] = render_to_string('email/pdk_external_request_data_request_email.txt', context=mail_context)

            send_mail(context['request_email_subject'], context['request_email'], settings.AUTOMATED_EMAIL_FROM_ADDRESS, [request.POST['email']], fail_silently=False)

    mail_context = {
        'requester_name': request.user.get_full_name(),
        'request_link': settings.SITE_URL + reverse('pdk_external_request_data_with_params', kwargs={'token': 'abc12345'})
    }

    context['request_email_subject'] = render_to_string('email/pdk_external_request_data_request_email_subject.txt', context=mail_context)
    context['request_email'] = render_to_string('email/pdk_external_request_data_request_email.txt', context=mail_context)

    return render(request, 'pdk_external_request_data_request.html', context=context)

def pdk_external_pending(request, identifier): # pylint: disable=unused-argument
    response = []

    data_request = ExternalDataRequest.objects.filter(identifier=identifier).first()

    if data_request is not None:
        for source in data_request.sources.all():
            data_file = data_request.data_files.filter(source=source).first()

            if data_file is None:
                response.append(source.identifier)

        try:
            settings.PDK_EXTERNAL_ADD_PENDING_ITEMS(response, data_request)
        except AttributeError:
            pass

    return HttpResponse(json.dumps(response, indent=2), content_type='application/json', status=200)

def pdk_external_uploads(request, identifier): # pylint: disable=unused-argument
    response = []

    data_request = ExternalDataRequest.objects.filter(identifier=identifier).first()

    if data_request is not None:
        for data_file in data_request.data_files.all().exclude(processed=None).order_by('uploaded'):
            response.append({
                'source': data_file.source.identifier,
                'uploaded': data_file.uploaded.isoformat()
            })

    return HttpResponse(json.dumps(response, indent=2), content_type='application/json', status=200)

def pdk_download_upload(request, file_id): # pylint: disable=unused-argument
    upload = get_object_or_404(ExternalDataRequestFile, pk=int(file_id))

    filename = upload.data_file.path

    response = FileResponse(io.open(filename, 'rb'), content_type='application/octet-stream') # pylint: disable=consider-using-with

    download_name = os.path.basename(upload.data_file.name)

    response['Content-Length'] = os.path.getsize(filename)
    response['Content-Disposition'] = 'attachment; filename=' + download_name

    return response
