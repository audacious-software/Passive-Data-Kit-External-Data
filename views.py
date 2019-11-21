# pylint: disable=no-member, line-too-long
# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import json
import random

from django.conf import settings
from django.http import HttpResponse
from django.shortcuts import render, redirect
from django.utils import timezone

from passive_data_kit.models import DataSource

from .models import ExternalDataSource, ExternalDataRequest, ExternalDataRequestFile


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

    return render(request, 'pdk_external_request_data_help.html', context=context)


def pdk_external_request_data(request): # pylint: disable=too-many-branches
    context = {}

    context['sources'] = ExternalDataSource.objects.all().order_by('priority')

    if request.method == 'GET':
        if 'pending_sites' in request.session:
            del request.session['pending_sites']

        if 'identifier' in request.session:
            del request.session['identifier']

        if 'email' in request.session:
            del request.session['email']

        return render(request, 'pdk_external_request_data_start.html', context=context)

    elif request.method == 'POST':
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

        data_request = ExternalDataRequest(email=request.session['email'], identifier=request.session['identifier'], requested=timezone.now())

        data_request.save()

        data_request.generate_content_token()

        for pending_site in pending_sites:
            for source in context['sources']:
                if source.identifier == pending_site['identifier']:
                    data_request.sources.add(source)

        data_request.save()

        return render(request, 'pdk_external_request_data_finished.html', context=context)

    return render(request, 'pdk_external_request_data_start.html', context=context)


def pdk_external_upload_data(request, token):
    context = {}

    data_request = ExternalDataRequest.objects.filter(token=token).first()

    if data_request is not None:
        context['data_request'] = data_request

        if request.method == 'POST':
            for source in data_request.sources.all():
                if source.identifier in request.FILES:
                    request_file = ExternalDataRequestFile(request=data_request, source=source, uploaded=timezone.now())
                    request_file.data_file = request.FILES[source.identifier]

                    request_file.save()

        return render(request, 'pdk_external_request_data_upload.html', context=context)
    else:
        return redirect('pdk_external_request_data')
    