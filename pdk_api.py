# pylint: disable=line-too-long, no-member, no-else-return

from __future__ import print_function

import csv
import importlib
import io
import json
import pkgutil
import tempfile
import traceback

import arrow
import pytz

from django.conf import settings
from django.db.models import Q
from django.http import HttpResponse
from django.template.loader import render_to_string
from django.utils import timezone

from passive_data_kit.models import DataSourceReference, DataPoint, DataGeneratorDefinition, DataServerApiToken, DataServerAccessRequestPending

from .models import ExternalDataSource, ExternalDataRequest
from .utils import finish_batch_inserts


CUSTOM_GENERATORS = (
    'pdk-external-events'
    'pdk-external-engagement'
)

def import_external_data(data_source, request_identifier, path):
    try:
        importer = importlib.import_module('passive_data_kit_external_data.importers.' + data_source)

        succeeded = importer.import_data(request_identifier, path)

        finish_batch_inserts()

        return succeeded
    except ImportError:
        pass
    except AttributeError:
        pass

    return False

def compile_report(generator, sources, data_start=None, data_end=None, date_type='created'): # pylint: disable=too-many-locals, too-many-branches, too-many-statements, too-many-return-statements
    now = arrow.get()

    here_tz = pytz.timezone(settings.TIME_ZONE)

    filename = tempfile.gettempdir() + '/pdk_export_' + str(now.timestamp) + str(now.microsecond / 1e6) + '.txt'

    if generator == 'pdk-external-events':
        with io.open(filename, 'wb') as outfile:
            writer = csv.writer(outfile, delimiter='\t')

            columns = [
                'Record ID',
                'Creation Date',
                'Creation Time',
                'Creation Time Zone',
                'Creation Unix Timestamp',
                'Recorded Date',
                'Recorded Time',
                'Recorded Time Zone',
                'Recorded Unix Timestamp',
                'Service',
                'Event',
                'Media Type',
                'Direction',
                'Outgoing Engagement Score',
                'Incoming Engagement Score',
            ]

            annotations = []

            for app in settings.INSTALLED_APPS:
                try:
                    pdk_api = importlib.import_module(app + '.pdk_external_api')

                    try:
                        annotations.extend(pdk_api.fetch_annotation_fields())
                    except TypeError as exception:
                        print('Verify that ' + app + ' implements all external_data_metadata arguments!')
                        raise exception
                except ImportError:
                    pass
                except AttributeError:
                    pass

            columns.extend(annotations)

            writer.writerow(columns)

            for source in sources: # pylint: disable=too-many-nested-blocks
                source_reference = DataSourceReference.reference_for_source(source)

                query = DataPoint.objects.filter(source_reference=source_reference).exclude(generator_identifier__startswith='pdk-external-engagement-')

                if data_start is not None:
                    if date_type == 'recorded':
                        query = query.filter(recorded__gte=data_start)
                    else:
                        query = query.filter(created__gte=data_start)

                if data_end is not None:
                    if date_type == 'recorded':
                        query = query.filter(recorded__lte=data_end)
                    else:
                        query = query.filter(created__lte=data_end)

                point_count = query.count()

                point_index = 0

                last_seen_created = None

                while point_index < point_count:
                    points = query.order_by('created')[point_index:(point_index+1000)]

                    for point in points:
                        if point.created != last_seen_created:
                            columns = []

                            columns.append(source)

                            created = point.created.astimezone(here_tz)

                            columns.append(created.date().isoformat())
                            columns.append(created.time().strftime('%H:%M:%S'))
                            columns.append(settings.TIME_ZONE)
                            columns.append(created.strftime("%s"))

                            recorded = point.recorded.astimezone(here_tz)

                            columns.append(recorded.date().isoformat())
                            columns.append(recorded.time().strftime('%H:%M:%S'))
                            columns.append(settings.TIME_ZONE)
                            columns.append(recorded.strftime("%s"))

                            metadata = None

                            for app in settings.INSTALLED_APPS:
                                if metadata is None:
                                    try:
                                        pdk_api = importlib.import_module(app + '.pdk_api')

                                        try:
                                            metadata = pdk_api.external_data_metadata(point)

                                        except TypeError as exception:
                                            print('Verify that ' + app + ' implements all external_data_metadata arguments!')
                                            raise exception
                                    except ImportError:
                                        metadata = None
                                    except AttributeError:
                                        metadata = None

                            if metadata is not None:
                                if 'service' in metadata:
                                    columns.append(metadata['service'])
                                else:
                                    columns.append('')

                                if 'event' in metadata:
                                    columns.append(metadata['event'])
                                else:
                                    columns.append(point.generator_identifier)

                                if 'media_type' in metadata:
                                    columns.append(metadata['media_type'].lower())
                                else:
                                    columns.append('')
                            else:
                                columns.append('')
                                columns.append(point.generator_identifier)
                                columns.append('')
                                columns.append('')

                            engagement = DataPoint.objects.filter(source_reference=source_reference, created=point.created, generator_identifier__startswith='pdk-external-engagement-').first()

                            if engagement is not None:
                                engagement_metadata = engagement.fetch_properties()

                                if 'engagement_direction' in engagement_metadata:
                                    columns.append(engagement_metadata['engagement_direction'])
                                else:
                                    columns.append('')

                                if 'outgoing_engagement' in engagement_metadata:
                                    columns.append(str(engagement_metadata['outgoing_engagement']))
                                else:
                                    columns.append('')

                                if 'incoming_engagement' in engagement_metadata:
                                    columns.append(str(engagement_metadata['incoming_engagement']))
                                else:
                                    columns.append('')
                            else:
                                columns.append('')
                                columns.append('')

                            annotation_values = {}

                            for app in settings.INSTALLED_APPS:
                                try:
                                    pdk_api = importlib.import_module(app + '.pdk_external_api')

                                    try:
                                        annotation_values.update(pdk_api.fetch_annotations(point.fetch_properties()))
                                    except TypeError as exception:
                                        traceback.print_exc()
                                        print('Verify that ' + app + ' implements all external_data_metadata arguments!')
                                        raise exception
                                except ImportError:
                                    pass
                                except AttributeError:
                                    pass

                            for key in annotations:
                                if key.lower() in annotation_values:
                                    columns.append(str(annotation_values[key.lower()]))
                                else:
                                    columns.append('')

                            writer.writerow(columns)

                            last_seen_created = point.created

                    point_index += 1000

        return filename

    elif generator == 'pdk-external-engagement':
        with io.open(filename, 'wb') as outfile:
            definition_query = None

            for definition in DataGeneratorDefinition.objects.filter(name__startswith='pdk-external-engagement-'):
                if definition_query is None:
                    definition_query = Q(generator_definition=definition)
                else:
                    definition_query = definition_query | Q(generator_definition=definition) # pylint: disable=unsupported-binary-operation

            writer = csv.writer(outfile, delimiter='\t')

            columns = [
                'Record ID',
                'Creation Date',
                'Creation Time',
                'Creation Time Zone',
                'Creation Unix Timestamp',
                'Service',
                'Engagement Type',
                'Direction',
                'Outgoing Engagement',
                'Incoming Engagement',
            ]

            writer.writerow(columns)

            for source in sources: # pylint: disable=too-many-nested-blocks
                source_reference = DataSourceReference.reference_for_source(source)

                query = DataPoint.objects.filter(source_reference=source_reference).filter(definition_query)

                if data_start is not None:
                    if date_type == 'recorded':
                        query = query.filter(recorded__gte=data_start)
                    else:
                        query = query.filter(created__gte=data_start)

                if data_end is not None:
                    if date_type == 'recorded':
                        query = query.filter(recorded__lte=data_end)
                    else:
                        query = query.filter(created__lte=data_end)

                point_count = query.count()

                point_index = 0

                last_seen = None

                while point_index < point_count:
                    points = query.order_by('created')[point_index:(point_index+1000)]

                    for point in points:
                        if last_seen is None or point.created > last_seen:
                            columns = []

                            columns.append(source)

                            created = point.created.astimezone(here_tz)

                            columns.append(created.date().isoformat())
                            columns.append(created.time().strftime('%H:%M:%S'))
                            columns.append(settings.TIME_ZONE)
                            columns.append(created.strftime("%s"))

                            columns.append(point.generator_identifier.replace('pdk-external-engagement-', ''))

                            metadata = point.fetch_properties()

                            if 'type' in metadata:
                                columns.append(metadata['type'].lower())
                            else:
                                columns.append('')

                            if 'engagement_direction' in metadata:
                                columns.append(metadata['engagement_direction'])
                            else:
                                columns.append('')

                            if 'outgoing_engagement' in metadata:
                                columns.append(metadata['outgoing_engagement'])
                            else:
                                columns.append('')

                            if 'incoming_engagement' in metadata:
                                columns.append(metadata['incoming_engagement'])
                            else:
                                columns.append('')

                            writer.writerow(columns)

                            last_seen = point.created

                    point_index += 1000
        return filename

    try:
        generator_module = importlib.import_module('.generators.' + generator.replace('-', '_'), package='passive_data_kit_external_data')

        output_file = None

        try:
            output_file = generator_module.compile_report(generator, sources, data_start=data_start, data_end=data_end, date_type=date_type)
        except TypeError:
            print('TODO: Update ' + generator + '.compile_report to support data_start, data_end, and date_type parameters!')

            output_file = generator_module.compile_report(generator, sources)

        if output_file is not None:
            return output_file
    except ImportError:
        pass
    except AttributeError:
        pass

    return None

def external_data_metadata(point):
    metadata = None

    generator_identifier = point.generator_identifier

    for app in settings.INSTALLED_APPS:
        for source in ExternalDataSource.objects.all():
            if metadata is None:
                try:
                    importer = importlib.import_module(app + '.importers.' + source.identifier)

                    metadata = importer.external_data_metadata(generator_identifier, point)
                except ImportError:
                    pass
                except AttributeError:
                    pass

    return metadata

def pdk_custom_source_header(source): # pylint: disable=unused-argument
    context = {}

#    data_request = ExternalDataRequest.objects.filter(identifier=source).first()

#    if data_request is not None:
#        for external_source in data_request.sources.all():
#            engagement_identifier = 'pdk-external-engagement-' + external_source.identifier

    return render_to_string('pdk_external_source_header.html', context)

def compile_visualization(identifier, points, folder, source=None):
    try:
        generator_module = importlib.import_module('.generators.' + identifier.replace('-', '_'), package='passive_data_kit_external_data')

        try:
            generator_module.compile_visualization(identifier, points, folder, source)
        except TypeError:
            generator_module.compile_visualization(identifier, points, folder)
    except ImportError:
        pass
    except AttributeError:
        pass

def visualization(source, generator):
    try:
        generator_module = importlib.import_module('.generators.' + generator.replace('-', '_'), package='passive_data_kit_external_data')

        output = generator_module.visualization(source, generator)

        if output is not None:
            return output
    except ImportError:
        traceback.print_exc()
        # pass
    except AttributeError:
        traceback.print_exc()
        # pass

    context = {}
    context['source'] = source
    context['generator_identifier'] = generator

    rows = []

    for point in DataPoint.objects.filter(source=source.identifier, generator_identifier=generator).order_by('-created')[:1000]:
        row = {}

        row['created'] = point.created
        row['value'] = '-'

        rows.append(row)

    context['table_rows'] = rows

    return render_to_string('pdk_generic_viz_template.html', context)

def update_data_type_definition(definition): # pylint: disable=too-many-branches
    for observed in definition['passive-data-metadata.generator-id']['observed']:
        if observed.startswith('pdk-external-'):
            tokens = observed.split('-')

            if len(tokens) > 2:
                for app in settings.INSTALLED_APPS:
                    try:
                        importer = importlib.import_module(app + '.importers.' + tokens[2])

                        importer.update_data_type_definition(definition)
                    except ImportError:
                        pass
                    except AttributeError:
                        pass

        if observed.startswith('pdk-external-engagement-'):
            importer = importlib.import_module('passive_data_kit_external_data.importers.engagement')

            importer.update_data_type_definition(definition)

    for app in settings.INSTALLED_APPS:
        try:
            annotators = importlib.import_module(app + '.annotators')

            prefix = annotators.__name__ + '.'

            for importer, modname, ispkg in pkgutil.iter_modules(annotators.__path__, prefix): # pylint: disable=unused-variable
                module = __import__(modname, fromlist='dummy')

                module.update_data_type_definition(definition)
        except ImportError:
            pass
        except AttributeError:
            pass

    # Strip out any remaining encrypted or hashed content

    for key in definition.keys():
        if 'pdk_encrypted_' in key or 'pdk_hashed_' in key:
            if 'observed' in definition[key]:
                del definition[key]['observed']

def pdk_data_point_query(request): # pylint: disable=too-many-locals, too-many-branches, too-many-statements
    if request.method == 'POST': # pylint: disable=too-many-nested-blocks
        page_size = int(request.POST['page_size'])
        page_index = int(request.POST['page_index'])

        filters = json.loads(request.POST['filters'])
        excludes = json.loads(request.POST['excludes'])
        order_bys = json.loads(request.POST['order_by'])

        found = False

        for filter_obj in filters:
            for field, value in list(filter_obj.items()):
                if value is not None:
                    if field == 'generator_identifier' and value == 'pdk-external-data-request':
                        found = True

        if found:
            query = ExternalDataRequest.objects.all()

            for filter_obj in filters:
                processed_filter = {}

                for field, value in list(filter_obj.items()):
                    if field.startswith('created'):
                        field = field.replace('created', 'requested')
                    elif field.startswith('recorded'):
                        field = field.replace('recorded', 'requested')
                    elif field == 'source':
                        field = 'identifier'

                    if value is not None:
                        if field.startswith('requested'):
                            value = arrow.get(value).datetime

                    if (field in ('generator_identifier',)) is False:
                        processed_filter[field] = value

                print('INC: %s' % processed_filter)

                query = query.filter(**processed_filter)

            for exclude in excludes:
                processed_exclude = {}

                for field, value in list(exclude.items()):
                    if field.startswith('created'):
                        field = field.replace('created', 'requested')
                    elif field.startswith('recorded'):
                        field = field.replace('recorded', 'requested')
                    elif field == 'source':
                        field = 'identifier'

                    if value is not None:
                        if field.startswith('requested'):
                            value = arrow.get(value).datetime

                    if (field in ('generator_identifier',)) is False:
                        processed_exclude[field] = value

                print('EXC: %s' % processed_filter)

                query = query.exclude(**processed_exclude)

            latest = query.order_by('-requested').first()

            if latest is not None:
                latest = latest.pk

            payload = {
                'latest': latest,
                'count': query.count(),
                'page_index': page_index,
                'page_size': page_size,
            }

            processed_order_by = []

            for order_by in order_bys:
                for item in order_by:
                    processed_order_by.append(item)

            if processed_order_by:
                query = query.order_by(*processed_order_by)

            matches = []

            if payload['count'] > 0:
                for item in query[(page_index * page_size):((page_index + 1) * page_size)]:
                    properties = {
                        'identifier': item.identifier,
                        'extras': json.loads(item.extras),
                        'data_sources': [],
                        'passive-data-metadata': {},
                    }

                    for source in item.sources.all():
                        source_info = {
                            'name': source.name,
                            'identifier': source.identifier
                        }

                        properties['data_sources'].append(source_info)

                    properties['passive-data-metadata']['pdk_server_created'] = arrow.get(item.requested).timestamp()
                    properties['passive-data-metadata']['pdk_server_recorded'] = arrow.get(item.requested).timestamp()

                    matches.append(properties)

            payload['matches'] = matches

            token = DataServerApiToken.objects.filter(token=request.POST['token']).first()

            access_request = DataServerAccessRequestPending()

            if token is not None:
                access_request.user_identifier = str(token.user.pk) + ': ' + str(token.user.username)
            else:
                access_request.user_identifier = 'api_token: ' + request.POST['token']

            access_request.request_type = 'api-data-points-request'
            access_request.request_time = timezone.now()
            access_request.request_metadata = json.dumps(request.POST, indent=2)
            access_request.successful = True
            access_request.save()

            return HttpResponse(json.dumps(payload, indent=2), content_type='application/json')

    return None
