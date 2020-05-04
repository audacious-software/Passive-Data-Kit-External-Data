# pylint: disable=line-too-long, no-member

import codecs
import csv
import cStringIO
import importlib
import tempfile
import traceback

import arrow

from django.conf import settings

from passive_data_kit.models import DataSourceReference, DataPoint

from .models import ExternalDataSource


CUSTOM_GENERATORS = (
    'pdk-external-events'
)

# https://docs.python.org/2.7/library/csv.html#examples

class UnicodeWriter: # pylint: disable=old-style-class
    def __init__(self, file_output, dialect=csv.excel, encoding="utf-8-sig", **kwds):
        self.queue = cStringIO.StringIO()
        self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
        self.stream = file_output
        self.encoder = codecs.getincrementalencoder(encoding)()
    def writerow(self, row):
        '''writerow(unicode) -> None
        This function takes a Unicode string and encodes it to the output.
        '''

        encoded_row = []

        for value in row:
            if isinstance(value, str):
                encoded_row.append(value.encode("utf-8"))
            else:
                encoded_row.append(value)

        self.writer.writerow(encoded_row)
        data = self.queue.getvalue()
        data = data.decode("utf-8")
        data = self.encoder.encode(data)
        self.stream.write(data)
        self.queue.truncate(0)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)

def import_external_data(data_source, request_identifier, path):
    try:
        importer = importlib.import_module('passive_data_kit_external_data.importers.' + data_source)

        return importer.import_data(request_identifier, path)
    except ImportError:
        pass
    except AttributeError:
        pass

    return False

def compile_report(generator, sources, data_start=None, data_end=None, date_type='created'): # pylint: disable=too-many-locals, too-many-branches, too-many-statements, too-many-return-statements
    try:
        if (generator in CUSTOM_GENERATORS) is False:
            return None

        now = arrow.get()
        filename = tempfile.gettempdir() + '/pdk_export_' + str(now.timestamp) + str(now.microsecond / 1e6) + '.txt'

        if generator == 'pdk-external-events':
            with open(filename, 'w') as outfile:
                writer = UnicodeWriter(outfile, delimiter='\t')

                columns = [
                    'Request ID',
                    'Date Created',
                    'Date Recorded',
                    'Service',
                    'Event',
                    'Direction',
                    'Media Type',
                ]

                writer.writerow(columns)

                for source in sources: # pylint: disable=too-many-nested-blocks
                    source_reference = DataSourceReference.reference_for_source(source)

                    query = DataPoint.objects.filter(source_reference=source_reference)

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
                                columns.append(point.created.isoformat())
                                columns.append(point.recorded.isoformat())

                                metadata = None

                                for app in settings.INSTALLED_APPS:
                                    if metadata is None:
                                        try:
                                            pdk_api = importlib.import_module(app + '.pdk_api')

                                            try:
                                                metadata = pdk_api.external_data_metadata(point)

                                            except TypeError as exception:
                                                print 'Verify that ' + app + ' implements all external_data_metadata arguments!'
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

                                    if 'direction' in metadata:
                                        columns.append(metadata['direction'])
                                    else:
                                        columns.append('')

                                    if 'media_type' in metadata:
                                        columns.append(metadata['media_type'])
                                    else:
                                        columns.append('')
                                else:
                                    columns.append('')
                                    columns.append(point.generator_identifier)
                                    columns.append('')
                                    columns.append('')

                                writer.writerow(columns)

                                last_seen_created = point.created

                        point_index += 1000
            return filename
    except: # pylint: disable=bare-except
        traceback.print_exc()

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
