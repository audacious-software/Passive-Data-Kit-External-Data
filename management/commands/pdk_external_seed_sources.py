# pylint: disable=no-member,line-too-long

from __future__ import print_function

from django.core.management.base import BaseCommand

from passive_data_kit.models import DataSource, DataSourceReference

from ...models import ExternalDataRequest

class Command(BaseCommand):
    help = 'Generates source records for data requests.'

    def add_arguments(self, parser):
        pass

    def handle(self, *args, **options): # pylint: disable=too-many-locals, too-many-branches, too-many-statements
        for request in ExternalDataRequest.objects.all():
            if request.identifier is not None and request.identifier != '' and DataSource.objects.filter(identifier=request.identifier).count() == 0:
                DataSource.objects.create(name=request.identifier, identifier=request.identifier)

                DataSourceReference.reference_for_source(request.identifier)

                print('Created %s.' % request.identifier)
