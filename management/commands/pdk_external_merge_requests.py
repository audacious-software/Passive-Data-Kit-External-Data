# pylint: disable=no-member,line-too-long

import json

from django.core.management.base import BaseCommand

from passive_data_kit.decorators import handle_lock

from ...models import ExternalDataRequest

class Command(BaseCommand):
    help = 'Merges two data requests by specifying PK'

    def add_arguments(self, parser):
        parser.add_argument('source_pk',
                            type=int,
                            help='PK of the source to merge')

        parser.add_argument('destination_pk',
                            type=int,
                            help='PK of the final destination of the merge')

    @handle_lock
    def handle(self, *args, **options):
        source = ExternalDataRequest.objects.get(pk=options['source_pk'])
        destination = ExternalDataRequest.objects.get(pk=options['destination_pk'])

        for file_obj in source.data_files.all():
            file_obj.request = destination
            file_obj.save()

        for source_obj in source.sources.all():
            destination.sources.add(source_obj)

        dest_extras = json.loads(destination.extras)
        source_extras = json.loads(source.extras)

        for key in source_extras.keys():
            if (key in dest_extras) is False:
                dest_extras[key] = source_extras[key]

        destination.extras = json.dumps(dest_extras, indent=2)

        destination.save()

        source.delete()
