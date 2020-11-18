# pylint: disable=no-member,line-too-long

from __future__ import print_function

from django.core.management.base import BaseCommand

from passive_data_kit.decorators import handle_lock
from ...models import ExternalDataRequest

class Command(BaseCommand):
    help = 'De-duplicates external data requests with identifiers.'

    def add_arguments(self, parser):
        pass

    @handle_lock
    def handle(self, *args, **options): # pylint: disable=too-many-locals, too-many-branches, too-many-statements
        identifiers = ExternalDataRequest.objects.order_by('identifier').distinct('identifier').values_list('identifier', flat=True)

        for identifier in identifiers:
            if ExternalDataRequest.objects.filter(identifier=identifier).count() > 1:
                print('De-duplicating request ' + identifier + '...')

                original_item = None

                delete_pks = []

                for request in ExternalDataRequest.objects.filter(identifier=identifier).order_by('pk'):
                    if original_item is None:
                        original_item = request
                    else:
                        original_item.merge(request)

                        delete_pks.append(request.pk)

                for request_pk in delete_pks:
                    ExternalDataRequest.objects.get(pk=request_pk).delete()
