# pylint: disable=no-member,line-too-long

from django.core.management.base import BaseCommand

from passive_data_kit.decorators import handle_lock
from ...models import ExternalDataRequest

class Command(BaseCommand):
    help = 'Sends data upload reminder e-mails'

    def add_arguments(self, parser):
        pass

    @handle_lock
    def handle(self, *args, **options): # pylint: disable=too-many-locals, too-many-branches, too-many-statements
        for request in ExternalDataRequest.objects.all():
            request.email_reminder()
