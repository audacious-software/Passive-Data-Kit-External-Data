# pylint: disable=no-member,line-too-long

import base64
import getpass
import os

from nacl.public import SealedBox, PrivateKey

from django.core.files.base import ContentFile
from django.core.management.base import BaseCommand

from passive_data_kit.decorators import handle_lock

from ...models import ExternalDataRequestFile

class Command(BaseCommand):
    help = 'Changes the identifier of content added to the server.'

    def add_arguments(self, parser):
        parser.add_argument('old-identifier', type=str, nargs=1, help='Original identifier used to tag content')
        parser.add_argument('new-identifier', type=str, nargs=1, help='New identifier used to tag content')

    @handle_lock
    def handle(self, *args, **options): # pylint: disable=too-many-locals, too-many-branches, too-many-statements
    	old_id = options.get('old_identifier')
    	new_id = options.get('new_identifier')
