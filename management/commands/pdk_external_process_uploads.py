# pylint: disable=no-member,line-too-long

from __future__ import print_function

import base64
import os

from nacl.public import SealedBox, PublicKey

from django.conf import settings
from django.core.files.base import ContentFile
from django.core.management.base import BaseCommand
from django.utils import timezone

from passive_data_kit.decorators import handle_lock
from ...models import ExternalDataRequestFile

class Command(BaseCommand):
    help = 'Process uploaded data files into Passive Data Kit.'

    def add_arguments(self, parser):
        parser.add_argument('--skip-encryption',
                            action='store_true',
                            dest='skip_encryption',
                            default=False,
                            help='Skips default encryption for uploaded files')

    @handle_lock
    def handle(self, *args, **options): # pylint: disable=too-many-locals, too-many-branches, too-many-statements
        for data_file in ExternalDataRequestFile.objects.filter(processed=None, skipped=None):
            print('Processing ' + str(data_file.data_file.path) + '...')

            if data_file.process() is False:
                print 'Unable to process ' + str(data_file.data_file.path) + '.'

                data_file.skipped = timezone.now()
                data_file.save()

            if options['skip_encryption'] is False:
                original_path = data_file.data_file.path

                box = SealedBox(PublicKey(base64.b64decode(settings.PDK_EXTERNAL_CONTENT_PUBLIC_KEY)))

                with open(data_file.data_file.path, 'rb') as original_file:
                    encrypted_str = box.encrypt(original_file.read())

                    filename = os.path.basename(data_file.data_file.path) + '.encrypted'

                    encrypted_file = ContentFile(encrypted_str)

                    data_file.data_file.save(filename, encrypted_file)

                os.remove(original_path)
