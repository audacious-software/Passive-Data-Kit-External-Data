# pylint: disable=no-member,line-too-long

from __future__ import print_function

import os

from django.conf import settings
from django.core.files import File
from django.core.management.base import BaseCommand

from passive_data_kit.decorators import handle_lock

from ...models import ExternalDataRequestFile

class Command(BaseCommand):
    help = 'Encrypts file using AES for larger files.'

    def add_arguments(self, parser):
        parser.add_argument('file_pk',
                            type=int,
                            help='PK of data file containing decrypted file')


    @handle_lock
    def handle(self, *args, **options): # pylint: disable=too-many-locals, too-many-branches, too-many-statements
        query = ExternalDataRequestFile.objects.filter(pk=options['file_pk'])

        for data_file in query.order_by('-pk'):
            original_path = data_file.data_file.path

            decrypted_path = data_file.data_file.path.replace('.aes', '') + '.tmp'

            cmd_line = 'openssl enc -d -aes-256-cbc -in'.split(' ')

            cmd_line.append(original_path)
            cmd_line.append('-out')
            cmd_line.append(decrypted_path)
            cmd_line.append('-k')
            cmd_line.append(settings.PDK_EXTERNAL_CONTENT_SYMETRIC_KEY)

            print(original_path)
            print(decrypted_path)

            os.system(' '.join(cmd_line)) # nosec

            new_filename = os.path.basename(decrypted_path).replace('.tmp', '')

            print(new_filename)

            data_file.data_file.save(new_filename, File(open(decrypted_path)))

            data_file.processed = None
            data_file.skipped = None
            data_file.save()

            os.remove(original_path)
            os.remove(decrypted_path)
