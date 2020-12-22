# pylint: disable=no-member,line-too-long

from __future__ import print_function

import os

from django.conf import settings
from django.core.files import File
from django.core.management.base import BaseCommand

from passive_data_kit.decorators import handle_lock

from ...models import ExternalDataRequestFile

class Command(BaseCommand):
    help = 'Decrypts content previously encrypted using server public key.'

    def add_arguments(self, parser):
        parser.add_argument('file_pk',
                            type=int,
                            help='PK of data file containing decrypted file')


    @handle_lock
    def handle(self, *args, **options): # pylint: disable=too-many-locals, too-many-branches, too-many-statements
        query = ExternalDataRequestFile.objects.filter(pk=options['file_pk'])

        for data_file in query.order_by('-pk'):
            original_path = data_file.data_file.path

            encrypted_path = data_file.data_file.path + '.aes-tmp'

            cmd_line = 'openssl enc -aes-256-cbc -in'.split(' ')

            cmd_line.append(original_path)
            cmd_line.append('-out')
            cmd_line.append(encrypted_path)
            cmd_line.append('-k')
            cmd_line.append(settings.PDK_EXTERNAL_CONTENT_SYMETRIC_KEY)

            os.system(' '.join(cmd_line))

            new_filename = os.path.basename(encrypted_path).replace('.aes-tmp', '.aes')

            data_file.data_file.save(new_filename, File(open(encrypted_path)))

            os.remove(original_path)
            os.remove(encrypted_path)
