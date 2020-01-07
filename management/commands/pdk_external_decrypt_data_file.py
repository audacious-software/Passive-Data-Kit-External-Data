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
    help = 'Decrypts content previously encrypted using server public key.'

    def add_arguments(self, parser):
        parser.add_argument('--key',
                            type=str,
                            dest='key',
                            required=False,
                            help='Base64-encoded private key corresponding to server\'s public key')

        parser.add_argument('--pk',
                            type=int,
                            dest='file_pk',
                            required=True,
                            help='PK of data file containing encrypted file')

    @handle_lock
    def handle(self, *args, **options): # pylint: disable=too-many-locals, too-many-branches, too-many-statements
        if options['key'] is None:
            options['key'] = getpass.getpass('Enter private key: ')

        for data_file in ExternalDataRequestFile.objects.filter(pk=options['file_pk']):
            box = SealedBox(PrivateKey(base64.b64decode(options['key'])))

            original_path = data_file.data_file.path

            with open(original_path, 'rb') as original_file:

                cleartext = box.decrypt(original_file.read())

                filename = os.path.basename(data_file.data_file.path).replace('.encrypted', '')

                decrypted_file = ContentFile(cleartext)

                data_file.data_file.save(filename, decrypted_file)

                os.remove(original_path)
