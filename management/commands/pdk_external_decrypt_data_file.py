# pylint: disable=no-member,line-too-long

from __future__ import print_function

import base64
import gc
import getpass
import os
import traceback

from nacl.exceptions import CryptoError
from nacl.public import SealedBox, PrivateKey

from django.core.files.base import ContentFile
from django.core.management.base import BaseCommand

from passive_data_kit.decorators import handle_lock

from ...models import ExternalDataRequestFile

class Command(BaseCommand):
    help = 'Decrypts content previously encrypted using server public key.'

    def add_arguments(self, parser):
        parser.add_argument('file_pk',
                            type=int,
                            nargs='+',
                            help='PK of data file containing encrypted file')

        parser.add_argument('--key',
                            type=str,
                            dest='key',
                            required=False,
                            help='Base64-encoded private key corresponding to server\'s public key')

    @handle_lock
    def handle(self, *args, **options): # pylint: disable=too-many-locals, too-many-branches, too-many-statements
        if options['key'] is None:
            options['key'] = getpass.getpass('Enter private key: ')

        for file_pk in options['file_pk']:
            query = ExternalDataRequestFile.objects.filter(pk=file_pk)

            if options['file_pk'] < 0:
                query = ExternalDataRequestFile.objects.filter(pk__gte=(0 - file_pk))

            for data_file in query.exclude(processed=None, skipped=None).order_by('pk'):
                box = SealedBox(PrivateKey(base64.b64decode(options['key'])))

                original_path = data_file.data_file.path

                try:
                    with open(original_path, 'rb') as original_file:
                        print('Decrypting content file for ' + str(data_file.pk) + '...')

                        try:
                            cleartext = box.decrypt(original_file.read())

                            filename = os.path.basename(data_file.data_file.path).replace('.encrypted', '')

                            decrypted_file = ContentFile(cleartext)

                            data_file.data_file.save(filename, decrypted_file)

                            os.remove(original_path)

                            data_file.processed = None
                            data_file.skipped = None

                            data_file.save()

                            print('Decrypted content file for ' + str(data_file.pk) + '.')
                        except CryptoError:
                            print('Unable to decrypt ' + str(data_file.pk) + ' (CryptoError). Please investigate manually.')

                            if options['file_pk'] > 0:
                                traceback.print_exc()
                        except MemoryError:
                            print('Unable to decrypt ' + str(data_file.pk) + ' (MemoryError). Please investigate manually.')

                            if options['file_pk'] > 0:
                                traceback.print_exc()
                except IOError:
                    print('Unable to decrypt ' + str(data_file.pk) + ' (IOError). Please investigate manually.')

                    if options['file_pk'] > 0:
                        traceback.print_exc()

                gc.collect()
