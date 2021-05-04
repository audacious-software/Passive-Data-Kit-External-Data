# pylint: disable=no-member,line-too-long

from __future__ import print_function

import base64
import getpass
import traceback

from nacl.exceptions import CryptoError
from nacl.public import SealedBox, PrivateKey

from django.core.management.base import BaseCommand

from passive_data_kit.decorators import handle_lock

class Command(BaseCommand):
    help = 'Decrypts content previously encrypted using server public key.'

    def add_arguments(self, parser):
        parser.add_argument('file',
                            type=str,
                            help='Path of encrypted file')

        parser.add_argument('--key',
                            type=str,
                            dest='key',
                            required=False,
                            help='Base64-encoded private key corresponding to server\'s public key')

    @handle_lock
    def handle(self, *args, **options): # pylint: disable=too-many-locals, too-many-branches, too-many-statements
        if options['key'] is None:
            options['key'] = getpass.getpass('Enter private key: ')

        box = SealedBox(PrivateKey(base64.b64decode(options['key'])))

        original_path = options['file']

        try:
            with open(original_path, 'rb') as original_file:
                print('Decrypting ' + str(original_path) + '...')

                try:
                    cleartext = box.decrypt(original_file.read())

                    filename = original_path.replace('.encrypted', '')

                    with open(filename, 'wb') as file_obj:
                        file_obj.write(cleartext)

                    print('Decrypted ' + str(original_path) + ' to ' + filename + '.')
                except CryptoError:
                    print('Unable to decrypt ' + str(original_path) + ' (CryptoError). Please investigate manually.')

                    if options['file_pk'] > 0:
                        traceback.print_exc()
                except MemoryError:
                    print('Unable to decrypt ' + str(original_path) + ' (MemoryError). Please investigate manually.')
        except IOError:
            traceback.print_exc()
            print('Unable to decrypt ' + str(original_path) + ' (IOError). Please investigate manually.')
