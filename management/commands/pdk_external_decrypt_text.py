# pylint: disable=no-member,line-too-long

from __future__ import print_function

import base64
import getpass

from six.moves import input

from nacl.public import Box, SealedBox, PrivateKey, PublicKey

from django.core.management.base import BaseCommand

from passive_data_kit.decorators import handle_lock

class Command(BaseCommand):
    help = 'Decrypts content previously encrypted using server public key.'

    def add_arguments(self, parser):
        parser.add_argument('--key',
                            type=str,
                            dest='key',
                            required=False,
                            help='Base64-encoded private key corresponding to server\'s public key')

        parser.add_argument('--public-key',
                            action='store_true',
                            required=False,
                            help='Flag to enable two-key decryption')

        parser.add_argument('--text',
                            type=str,
                            dest='text',
                            required=False,
                            help='Base64-encoded encrypted text')

    @handle_lock
    def handle(self, *args, **options): # pylint: disable=too-many-locals, too-many-branches, too-many-statements
        print(str(options))

        if options['key'] is None:
            options['key'] = getpass.getpass('Enter private key: ')

        if options['text'] is None:
            options['text'] = input('Enter encrypted text: ')

        if options['public_key']:
            options['public_key'] = getpass.getpass('Enter public key: ')

            box = Box(PrivateKey(base64.b64decode(options['key'])), PublicKey(base64.b64decode(options['public_key'])))

            cleartext = box.decrypt(base64.b64decode(options['text']))

            print(cleartext)
        else:
            box = SealedBox(PrivateKey(base64.b64decode(options['key'])))

            cleartext = box.decrypt(base64.b64decode(options['text']))

            print(cleartext)
