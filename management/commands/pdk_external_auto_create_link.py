# pylint: disable=no-member,line-too-long

from __future__ import print_function

from requests import Request

from django.conf import settings
from django.core.management.base import BaseCommand
from django.urls import reverse

from passive_data_kit.decorators import handle_lock

from ...utils import secret_encrypt_content

class Command(BaseCommand):
    help = 'Generates new data request link using private key.'

    def add_arguments(self, parser):
        parser.add_argument('identifier',
                            type=str,
                            help='Identifier of new data request')

        parser.add_argument('email',
                            type=str,
                            help='E-mail address of new data request')

        parser.add_argument('sites',
                            type=str,
                            help='Comma-separated list of data sources to enable for new data request')

    @handle_lock
    def handle(self, *args, **options): # pylint: disable=too-many-locals, too-many-branches, too-many-statements
        tokens_clear = '%s:%s:%s' % (options['identifier'], options['email'], options['sites'])

        enc_tokens = secret_encrypt_content(tokens_clear.encode('utf-8')).decode('utf-8')

        base_url = '%s%s' % (settings.SITE_URL, reverse('pdk_external_request_data'))

        params = {
            'auto': enc_tokens
        }

        request = Request('GET', base_url, params=params).prepare()

        print('Auto-creation link: %s' % request.url)
