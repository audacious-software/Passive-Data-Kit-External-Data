# pylint: disable=no-member,line-too-long

from __future__ import print_function

import base64
import os
import sys

from nacl.public import SealedBox, PublicKey

from django.conf import settings
from django.core.files.base import ContentFile
from django.core.management import call_command
from django.core.management.base import BaseCommand
from django.db.models import Q
from django.utils import timezone

from passive_data_kit.decorators import handle_lock
from passive_data_kit.models import DataGeneratorDefinition, DataPoint, install_supports_jsonfield

from ...models import ExternalDataRequestFile

class Command(BaseCommand):
    help = 'Process uploaded data files into Passive Data Kit.'

    def add_arguments(self, parser):
        pass

    @handle_lock
    def handle(self, *args, **options): # pylint: disable=too-many-locals, too-many-branches, too-many-statements
        query = None
        
        for definition in DataGeneratorDefinition.objects.filter(generator_identifier__startswith='pdk-external-engagement'):
            if query is None:
                query = Q(generator_definition=definition)
            else:
                query = query | Q(generator_definition=definition)
                
        points = DataPoint.objects.filter(query)
        
        points_count = points.count()

        print('COUNT: ' + str(points_count))

        index = 0
        
        while index < points_count:
            print('PROGRESS: ' + str(index) + ' / ' + str(points_count))
            
            for point in points.order_by('pk')[index:(index + 5000)]:
                properties = point.fetch_properties()
                
                if ('engagement_direction' in properties) is False:
                    changed = False
                    if properties['incoming_engagement'] > 0.3:
                        properties['engagement_direction'] = 'incoming'
                        
                        changed = True
                    elif properties['incoming_engagement'] > 0:
                        properties['engagement_direction'] = 'incoming'
                        properties['incoming_engagement'] = 0

                        changed = True
                    elif properties['outgoing_engagement'] > 0.3:
                        properties['engagement_direction'] = 'outgoing'

                        changed = True
                    elif properties['outgoing_engagement'] > 0:
                        properties['engagement_direction'] = 'incoming'
                        properties['outgoing_engagement'] = 0

                        changed = True
                        
                    if changed:
                        if install_supports_jsonfield():
                            point.properties = properties
                        else:
                            point.properties = json.dumps(properties, indent=2)
                            
                        point.fetch_secondary_identifier(skip_save=True, properties=properties)

                        point.save()
                        
            index += 5000                       
        
        
