# pylint: disable=line-too-long, no-member

import copy
import json
import re
import zipfile

import arrow

from passive_data_kit.models import DataPoint

from ..models import annotate_field
from ..utils import encrypt_content, create_engagement_event, queue_batch_insert

def process_conversations(request_identifier, conversations_raw):
    conversations = json.loads(conversations_raw)

    for conversation in conversations: # pylint: disable=too-many-nested-blocks
        conversation_title = conversation.get('title', None)
        conversation_metadata = copy.deepcopy(conversation)

        del conversation_metadata['mapping']

        for msg_id, message in conversation.get('mapping', {}).items():
            message_obj = message.get('message', None)

            if message_obj is not None:
                data_point = {
                    'message_id': msg_id
                }

                if message_obj.get('content', {}).get('content_type', None) == 'text':
                    create_ts = message_obj.get('create_time', 0)

                    if create_ts is not None:
                        created = arrow.get(create_ts).datetime

                        content_text = ''

                        for part in message_obj.get('content', {}).get('parts', []):
                            if content_text == '':
                                content_text = part
                            else:
                                content_text = '%s\n%s' % (content_text, part)

                        author = message_obj.get('author', {}).get('role', 'unknown')

                        data_point['author'] = author
                        data_point['pdk_encrypted_conversation_title'] = encrypt_content(conversation_title.encode('utf-8'))
                        data_point['conversation_metadata'] = conversation_metadata

                        data_point['pdk_encrypted_content'] = encrypt_content(content_text.encode('utf-8'))

                        annotate_field(data_point, 'content', content_text)

                        if data_point['author'] == 'user':
                            create_engagement_event(source='chatgpt', identifier=request_identifier, outgoing_engagement=1.0, engagement_type='message', start=created)
                        elif data_point['author'] == 'system':
                            create_engagement_event(source='chatgpt', identifier=request_identifier, incoming_engagement=1.0, engagement_type='message', start=created)

                        queue_batch_insert(DataPoint.objects.create_data_point('pdk-external-chatgpt-message', request_identifier, data_point, user_agent='Passive Data Kit External Importer', created=created, skip_save=True, skip_extract_secondary_identifier=True))

    return True

def import_data(request_identifier, path): # pylint: disable=too-many-branches
    with zipfile.ZipFile(path) as content_bundle:
        for content_file in content_bundle.namelist():
            with content_bundle.open(content_file) as opened_file:
                if re.match(r'^conversations.json', content_file):
                    return process_conversations(request_identifier, opened_file.read())

    return True

def external_data_metadata(generator_identifier, point): # pylint: disable=unused-argument
    if generator_identifier.startswith('pdk-external-chatgpt') is False:
        return None

    metadata = {}
    metadata['service'] = 'Chat GPT'
    metadata['event'] = generator_identifier

    return metadata

def data_type_name(definition):
    for observed in definition['passive-data-metadata.generator-id']['observed']:
        if observed.startswith('pdk-external-engagement-'):
            return 'Chat GPT: Engagement Event'

        if observed == 'pdk-external-chatgpt-message':
            return 'Chat GPT: Message'

    return None

def data_type_category(identifier):
    if identifier.startswith('pdk-external-engagement-'):
        return 'Passive Data Kit: External Data'

    return 'Passive Data Kit: Chat GPT'
