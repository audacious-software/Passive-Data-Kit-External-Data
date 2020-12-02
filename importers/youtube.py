# pylint: disable=line-too-long

from __future__ import print_function

import json
import re
import traceback
import zipfile

import arrow

from passive_data_kit.models import DataPoint
from passive_data_kit_external_data.models import annotate_field

from ..utils import encrypt_content, create_engagement_event

def process_watch_history(request_identifier, file_json):
    watch_history = json.loads(file_json)

    for watch in watch_history:
        created = arrow.get(watch['time']).datetime

        annotate_field(watch, 'title', watch['title'])

        DataPoint.objects.create_data_point('pdk-external-youtube-watch', request_identifier, watch, user_agent='Passive Data Kit External Importer', created=created)

        create_engagement_event(source='youtube', identifier=request_identifier, engagement_level=0.5, engagement_type='watch', start=created)


def process_search_history(request_identifier, file_json):
    search_history = json.loads(file_json)

    for search in search_history:
        created = arrow.get(search['time']).datetime

        search['pdk_encrypted_title'] = encrypt_content(search['title'].encode('utf-8'))

        annotate_field(search['title'], 'title', search['title']['title'])

        del search['title']

        if 'titleUrl' in search:
            search['pdk_encrypted_titleUrl'] = encrypt_content(search['titleUrl'].encode('utf-8'))
            search['pdk_length_titleUrl'] = len(search['titleUrl'])

            del search['titleUrl']

        DataPoint.objects.create_data_point('pdk-external-youtube-search', request_identifier, search, user_agent='Passive Data Kit External Importer', created=created)

        create_engagement_event(source='youtube', identifier=request_identifier, engagement_level=1.0, engagement_type='search', start=created)


def process_uploads(request_identifier, file_json):
    uploads = json.loads(file_json)

    for upload in uploads:
        upload['pdk_encrypted_title'] = encrypt_content(upload['snippet']['title'].encode('utf-8'))

        annotate_field(upload, 'title', upload['snippet']['title'])

        created = arrow.get(upload['snippet']['publishedAt']).datetime

        if 'snippet' in upload:
            snippet_str = json.dumps(upload['snippet'], indent=2)
            upload['pdk_encrypted_snippet'] = encrypt_content(snippet_str.encode('utf-8'))

            del upload['snippet']

        if 'contentDetails' in upload:
            content_details_str = json.dumps(upload['contentDetails'], indent=2)
            upload['pdk_encrypted_contentDetails'] = encrypt_content(content_details_str.encode('utf-8'))

            del upload['contentDetails']

        DataPoint.objects.create_data_point('pdk-external-youtube-upload', request_identifier, upload, user_agent='Passive Data Kit External Importer', created=created)

        create_engagement_event(source='youtube', identifier=request_identifier, engagement_level=1.0, engagement_type='upload', start=created)


def process_likes(request_identifier, file_json):
    likes = json.loads(file_json)

    for like in likes:
        like['pdk_encrypted_title'] = encrypt_content(like['snippet']['title'].encode('utf-8'))
        like['pdk_length_title'] = len(like['snippet']['title'])

        annotate_field(like, 'title', like['snippet']['title'])

        created = arrow.get(like['snippet']['publishedAt']).datetime

        if 'snippet' in like:
            snippet_str = json.dumps(like['snippet'], indent=2)
            like['pdk_encrypted_snippet'] = encrypt_content(snippet_str.encode('utf-8'))

            del like['snippet']

        if 'contentDetails' in like:
            content_details_str = json.dumps(like['contentDetails'], indent=2)
            like['pdk_encrypted_contentDetails'] = encrypt_content(content_details_str.encode('utf-8'))

            del like['contentDetails']

        DataPoint.objects.create_data_point('pdk-external-youtube-like', request_identifier, like, user_agent='Passive Data Kit External Importer', created=created)

        create_engagement_event(source='youtube', identifier=request_identifier, engagement_level=0.5, engagement_type='reaction', start=created)


def import_data(request_identifier, path):
    content_bundle = zipfile.ZipFile(path)

    for content_file in content_bundle.namelist():
        try:
            if content_file.endswith('/'):
                pass
            elif re.match(r'^.*\/watch-history.json', content_file):
                process_watch_history(request_identifier, content_bundle.open(content_file).read())
            elif re.match(r'^.*\/search-history.json', content_file):
                process_search_history(request_identifier, content_bundle.open(content_file).read())
            elif re.match(r'^.*\/uploads.json', content_file):
                process_uploads(request_identifier, content_bundle.open(content_file).read())
            elif re.match(r'^.*\/likes.json', content_file):
                process_likes(request_identifier, content_bundle.open(content_file).read())
            else:
                print('[' + request_identifier + ']: Unable to process: ' + content_file)
        except: # pylint: disable=bare-except
            traceback.print_exc()
            return False

    return True

def external_data_metadata(generator_identifier, point): # pylint: disable=unused-argument
    if generator_identifier.startswith('pdk-external-youtube') is False:
        return None

    metadata = {}
    metadata['service'] = 'YouTube'
    metadata['event'] = generator_identifier

    if generator_identifier == 'pdk-external-youtube-watch':
        metadata['event'] = 'Watched Video'
        metadata['direction'] = 'Incoming'
        metadata['media_type'] = 'Video'
    elif generator_identifier == 'pdk-external-youtube-search':
        metadata['event'] = 'Search'
        metadata['direction'] = 'Incoming'
        metadata['media_type'] = 'Search'
    elif generator_identifier == 'pdk-external-youtube-upload':
        metadata['event'] = 'Video Upload'
        metadata['direction'] = 'Outgoing'
        metadata['media_type'] = 'Video'
    elif generator_identifier == 'pdk-external-youtube-like':
        metadata['event'] = 'Positive Reaction'
        metadata['direction'] = 'Outgoing'
        metadata['media_type'] = 'Reaction'

    return metadata
