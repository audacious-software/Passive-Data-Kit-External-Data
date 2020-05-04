# pylint: disable=line-too-long

import json
import re
import traceback
import zipfile

import arrow
import pytz

from passive_data_kit.models import DataPoint

from ..utils import hash_content, encrypt_content

def process_comments(request_identifier, comments_raw):
    comments = json.loads(comments_raw)

    if isinstance(comments, dict) is False:
        return

    for key in comments:
        comment_list = comments[key]

        for comment in comment_list:
            comment_point = {}

            created = arrow.get(comment[0]).replace(tzinfo=pytz.timezone('US/Pacific')).datetime

            comment_point['pdk_encrypted_comment'] = encrypt_content(comment[1].encode('utf-8'))
            comment_point['pdk_length_comment'] = len(comment[1])

            comment_point['pdk_hashed_profile'] = hash_content(comment[2])
            comment_point['pdk_encrypted_profile'] = encrypt_content(comment[2].encode('utf-8'))

            DataPoint.objects.create_data_point('pdk-external-instagram-comment', request_identifier, comment_point, user_agent='Passive Data Kit External Importer', created=created)

def process_media(request_identifier, media_raw):
    media = json.loads(media_raw)

    if 'photos' in media:
        for photo in media['photos']:
            created = arrow.get(photo['taken_at']).replace(tzinfo=pytz.timezone('US/Pacific')).datetime

            photo['pdk_encrypted_caption'] = encrypt_content(photo['caption'].encode('utf-8'))
            photo['pdk_length_caption'] = len(photo['caption'])

            del photo['caption']

            if 'location' in photo:
                photo['pdk_encrypted_location'] = encrypt_content(photo['location'].encode('utf-8'))
                photo['pdk_length_location'] = len(photo['location'])

                del photo['location']

            DataPoint.objects.create_data_point('pdk-external-instagram-photo', request_identifier, photo, user_agent='Passive Data Kit External Importer', created=created)

    if 'videos' in media:
        for video in media['videos']:
            created = arrow.get(video['taken_at']).replace(tzinfo=pytz.timezone('US/Pacific')).datetime

            video['pdk_encrypted_caption'] = encrypt_content(video['caption'].encode('utf-8'))
            video['pdk_length_caption'] = len(video['caption'])

            del video['caption']

            if 'location' in video:
                video['pdk_encrypted_location'] = encrypt_content(video['location'].encode('utf-8'))
                video['pdk_length_location'] = len(video['location'])

                del video['location']

            DataPoint.objects.create_data_point('pdk-external-instagram-video', request_identifier, video, user_agent='Passive Data Kit External Importer', created=created)

def import_data(request_identifier, path):
    content_bundle = zipfile.ZipFile(path)

    for content_file in content_bundle.namelist():
        try:
            if content_file.endswith('/'):
                pass
            elif re.match(r'^comments\.json', content_file):
                process_comments(request_identifier, content_bundle.open(content_file).read())
            elif re.match(r'^media\.json', content_file):
                process_media(request_identifier, content_bundle.open(content_file).read())
            else:
                print '[' + request_identifier + ']: Unable to process: ' + content_file
        except: # pylint: disable=bare-except
            traceback.print_exc()
            return False

    return True

def external_data_metadata(generator_identifier, point): # pylint: disable=unused-argument
    if generator_identifier.startswith('pdk-external-instagram') is False:
        return None

    metadata = {}
    metadata['service'] = 'Instagram'
    metadata['event'] = generator_identifier

    if generator_identifier == 'pdk-external-instagram-comment':
        metadata['event'] = 'Added Comment'
        metadata['direction'] = 'Outgoing'
        metadata['media_type'] = 'Text'
    elif generator_identifier == 'pdk-external-instagram-photo':
        metadata['event'] = 'Photo Upload'
        metadata['direction'] = 'Outgoing'
        metadata['media_type'] = 'Image'
    elif generator_identifier == 'pdk-external-instagram-video':
        metadata['event'] = 'Video Upload'
        metadata['direction'] = 'Outgoing'
        metadata['media_type'] = 'Video'

    return metadata
