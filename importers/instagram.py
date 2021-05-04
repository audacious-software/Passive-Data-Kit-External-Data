# pylint: disable=line-too-long

from __future__ import print_function

import json
import re
import traceback
import zipfile

import arrow
import pytz

from passive_data_kit.models import DataPoint

from passive_data_kit_external_data.models import annotate_field

from ..utils import hash_content, encrypt_content, create_engagement_event

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

            annotate_field(comment_point, 'comment', comment[1])

            comment_point['pdk_hashed_profile'] = hash_content(comment[2])
            comment_point['pdk_encrypted_profile'] = encrypt_content(comment[2].encode('utf-8'))

            DataPoint.objects.create_data_point('pdk-external-instagram-comment', request_identifier, comment_point, user_agent='Passive Data Kit External Importer', created=created)

            create_engagement_event(source='instagram', identifier=request_identifier, outgoing_engagement=1.0, engagement_type='comment', start=created)

def process_media(request_identifier, media_raw):
    media = json.loads(media_raw)

    if 'photos' in media:
        for photo in media['photos']:
            created = arrow.get(photo['taken_at']).replace(tzinfo=pytz.timezone('US/Pacific')).datetime

            photo['pdk_encrypted_caption'] = encrypt_content(photo['caption'].encode('utf-8'))

            annotate_field(photo, 'caption', photo['caption'])

            del photo['caption']

            if 'location' in photo:
                photo['pdk_encrypted_location'] = encrypt_content(photo['location'].encode('utf-8'))

                annotate_field(photo, 'location', photo['location'])

                del photo['location']

            DataPoint.objects.create_data_point('pdk-external-instagram-photo', request_identifier, photo, user_agent='Passive Data Kit External Importer', created=created)

            create_engagement_event(source='instagram', identifier=request_identifier, outgoing_engagement=1.0, engagement_type='photo', start=created)

    if 'videos' in media:
        for video in media['videos']:
            created = arrow.get(video['taken_at']).replace(tzinfo=pytz.timezone('US/Pacific')).datetime

            video['pdk_encrypted_caption'] = encrypt_content(video['caption'].encode('utf-8'))

            annotate_field(video, 'caption', video['caption'])

            del video['caption']

            if 'location' in video:
                video['pdk_encrypted_location'] = encrypt_content(video['location'].encode('utf-8'))

                annotate_field(video, 'location', video['location'])

                del video['location']

            DataPoint.objects.create_data_point('pdk-external-instagram-video', request_identifier, video, user_agent='Passive Data Kit External Importer', created=created)

            create_engagement_event(source='instagram', identifier=request_identifier, outgoing_engagement=1.0, engagement_type='video', start=created)

def process_likes(request_identifier, likes_raw):
    likes = json.loads(likes_raw)

    if isinstance(likes, dict) is False:
        return

    keys = ['media', 'comment']

    for key in keys:
        likes_list = likes[key + '_likes']

        for like in likes_list:
            created = arrow.get(like[0]).datetime

            reaction = {
                'timestamp': like[0],
                'pdk_hashed_target': hash_content(like[1].encode('utf-8'))
            }

            reaction['content_type'] = key
            reaction['reaction'] = 'like'

            DataPoint.objects.create_data_point('pdk-external-instagram-reaction', request_identifier, reaction, user_agent='Passive Data Kit External Importer', created=created)

            create_engagement_event(source='instagram', identifier=request_identifier, outgoing_engagement=0.5, engagement_type='reaction', start=created)

def process_messages(request_identifier, username, messages_raw):
    conversations = json.loads(messages_raw)

    if isinstance(conversations, list) is False:
        return

    for conversation in conversations:
        hashed_participants = []

        for participant in conversation['participants']:
            if participant != username:
                hashed_participants = hash_content(participant)

        for message in conversation['conversation']:
            pdk_message = {
                'pdk_recipients_count': len(conversation['participants']) - 1,
                'pdk_hashed_senderId': hash_content(message['sender']),
                'pdk_encrypted_sender': encrypt_content(message['sender'].encode('utf-8')),
                'pdk_hashed_participants': hashed_participants,
                'created_at': message['created_at']
            }

            if 'text' in message and message['text'] is not None:
                annotate_field(pdk_message, 'text', message['text'])
                pdk_message['pdk_encrypted_text'] = encrypt_content(message['text'].encode('utf-8'))

            if 'media_url' in message:
                pdk_message['pdk_encrypted_media_url'] = encrypt_content(message['media_url'].encode('utf-8'))

            created = arrow.get(message['created_at']).datetime

            DataPoint.objects.create_data_point('pdk-external-instagram-direct-message', request_identifier, pdk_message, user_agent='Passive Data Kit External Importer', created=created)

            if message['sender'] == username:
                create_engagement_event(source='twitter', identifier=request_identifier, outgoing_engagement=1.0, engagement_type='message', start=created)
            else:
                create_engagement_event(source='twitter', identifier=request_identifier, incoming_engagement=1.0, engagement_type='message', start=created)

def process_seen_content(request_identifier, seen_raw):
    seen = json.loads(seen_raw)

    if isinstance(seen, dict) is False:
        return

    for item_seen in seen['chaining_seen']:
        created = arrow.get(item_seen['timestamp']).datetime

        reaction = {
            'timestamp': item_seen['timestamp'],
            'pdk_hashed_target': hash_content(item_seen['username'].encode('utf-8'))
        }

        DataPoint.objects.create_data_point('pdk-external-instagram-page-visit', request_identifier, reaction, user_agent='Passive Data Kit External Importer', created=created)

        create_engagement_event(source='instagram', identifier=request_identifier, outgoing_engagement=0.5, engagement_type='page', start=created)

    if 'ads_clicked' in seen:
        for item_seen in seen['ads_clicked']:
            created = arrow.get(item_seen['timestamp']).datetime

            ad_clicked = {
                'timestamp': item_seen['timestamp'],
                'pdk_hashed_target': hash_content(item_seen['caption'].encode('utf-8'))
            }

            DataPoint.objects.create_data_point('pdk-external-instagram-ad-clicked', request_identifier, ad_clicked, user_agent='Passive Data Kit External Importer', created=created)

            create_engagement_event(source='instagram', identifier=request_identifier, outgoing_engagement=0.5, engagement_type='advertising', start=created)

def process_searches(request_identifier, searches_raw):
    searches = json.loads(searches_raw)

    if isinstance(searches, dict) is False:
        return

    for search in searches['main_search_history']:
        created = arrow.get(search['time']).datetime

        search_click = {
            'timestamp': search['time'],
            'pdk_hashed_target': hash_content(search['search_click'].encode('utf-8'))
        }

        DataPoint.objects.create_data_point('pdk-external-instagram-search-click', request_identifier, search_click, user_agent='Passive Data Kit External Importer', created=created)

        create_engagement_event(source='instagram', identifier=request_identifier, outgoing_engagement=0.5, engagement_type='search', start=created)

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
            elif re.match(r'^likes\.json', content_file):
                process_likes(request_identifier, content_bundle.open(content_file).read())
            elif re.match(r'^seen_content\.json', content_file):
                process_seen_content(request_identifier, content_bundle.open(content_file).read())
            elif re.match(r'^searches\.json', content_file):
                process_searches(request_identifier, content_bundle.open(content_file).read())
            elif re.match(r'^messages\.json', content_file):
                profile_json = json.loads(content_bundle.open('profile.json').read())

                username = profile_json['username']

                process_messages(request_identifier, username, content_bundle.open(content_file).read())
            else:
                print('[' + request_identifier + ']: Unable to process: ' + content_file)
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
