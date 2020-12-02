# pylint: disable=line-too-long

from __future__ import print_function

import json
import re
import traceback
import zipfile

import arrow

from django.utils import timezone

from passive_data_kit.models import DataPoint
from passive_data_kit_external_data.models import annotate_field

from ..utils import hash_content, encrypt_content, create_engagement_event

def process_likes(request_identifier, likes_raw):
    likes_raw = likes_raw.replace('window.YTD.like.part0 = ', '')

    likes = json.loads(likes_raw)

    for like in likes:
        pdk_like = {
            'pdk_hashed_tweetId': hash_content(like['like']['tweetId']),
            'pdk_encrypted_tweetId': encrypt_content(like['like']['tweetId'].encode('utf-8')),
            'pdk_encrypted_fullText': encrypt_content(like['like']['fullText'].encode('utf-8')),
        }

        annotate_field(pdk_like, 'fullText', like['like']['fullText'])

        created = timezone.now() # No timestamp available in this file!

        DataPoint.objects.create_data_point('pdk-external-twitter-like', request_identifier, pdk_like, user_agent='Passive Data Kit External Importer', created=created)

        create_engagement_event(source='twitter', identifier=request_identifier, engagement_level=0.5, engagement_type='reaction', start=created)


def process_tweets(request_identifier, tweets_raw):
    tweets_raw = tweets_raw.replace('window.YTD.tweet.part0 = ', '')

    tweets = json.loads(tweets_raw)

    for tweet in tweets:
        if 'tweet' in tweet:
            tweet = tweet['tweet']

        if 'id' in tweet:
            tweet['pdk_hashed_id'] = hash_content(tweet['id'])
            tweet['pdk_encrypted_id'] = encrypt_content(tweet['id'].encode('utf-8'))
            del tweet['id']

        if 'id_str' in tweet:
            tweet['pdk_hashed_id_str'] = hash_content(tweet['id_str'])
            tweet['pdk_encrypted_id_str'] = encrypt_content(tweet['id_str'].encode('utf-8'))
            del tweet['id_str']

        if 'full_text' in tweet:
            tweet['pdk_encrypted_full_text'] = encrypt_content(tweet['full_text'].encode('utf-8'))

            annotate_field(tweet, 'full_text', tweet['full_text'])

            del tweet['full_text']

        if 'entities' in tweet:
            entities_str = json.dumps(tweet['entities'], indent=2)
            tweet['pdk_encrypted_entities'] = encrypt_content(entities_str.encode('utf-8'))

            del tweet['entities']

        if 'urls' in tweet:
            urls_str = json.dumps(tweet['urls'], indent=2)
            tweet['pdk_encrypted_urls'] = urls_str(entities_str.encode('utf-8'))

            del tweet['urls']

        created = arrow.get(tweet['created_at'], 'ddd MMM DD HH:mm:ss Z YYYY').datetime

        DataPoint.objects.create_data_point('pdk-external-twitter-tweet', request_identifier, tweet, user_agent='Passive Data Kit External Importer', created=created)

        create_engagement_event(source='twitter', identifier=request_identifier, engagement_level=1.0, engagement_type='post', start=created)


def process_direct_messages(request_identifier, messages_raw):
    messages_raw = messages_raw.replace('window.YTD.direct_message.part0 = ', '')

    conversations = json.loads(messages_raw)

    for conversation in conversations:
        for message in conversation['dmConversation']['messages']:
            msg_data = message['messageCreate']

            pdk_message = {
                'pdk_hashed_recipientId': hash_content(msg_data['recipientId']),
                'pdk_encrypted_recipientId': encrypt_content(msg_data['recipientId'].encode('utf-8')),
                'pdk_hashed_senderId': hash_content(msg_data['senderId']),
                'pdk_encrypted_senderId': encrypt_content(msg_data['senderId'].encode('utf-8')),
                'pdk_encrypted_text': encrypt_content(msg_data['text'].encode('utf-8')),
                'id': msg_data['id'],
                'conversationId': conversation['dmConversation']['conversationId'],
                'createdAt': msg_data['createdAt']
            }

            annotate_field(pdk_message, 'text', msg_data['text'])

            if msg_data['mediaUrls']:
                media_urls_str = json.dumps(msg_data['mediaUrls'], indent=2)
                pdk_message['pdk_encrypted_mediaUrls'] = encrypt_content(media_urls_str.encode('utf-8'))

            created = arrow.get(msg_data['createdAt']).datetime

            DataPoint.objects.create_data_point('pdk-external-twitter-direct-message', request_identifier, pdk_message, user_agent='Passive Data Kit External Importer', created=created)

            create_engagement_event(source='twitter', identifier=request_identifier, engagement_level=1.0, engagement_type='message', start=created)


def import_data(request_identifier, path):
    content_bundle = zipfile.ZipFile(path)

    for content_file in content_bundle.namelist():
        try:
            if content_file.endswith('/'):
                pass
            elif re.match(r'^direct-message\.js', content_file):
                process_direct_messages(request_identifier, content_bundle.open(content_file).read())
            elif re.match(r'^like\.js', content_file):
                process_likes(request_identifier, content_bundle.open(content_file).read())
            elif re.match(r'^tweet\.js', content_file):
                process_tweets(request_identifier, content_bundle.open(content_file).read())
            else:
                print('[' + request_identifier + ']: Unable to process: ' + content_file)
        except: # pylint: disable=bare-except
            traceback.print_exc()
            return False

    return True

def external_data_metadata(generator_identifier, point):
    if generator_identifier.startswith('pdk-external-twitter') is False:
        return None

    metadata = {}
    metadata['service'] = 'Twitter'
    metadata['event'] = generator_identifier

    if generator_identifier == 'pdk-external-twitter-like':
        metadata['event'] = 'Positive Reaction'
        metadata['direction'] = 'Outgoing'
        metadata['media_type'] = 'Reaction'
    elif generator_identifier == 'pdk-external-twitter-tweet':
        metadata['event'] = 'Post Upload'
        metadata['direction'] = 'Outgoing'
        metadata['media_type'] = 'Text'

        properties = point.fetch_properties()

        if 'pdk_encrypted_urls' in properties:
            metadata['media_type'] = 'Link'
        elif 'pdk_encrypted_entities' in properties:
            metadata['media_type'] = 'Image / Video'
    elif generator_identifier == 'pdk-external-twitter-direct-message':
        metadata['event'] = 'Direct Message'
        metadata['direction'] = 'Outgoing'
        metadata['media_type'] = 'Text'

    return metadata
