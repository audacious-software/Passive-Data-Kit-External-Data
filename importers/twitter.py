# pylint: disable=line-too-long

import json
import re
import traceback
import zipfile

import arrow

from django.utils import timezone

from passive_data_kit.models import DataPoint

from ..utils import hash_content, encrypt_content

def process_likes(request_identifier, likes_raw):
    likes_raw = likes_raw.replace('window.YTD.like.part0 = ', '')

    likes = json.loads(likes_raw)

    for like in likes:
        pdk_like = {
            'pdk_hashed_tweetId': hash_content(like['like']['tweetId']),
            'pdk_encrypted_tweetId': encrypt_content(like['like']['tweetId'].encode('utf-8')),
            'pdk_encrypted_fullText': encrypt_content(like['like']['fullText'].encode('utf-8')),
            'pdk_length_fullText': len(like['like']['fullText']),
        }

        created = timezone.now() # No timestamp available in this file!

        DataPoint.objects.create_data_point('pdk-external-twitter-like', request_identifier, pdk_like, user_agent='Passive Data Kit External Importer', created=created)

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
            tweet['pdk_length_full_text'] = len(tweet['full_text'])

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
                'pdk_length_text': len(msg_data['text']),
                'id': msg_data['id'],
                'conversationId': conversation['dmConversation']['conversationId'],
                'createdAt': msg_data['createdAt']
            }

            if msg_data['mediaUrls']:
                media_urls_str = json.dumps(msg_data['mediaUrls'], indent=2)
                pdk_message['pdk_encrypted_mediaUrls'] = encrypt_content(media_urls_str.encode('utf-8'))

            created = arrow.get(msg_data['createdAt']).datetime

            DataPoint.objects.create_data_point('pdk-external-twitter-direct-message', request_identifier, pdk_message, user_agent='Passive Data Kit External Importer', created=created)


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
                print '[' + request_identifier + ']: Unable to process: ' + content_file
        except: # pylint: disable=bare-except
            traceback.print_exc()
            return False

    return True
