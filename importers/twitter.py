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

from ..utils import hash_content, encrypt_content, create_engagement_event, include_data

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

        create_engagement_event(source='twitter', identifier=request_identifier, outgoing_engagement=0.5, engagement_type='reaction', start=created)


def process_tweets(request_identifier, tweets_raw):
    tweets_raw = tweets_raw.replace('window.YTD.tweet.part0 = ', '')

    tweets = json.loads(tweets_raw)

    for tweet in tweets:
        if 'tweet' in tweet:
            tweet = tweet['tweet']

        created = arrow.get(tweet['created_at'], 'ddd MMM DD HH:mm:ss Z YYYY').datetime

        if include_data(request_identifier, created, tweet):
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

            DataPoint.objects.create_data_point('pdk-external-twitter-tweet', request_identifier, tweet, user_agent='Passive Data Kit External Importer', created=created)

            create_engagement_event(source='twitter', identifier=request_identifier, outgoing_engagement=1.0, engagement_type='post', start=created)

def process_direct_messages(request_identifier, messages_raw): # pylint: disable=too-many-branches
    messages_raw = messages_raw.replace('window.YTD.direct_message.part0 = ', '')
    messages_raw = messages_raw.replace('window.YTD.direct_messages.part0 = ', '')

    conversations = json.loads(messages_raw)

    my_ids = []

    for conversation in conversations:
        if len(my_ids) != 1:
            tokens = conversation['dmConversation']['conversationId'].split('-')

            if len(my_ids) == 0: # pylint: disable=len-as-condition
                my_ids = tokens
            else:
                my_ids = list(set().union(my_ids, tokens))

    if len(my_ids) > 0: # pylint: disable=len-as-condition, too-many-nested-blocks
        my_id = my_ids[0]

        for conversation in conversations:
            for message in conversation['dmConversation']['messages']:
                if 'messageCreate' in message:
                    msg_data = message['messageCreate']

                    created = arrow.get(msg_data['createdAt']).datetime

                    if include_data(request_identifier, created, msg_data):
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


                        DataPoint.objects.create_data_point('pdk-external-twitter-direct-message', request_identifier, pdk_message, user_agent='Passive Data Kit External Importer', created=created)

                        if my_id == msg_data['senderId']:
                            create_engagement_event(source='twitter', identifier=request_identifier, outgoing_engagement=1.0, engagement_type='message', start=created)
                        else:
                            create_engagement_event(source='twitter', identifier=request_identifier, incoming_engagement=1.0, engagement_type='message', start=created)
                elif 'reactionCreate' in message:
                    msg_data = message['reactionCreate']

                    created = arrow.get(msg_data['createdAt']).datetime

                    if include_data(request_identifier, created, msg_data):
                        pdk_message = {
                            'pdk_hashed_senderId': hash_content(msg_data['senderId']),
                            'pdk_encrypted_senderId': encrypt_content(msg_data['senderId'].encode('utf-8')),
                            'eventId': msg_data['eventId'],
                            'reactionKey': msg_data['reactionKey'],
                            'createdAt': msg_data['createdAt']
                        }

                        DataPoint.objects.create_data_point('pdk-external-twitter-direct-message-reaction', request_identifier, pdk_message, user_agent='Passive Data Kit External Importer', created=created)

                        if my_id == msg_data['senderId']:
                            create_engagement_event(source='twitter', identifier=request_identifier, outgoing_engagement=0.5, engagement_type='reaction', start=created)
                        else:
                            create_engagement_event(source='twitter', identifier=request_identifier, incoming_engagement=0.5, engagement_type='reaction', start=created)
                else:
                    print('TWITTER/MSG: ' + json.dumps(message, indent=2))

def process_ad_impressions(request_identifier, ads_raw):
    ads_raw = ads_raw.replace('window.YTD.ad_impressions.part0 = ', '')

    ads = json.loads(ads_raw)

    for ad_view in ads:
        for impression in ad_view['ad']['adsUserData']['adImpressions']['impressions']:
            created = arrow.get(impression['impressionTime']).datetime

            if include_data(request_identifier, created, impression):
                if 'promotedTweetInfo' in impression:
                    annotate_field(impression, 'tweet_text', impression['promotedTweetInfo']['tweetText'])

                DataPoint.objects.create_data_point('pdk-external-twitter-ad-viewed', request_identifier, impression, user_agent='Passive Data Kit External Importer', created=created)

                create_engagement_event(source='twitter', identifier=request_identifier, outgoing_engagement=0.0, engagement_type='advertising', start=created)

def process_ad_engagements(request_identifier, ads_raw):
    ads_raw = ads_raw.replace('window.YTD.ad_engagements.part0 = ', '')

    ads = json.loads(ads_raw)

    for ad_view in ads:
        for engagement in ad_view['ad']['adsUserData']['adEngagements']['engagements']:
            for engagement_attribute in engagement['engagementAttributes']:
                created = arrow.get(engagement_attribute['engagementTime']).datetime

                if include_data(request_identifier, created, engagement_attribute):
                    payload = engagement['impressionAttributes']
                    payload['engagementAttribute'] = engagement_attribute

                    DataPoint.objects.create_data_point('pdk-external-twitter-ad-engagement', request_identifier, payload, user_agent='Passive Data Kit External Importer', created=created)

                    create_engagement_event(source='twitter', identifier=request_identifier, outgoing_engagement=0.0, engagement_type='advertising', start=created)

def import_data(request_identifier, path): # pylint: disable=too-many-branches
    with zipfile.ZipFile(path) as content_bundle:
        skip_files = [
            'data/manifest.js',
            'data/account-creation-ip.js',
            'data/account-suspension.js',
            'data/account-timezone.js',
            'data/account.js',
            'data/ad-mobile-conversions-attributed.js',
            'data/ad-mobile-conversions-unattributed.js',
            'data/ad-online-conversions-attributed.js',
            'data/ad-online-conversions-unattributed.js',
            'data/ageinfo.js',
            'data/block.js',
            'data/branch-links.js',
            'data/connected-application.js',
            'data/contact.js',
            'data/device-token.js',
            'data/direct-message-group-headers.js',
            'data/direct-message-headers.js',
            'data/direct-messages-group.js',
            'data/email-address-change.js',
            'data/follower.js',
            'data/following.js',
            'data/ip-audit.js',
            'data/like.js',
            'data/moment.js',
            'data/mute.js',
            'data/ni-devices.js',
            'data/periscope-account-information.js',
            'data/periscope-ban-information.js',
            'data/periscope-broadcast-metadata.js',
            'data/periscope-profile-description.js',
            'data/personalization.js',
            'data/phone-number.js',
            'data/profile.js',
            'data/saved-search.js',
            'data/screen-name-change.js',
            'data/verified.js',
        ]

        for content_file in content_bundle.namelist():
            with content_bundle.open(content_file) as opened_file:
                try:
                    if content_file.endswith('/'):
                        pass
                    elif content_file in skip_files:
                        pass
                    elif 'assets/' in content_file:
                        pass
                    elif '.i18n.' in content_file:
                        pass
                    elif content_file.endswith('.png'):
                        pass
                    elif content_file.endswith('.svg'):
                        pass
                    elif content_file.endswith('.jpg'):
                        pass
                    elif content_file.endswith('.mp4'):
                        pass
                    elif re.match(r'.*\/direct-message\.js', content_file):
                        process_direct_messages(request_identifier, opened_file.read())
                    elif re.match(r'.*\/direct-messages\.js', content_file):
                        process_direct_messages(request_identifier, opened_file.read())
                    # elif re.match(r'^like\.js', content_file[-1]):
                    #    process_likes(request_identifier, content_bundle.open(content_file).read())
                    elif re.match(r'.*\/tweet\.js', content_file):
                        process_tweets(request_identifier, opened_file.read())
                    elif re.match(r'.*\/ad-impressions\.js', content_file):
                        process_ad_impressions(request_identifier, opened_file.read())
                    elif re.match(r'.*\/ad-engagements\.js', content_file):
                        process_ad_engagements(request_identifier, opened_file.read())
                    else:
                        print('TWITTER[' + request_identifier + ']: Unable to process: ' + content_file + ' -- ' + str(content_bundle.getinfo(content_file).file_size))
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
