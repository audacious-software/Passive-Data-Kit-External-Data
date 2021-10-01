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

from ..utils import hash_content, encrypt_content, create_engagement_event, queue_batch_insert, include_data

def process_ads_viewed(request_identifier, ads_viewed_raw):
    ads_viewed = json.loads(ads_viewed_raw)

    if isinstance(ads_viewed, dict) is False:
        return

    if ('impressions_history_ads_seen' in ads_viewed) is False:
        return

    for ad_viewed in ads_viewed['impressions_history_ads_seen']:
        created = arrow.get(ad_viewed['string_map_data']['Time']['timestamp']).datetime

        if include_data(request_identifier, created, ad_viewed):
            queue_batch_insert(DataPoint.objects.create_data_point('pdk-external-instagram-ad-viewed', request_identifier, ad_viewed, user_agent='Passive Data Kit External Importer', created=created, skip_save=True, skip_extract_secondary_identifier=True))

            create_engagement_event(source='instagram', identifier=request_identifier, outgoing_engagement=0.0, engagement_type='advertising', start=created)

def process_posts_viewed(request_identifier, posts_viewed_raw):
    posts_viewed = json.loads(posts_viewed_raw)

    if isinstance(posts_viewed, dict) is False:
        return

    if ('impressions_history_posts_seen' in posts_viewed) is False:
        return

    for post_viewed in posts_viewed['impressions_history_posts_seen']:
        created = arrow.get(post_viewed['string_map_data']['Time']['timestamp']).datetime

        if include_data(request_identifier, created, post_viewed):
            queue_batch_insert(DataPoint.objects.create_data_point('pdk-external-instagram-post-viewed', request_identifier, post_viewed, user_agent='Passive Data Kit External Importer', created=created, skip_save=True, skip_extract_secondary_identifier=True))

            create_engagement_event(source='instagram', identifier=request_identifier, outgoing_engagement=0.0, engagement_type='post', start=created)

def process_suggested_accounts_viewed(request_identifier, suggested_accounts_viewed_raw): # pylint: disable=invalid-name
    suggested_accounts_viewed = json.loads(suggested_accounts_viewed_raw)

    if isinstance(suggested_accounts_viewed, dict) is False:
        return

    if ('impressions_history_chaining_seen' in suggested_accounts_viewed) is False:
        return

    for account_viewed in suggested_accounts_viewed['impressions_history_chaining_seen']:
        created = arrow.get(account_viewed['string_map_data']['Time']['timestamp']).datetime

        if include_data(request_identifier, created, account_viewed):
            queue_batch_insert(DataPoint.objects.create_data_point('pdk-external-instagram-profile-viewed', request_identifier, account_viewed, user_agent='Passive Data Kit External Importer', created=created, skip_save=True, skip_extract_secondary_identifier=True))

            create_engagement_event(source='instagram', identifier=request_identifier, outgoing_engagement=0.0, engagement_type='profile', start=created)

def process_videos_watched(request_identifier, videos_watched_raw):
    videos_watched = json.loads(videos_watched_raw)

    if isinstance(videos_watched, dict) is False:
        return

    if ('impressions_history_videos_watched' in videos_watched) is False:
        return

    for video_watched in videos_watched['impressions_history_videos_watched']:
        created = arrow.get(video_watched['string_map_data']['Time']['timestamp']).datetime

        if include_data(request_identifier, created, video_watched):
            queue_batch_insert(DataPoint.objects.create_data_point('pdk-external-instagram-video-watched', request_identifier, video_watched, user_agent='Passive Data Kit External Importer', created=created, skip_save=True, skip_extract_secondary_identifier=True))

            create_engagement_event(source='instagram', identifier=request_identifier, outgoing_engagement=0.0, engagement_type='video', start=created)

def process_post_comments(request_identifier, post_comments_raw):
    post_comments = json.loads(post_comments_raw)

    if isinstance(post_comments, dict) is False:
        return

    if ('comments_media_comments' in post_comments) is False:
        return

    for post_comment in post_comments['comments_media_comments']:
        post_comment['encrypted_title'] = encrypt_content(post_comment['title'].encode('utf-8'))
        del post_comment['title']

        post_comment['string_list_data']['encrypted_value'] = encrypt_content(post_comment['string_list_data']['value'].encode('utf-8'))
        annotate_field(post_comment['string_list_data'], 'value', post_comment['string_list_data']['value'])
        del post_comment['string_list_data']['value']

        created = arrow.get(post_comment['string_map_data']['Time']['timestamp']).datetime

        if include_data(request_identifier, created, post_comment):
            queue_batch_insert(DataPoint.objects.create_data_point('pdk-external-instagram-comment-posted', request_identifier, post_comment, user_agent='Passive Data Kit External Importer', created=created, skip_save=True, skip_extract_secondary_identifier=True))

            create_engagement_event(source='instagram', identifier=request_identifier, outgoing_engagement=1.0, engagement_type='comment', start=created)

def process_posts_made(request_identifier, posts_made_raw):
    posts_made = json.loads(posts_made_raw)

    if isinstance(posts_made, list) is False:
        return

    for post in posts_made:
        created = arrow.get(post['media'][0]['creation_timestamp']).datetime

        if include_data(request_identifier, created, post):
            for media in post['media']:
                media['encrypted_title'] = encrypt_content(media['title'].encode('utf-8'))
                annotate_field(media, 'title', media['title'])
                del media['title']

                try:
                    del media['media_metadata']['photo_metadata']['exif_data']
                except KeyError:
                    pass

            queue_batch_insert(DataPoint.objects.create_data_point('pdk-external-instagram-post', request_identifier, post, user_agent='Passive Data Kit External Importer', created=created, skip_save=True, skip_extract_secondary_identifier=True))

            create_engagement_event(source='instagram', identifier=request_identifier, outgoing_engagement=1.0, engagement_type='post', start=created)

def process_liked_comments(request_identifier, liked_comments_raw):
    liked_comments = json.loads(liked_comments_raw)

    if isinstance(liked_comments, dict) is False:
        return

    if ('likes_comment_likes' in liked_comments) is False:
        return

    for liked_comment in liked_comments['likes_comment_likes']:
        created = arrow.get(liked_comment['string_map_data']['timestamp']).datetime

        if include_data(request_identifier, created, liked_comment):
            liked_comment['encrypted_title'] = encrypt_content(liked_comment['title'].encode('utf-8'))
            del liked_comment['title']

            liked_comment['string_list_data']['encrypted_href'] = encrypt_content(liked_comment['string_list_data']['href'].encode('utf-8'))
            del liked_comment['string_list_data']['href']

            queue_batch_insert(DataPoint.objects.create_data_point('pdk-external-instagram-comment-like', request_identifier, liked_comment, user_agent='Passive Data Kit External Importer', created=created, skip_save=True, skip_extract_secondary_identifier=True))

            create_engagement_event(source='instagram', identifier=request_identifier, outgoing_engagement=0.5, engagement_type='reaction', start=created)

def process_liked_posts(request_identifier, liked_posts_raw):
    liked_posts = json.loads(liked_posts_raw)

    if isinstance(liked_posts, dict) is False:
        return

    if ('likes_media_likes' in liked_posts) is False:
        return

    for liked_post in liked_posts['likes_media_likes']:
        created = arrow.get(liked_post['string_map_data']['timestamp']).datetime

        if include_data(request_identifier, created, liked_post):
            liked_post['encrypted_title'] = encrypt_content(liked_post['title'].encode('utf-8'))
            del liked_post['title']

            liked_post['string_list_data']['encrypted_href'] = encrypt_content(liked_post['string_list_data']['href'].encode('utf-8'))
            del liked_post['string_list_data']['href']


            queue_batch_insert(DataPoint.objects.create_data_point('pdk-external-instagram-post-like', request_identifier, liked_post, user_agent='Passive Data Kit External Importer', created=created, skip_save=True, skip_extract_secondary_identifier=True))

            create_engagement_event(source='instagram', identifier=request_identifier, outgoing_engagement=0.5, engagement_type='reaction', start=created)

def process_login_activity(request_identifier, login_activity_raw):
    login_activity = json.loads(login_activity_raw)

    if isinstance(login_activity, dict) is False:
        return

    if ('account_history_login_history' in login_activity) is False:
        return

    for login in login_activity['account_history_login_history']:
        created = arrow.get(login['string_map_data']['Time']['timestamp']).datetime

        if include_data(request_identifier, created, login):
            login['string_map_data']['IP Address']['encrypted_value'] = encrypt_content(login['string_map_data']['IP Address']['value'] .encode('utf-8'))
            del login['string_map_data']['IP Address']['value']

            queue_batch_insert(DataPoint.objects.create_data_point('pdk-external-instagram-login', request_identifier, login, user_agent='Passive Data Kit External Importer', created=created, skip_save=True, skip_extract_secondary_identifier=True))

            create_engagement_event(source='instagram', identifier=request_identifier, outgoing_engagement=0.0, engagement_type='login', start=created)

def process_account_history(request_identifier, history_raw):
    history = json.loads(history_raw)

    if isinstance(history, dict) is False:
        return

    for login in history['login_history']:
        created = arrow.get(login['timestamp']).datetime

        if include_data(request_identifier, created, login):
            login['ip_address_encrypted_value'] = encrypt_content(login['ip_address'] .encode('utf-8'))

            del login['ip_address']
            del login['device_id']

            queue_batch_insert(DataPoint.objects.create_data_point('pdk-external-instagram-login', request_identifier, login, user_agent='Passive Data Kit External Importer', created=created, skip_save=True, skip_extract_secondary_identifier=True))

            create_engagement_event(source='instagram', identifier=request_identifier, outgoing_engagement=0.0, engagement_type='login', start=created)

def process_stories(request_identifier, stories_raw):
    stories = json.loads(stories_raw)

    if isinstance(stories, dict) is False:
        return

    for action_type, actions in stories.iteritems():
        for action in actions:
            created = arrow.get(action[0]).datetime

            if include_data(request_identifier, created, action):
                payload = {
                    'action_type': action_type,
                    'target_encrypted_value': encrypt_content(action[1].encode('utf-8'))
                }

                queue_batch_insert(DataPoint.objects.create_data_point('pdk-external-instagram-story-action', request_identifier, payload, user_agent='Passive Data Kit External Importer', created=created, skip_save=True, skip_extract_secondary_identifier=True))

                create_engagement_event(source='instagram', identifier=request_identifier, outgoing_engagement=0.5, engagement_type='story', start=created)

def process_logout_activity(request_identifier, logout_activity_raw):
    logout_activity = json.loads(logout_activity_raw)

    if isinstance(logout_activity, dict) is False:
        return

    if ('account_history_logout_history' in logout_activity) is False:
        return

    for logout in logout_activity['account_history_logout_history']:
        created = arrow.get(logout['string_map_data']['Time']['timestamp']).datetime

        if include_data(request_identifier, created, logout):
            logout['string_map_data']['IP Address']['encrypted_value'] = encrypt_content(logout['string_map_data']['IP Address']['value'] .encode('utf-8'))
            del logout['string_map_data']['IP Address']['value']

            queue_batch_insert(DataPoint.objects.create_data_point('pdk-external-instagram-login', request_identifier, logout, user_agent='Passive Data Kit External Importer', created=created, skip_save=True, skip_extract_secondary_identifier=True))

            create_engagement_event(source='instagram', identifier=request_identifier, outgoing_engagement=0.0, engagement_type='logout', start=created)

def process_messages_new(request_identifier, username, messages_raw):
    messages = json.loads(messages_raw)

    if isinstance(messages, dict) is False:
        return

    for message in messages['messages']:
        created = arrow.get(message['timestamp_ms'] / 1000).datetime

        if include_data(request_identifier, created, message):
            pdk_message = {
                'pdk_recipients_count': len(messages['participants']) - 1,
                'pdk_hashed_senderId': hash_content(message['sender_name'].encode('utf-8')),
                'pdk_encrypted_sender': encrypt_content(message['sender_name'].encode('utf-8')),
                'created_at': message['timestamp_ms']
            }

            if 'content' in message and message['content'] is not None:
                annotate_field(pdk_message, 'content', message['content'])
                pdk_message['pdk_encrypted_content'] = encrypt_content(message['content'].encode('utf-8'))

            if 'share' in message:
                pdk_message['pdk_encrypted_media_url'] = encrypt_content(message['share']['link'].encode('utf-8'))

                if 'share_text' in message['share']:
                    annotate_field(pdk_message, 'share_text', message['share']['share_text'])

            queue_batch_insert(DataPoint.objects.create_data_point('pdk-external-instagram-direct-message', request_identifier, pdk_message, user_agent='Passive Data Kit External Importer', created=created, skip_save=True, skip_extract_secondary_identifier=True))

            if message['sender_name'] == username:
                create_engagement_event(source='instagram', identifier=request_identifier, outgoing_engagement=1.0, engagement_type='message', start=created)
            else:
                create_engagement_event(source='instagram', identifier=request_identifier, incoming_engagement=1.0, engagement_type='message', start=created)

# Older format?

def process_comments(request_identifier, comments_raw):
    comments = json.loads(comments_raw)

    if isinstance(comments, dict) is False:
        return

    for key in comments:
        comment_list = comments[key]

        for comment in comment_list:
            created = arrow.get(comment[0]).replace(tzinfo=pytz.timezone('US/Pacific')).datetime

            if include_data(request_identifier, created, comment):
                comment_point = {}


                comment_point['pdk_encrypted_comment'] = encrypt_content(comment[1].encode('utf-8'))

                annotate_field(comment_point, 'comment', comment[1])

                comment_point['pdk_hashed_profile'] = hash_content(comment[2])
                comment_point['pdk_encrypted_profile'] = encrypt_content(comment[2].encode('utf-8'))

                queue_batch_insert(DataPoint.objects.create_data_point('pdk-external-instagram-comment', request_identifier, comment_point, user_agent='Passive Data Kit External Importer', created=created, skip_save=True, skip_extract_secondary_identifier=True))

                create_engagement_event(source='instagram', identifier=request_identifier, outgoing_engagement=1.0, engagement_type='comment', start=created)

def process_media(request_identifier, media_raw):
    media = json.loads(media_raw)

    if 'photos' in media:
        for photo in media['photos']:
            created = arrow.get(photo['taken_at']).replace(tzinfo=pytz.timezone('US/Pacific')).datetime

            if include_data(request_identifier, created, photo):
                photo['pdk_encrypted_caption'] = encrypt_content(photo['caption'].encode('utf-8'))

                annotate_field(photo, 'caption', photo['caption'])

                del photo['caption']

                if 'location' in photo:
                    photo['pdk_encrypted_location'] = encrypt_content(photo['location'].encode('utf-8'))

                    annotate_field(photo, 'location', photo['location'])

                    del photo['location']

                queue_batch_insert(DataPoint.objects.create_data_point('pdk-external-instagram-photo', request_identifier, photo, user_agent='Passive Data Kit External Importer', created=created, skip_save=True, skip_extract_secondary_identifier=True))

                create_engagement_event(source='instagram', identifier=request_identifier, outgoing_engagement=1.0, engagement_type='photo', start=created)

    if 'videos' in media:
        for video in media['videos']:
            created = arrow.get(video['taken_at']).replace(tzinfo=pytz.timezone('US/Pacific')).datetime

            if include_data(request_identifier, created, video):
                video['pdk_encrypted_caption'] = encrypt_content(video['caption'].encode('utf-8'))

                annotate_field(video, 'caption', video['caption'])

                del video['caption']

                if 'location' in video:
                    video['pdk_encrypted_location'] = encrypt_content(video['location'].encode('utf-8'))

                    annotate_field(video, 'location', video['location'])

                    del video['location']

                queue_batch_insert(DataPoint.objects.create_data_point('pdk-external-instagram-video', request_identifier, video, user_agent='Passive Data Kit External Importer', created=created, skip_save=True, skip_extract_secondary_identifier=True))

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

            if include_data(request_identifier, created, like):
                reaction = {
                    'timestamp': like[0],
                    'pdk_hashed_target': hash_content(like[1].encode('utf-8'))
                }

                reaction['content_type'] = key
                reaction['reaction'] = 'like'

                queue_batch_insert(DataPoint.objects.create_data_point('pdk-external-instagram-reaction', request_identifier, reaction, user_agent='Passive Data Kit External Importer', created=created, skip_save=True, skip_extract_secondary_identifier=True))

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
            created = arrow.get(message['created_at']).datetime

            if include_data(request_identifier, created, message):
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

                queue_batch_insert(DataPoint.objects.create_data_point('pdk-external-instagram-direct-message', request_identifier, pdk_message, user_agent='Passive Data Kit External Importer', created=created, skip_save=True, skip_extract_secondary_identifier=True))

                if message['sender'] == username:
                    create_engagement_event(source='instagram', identifier=request_identifier, outgoing_engagement=1.0, engagement_type='message', start=created)
                else:
                    create_engagement_event(source='instagram', identifier=request_identifier, incoming_engagement=1.0, engagement_type='message', start=created)

def process_seen_content(request_identifier, seen_raw):
    seen = json.loads(seen_raw)

    if isinstance(seen, dict) is False:
        return

    for item_seen in seen['chaining_seen']:
        created = arrow.get(item_seen['timestamp']).datetime

        if include_data(request_identifier, created, item_seen):
            reaction = {
                'timestamp': item_seen['timestamp'],
                'pdk_hashed_target': hash_content(item_seen['username'].encode('utf-8'))
            }

            queue_batch_insert(DataPoint.objects.create_data_point('pdk-external-instagram-page-visit', request_identifier, reaction, user_agent='Passive Data Kit External Importer', created=created, skip_save=True, skip_extract_secondary_identifier=True))

            create_engagement_event(source='instagram', identifier=request_identifier, outgoing_engagement=0.5, engagement_type='page', start=created)

    if 'ads_clicked' in seen:
        for item_seen in seen['ads_clicked']:
            created = arrow.get(item_seen['timestamp']).datetime

            if include_data(request_identifier, created, item_seen):
                ad_clicked = {
                    'timestamp': item_seen['timestamp'],
                    'pdk_hashed_target': hash_content(item_seen['caption'].encode('utf-8'))
                }

                queue_batch_insert(DataPoint.objects.create_data_point('pdk-external-instagram-ad-clicked', request_identifier, ad_clicked, user_agent='Passive Data Kit External Importer', created=created, skip_save=True, skip_extract_secondary_identifier=True))

                create_engagement_event(source='instagram', identifier=request_identifier, outgoing_engagement=0.5, engagement_type='advertising', start=created)

def process_searches(request_identifier, searches_raw):
    searches = json.loads(searches_raw)

    if isinstance(searches, dict) is False:
        return

    for search in searches['main_search_history']:
        created = arrow.get(search['time']).datetime

        if include_data(request_identifier, created, search):
            search_click = {
                'timestamp': search['time'],
                'pdk_hashed_target': hash_content(search['search_click'].encode('utf-8'))
            }

            queue_batch_insert(DataPoint.objects.create_data_point('pdk-external-instagram-search-click', request_identifier, search_click, user_agent='Passive Data Kit External Importer', created=created, skip_save=True, skip_extract_secondary_identifier=True))

            create_engagement_event(source='instagram', identifier=request_identifier, outgoing_engagement=0.5, engagement_type='search', start=created)

def process_connections_events(request_identifier, json_string): # pylint: disable=too-many-branches
    friends_history = json.loads(json_string)

    incoming_contact_categories = [
        'follow_requests_sent',
        'following',
    ]

    for contact_category in incoming_contact_categories:
        if contact_category in friends_history:
            for contact, contact_date in friends_history[contact_category].iteritems():
                created = arrow.get(contact_date).datetime

                if include_data(request_identifier, created, contact):
                    payload = {
                        'pdk_encrypted_username': encrypt_content(contact.encode('utf-8')),
                        'connection_date': contact_date
                    }

                    queue_batch_insert(DataPoint.objects.create_data_point('pdk-external-instagram-requested-contact', request_identifier, payload, user_agent='Passive Data Kit External Importer', created=created, skip_save=True, skip_extract_secondary_identifier=True))

                    create_engagement_event(source='instagram', identifier=request_identifier, outgoing_engagement=0.5, engagement_type='requested-contact', start=created)

    if 'following_hashtags' in friends_history:
        for contact, contact_date in friends_history['following_hashtags'].iteritems():
            created = arrow.get(contact_date).datetime

            if include_data(request_identifier, created, contact):
                payload = {
                    'pdk_encrypted_hashtag': encrypt_content(contact.encode('utf-8')),
                    'connection_date': contact_date
                }


                queue_batch_insert(DataPoint.objects.create_data_point('pdk-external-instagram-followed-topic', request_identifier, payload, user_agent='Passive Data Kit External Importer', created=created, skip_save=True, skip_extract_secondary_identifier=True))

                create_engagement_event(source='instagram', identifier=request_identifier, outgoing_engagement=0.5, engagement_type='followed-topic', start=created)

    if 'followers' in friends_history:
        for contact, contact_date in friends_history['followers'].iteritems():
            created = arrow.get(contact_date).datetime

            if include_data(request_identifier, created, contact):
                payload = {
                    'pdk_encrypted_username': encrypt_content(contact.encode('utf-8')),
                    'connection_date': contact_date
                }

                queue_batch_insert(DataPoint.objects.create_data_point('pdk-external-instagram-added-contact', request_identifier, payload, user_agent='Passive Data Kit External Importer', created=created, skip_save=True, skip_extract_secondary_identifier=True))

                create_engagement_event(source='instagram', identifier=request_identifier, incoming_engagement=0.5, engagement_type='added-contact', start=created)

    if 'dismissed_suggested_users' in friends_history:
        for contact, contact_date in friends_history['dismissed_suggested_users'].iteritems():
            created = arrow.get(contact_date).datetime

            if include_data(request_identifier, created, contact):
                payload = {
                    'pdk_encrypted_username': encrypt_content(contact.encode('utf-8')),
                    'connection_date': contact_date
                }

                queue_batch_insert(DataPoint.objects.create_data_point('pdk-external-instagram-deleted-contact', request_identifier, payload, user_agent='Passive Data Kit External Importer', created=created, skip_save=True, skip_extract_secondary_identifier=True))

                create_engagement_event(source='instagram', identifier=request_identifier, incoming_engagement=0.5, engagement_type='deleted-contact', start=created)

    if 'blocked_users' in friends_history:
        for contact, contact_date in friends_history['blocked_users'].iteritems():
            created = arrow.get(contact_date).datetime

            if include_data(request_identifier, created, contact):
                payload = {
                    'pdk_encrypted_username': encrypt_content(contact.encode('utf-8')),
                    'connection_date': contact_date
                }

                queue_batch_insert(DataPoint.objects.create_data_point('pdk-external-instagram-blocked-contact', request_identifier, payload, user_agent='Passive Data Kit External Importer', created=created, skip_save=True, skip_extract_secondary_identifier=True))

                create_engagement_event(source='instagram', identifier=request_identifier, outgoing_engagement=0.5, engagement_type='blocked-contact', start=created)

def process_save_events(request_identifier, json_string):
    save_history = json.loads(json_string)

    if 'saved_media' in save_history:
        for saved_item in save_history['saved_media']:
            created = arrow.get(saved_item[0]).datetime

            if include_data(request_identifier, created, saved_item):
                payload = {
                    'pdk_encrypted_username': encrypt_content(saved_item[1].encode('utf-8')),
                    'save_date': saved_item[0]
                }

                queue_batch_insert(DataPoint.objects.create_data_point('pdk-external-instagram-saved-media', request_identifier, payload, user_agent='Passive Data Kit External Importer', created=created, skip_save=True, skip_extract_secondary_identifier=True))

                create_engagement_event(source='instagram', identifier=request_identifier, outgoing_engagement=0.5, engagement_type='saved-media', start=created)

def import_data(request_identifier, path): # pylint: disable=too-many-branches, too-many-statements
    content_bundle = zipfile.ZipFile(path)

    skip_files = [
        'autofill.json',
        'uploaded_contacts.json',
        'checkout.json',
        'profile.json',
        'settings.json',
        'information_about_you.json',
        'devices.json',
        'shopping.json',
        'guides.json',
    ]

    for content_file in content_bundle.namelist():
        try:
            if content_file.endswith('/'):
                pass
            elif 'no-data' in content_file:
                pass
            elif 'media/archived_posts' in content_file:
                pass
            elif content_file in skip_files:
                pass
            elif content_file.endswith('.mp4'):
                pass
            elif content_file.endswith('.m4a'):
                pass
            elif content_file.endswith('.jpg'):
                pass
            elif re.match(r'^messages\/.*\/message_.*\.html', content_file):
                pass
            elif re.match(r'^comments\.json', content_file):
                process_comments(request_identifier, content_bundle.open(content_file).read())
            elif re.match(r'^stories_activities\.json', content_file):
                process_stories(request_identifier, content_bundle.open(content_file).read())
            elif re.match(r'^connections\.json', content_file):
                process_connections_events(request_identifier, content_bundle.open(content_file).read())
            elif re.match(r'^saved\.json', content_file):
                process_save_events(request_identifier, content_bundle.open(content_file).read())
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
            elif re.match(r'^ads_and_content\/ads_viewed\.json', content_file):
                process_ads_viewed(request_identifier, content_bundle.open(content_file).read())
            elif re.match(r'^ads_and_content\/posts_viewed\.json', content_file):
                process_posts_viewed(request_identifier, content_bundle.open(content_file).read())
            elif re.match(r'^ads_and_content\/suggested_accounts_viewed\.json', content_file):
                process_suggested_accounts_viewed(request_identifier, content_bundle.open(content_file).read())
            elif re.match(r'^ads_and_content\/videos_watched\.json', content_file):
                process_videos_watched(request_identifier, content_bundle.open(content_file).read())
            elif re.match(r'^comments\/post_comments\.json', content_file):
                process_post_comments(request_identifier, content_bundle.open(content_file).read())
            elif re.match(r'^posts\/post_.*\.json', content_file):
                process_posts_made(request_identifier, content_bundle.open(content_file).read())
            elif re.match(r'^likes\/liked_comments.json', content_file):
                process_liked_comments(request_identifier, content_bundle.open(content_file).read())
            elif re.match(r'^login_and_account_creation\/login_activity.json', content_file):
                process_login_activity(request_identifier, content_bundle.open(content_file).read())
            elif re.match(r'^account_history.json', content_file):
                process_account_history(request_identifier, content_bundle.open(content_file).read())
            elif re.match(r'^messages\/.*\/message_.*\.json', content_file):
                try:
                    profile_json = json.loads(content_bundle.open('account_information/personal_information.json').read())

                    username = profile_json['profile_user'][0]['string_map_data']['Name']['value']

                    process_messages_new(request_identifier, username, content_bundle.open(content_file).read())
                except KeyError:
                    pass
            else:
                print('INSTAGRAM[' + request_identifier + ']: Unable to process: ' + content_file + ' -- ' + str(content_bundle.getinfo(content_file).file_size))
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

def update_data_type_definition(definition):
    if 'pdk-external-instagram-photo' in definition['passive-data-metadata.generator-id']['observed']:
        if 'pdk_encrypted_caption' in definition:
            del definition['pdk_encrypted_caption']['observed']

            definition['pdk_encrypted_caption']['is_freetext'] = True

            definition['pdk_encrypted_caption']['pdk_variable_name'] = 'Encrypted photo caption'
            definition['pdk_encrypted_caption']['pdk_variable_description'] = 'Encrypted caption of the photo, saved for use later (with proper authorizations and keys).'

        if 'taken_at' in definition:
            del definition['taken_at']['observed']

            definition['taken_at']['is_freetext'] = False

            definition['taken_at']['pdk_variable_name'] = 'Photo capture timestamp'
            definition['taken_at']['pdk_variable_description'] = 'ISO-8601 timestamp of the time the photo was originally taken.'

        if 'path' in definition:
            del definition['path']['observed']

            definition['path']['is_freetext'] = False

            definition['path']['pdk_variable_name'] = 'Photo path in export file'
            definition['path']['pdk_variable_description'] = 'File path of the photo file in the uploaded Instagram data export.'
