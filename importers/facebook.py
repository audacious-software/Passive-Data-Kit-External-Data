# pylint: disable=line-too-long

from __future__ import print_function

import copy
import json
import re
import traceback
import zipfile

import arrow

from passive_data_kit.models import DataPoint
from passive_data_kit_external_data.models import annotate_field

from ..utils import hash_content, encrypt_content, create_engagement_event, queue_batch_insert, include_data

def process_comments(request_identifier, comments_raw): # pylint: disable=too-many-branches
    comments = json.loads(comments_raw)

    if 'comments' in comments: # pylint: disable=too-many-nested-blocks
        for comment in comments['comments']: # pylint: disable=too-many-nested-blocks
            comment = copy.deepcopy(comment)

            created = arrow.get(comment['timestamp']).datetime

            if include_data(request_identifier, created, comment):
                if 'title' in comment:
                    comment['pdk_encrypted_title'] = encrypt_content(comment['title'].encode('utf-8'))

                    annotate_field(comment, 'title', comment['title'])

                    del comment['title']

                if 'data' in comment:
                    data = comment['data']

                    for datum in data:
                        if 'comment' in datum:
                            comment_obj = datum['comment']

                            if 'comment' in comment_obj:
                                comment_obj['pdk_encrypted_comment'] = encrypt_content(comment_obj['comment'].encode('utf-8'))

                                annotate_field(comment_obj, 'comment', comment_obj['comment'])

                                del comment_obj['comment']

                            if 'author' in comment_obj:
                                comment_obj['pdk_hashed_author'] = hash_content(comment_obj['author'])
                                comment_obj['pdk_encrypted_author'] = encrypt_content(comment_obj['author'].encode('utf-8'))

                                del comment_obj['author']

                queue_batch_insert(DataPoint.objects.create_data_point('pdk-external-facebook-comment', request_identifier, comment, user_agent='Passive Data Kit External Importer', created=created, skip_save=True, skip_extract_secondary_identifier=True))

                create_engagement_event(source='facebook', identifier=request_identifier, outgoing_engagement=1.0, engagement_type='comment', start=created)

    if 'comments_v2' in comments: # pylint: disable=too-many-nested-blocks
        for comment in comments['comments_v2']: # pylint: disable=too-many-nested-blocks
            comment = copy.deepcopy(comment)

            created = arrow.get(comment['timestamp']).datetime

            if include_data(request_identifier, created, comment):
                if 'title' in comment:
                    comment['pdk_encrypted_title'] = encrypt_content(comment['title'].encode('utf-8'))

                    annotate_field(comment, 'title', comment['title'])

                    del comment['title']

                if 'data' in comment:
                    data = comment['data']

                    for datum in data:
                        if 'comment' in datum:
                            comment_obj = datum['comment']

                            if 'comment' in comment_obj:
                                comment_obj['pdk_encrypted_comment'] = encrypt_content(comment_obj['comment'].encode('utf-8'))

                                annotate_field(comment_obj, 'comment', comment_obj['comment'])

                                del comment_obj['comment']

                            if 'author' in comment_obj:
                                comment_obj['pdk_hashed_author'] = hash_content(comment_obj['author'])
                                comment_obj['pdk_encrypted_author'] = encrypt_content(comment_obj['author'].encode('utf-8'))

                                del comment_obj['author']

                queue_batch_insert(DataPoint.objects.create_data_point('pdk-external-facebook-comment', request_identifier, comment, user_agent='Passive Data Kit External Importer', created=created, skip_save=True, skip_extract_secondary_identifier=True))

                create_engagement_event(source='facebook', identifier=request_identifier, outgoing_engagement=1.0, engagement_type='comment', start=created)

def process_posts(request_identifier, posts_raw): # pylint: disable=too-many-branches, too-many-statements
    posts = json.loads(posts_raw)

    source = 'user'

    if isinstance(posts, dict):
        source = 'others'

        if 'wall_posts_sent_to_you' in posts and 'activity_log_data' in posts['wall_posts_sent_to_you']:
            posts = posts['wall_posts_sent_to_you']['activity_log_data']

    if 'timestamp' in posts:
        posts = [posts]

    for post in posts: # pylint: disable=too-many-nested-blocks
        post = copy.deepcopy(post)

        if isinstance(post, dict):
            created = arrow.get(post['timestamp']).datetime

            if include_data(request_identifier, created, post):
                if 'title' in post:
                    post['pdk_encrypted_title'] = encrypt_content(post['title'].encode('utf-8'))

                    annotate_field(post, 'title', post['title'])

                    del post['title']

                if 'data' in post:
                    for datum in post['data']:
                        if 'post' in datum:
                            datum['pdk_encrypted_post'] = encrypt_content(datum['post'].encode('utf-8'))

                            annotate_field(datum, 'post', datum['post'])

                            del datum['post']

                if 'attachments' in post:
                    for attachment in post['attachments']:
                        if 'data' in attachment:
                            for datum in attachment['data']:
                                if 'event' in datum:
                                    event = datum['event']

                                    if 'name' in event:
                                        event['pdk_encrypted_name'] = encrypt_content(event['name'].encode('utf-8'))

                                        annotate_field(event, 'name', event['name'])

                                        del event['name']

                                    if 'description' in event:
                                        event['pdk_encrypted_description'] = encrypt_content(event['description'].encode('utf-8'))

                                        annotate_field(event, 'description', event['description'])

                                        del event['description']

                                    if 'place' in event:
                                        place_str = json.dumps(event['place'], indent=2)
                                        event['pdk_encrypted_place'] = encrypt_content(place_str.encode('utf-8'))

                                        annotate_field(event, 'place', place_str)

                                        del event['place']

                                if 'external_context' in datum:
                                    external_context = datum['external_context']

                                    if 'url' in external_context:
                                        external_context['pdk_encrypted_url'] = encrypt_content(external_context['url'].encode('utf-8'))

                                        annotate_field(external_context, 'url', external_context['url'])

                                        del external_context['url']

                                if 'media' in datum:
                                    media = datum['media']

                                    if 'title' in media:
                                        media['pdk_encrypted_title'] = encrypt_content(media['title'].encode('utf-8'))

                                        annotate_field(media, 'title', media['title'])

                                        del media['title']

                                    if 'description' in media:
                                        media['pdk_encrypted_description'] = encrypt_content(media['description'].encode('utf-8'))

                                        annotate_field(media, 'description', media['description'])

                                        del media['description']

                                    if 'uri' in media:
                                        media['pdk_encrypted_uri'] = encrypt_content(media['uri'].encode('utf-8'))

                                        annotate_field(media, 'uri', media['uri'])

                                        del media['uri']

                                    if 'media_metadata' in media:
                                        metadata_str = json.dumps(media['media_metadata'], indent=2)
                                        media['pdk_encrypted_media_metadata'] = encrypt_content(metadata_str.encode('utf-8'))

                                        del media['media_metadata']

                                if 'place' in datum:
                                    place_str = json.dumps(datum['place'], indent=2)
                                    datum['pdk_encrypted_place'] = encrypt_content(place_str.encode('utf-8'))

                                    del datum['place']

                post['pdk_facebook_source'] = source

                queue_batch_insert(DataPoint.objects.create_data_point('pdk-external-facebook-post', request_identifier, post, user_agent='Passive Data Kit External Importer', created=created, skip_save=True, skip_extract_secondary_identifier=True))

                create_engagement_event(source='facebook', identifier=request_identifier, outgoing_engagement=1.0, engagement_type='post', start=created)

def process_viewed(request_identifier, viewed_raw): # pylint: disable=too-many-branches, too-many-statements
    metadata = json.loads(viewed_raw)

    for thing in metadata['viewed_things']: # pylint: disable=too-many-nested-blocks
        if thing['name'] == 'Facebook Watch Videos and Shows':
            for child in thing['children']:
                if child['name'] == 'Shows':
                    for entry in child['entries']:
                        created = arrow.get(entry['timestamp']).datetime

                        if include_data(request_identifier, created, entry):
                            entry['data']['pdk_encrypted_uri'] = encrypt_content(entry['data']['uri'].encode('utf-8'))
                            entry['data']['pdk_hashed_uri'] = hash_content(entry['data']['uri'].encode('utf-8'))

                            del entry['data']['uri']

                            entry['data']['pdk_encrypted_name'] = encrypt_content(entry['data']['name'].encode('utf-8'))
                            entry['data']['pdk_hashed_name'] = hash_content(entry['data']['name'].encode('utf-8'))

                            annotate_field(entry, 'name', entry['data']['name'])

                            del entry['data']['name']

                            queue_batch_insert(DataPoint.objects.create_data_point('pdk-external-facebook-watch', request_identifier, entry, user_agent='Passive Data Kit External Importer', created=created, skip_save=True, skip_extract_secondary_identifier=True))

                            create_engagement_event(source='facebook', identifier=request_identifier, outgoing_engagement=0.0, engagement_type='video', start=created)

                elif child['name'] == 'Time Viewed':
                    for entry in child['entries']:
                        created = arrow.get(entry['timestamp']).datetime

                        if include_data(request_identifier, created, entry):
                            entry['data']['pdk_encrypted_uri'] = encrypt_content(entry['data']['uri'].encode('utf-8'))
                            entry['data']['pdk_hashed_uri'] = hash_content(entry['data']['uri'].encode('utf-8'))

                            del entry['data']['uri']

                            entry['data']['pdk_encrypted_name'] = encrypt_content(entry['data']['name'].encode('utf-8'))
                            entry['data']['pdk_hashed_name'] = hash_content(entry['data']['name'].encode('utf-8'))

                            annotate_field(entry, 'name', entry['data']['name'])

                            del entry['data']['name']

                            queue_batch_insert(DataPoint.objects.create_data_point('pdk-external-facebook-watch', request_identifier, entry, user_agent='Passive Data Kit External Importer', created=created, skip_save=True, skip_extract_secondary_identifier=True))

                            create_engagement_event(source='facebook', identifier=request_identifier, outgoing_engagement=0.0, engagement_type='video', start=created, duration=entry['data']['watch_position_seconds'])

        elif thing['name'] == 'Facebook Live Videos':
            for entry in thing['entries']:
                created = arrow.get(entry['timestamp']).datetime

                if include_data(request_identifier, created, entry):
                    entry['data']['pdk_encrypted_uri'] = encrypt_content(entry['data']['uri'].encode('utf-8'))
                    entry['data']['pdk_hashed_uri'] = hash_content(entry['data']['uri'].encode('utf-8'))

                    del entry['data']['uri']

                    entry['data']['pdk_encrypted_name'] = encrypt_content(entry['data']['name'].encode('utf-8'))
                    entry['data']['pdk_hashed_name'] = hash_content(entry['data']['name'].encode('utf-8'))

                    annotate_field(entry, 'name', entry['data']['name'])

                    del entry['data']['name']

                    queue_batch_insert(DataPoint.objects.create_data_point('pdk-external-facebook-watch', request_identifier, entry, user_agent='Passive Data Kit External Importer', created=created, skip_save=True, skip_extract_secondary_identifier=True))

                    create_engagement_event(source='facebook', identifier=request_identifier, outgoing_engagement=0.0, engagement_type='video', start=created)

        elif thing['name'] == 'Articles':
            for entry in thing['entries']:
                created = arrow.get(entry['timestamp']).datetime

                if include_data(request_identifier, created, entry):
                    entry['data']['pdk_encrypted_uri'] = encrypt_content(entry['data']['uri'].encode('utf-8'))
                    entry['data']['pdk_hashed_uri'] = hash_content(entry['data']['uri'].encode('utf-8'))

                    del entry['data']['uri']

                    entry['data']['pdk_encrypted_share'] = encrypt_content(entry['data']['share'].encode('utf-8'))
                    entry['data']['pdk_hashed_share'] = hash_content(entry['data']['share'].encode('utf-8'))

                    del entry['data']['share']

                    entry['data']['pdk_encrypted_name'] = encrypt_content(entry['data']['name'].encode('utf-8'))
                    entry['data']['pdk_hashed_name'] = hash_content(entry['data']['name'].encode('utf-8'))

                    annotate_field(entry, 'name', entry['data']['name'])

                    del entry['data']['name']

                    queue_batch_insert(DataPoint.objects.create_data_point('pdk-external-facebook-link', request_identifier, entry, user_agent='Passive Data Kit External Importer', created=created, skip_save=True, skip_extract_secondary_identifier=True))

                    create_engagement_event(source='facebook', identifier=request_identifier, outgoing_engagement=0.0, engagement_type='link', start=created)

        elif thing['name'] == 'Marketplace Interactions':
            for child in thing['children']:
                if child['name'] == 'Marketplace Items':
                    for entry in child['entries']:
                        created = arrow.get(entry['timestamp']).datetime

                        if include_data(request_identifier, created, entry):
                            entry['data']['pdk_encrypted_uri'] = encrypt_content(entry['data']['uri'].encode('utf-8'))
                            entry['data']['pdk_hashed_uri'] = hash_content(entry['data']['uri'].encode('utf-8'))

                            del entry['data']['uri']

                            entry['data']['pdk_encrypted_name'] = encrypt_content(entry['data']['name'].encode('utf-8'))
                            entry['data']['pdk_hashed_name'] = hash_content(entry['data']['name'].encode('utf-8'))

                            annotate_field(entry, 'name', entry['data']['name'])

                            del entry['data']['name']

                            queue_batch_insert(DataPoint.objects.create_data_point('pdk-external-facebook-market', request_identifier, entry, user_agent='Passive Data Kit External Importer', created=created, skip_save=True, skip_extract_secondary_identifier=True))

                            create_engagement_event(source='facebook', identifier=request_identifier, outgoing_engagement=0.0, engagement_type='shopping', start=created)

        elif thing['name'] == 'Ads':
            for entry in thing['entries']:
                created = arrow.get(entry['timestamp']).datetime

                if include_data(request_identifier, created, entry):
                    if 'uri' in entry['data']:
                        entry['data']['pdk_encrypted_uri'] = encrypt_content(entry['data']['uri'].encode('utf-8'))
                        entry['data']['pdk_hashed_uri'] = hash_content(entry['data']['uri'].encode('utf-8'))

                        del entry['data']['uri']

                    entry['data']['pdk_encrypted_name'] = encrypt_content(entry['data']['name'].encode('utf-8'))
                    entry['data']['pdk_hashed_name'] = hash_content(entry['data']['name'].encode('utf-8'))

                    annotate_field(entry, 'name', entry['data']['name'])

                    del entry['data']['name']

                    queue_batch_insert(DataPoint.objects.create_data_point('pdk-external-facebook-ad-viewed', request_identifier, entry, user_agent='Passive Data Kit External Importer', created=created, skip_save=True, skip_extract_secondary_identifier=True))

                    create_engagement_event(source='facebook', identifier=request_identifier, outgoing_engagement=0.0, engagement_type='advertising', start=created)

def process_visited(request_identifier, viewed_raw): # pylint: disable=too-many-branches
    metadata = json.loads(viewed_raw)

    for thing in metadata['visited_things']:
        if thing['name'] == 'Profile visits':
            for entry in thing['entries']:
                created = arrow.get(entry['timestamp']).datetime

                if include_data(request_identifier, created, entry):
                    entry['data']['pdk_encrypted_uri'] = encrypt_content(entry['data']['uri'].encode('utf-8'))
                    entry['data']['pdk_hashed_uri'] = hash_content(entry['data']['uri'].encode('utf-8'))

                    del entry['data']['uri']

                    entry['data']['pdk_encrypted_name'] = encrypt_content(entry['data']['name'].encode('utf-8'))
                    entry['data']['pdk_hashed_name'] = hash_content(entry['data']['name'].encode('utf-8'))

                    annotate_field(entry, 'name', entry['data']['name'])

                    del entry['data']['name']

                    queue_batch_insert(DataPoint.objects.create_data_point('pdk-external-facebook-profile-visit', request_identifier, entry, user_agent='Passive Data Kit External Importer', created=created, skip_save=True, skip_extract_secondary_identifier=True))

                    create_engagement_event(source='facebook', identifier=request_identifier, outgoing_engagement=0.0, engagement_type='profile', start=created)

        elif thing['name'] == 'Page visits':
            for entry in thing['entries']:
                created = arrow.get(entry['timestamp']).datetime

                if include_data(request_identifier, created, entry):
                    entry['data']['pdk_encrypted_uri'] = encrypt_content(entry['data']['uri'].encode('utf-8'))
                    entry['data']['pdk_hashed_uri'] = hash_content(entry['data']['uri'].encode('utf-8'))

                    del entry['data']['uri']

                    entry['data']['pdk_encrypted_name'] = encrypt_content(entry['data']['name'].encode('utf-8'))
                    entry['data']['pdk_hashed_name'] = hash_content(entry['data']['name'].encode('utf-8'))

                    annotate_field(entry, 'name', entry['data']['name'])

                    del entry['data']['name']

                    queue_batch_insert(DataPoint.objects.create_data_point('pdk-external-facebook-page-visit', request_identifier, entry, user_agent='Passive Data Kit External Importer', created=created, skip_save=True, skip_extract_secondary_identifier=True))

                    create_engagement_event(source='facebook', identifier=request_identifier, outgoing_engagement=0.0, engagement_type='page', start=created)

        elif thing['name'] == 'Events visited':
            for entry in thing['entries']:
                created = arrow.get(entry['timestamp']).datetime

                if include_data(request_identifier, created, entry):
                    entry['data']['pdk_encrypted_uri'] = encrypt_content(entry['data']['uri'].encode('utf-8'))
                    entry['data']['pdk_hashed_uri'] = hash_content(entry['data']['uri'].encode('utf-8'))

                    del entry['data']['uri']

                    entry['data']['pdk_encrypted_name'] = encrypt_content(entry['data']['name'].encode('utf-8'))
                    entry['data']['pdk_hashed_name'] = hash_content(entry['data']['name'].encode('utf-8'))

                    annotate_field(entry, 'name', entry['data']['name'])

                    del entry['data']['name']

                    queue_batch_insert(DataPoint.objects.create_data_point('pdk-external-facebook-event-visit', request_identifier, entry, user_agent='Passive Data Kit External Importer', created=created, skip_save=True, skip_extract_secondary_identifier=True))

                    create_engagement_event(source='facebook', identifier=request_identifier, outgoing_engagement=0.0, engagement_type='event', start=created)

        elif thing['name'] == 'Groups visited':
            for entry in thing['entries']:
                created = arrow.get(entry['timestamp']).datetime

                if include_data(request_identifier, created, entry):
                    entry['data']['pdk_encrypted_uri'] = encrypt_content(entry['data']['uri'].encode('utf-8'))
                    entry['data']['pdk_hashed_uri'] = hash_content(entry['data']['uri'].encode('utf-8'))

                    del entry['data']['uri']

                    entry['data']['pdk_encrypted_name'] = encrypt_content(entry['data']['name'].encode('utf-8'))
                    entry['data']['pdk_hashed_name'] = hash_content(entry['data']['name'].encode('utf-8'))

                    annotate_field(entry, 'name', entry['data']['name'])

                    del entry['data']['name']

                    queue_batch_insert(DataPoint.objects.create_data_point('pdk-external-facebook-group-visit', request_identifier, entry, user_agent='Passive Data Kit External Importer', created=created, skip_save=True, skip_extract_secondary_identifier=True))

                    create_engagement_event(source='facebook', identifier=request_identifier, outgoing_engagement=0.0, engagement_type='group', start=created)

def process_page_reactions(request_identifier, reactions_raw):
    reactions = json.loads(reactions_raw)

    for reaction in reactions['page_likes']:
        created = arrow.get(reaction['timestamp']).datetime

        if include_data(request_identifier, created, reaction):
            if 'name' in reaction:
                reaction['pdk_encrypted_name'] = encrypt_content(reaction['name'].encode('utf-8'))

                annotate_field(reaction, 'name', reaction['name'])

                del reaction['name']

            reaction['content_type'] = 'page'
            reaction['reaction'] = 'like'

            queue_batch_insert(DataPoint.objects.create_data_point('pdk-external-facebook-reaction', request_identifier, reaction, user_agent='Passive Data Kit External Importer', created=created, skip_save=True, skip_extract_secondary_identifier=True))

            create_engagement_event(source='facebook', identifier=request_identifier, outgoing_engagement=0.5, engagement_type='reaction', start=created)

def process_post_comment_reactions(request_identifier, reactions_raw): # pylint: disable=too-many-branches, too-many-statements
    reactions = json.loads(reactions_raw)

    if 'reactions' in reactions: # pylint: disable=too-many-nested-blocks
        for reaction in reactions['reactions']: # pylint: disable=too-many-nested-blocks
            created = arrow.get(reaction['timestamp']).datetime

            if include_data(request_identifier, created, reaction):
                if 'title' in reaction:
                    reaction['pdk_encrypted_title'] = encrypt_content(reaction['title'].encode('utf-8'))

                    annotate_field(reaction, 'title', reaction['title'])

                    if '\'s post' in reaction['title']:
                        reaction['content_type'] = 'post'
                    elif '\'s comment' in reaction['title']:
                        reaction['content_type'] = 'comment'
                    elif '\'s photo' in reaction['title']:
                        reaction['content_type'] = 'photo'
                    elif '\'s video' in reaction['title']:
                        reaction['content_type'] = 'video'
                    else:
                        reaction['content_type'] = 'unknown'

                    del reaction['title']

                if 'data' in reaction:
                    for data_item in reaction['data']:
                        if 'reaction' in data_item:
                            data_item['reaction']['reaction'] = data_item['reaction']['reaction'].lower()

                            if 'actor' in data_item['reaction']:
                                data_item['reaction']['pdk_encrypted_actor'] = encrypt_content(data_item['reaction']['actor'].encode('utf-8'))

                                annotate_field(data_item['reaction'], 'actor', data_item['reaction']['actor'])

                                del data_item['reaction']['actor']

                    queue_batch_insert(DataPoint.objects.create_data_point('pdk-external-facebook-reaction', request_identifier, reaction, user_agent='Passive Data Kit External Importer', created=created, skip_save=True, skip_extract_secondary_identifier=True))

                    create_engagement_event(source='facebook', identifier=request_identifier, outgoing_engagement=0.5, engagement_type='reaction', start=created)

    if 'reactions_v2' in reactions: # pylint: disable=too-many-nested-blocks
        for reaction in reactions['reactions_v2']: # pylint: disable=too-many-nested-blocks
            created = arrow.get(reaction['timestamp']).datetime

            if include_data(request_identifier, created, reaction):
                if 'title' in reaction:
                    reaction['pdk_encrypted_title'] = encrypt_content(reaction['title'].encode('utf-8'))

                    annotate_field(reaction, 'title', reaction['title'])

                    if '\'s post' in reaction['title']:
                        reaction['content_type'] = 'post'
                    elif '\'s comment' in reaction['title']:
                        reaction['content_type'] = 'comment'
                    elif '\'s photo' in reaction['title']:
                        reaction['content_type'] = 'photo'
                    elif '\'s video' in reaction['title']:
                        reaction['content_type'] = 'video'
                    else:
                        reaction['content_type'] = 'unknown'

                    del reaction['title']

                if 'data' in reaction:
                    for data_item in reaction['data']:
                        if 'reaction' in data_item:
                            data_item['reaction']['reaction'] = data_item['reaction']['reaction'].lower()

                            if 'actor' in data_item['reaction']:
                                data_item['reaction']['pdk_encrypted_actor'] = encrypt_content(data_item['reaction']['actor'].encode('utf-8'))

                                annotate_field(data_item['reaction'], 'actor', data_item['reaction']['actor'])

                                del data_item['reaction']['actor']

                    queue_batch_insert(DataPoint.objects.create_data_point('pdk-external-facebook-reaction', request_identifier, reaction, user_agent='Passive Data Kit External Importer', created=created, skip_save=True, skip_extract_secondary_identifier=True))

                    create_engagement_event(source='facebook', identifier=request_identifier, outgoing_engagement=0.5, engagement_type='reaction', start=created)

def process_messages(request_identifier, messages_raw, full_names):
    messages = json.loads(messages_raw)

    for message in messages['messages']:
        message = copy.deepcopy(message)

        created = None

        try:
            created = arrow.get(message['timestamp_ms']).datetime
        except ValueError:
            try:
                created = arrow.get(message['timestamp_ms'] / 1000).datetime
            except ValueError:
                pass

        if created is not None and include_data(request_identifier, created, message):
            if 'content' in message:
                message['pdk_encrypted_content'] = encrypt_content(message['content'].encode('utf-8'))

                annotate_field(message, 'content', message['content'])

                del message['content']

            if 'share' in message:
                share = message['share']

                for share_key in copy.deepcopy(share):
                    if share_key == 'link':
                        share['pdk_encrypted_link'] = encrypt_content(share[share_key].encode('utf-8'))

                        annotate_field(share, 'link', share[share_key])

                        del share[share_key]

            if message['sender_name'] in full_names:
                message['pdk_direction'] = 'outgoing'

                create_engagement_event(source='facebook', identifier=request_identifier, outgoing_engagement=1.0, engagement_type='message', start=created)
            else:
                message['pdk_direction'] = 'incoming'

                create_engagement_event(source='facebook', identifier=request_identifier, incoming_engagement=1.0, engagement_type='message', start=created)

            queue_batch_insert(DataPoint.objects.create_data_point('pdk-external-facebook-message', request_identifier, message, user_agent='Passive Data Kit External Importer', created=created, skip_save=True, skip_extract_secondary_identifier=True))

def process_search_history(request_identifier, searches_raw):
    searches = json.loads(searches_raw)

    for search in searches['searches']:
        created = None

        try:
            created = arrow.get(search['timestamp']).datetime
        except ValueError:
            try:
                created = arrow.get(search['timestamp'] / 1000).datetime
            except ValueError:
                pass

        if created is not None and include_data(request_identifier, created, search): # pylint: disable=too-many-nested-blocks
            if 'attachments' in search:
                for attachment in search['attachments']:
                    if 'data' in attachment:
                        for data in attachment['data']:
                            if 'text' in data:
                                payload = {
                                    'pdk_encrypted_query': encrypt_content(data['text'].encode('utf-8'))
                                }

                                annotate_field(payload, 'query', data['text'])

                                create_engagement_event(source='facebook', identifier=request_identifier, outgoing_engagement=0.5, engagement_type='search', start=created)
                                queue_batch_insert(DataPoint.objects.create_data_point('pdk-external-facebook-search', request_identifier, payload, user_agent='Passive Data Kit External Importer', created=created, skip_save=True, skip_extract_secondary_identifier=True))

def import_data(request_identifier, path): # pylint: disable=too-many-branches, too-many-statements
    with zipfile.ZipFile(path) as content_bundle:
        full_names = []

        for content_file in content_bundle.namelist():
            try:
                with content_bundle.open(content_file) as opened_file:
                    if re.match(r'^messages/inbox/.*\.json', content_file):
                        if len(full_names) == 0: # pylint: disable=len-as-condition
                            try:
                                with content_bundle.open('messages/autofill_information.json') as autofill_file:
                                    autofill = json.loads(autofill_file.read())

                                    full_names.extend(autofill['autofill_information_v2']['FULL_NAME'])
                            except KeyError:
                                pass # missing autofill_information.json

                        process_messages(request_identifier, opened_file.read(), full_names)
                    elif content_file.endswith('/'):
                        pass
                    elif content_file.lower().endswith('.jpg'):
                        pass
                    elif content_file.lower().endswith('.png'):
                        pass
                    elif content_file.lower().endswith('.mp4'):
                        pass
                    elif content_file.lower().endswith('.gif'):
                        pass
                    elif content_file.lower().endswith('.pdf'):
                        pass
                    elif content_file.lower().endswith('.webp'):
                        pass
                    elif content_file.lower().endswith('.aac'):
                        pass
                    elif content_file.lower().endswith('.mp3'):
                        pass
                    elif content_file.lower().endswith('.psd'):
                        pass
                    elif content_file.lower().endswith('.docx'):
                        pass
                    elif content_file.lower().endswith('.otf'):
                        pass
                    elif content_file.lower().endswith('.xml'):
                        pass
                    elif content_file.lower().endswith('.zip'):
                        pass
                    elif content_file.lower().endswith('.rar'):
                        pass
                    elif re.match(r'^photos_and_videos\/', content_file):
                        pass
                    elif re.match(r'^comments\/.*\.json', content_file):
                        process_comments(request_identifier, opened_file.read())
                    elif re.match(r'^comments_and_reactions\/comments.json', content_file):
                        process_comments(request_identifier, opened_file.read())
                    elif re.match(r'^posts\/.*\.json', content_file):
                        process_posts(request_identifier, opened_file.read())
                    elif re.match(r'^about_you\/viewed.json', content_file):
                        process_viewed(request_identifier, opened_file.read())
                    elif re.match(r'^about_you\/visited.json', content_file):
                        process_visited(request_identifier, opened_file.read())
                    elif re.match(r'^likes_and_reactions\/pages.json', content_file):
                        process_page_reactions(request_identifier, opened_file.read())
                    elif re.match(r'^likes_and_reactions\/posts_and_comments.json', content_file):
                        process_post_comment_reactions(request_identifier, opened_file.read())
                    elif re.match(r'^comments_and_reactions\/posts_and_comments.json', content_file):
                        process_post_comment_reactions(request_identifier, opened_file.read())
                    elif re.match(r'^search_history\/your_search_history.json', content_file):
                        process_search_history(request_identifier, opened_file.read())
                    else:
                        print('FACEBOOK[' + request_identifier + ']: Unable to process: ' + content_file + ' -- ' + str(content_bundle.getinfo(content_file).file_size))
            except: # pylint: disable=bare-except
                traceback.print_exc()
                return False

    return True

def external_data_metadata(generator_identifier, point):
    if generator_identifier.startswith('pdk-external-facebook') is False:
        return None

    metadata = {}
    metadata['service'] = 'Facebook'
    metadata['event'] = generator_identifier

    if generator_identifier == 'pdk-external-facebook-comment':
        metadata['event'] = 'Upload Comment'
        metadata['direction'] = 'Outgoing'
        metadata['media_type'] = 'Text'
    elif generator_identifier == 'pdk-external-facebook-post':
        metadata['event'] = 'Upload Post'
        metadata['direction'] = 'Outgoing'
        metadata['media_type'] = 'Text'

        properties = point.fetch_properties()

        if 'pdk_encrypted_url' in properties:
            metadata['media_type'] = 'Link'

        if 'pdk_encrypted_media_metadata' in properties:
            metadata['media_type'] = 'Multimedia'

        if 'pdk_encrypted_place' in properties:
            metadata['media_type'] = 'Location'

    return metadata

def update_data_type_definition(definition): # pylint: disable=too-many-statements
    if 'pdk-external-facebook-post' in definition['passive-data-metadata.generator-id']['observed']:
        del definition['attachments']

        if 'pdk_encrypted_title' in definition:
            definition['pdk_encrypted_title']['is_freetext'] = True
            definition['pdk_encrypted_title']['pdk_variable_name'] = 'Encrypted post title'
            definition['pdk_encrypted_title']['pdk_variable_description'] = 'Encrypted title of the original post, saved for use later (with proper authorizations and keys).'
            definition['pdk_encrypted_title']['pdk_codebook_order'] = 0
            definition['pdk_encrypted_title']['pdk_codebook_group'] = 'Passive Data Kit: External Data: Facebook Post'

        if 'data[].pdk_encrypted_post' in definition:
            definition['data[].pdk_encrypted_post']['is_freetext'] = True
            definition['data[].pdk_encrypted_post']['pdk_variable_name'] = 'Encrypted post contents'
            definition['data[].pdk_encrypted_post']['pdk_variable_description'] = 'Encrypted contents of the original post, saved for use later (with proper authorizations and keys).'
            definition['data[].pdk_encrypted_post']['pdk_codebook_order'] = 1
            definition['data[].pdk_encrypted_post']['pdk_codebook_group'] = 'Passive Data Kit: External Data: Facebook Post'

        if 'attachments[].data[].media.pdk_encrypted_uri' in definition:
            definition['attachments[].data[].media.pdk_encrypted_uri']['is_freetext'] = True
            definition['attachments[].data[].media.pdk_encrypted_uri']['pdk_variable_name'] = 'Encrypted remote content URI'
            definition['attachments[].data[].media.pdk_encrypted_uri']['pdk_variable_description'] = 'Encrypted contents of the original post media URI, saved for use later (with proper authorizations and keys).'
            definition['attachments[].data[].media.pdk_encrypted_uri']['pdk_codebook_order'] = 2
            definition['attachments[].data[].media.pdk_encrypted_uri']['pdk_codebook_group'] = 'Passive Data Kit: External Data: Facebook Post'

        if 'attachments[].data[].media.pdk_encrypted_description' in definition:
            definition['attachments[].data[].media.pdk_encrypted_description']['is_freetext'] = True
            definition['attachments[].data[].media.pdk_encrypted_description']['pdk_variable_name'] = 'Encrypted media description'
            definition['attachments[].data[].media.pdk_encrypted_description']['pdk_variable_description'] = 'Encrypted description of media item attached to the post, saved for use later (with proper authorizations and keys).'
            definition['attachments[].data[].media.pdk_encrypted_description']['pdk_codebook_order'] = 3
            definition['attachments[].data[].media.pdk_encrypted_description']['pdk_codebook_group'] = 'Passive Data Kit: External Data: Facebook Post'

        if 'attachments[].data[].media.pdk_encrypted_media_metadata' in definition:
            definition['attachments[].data[].media.pdk_encrypted_media_metadata']['is_freetext'] = True
            definition['attachments[].data[].media.pdk_encrypted_media_metadata']['pdk_variable_name'] = 'Encrypted media metadata'
            definition['attachments[].data[].media.pdk_encrypted_media_metadata']['pdk_variable_description'] = 'Encrypted metadata of media item attached to the post, saved for use later (with proper authorizations and keys).'
            definition['attachments[].data[].media.pdk_encrypted_media_metadata']['pdk_codebook_order'] = 4
            definition['attachments[].data[].media.pdk_encrypted_media_metadata']['pdk_codebook_group'] = 'Passive Data Kit: External Data: Facebook Post'

        if 'attachments[].data[].external_context.pdk_encrypted_url' in definition:
            definition['attachments[].data[].external_context.pdk_encrypted_url']['is_freetext'] = True
            definition['attachments[].data[].external_context.pdk_encrypted_url']['pdk_variable_name'] = 'Encrypted URL'
            definition['attachments[].data[].external_context.pdk_encrypted_url']['pdk_variable_description'] = 'Encrypted contents of the original URL shared in the post, saved for use later (with proper authorizations and keys).'
            definition['attachments[].data[].external_context.pdk_encrypted_url']['pdk_codebook_order'] = 5
            definition['attachments[].data[].external_context.pdk_encrypted_url']['pdk_codebook_group'] = 'Passive Data Kit: External Data: Facebook Post'

        if 'attachments[].data[].media.pdk_encrypted_title' in definition:
            definition['attachments[].data[].media.pdk_encrypted_title']['is_freetext'] = True
            definition['attachments[].data[].media.pdk_encrypted_title']['pdk_variable_name'] = 'Encrypted remote content title'
            definition['attachments[].data[].media.pdk_encrypted_title']['pdk_variable_description'] = 'Encrypted contents of the original post media title, saved for use later (with proper authorizations and keys).'
            definition['attachments[].data[].media.pdk_encrypted_title']['pdk_codebook_order'] = 6
            definition['attachments[].data[].media.pdk_encrypted_title']['pdk_codebook_group'] = 'Passive Data Kit: External Data: Facebook Post'

        if 'attachments[].data[].pdk_encrypted_place' in definition:
            definition['attachments[].data[].pdk_encrypted_place']['is_freetext'] = True
            definition['attachments[].data[].pdk_encrypted_place']['pdk_variable_name'] = 'Encrypted place name'
            definition['attachments[].data[].pdk_encrypted_place']['pdk_variable_description'] = 'Encrypted name of the place tagged on the post, saved for use later (with proper authorizations and keys).'
            definition['attachments[].data[].pdk_encrypted_place']['pdk_codebook_order'] = 7
            definition['attachments[].data[].pdk_encrypted_place']['pdk_codebook_group'] = 'Passive Data Kit: External Data: Facebook Post'
