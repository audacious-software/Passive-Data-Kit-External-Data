# pylint: disable=line-too-long

from __future__ import print_function

import json
import re
import traceback
import zipfile

import arrow

from passive_data_kit.models import DataPoint

from ..utils import hash_content, encrypt_content, create_engagement_event, include_data

def process_chat_history(request_identifier, json_string):
    chat_history = json.loads(json_string)

    for message in chat_history['Received Chat History']:
        created = arrow.get(message['Created'], 'YYYY-MM-DD HH:mm:ss ZZZ').datetime

        if include_data(request_identifier, created, message):
            pdk_message = {
                'pdk_hashed_from': hash_content(message['From'].encode('utf-8')),
                'pdk_encrypted_from': encrypt_content(message['From'].encode('utf-8')),
                'pdk_length_from': len(message['From']),
                'media_type': message['Media Type'],
                'created': message['Created'],
            }

            DataPoint.objects.create_data_point('pdk-external-snapchat-chat-received', request_identifier, pdk_message, user_agent='Passive Data Kit External Importer', created=created)

            create_engagement_event(source='snapchat', identifier=request_identifier, incoming_engagement=1.0, engagement_type='message', start=created)

    for message in chat_history['Sent Chat History']:
        created = arrow.get(message['Created'], 'YYYY-MM-DD HH:mm:ss ZZZ').datetime

        if include_data(request_identifier, created, message):
            pdk_message = {
                'pdk_hashed_to': hash_content(message['To'].encode('utf-8')),
                'pdk_encrypted_to': encrypt_content(message['To'].encode('utf-8')),
                'pdk_length_to': len(message['To']),
                'media_type': message['Media Type'],
                'created': message['Created'],
            }

            DataPoint.objects.create_data_point('pdk-external-snapchat-chat-sent', request_identifier, pdk_message, user_agent='Passive Data Kit External Importer', created=created)

            create_engagement_event(source='snapchat', identifier=request_identifier, outgoing_engagement=1.0, engagement_type='message', start=created)


def process_memories_history(request_identifier, json_string):
    memories_history = json.loads(json_string)

    for media in memories_history['Saved Media']:
        created = arrow.get(media['Date'], 'YYYY-MM-DD HH:mm:ss ZZZ').datetime

        if include_data(request_identifier, created, media):
            pdk_media = {
                'pdk_hashed_download_link': hash_content(media['Download Link'].encode('utf-8')),
                'pdk_encrypted_download_link': encrypt_content(media['Download Link'].encode('utf-8')),
                'pdk_length_download_link': len(media['Download Link']),
                'media_type': media['Media Type'],
                'date': media['Date'],
            }

            DataPoint.objects.create_data_point('pdk-external-snapchat-memory-history', request_identifier, pdk_media, user_agent='Passive Data Kit External Importer', created=created)

            create_engagement_event(source='snapchat', identifier=request_identifier, outgoing_engagement=1.0, engagement_type='memory', start=created)


def process_shared_story(request_identifier, json_string):
    shared_story = json.loads(json_string)

    if 'Shared Story' in shared_story:
        for story in shared_story['Shared Story']:
            created = arrow.get(story['Created'], 'YYYY-MM-DD HH:mm:ss ZZZ').datetime

            if include_data(request_identifier, created, story):
                pdk_story = {
                    'pdk_hashed_story_id': hash_content(story['Story Id'].encode('utf-8')),
                    'pdk_encrypted_story_id': encrypt_content(story['Story Id'].encode('utf-8')),
                    'pdk_length_story_id': len(story['Story Id']),
                    'create_time': story['Created'],
                    'content': [],
                }

                if 'Status' in story:
                    pdk_story['status'] = story['Status']

                for item in story['Content']:
                    content_obj = {
                        'pdk_hashed_item': hash_content(item),
                        'pdk_encrypted_item': encrypt_content(item.encode('utf-8')),
                        'pdk_length_item': len(item),
                        'item_extension': item.split('.')[-1],
                    }

                    pdk_story['content'].append(content_obj)

                DataPoint.objects.create_data_point('pdk-external-snapchat-shared-story', request_identifier, pdk_story, user_agent='Passive Data Kit External Importer', created=created)

                create_engagement_event(source='snapchat', identifier=request_identifier, outgoing_engagement=1.0, engagement_type='share', start=created)

    if 'Spotlight History' in shared_story:
        for story in shared_story['Spotlight History']:
            created = arrow.get(story['Story Date'], 'YYYY-MM-DD HH:mm:ss ZZZ').datetime

            if include_data(request_identifier, created, story):
                pdk_story = {
                    'pdk_hashed_story_url': hash_content(story['Story URL'].encode('utf-8')),
                    'pdk_encrypted_story_url': encrypt_content(story['Story URL'].encode('utf-8')),
                    'create_time': story['Story Date'],
                    'view_duration': float(story['View Time'].replace(' seconds', '')),
                }

                DataPoint.objects.create_data_point('pdk-external-snapchat-spoytlight-history', request_identifier, pdk_story, user_agent='Passive Data Kit External Importer', created=created)

                create_engagement_event(source='snapchat', identifier=request_identifier, outgoing_engagement=0.0, engagement_type='post', start=created, duration=pdk_story['view_duration'])


def process_snap_history(request_identifier, json_string):
    snap_history = json.loads(json_string)

    for snap in snap_history['Received Snap History']:
        created = arrow.get(snap['Created'], 'YYYY-MM-DD HH:mm:ss ZZZ').datetime

        if include_data(request_identifier, created, snap):
            pdk_snap = {
                'pdk_hashed_from': hash_content(snap['From'].encode('utf-8')),
                'pdk_encrypted_from': encrypt_content(snap['From'].encode('utf-8')),
                'pdk_length_from': len(snap['From']),
                'create_time': snap['Created'],
                'media_type': snap['Media Type'],
            }

            DataPoint.objects.create_data_point('pdk-external-snapchat-snap-received', request_identifier, pdk_snap, user_agent='Passive Data Kit External Importer', created=created)

            create_engagement_event(source='snapchat', identifier=request_identifier, incoming_engagement=1.0, engagement_type='message', start=created)

    for snap in snap_history['Sent Snap History']:
        created = arrow.get(snap['Created'], 'YYYY-MM-DD HH:mm:ss ZZZ').datetime

        if include_data(request_identifier, created, snap):
            pdk_snap = {
                'pdk_hashed_to': hash_content(snap['To'].encode('utf-8')),
                'pdk_encrypted_to': encrypt_content(snap['To'].encode('utf-8')),
                'pdk_length_to': len(snap['To']),
                'create_time': snap['Created'],
                'media_type': snap['Media Type'],
            }

            DataPoint.objects.create_data_point('pdk-external-snapchat-snap-sent', request_identifier, pdk_snap, user_agent='Passive Data Kit External Importer', created=created)

            create_engagement_event(source='snapchat', identifier=request_identifier, outgoing_engagement=1.0, engagement_type='message', start=created)


def process_support_notes(request_identifier, json_string):
    support_notes = json.loads(json_string)

    for report_type in support_notes.keys():
        for note in support_notes[report_type]:
            created = arrow.get(note['Create Time'], 'YYYY-MM-DD HH:mm:ss ZZZ').datetime

            if include_data(request_identifier, created, note):
                pdk_note = {
                    'pdk_hashed_subject': hash_content(note['Subject'].encode('utf-8')),
                    'pdk_encrypted_subject': encrypt_content(note['Subject'].encode('utf-8')),
                    'pdk_length_subject': len(note['Subject']),
                    'pdk_hashed_message': hash_content(note['Message'].encode('utf-8')),
                    'pdk_encrypted_message': encrypt_content(note['Message'].encode('utf-8')),
                    'pdk_length_message': len(note['Message']),
                    'create_time': note['Create Time'],
                    'note_type': report_type,
                }

                DataPoint.objects.create_data_point('pdk-external-snapchat-support-note', request_identifier, pdk_note, user_agent='Passive Data Kit External Importer', created=created)

                create_engagement_event(source='snapchat', identifier=request_identifier, outgoing_engagement=1.0, engagement_type='support', start=created)


def process_account_events(request_identifier, json_string):
    account_events = json.loads(json_string)

    for login in account_events['Login History']:
        created = arrow.get(login['Created'], 'YYYY-MM-DD HH:mm:ss ZZZ').datetime

        if include_data(request_identifier, created, login):
            pdk_login = {
                'pdk_hashed_ip_address': hash_content(login['IP']),
                'pdk_encrypted_ip_address': encrypt_content(login['IP'].encode('utf-8')),
                'country': login['Country'],
                'status': login['Status'],
                'device': login['Device'],
                'created': login['Created']
            }

            DataPoint.objects.create_data_point('pdk-external-snapchat-login', request_identifier, pdk_login, user_agent='Passive Data Kit External Importer', created=created)

            create_engagement_event(source='snapchat', identifier=request_identifier, outgoing_engagement=0.5, engagement_type='login', start=created)

def process_story_history(request_identifier, json_string):
    story_history = json.loads(json_string)

    if 'Your Story Views' in story_history:
        for view in story_history['Your Story Views']:
            if 'View' in view:
                created = arrow.get(view['View Date'], 'YYYY-MM-DD HH:mm:ss ZZZ').datetime

                if include_data(request_identifier, created, view):
                    view['pdk_hashed_viewer'] = hash_content(view['View'])

                    del view['View']


                    DataPoint.objects.create_data_point('pdk-external-snapchat-story-view', request_identifier, view, user_agent='Passive Data Kit External Importer', created=created)

                    create_engagement_event(source='snapchat', identifier=request_identifier, incoming_engagement=0.0, engagement_type='story-view', start=created)

    if 'Friend and Public Story Views' in story_history:
        for view in story_history['Friend and Public Story Views']:
            if 'View' in view:
                created = arrow.get(view['View Date'], 'YYYY-MM-DD HH:mm:ss ZZZ').datetime

                if include_data(request_identifier, created, view):
                    view['pdk_hashed_viewer'] = hash_content(view['View'])

                    del view['View']

                    DataPoint.objects.create_data_point('pdk-external-snapchat-story-view', request_identifier, view, user_agent='Passive Data Kit External Importer', created=created)

                    create_engagement_event(source='snapchat', identifier=request_identifier, outgoing_engagement=0.0, engagement_type='story-view', start=created)

def process_friends_events(request_identifier, json_string): # pylint: disable=too-many-branches
    friends_history = json.loads(json_string)

    if 'Deleted Friends' in friends_history:
        for action in friends_history['Deleted Friends']:
            if 'Creation Timestamp' in action:
                created = arrow.get(action['Creation Timestamp'], 'YYYY-MM-DD HH:mm:ss ZZZ').datetime

                if include_data(request_identifier, created, action):
                    action['pdk_encrypted_username'] = encrypt_content(action['Username'].encode('utf-8'))
                    del action['Username']

                    action['pdk_encrypted_display_name'] = encrypt_content(action['Display Name'].encode('utf-8'))
                    del action['Display Name']

                    DataPoint.objects.create_data_point('pdk-external-snapchat-deleted-contact', request_identifier, action, user_agent='Passive Data Kit External Importer', created=created)

                    create_engagement_event(source='snapchat', identifier=request_identifier, outgoing_engagement=0.5, engagement_type='deleted-contact', start=created)

    if 'Friend Requests Sent' in friends_history:
        for action in friends_history['Friend Requests Sent']:
            if 'Creation Timestamp' in action:
                created = arrow.get(action['Creation Timestamp'], 'YYYY-MM-DD HH:mm:ss ZZZ').datetime

                if include_data(request_identifier, created, action):
                    action['pdk_encrypted_username'] = encrypt_content(action['Username'].encode('utf-8'))
                    del action['Username']

                    action['pdk_encrypted_display_name'] = encrypt_content(action['Display Name'].encode('utf-8'))
                    del action['Display Name']

                    DataPoint.objects.create_data_point('pdk-external-snapchat-requested-contact', request_identifier, action, user_agent='Passive Data Kit External Importer', created=created)

                    create_engagement_event(source='snapchat', identifier=request_identifier, outgoing_engagement=0.5, engagement_type='deleted-contact', start=created)

    if 'Blocked Users' in friends_history:
        for action in friends_history['Blocked Users']:
            if 'Creation Timestamp' in action:
                created = arrow.get(action['Creation Timestamp'], 'YYYY-MM-DD HH:mm:ss ZZZ').datetime

                if include_data(request_identifier, created, action):
                    action['pdk_encrypted_username'] = encrypt_content(action['Username'].encode('utf-8'))
                    del action['Username']

                    action['pdk_encrypted_display_name'] = encrypt_content(action['Display Name'].encode('utf-8'))
                    del action['Display Name']

                    DataPoint.objects.create_data_point('pdk-external-snapchat-blocked-contact', request_identifier, action, user_agent='Passive Data Kit External Importer', created=created)

                    create_engagement_event(source='snapchat', identifier=request_identifier, outgoing_engagement=0.5, engagement_type='blocked-contact', start=created)

    if 'Friends' in friends_history:
        for action in friends_history['Friends']:
            if 'Creation Timestamp' in action:
                created = arrow.get(action['Creation Timestamp'], 'YYYY-MM-DD HH:mm:ss ZZZ').datetime

                if include_data(request_identifier, created, action):
                    action['pdk_encrypted_username'] = encrypt_content(action['Username'].encode('utf-8'))
                    del action['Username']

                    action['pdk_encrypted_display_name'] = encrypt_content(action['Display Name'].encode('utf-8'))
                    del action['Display Name']

                    DataPoint.objects.create_data_point('pdk-external-snapchat-added-contact', request_identifier, action, user_agent='Passive Data Kit External Importer', created=created)

                    create_engagement_event(source='snapchat', identifier=request_identifier, outgoing_engagement=0.5, engagement_type='added-contact', start=created)

def process_scan_events(request_identifier, json_string):
    scan_history = json.loads(json_string)

    if 'Scan History' in scan_history:
        for action in scan_history['Scan History']:
            created = arrow.get(action['Date'], 'YYYY-MM-DD HH:mm:ss ZZZ').datetime

            if include_data(request_identifier, created, action):
                action['pdk_encrypted_scan_image'] = encrypt_content(action['Scan Image'].encode('utf-8'))
                del action['Scan Image']

                action['pdk_encrypted_location'] = encrypt_content(action['Location'].encode('utf-8'))
                del action['Location']

                DataPoint.objects.create_data_point('pdk-external-snapchat-scan', request_identifier, action, user_agent='Passive Data Kit External Importer', created=created)

                create_engagement_event(source='snapchat', identifier=request_identifier, outgoing_engagement=0.5, engagement_type='scanned-code', start=created)

def process_snap_map_events(request_identifier, json_string):
    map_history = json.loads(json_string)

    if 'Snap Map Places History' in map_history:
        for action in map_history['Snap Map Places History']:
            created = arrow.get(action['Date'], 'YYYY-MM-DD HH:mm:ss ZZZ').datetime

            if include_data(request_identifier, created, action):
                action['pdk_encrypted_place_location'] = encrypt_content(action['Place Location'].encode('utf-8'))
                del action['Place Location']

                action['pdk_encrypted_place'] = encrypt_content(action['Place'].encode('utf-8'))
                del action['Place']

                DataPoint.objects.create_data_point('pdk-external-snapchat-place-share', request_identifier, action, user_agent='Passive Data Kit External Importer', created=created)

                create_engagement_event(source='snapchat', identifier=request_identifier, outgoing_engagement=1.0, engagement_type='shared-place', start=created)

def process_talk_events(request_identifier, json_string): # pylint: disable=too-many-branches
    talk_history = json.loads(json_string)

    if 'Incoming Calls' in talk_history:
        for action in talk_history['Incoming Calls']:
            created = arrow.get(action['Date & Time'], 'YYYY-MM-DD HH:mm:ss ZZZ').datetime

            if include_data(request_identifier, created, action):
                action['pdk_encrypted_city'] = encrypt_content(action['City'].encode('utf-8'))
                del action['City']

                DataPoint.objects.create_data_point('pdk-external-snapchat-call', request_identifier, action, user_agent='Passive Data Kit External Importer', created=created)

                create_engagement_event(source='snapchat', identifier=request_identifier, incoming_engagement=1.0, engagement_type='call', start=created, duration=action['Length (sec)'])

    if 'Outgoing Calls' in talk_history:
        for action in talk_history['Outgoing Calls']:
            created = arrow.get(action['Date & Time'], 'YYYY-MM-DD HH:mm:ss ZZZ').datetime

            if include_data(request_identifier, created, action):
                action['pdk_encrypted_city'] = encrypt_content(action['City'].encode('utf-8'))
                del action['City']

                DataPoint.objects.create_data_point('pdk-external-snapchat-call', request_identifier, action, user_agent='Passive Data Kit External Importer', created=created)

                create_engagement_event(source='snapchat', identifier=request_identifier, outgoing_engagement=1.0, engagement_type='call', start=created, duration=action['Length (sec)'])

    if 'Completed Calls' in talk_history:
        for action in talk_history['Completed Calls']:
            created = arrow.get(action['Date & Time'], 'YYYY-MM-DD HH:mm:ss ZZZ').datetime

            if include_data(request_identifier, created, action):
                action['pdk_encrypted_city'] = encrypt_content(action['City'].encode('utf-8'))
                del action['City']

                DataPoint.objects.create_data_point('pdk-external-snapchat-call', request_identifier, action, user_agent='Passive Data Kit External Importer', created=created)

                create_engagement_event(source='snapchat', identifier=request_identifier, outgoing_engagement=1.0, engagement_type='call', start=created, duration=action['Length (sec)'])

    if 'Chat Sessions' in talk_history:
        print('CHAT SESSIONS: ' + json.dumps(talk_history['Chat Sessions'], indent=2))

    if 'Game Sessions' in talk_history:
        for action in talk_history['Game Sessions']:
            created = arrow.get(action['Date & Time'], 'YYYY-MM-DD HH:mm:ss ZZZ').datetime

            if include_data(request_identifier, created, action):
                action['pdk_encrypted_city'] = encrypt_content(action['City'].encode('utf-8'))
                del action['City']

                DataPoint.objects.create_data_point('pdk-external-snapchat-call', request_identifier, action, user_agent='Passive Data Kit External Importer', created=created)

                create_engagement_event(source='snapchat', identifier=request_identifier, incoming_engagement=1.0, outgoing_engagement=1.0, engagement_type='call', start=created, duration=action['Length (sec)'])

def process_search_history(request_identifier, json_string):
    searches = json.loads(json_string)

    for search in searches:
        created = arrow.get(search['Date and time (hourly)'], 'YYYY-MM-DD HH:mm:ss ZZZ').datetime

        if include_data(request_identifier, created, search):
            search['pdk_encrypted_query'] = encrypt_content(search['Search Term'].encode('utf-8'))
            del search['Search Term']

            search['pdk_encrypted_location'] = encrypt_content(search['Location'].encode('utf-8'))
            del search['Location']

            DataPoint.objects.create_data_point('pdk-external-snapchat-search', request_identifier, search, user_agent='Passive Data Kit External Importer', created=created)

            create_engagement_event(source='snapchat', identifier=request_identifier, outgoing_engagement=0.5, engagement_type='search', start=created)

def import_data(request_identifier, path): # pylint: disable=too-many-branches
    content_bundle = zipfile.ZipFile(path)

    skip_files = [
        'json/account_history.json',
        'json/bitmoji.json ',
        'json/bitmoji_kit_user.json',
        'json/community_lenses.json',
        'json/connected_apps.json',
        'json/email_campaign_history.json',
        'json/in_app_reports.json',
        'json/location_history.json',
        'json/ranking.json',
        'json/snap_pro.json',
        'json/subscriptions.json',
        'json/terms_history.json',
        'json/user_profile.json',
    ]

    for content_file in content_bundle.namelist():
        try:
            if content_file.endswith('/'):
                pass
            elif content_file.endswith('.html'):
                pass
            elif content_file in skip_files:
                pass
            elif re.match(r'.*\/talk_history\.json$', content_file):
                process_talk_events(request_identifier, content_bundle.open(content_file).read())
            elif re.match(r'.*\/snap_map_places_history\.json$', content_file):
                process_snap_map_events(request_identifier, content_bundle.open(content_file).read())
            elif re.match(r'.*\/scans\.json$', content_file):
                process_scan_events(request_identifier, content_bundle.open(content_file).read())
            elif re.match(r'.*\/friends\.json$', content_file):
                process_friends_events(request_identifier, content_bundle.open(content_file).read())
            elif re.match(r'.*\/account\.json$', content_file):
                process_account_events(request_identifier, content_bundle.open(content_file).read())
            elif re.match(r'.*\/chat_history\.json$', content_file):
                process_chat_history(request_identifier, content_bundle.open(content_file).read())
            elif re.match(r'.*\/memories_history\.json$', content_file):
                process_memories_history(request_identifier, content_bundle.open(content_file).read())
            elif re.match(r'.*\/shared_story\.json$', content_file):
                process_shared_story(request_identifier, content_bundle.open(content_file).read())
            elif re.match(r'.*\/snap_history\.json$', content_file):
                process_snap_history(request_identifier, content_bundle.open(content_file).read())
            elif re.match(r'.*\/support_note\.json$', content_file):
                process_support_notes(request_identifier, content_bundle.open(content_file).read())
            elif re.match(r'.*\/search_history\.json$', content_file):
                process_search_history(request_identifier, content_bundle.open(content_file).read())
            elif re.match(r'.*\/story_history\.json$', content_file):
                process_story_history(request_identifier, content_bundle.open(content_file).read())
            else:
                print('SNAPCHAT[' + request_identifier + ']: Unable to process: ' + content_file + ' -- ' + str(content_bundle.getinfo(content_file).file_size))
        except: # pylint: disable=bare-except
            traceback.print_exc()
            return False

    return True

def external_data_metadata(generator_identifier, point):
    if generator_identifier.startswith('pdk-external-snapchat') is False:
        return None

    metadata = {}
    metadata['service'] = 'Snapchat'
    metadata['event'] = generator_identifier

    if generator_identifier == 'pdk-external-snapchat-chat-received':
        metadata['event'] = 'Direct Message'
        metadata['direction'] = 'Incoming'

        properties = point.fetch_properties()

        metadata['media_type'] = properties['media_type']
    elif generator_identifier == 'pdk-external-snapchat-chat-sent':
        metadata['event'] = 'Direct Message'
        metadata['direction'] = 'Outgoing'

        properties = point.fetch_properties()

        metadata['media_type'] = properties['media_type']
    elif generator_identifier == 'pdk-external-snapchat-snap-received':
        metadata['event'] = 'Upload Post'
        metadata['direction'] = 'Incoming'

        properties = point.fetch_properties()
        metadata['media_type'] = properties['media_type']

    elif generator_identifier == 'pdk-external-snapchat-snap-sent':
        metadata['event'] = 'Upload Post'
        metadata['direction'] = 'Outgoing'

        properties = point.fetch_properties()
        metadata['media_type'] = properties['media_type']

    return metadata
