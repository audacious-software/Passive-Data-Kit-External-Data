# pylint: disable=line-too-long

import json
import re
import traceback
import zipfile

import arrow

from passive_data_kit.models import DataPoint

from ..utils import hash_content, encrypt_content

def process_dashboard(request_identifier, dashboard):
    for item in dashboard:
        created = arrow.get(item['serve_time']).datetime

        DataPoint.objects.create_data_point('pdk-external-tumblr-dashboard-item', request_identifier, item, user_agent='Passive Data Kit External Importer', created=created)

def process_unfollows(request_identifier, unfollows):
    for item in unfollows:
        pdk_item = {
            'pdk_hashed_blog_name': hash_content(item['blog_name']),
            'pdk_encrypted_blog_name': encrypt_content(item['blog_name'].encode('utf-8')),
            'pdk_length_blog_name': len(item['blog_name']),
            'timestamp': item['timestamp'],
        }

        created = arrow.get(item['timestamp']).datetime

        DataPoint.objects.create_data_point('pdk-external-tumblr-unfollow', request_identifier, pdk_item, user_agent='Passive Data Kit External Importer', created=created)

def process_ads_served(request_identifier, ads_served):
    for item in ads_served:
        pdk_item = {
            'pdk_hashed_post_url': hash_content(item['post_url']),
            'pdk_encrypted_post_url': encrypt_content(item['post_url'].encode('utf-8')),
            'pdk_hashed_placement_id': hash_content(item['placement_id']),
            'pdk_encrypted_placement_id': encrypt_content(item['placement_id'].encode('utf-8')),
            'serve_time': item['serve_time'],
            'viewed': item['viewed'],
            'interacted': item['interacted'],
        }

        try:
            created = arrow.get(item['serve_time']).datetime

            DataPoint.objects.create_data_point('pdk-external-tumblr-ads-served', request_identifier, pdk_item, user_agent='Passive Data Kit External Importer', created=created)
        except arrow.ParserError:
            print '[' + request_identifier + ']: Skipped ad_served: Unable to parse date: "' + str(item['serve_time']) + '".'

def process_active_times(request_identifier, active_times):
    for item in active_times:
        pdk_item = {
            'timestamp': item,
        }

        created = arrow.get(item).datetime

        DataPoint.objects.create_data_point('pdk-external-tumblr-active-time', request_identifier, pdk_item, user_agent='Passive Data Kit External Importer', created=created)

def process_api_applications_used(request_identifier, api_applications_used):
    for item in api_applications_used:
        created = arrow.get(item['session_created_time']).datetime

        DataPoint.objects.create_data_point('pdk-external-tumblr-api-session', request_identifier, item, user_agent='Passive Data Kit External Importer', created=created)

def process_push_notifications(request_identifier, notifications):
    for item in notifications:
        pdk_item = {
            'pdk_hashed_from_blog': hash_content(item['from_blog']),
            'pdk_encrypted_from_blog': encrypt_content(item['from_blog'].encode('utf-8')),
            'pdk_hashed_to_blog': hash_content(item['to_blog']),
            'pdk_encrypted_to_blog': encrypt_content(item['to_blog'].encode('utf-8')),
            'timestamp': item['timestamp'],
            'notification_type': item['notification_type'],
            'device': item['device'],
            'follow_up_action': item['follow_up_action'],
            'app_version': item['app_version'],
        }

        created = arrow.get(item['timestamp']).datetime

        DataPoint.objects.create_data_point('pdk-external-tumblr-push-notification-open', request_identifier, pdk_item, user_agent='Passive Data Kit External Importer', created=created)

def process_push_notification_settings(request_identifier, settings): # pylint: disable=invalid-name
    for item in settings:
        created = arrow.get(item['timestamp']).datetime

        DataPoint.objects.create_data_point('pdk-external-tumblr-push-notification-setting', request_identifier, item, user_agent='Passive Data Kit External Importer', created=created)

def process_payload(request_identifier, payload_json):
    payloads = json.loads(payload_json)

    for payload in payloads:
        data = payload['data']

        if 'dashboard' in data:
            process_dashboard(request_identifier, data['dashboard'])

        if 'unfollows' in data:
            process_unfollows(request_identifier, data['unfollows'])

        if 'ads_analytics' in data:
            process_ads_served(request_identifier, data['ads_analytics'])

        if 'active_times' in data:
            process_active_times(request_identifier, data['active_times'])

        if 'api_applications_used' in data:
            process_api_applications_used(request_identifier, data['api_applications_used'])

        if 'push_notification_opens' in data:
            process_push_notifications(request_identifier, data['push_notification_opens'])

        if 'push_notification_settings' in data:
            process_push_notification_settings(request_identifier, data['push_notification_settings'])


def import_data(request_identifier, path):
    content_bundle = zipfile.ZipFile(path)

    for content_file in content_bundle.namelist():
        try:
            if content_file.endswith('/'):
                pass
            elif re.match(r'^payload.*\.json', content_file):
                process_payload(request_identifier, content_bundle.open(content_file).read())
            else:
                print '[' + request_identifier + ']: Unable to process: ' + content_file
        except: # pylint: disable=bare-except
            traceback.print_exc()
            return False

    return True

def external_data_metadata(generator_identifier, point): # pylint: disable=unused-argument
    if generator_identifier.startswith('pdk-external-tumblr') is False:
        return None

    metadata = {}
    metadata['service'] = 'Tumblr'
    metadata['event'] = generator_identifier

    if generator_identifier == 'pdk-external-tumblr-unfollow':
        metadata['event'] = 'Unfollow'
        metadata['direction'] = 'Outgoing'
        metadata['media_type'] = 'Relationship'
    elif generator_identifier == 'pdk-external-tumblr-ads-served':
        metadata['event'] = 'Viewed Ad'
        metadata['direction'] = 'Incoming'
        metadata['media_type'] = 'Advertisement'

    return metadata
