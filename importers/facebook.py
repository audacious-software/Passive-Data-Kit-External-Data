# pylint: disable=line-too-long

import json
import re
import traceback
import zipfile

import arrow

from passive_data_kit.models import DataPoint

from ..utils import hash_content, encrypt_content

def process_comments(request_identifier, comments_raw):
    comments = json.loads(comments_raw)

    for comment in comments['comments']:
        created = arrow.get(comment['timestamp']).datetime

        if 'title' in comment:
            comment['pdk_encrypted_title'] = encrypt_content(comment['title'].encode('utf-8'))
            comment['pdk_length_title'] = len(comment['title'])

            del comment['title']

        if 'data' in comment:
            data = comment['data']

            for datum in data:
                if 'comment' in datum:
                    comment_obj = datum['comment']

                    if 'comment' in comment_obj:
                        comment_obj['pdk_encrypted_comment'] = encrypt_content(comment_obj['comment'].encode('utf-8'))
                        comment_obj['pdk_length_comment'] = len(comment_obj['comment'])

                        del comment_obj['comment']

                    if 'author' in comment_obj:
                        comment_obj['pdk_hashed_author'] = hash_content(comment_obj['author'])
                        comment_obj['pdk_encrypted_author'] = encrypt_content(comment_obj['author'].encode('utf-8'))

                        del comment_obj['author']

        DataPoint.objects.create_data_point('pdk-external-facebook-comment', request_identifier, comment, user_agent='Passive Data Kit External Importer', created=created)


def process_posts(request_identifier, posts_raw): # pylint: disable=too-many-branches, too-many-statements
    posts = json.loads(posts_raw)

    source = 'user'

    if isinstance(posts, dict):
        source = 'others'

        if 'wall_posts_sent_to_you' in posts and 'activity_log_data' in posts['wall_posts_sent_to_you']:
            posts = posts['wall_posts_sent_to_you']['activity_log_data']

    for post in posts: # pylint: disable=too-many-nested-blocks
        created = arrow.get(post['timestamp']).datetime

        if 'title' in post:
            post['pdk_encrypted_title'] = encrypt_content(post['title'].encode('utf-8'))
            post['pdk_length_title'] = len(post['title'])

            del post['title']

        if 'data' in post:
            for datum in post['data']:
                if 'post' in datum:
                    datum['pdk_encrypted_post'] = encrypt_content(datum['post'].encode('utf-8'))
                    datum['pdk_length_post'] = len(datum['post'])

                    del datum['post']

        if 'attachments' in post:
            for attachment in post['attachments']:
                if 'data' in attachment:
                    for datum in attachment['data']:
                        if 'event' in datum:
                            event = datum['event']

                            if 'name' in event:
                                event['pdk_encrypted_name'] = encrypt_content(event['name'].encode('utf-8'))
                                event['pdk_length_name'] = len(event['name'])

                                del event['name']

                            if 'description' in event:
                                event['pdk_encrypted_description'] = encrypt_content(event['description'].encode('utf-8'))
                                event['pdk_length_description'] = len(event['description'])

                                del event['description']

                            if 'place' in event:
                                place_str = json.dumps(event['place'], indent=2)
                                event['pdk_encrypted_place'] = encrypt_content(place_str.encode('utf-8'))

                                del event['place']

                        if 'external_context' in datum:
                            external_context = datum['external_context']

                            if 'url' in external_context:
                                external_context['pdk_encrypted_url'] = encrypt_content(external_context['url'].encode('utf-8'))
                                external_context['pdk_length_url'] = len(external_context['url'])

                                del external_context['url']


                        if 'media' in datum:
                            media = datum['media']

                            if 'title' in media:
                                media['pdk_encrypted_title'] = encrypt_content(media['title'].encode('utf-8'))
                                media['pdk_length_title'] = len(media['title'])

                                del media['title']

                            if 'description' in media:
                                media['pdk_encrypted_description'] = encrypt_content(media['description'].encode('utf-8'))
                                media['pdk_length_description'] = len(media['description'])

                                del media['description']

                            if 'uri' in media:
                                media['pdk_encrypted_uri'] = encrypt_content(media['uri'].encode('utf-8'))
                                media['pdk_length_uri'] = len(media['uri'])

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

        DataPoint.objects.create_data_point('pdk-external-facebook-post', request_identifier, post, user_agent='Passive Data Kit External Importer', created=created)


def import_data(request_identifier, path):
    content_bundle = zipfile.ZipFile(path)

    for content_file in content_bundle.namelist():
        try:
            if content_file.endswith('/'):
                pass
            elif re.match(r'^comments\/.*\.json', content_file):
                process_comments(request_identifier, content_bundle.open(content_file).read())
            elif re.match(r'^posts\/.*\.json', content_file):
                process_posts(request_identifier, content_bundle.open(content_file).read())
            else:
                print '[' + request_identifier + ']: Unable to process: ' + content_file
        except: # pylint: disable=bare-except
            traceback.print_exc()
            return False

    return True
