# pylint: disable=line-too-long

import string

SKIP_FIELD_NAMES = (
    'url',
)

def annotate(content, field_name=None):
    if field_name in SKIP_FIELD_NAMES:
        return {}

    annotations = {}

    annotation_field = 'pdk_word_count'

    if field_name is not None:
        annotation_field = 'pdk_word_count_' + field_name

    non_punc = set(string.punctuation)

    content_nopunc = ''.join(ch for ch in content if ch not in non_punc)

    while '  ' in content_nopunc:
        content_nopunc = content_nopunc.replace('  ', ' ')

    content_nopunc = content_nopunc.strip()

    words = content_nopunc.split(' ')

    annotations[annotation_field] = len(words)

    return annotations


def fetch_annotation_fields():
    return ['pdk_word_count']


def fetch_annotations(properties):
    if isinstance(properties, dict) is False:
        return 0

    if 'pdk_word_count' in properties:
        return {
            'pdk_word_count': properties['pdk_word_count']
        }

    max_length = 0

    for key in properties:
        if key.startswith('pdk_word_count_'):
            if properties[key] > max_length:
                max_length = properties[key]
        else:
            value = properties[key]

            if isinstance(value, dict):
                dict_max_length = fetch_annotations(value)

                if 'pdk_word_count' in dict_max_length and dict_max_length['pdk_word_count'] > max_length:
                    max_length = dict_max_length['pdk_word_count']
            if isinstance(value, list):
                for item in value:
                    list_max_length = fetch_annotations(item)

                    if 'pdk_word_count' in list_max_length and list_max_length['pdk_word_count'] > max_length:
                        max_length = list_max_length['pdk_word_count']

        return {
            'pdk_word_count': max_length
        }
