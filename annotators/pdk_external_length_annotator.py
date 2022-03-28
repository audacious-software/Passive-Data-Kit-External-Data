# pylint: disable=line-too-long

import json

def annotate(content, field_name=None):
    annotations = {}

    annotation_field = 'pdk_length'

    if field_name is not None:
        annotation_field = 'pdk_length_' + field_name

    annotations[annotation_field] = len(content)

    return annotations


def fetch_annotation_fields():
    return ['pdk_length']


def fetch_annotations(properties):
    if isinstance(properties, dict) is False:
        return {}

    if 'pdk_length' in properties:
        return {
            'pdk_length': properties['pdk_length']
        }

    max_length = 0

    for key in properties:
        if key.startswith('pdk_length_') and ('url' in key.lower()) is False:
            if properties[key] > max_length:
                max_length = properties[key]
        else:
            value = properties[key]

            if isinstance(value, dict):
                dict_max_length = fetch_annotations(value)

                if 'pdk_length' in dict_max_length and dict_max_length['pdk_length'] > max_length:
                    max_length = dict_max_length['pdk_word_count']
            elif isinstance(value, list):
                for item in value:
                    list_max_length = fetch_annotations(item)

                    if 'pdk_length' in list_max_length and list_max_length['pdk_length'] > max_length:
                        max_length = list_max_length['pdk_length']

    return {
        'pdk_length': max_length
    }

def update_data_type_definition(definition):
    for key in definition.keys():
        if 'pdk_length_' in key:
            definition[key]['pdk_variable_name'] = 'Content length'
            definition[key]['pdk_variable_description'] = 'Counts the number of characters in the provided content.'
