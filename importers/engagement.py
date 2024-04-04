# pylint: disable=line-too-long

import importlib

from django.conf import settings

def update_data_type_definition(definition, override_existing=False): # pylint: disable=unused-argument
    if 'type' in definition:
        definition['type']['pdk_variable_name'] = 'Engagement type'
        definition['type']['pdk_variable_description'] = 'Type of the engagement.'
        definition['type']['pdk_codebook_group'] = 'Passive Data Kit: External Data'
        definition['type']['pdk_codebook_order'] = 1

        definition['type']['pdk_codebook_observed_descriptions'] = {
            'post': 'Posted content or media to share with a limited or open audience of others.',
            'reaction': 'Reacted to content in the form of likes, loves, or other sentiments (emojis).',
            'comment': 'Added a comment to content posted by themselves or others.',
            'video': 'Viewed or shared video content.',
            'group': 'Viewed or interacted with a group page or other community destination.',
            'profile': 'Viewed or interacted with their own or another\'s profile page.',
            'advertising': 'Exposed to an advertisement.',
            'shopping': 'Exposed to an advertisement.',
            'link': 'Viewed or interacted with their own or another\'s link or article.',
            'event': 'Viewed or interacted with their own or another\'s event page.',
            'page': 'Viewed or interacted with their own or another\'s page.',
            'search': 'Searched for social media content.',
            'message': 'Sent or received a private direct message to another or a group.'
        }

    if 'incoming_engagement' in definition:
        definition['incoming_engagement']['pdk_variable_name'] = 'Incoming engagement'
        definition['incoming_engagement']['pdk_variable_description'] = 'Measures the incoming enagement, which consists of communication or interactions originating from outside sources or contacts, directed to the source.'
        definition['incoming_engagement']['types'] = ['integer', 'real']
        # definition['incoming_engagement']['range'] = [0.0, 1.0]
        definition['incoming_engagement']['pdk_codebook_group'] = 'Passive Data Kit: External Data'
        definition['incoming_engagement']['pdk_codebook_order'] = 2

    if 'outgoing_engagement' in definition:
        definition['outgoing_engagement']['pdk_variable_name'] = 'Outgoing engagement'
        definition['outgoing_engagement']['pdk_variable_description'] = 'Measures the outgoing enagement, which consists of communication or interactions originating from data source, directed to the outside contacts or third-parties.'
        definition['outgoing_engagement']['types'] = ['integer', 'real']
        # definition['outgoing_engagement']['range'] = [0.0, 1.0]
        definition['outgoing_engagement']['pdk_codebook_group'] = 'Passive Data Kit: External Data'
        definition['outgoing_engagement']['pdk_codebook_order'] = 2

    if 'duration' in definition:
        definition['duration']['pdk_variable_name'] = 'Engagement duration'
        definition['duration']['pdk_variable_description'] = 'Duration of the interaction, measured in seconds.'
        definition['duration']['types'] = ['integer', 'real']
        definition['duration']['pdk_codebook_group'] = 'Passive Data Kit: External Data'

        if 'observed' in definition['duration']:
            min_value = None
            max_value = None

            for value in definition['duration']['observed']:
                numeric_value = float(value)

                if min_value is None:
                    min_value = numeric_value

                if max_value is None:
                    max_value = numeric_value

                min_value = min(min_value, numeric_value)

                max_value = max(max_value, numeric_value)

            del definition['duration']['observed']

            definition['duration']['range'] = [min_value, max_value]

        definition['duration']['pdk_codebook_order'] = 4

    if 'direction' in definition:
        definition['direction']['pdk_variable_name'] = 'Engagement direction'
        definition['direction']['pdk_variable_description'] = 'Direction of the engagement. *outgoing* indicates that the user is initiating and maintaining engagement with others, while *incoming* indicates that others are initiating and maintaining engagement with the user.'
        definition['direction']['pdk_codebook_group'] = 'Passive Data Kit: External Data (Deprecated)'
        definition['direction']['pdk_codebook_deprecated'] = True

    if 'engagement_direction' in definition:
        definition['engagement_direction']['pdk_variable_name'] = 'Engagement direction'
        definition['engagement_direction']['pdk_variable_description'] = 'Direction of the engagement. *outgoing* indicates that the user is initiating and maintaining engagement with others, while *incoming* indicates that others are initiating and maintaining engagement with the user.'
        definition['engagement_direction']['pdk_codebook_group'] = 'Passive Data Kit: External Data (Deprecated)'
        definition['engagement_direction']['pdk_codebook_deprecated'] = True

def data_type_name(definition):
    for observed in definition['passive-data-metadata.generator-id']['observed']:
        if observed.startswith('pdk-external-engagement-'):
            tokens = observed.split('-')

            if len(tokens) > 3:
                for app in settings.INSTALLED_APPS:
                    try:
                        importer = importlib.import_module(app + '.importers.' + tokens[3])

                        return importer.data_type_name(definition)
                    except ImportError:
                        pass
                    except AttributeError:
                        pass

    return None

def data_type_category(definition):
    for observed in definition['passive-data-metadata.generator-id']['observed']:
        if observed.startswith('pdk-external-engagement-'):
            tokens = observed.split('-')

            if len(tokens) > 3:
                for app in settings.INSTALLED_APPS:
                    try:
                        importer = importlib.import_module(app + '.importers.' + tokens[3])

                        return importer.data_type_category(definition)
                    except ImportError:
                        pass
                    except AttributeError:
                        pass

    return None
