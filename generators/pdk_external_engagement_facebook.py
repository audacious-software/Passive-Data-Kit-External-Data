# pylint: disable=line-too-long, no-member

import io
import json
import time

from django.conf import settings
from django.template.loader import render_to_string

def generator_name(identifier): # pylint: disable=unused-argument
    return 'Facebook Engagement'

def visualization(source, generator): # pylint: disable=unused-argument
    filename = settings.MEDIA_ROOT + '/pdk_visualizations/' + source.identifier + '/pdk-external-engagement-facebook/events.json'


    with io.open(filename, encoding='utf-8') as infile:
        data = json.load(infile)

        context = {}

        context['external_source_name'] = generator_name(generator)

        context['events'] = data

        return render_to_string('pdk_external_engagement_viz_template.html', context)

    return None

def compile_visualization(identifier, points, folder): # pylint: disable=unused-argument
    active = []
    passive = []
    unknown = []

    for point in points:
        engagement_bin = unknown

        metadata = point.fetch_properties()

        if 'engagement_level' in metadata:
            if metadata['engagement_level'] > 0:
                engagement_bin = active
            else:
                engagement_bin = passive
        else:
            if point.secondary_identifier == 'active':
                engagement_bin = active
            elif point.secondary_identifier == 'passive':
                engagement_bin = passive

        timestamp = time.mktime(point.created.timetuple())

        engagement_bin.append({
            'timestamp': timestamp,
            'event': metadata['type'],
            'level': metadata['engagement_level'],
        })

    active.sort(key=lambda item: item['timestamp'])
    passive.sort(key=lambda item: item['timestamp'])
    unknown.sort(key=lambda item: item['timestamp'])

    timestamps = {
        'active': active,
        'passive': passive,
        'unknown': unknown
    }

    with io.open(folder + '/events.json', 'w', encoding='utf-8') as outfile:
        json.dump(timestamps, outfile, indent=2)
