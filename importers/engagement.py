# pylint: disable=line-too-long

def update_data_type_definition(definition):
    if 'type' in definition:
        definition['type']['pdk_variable_name'] = 'Engagement type'
        definition['type']['pdk_variable_description'] = 'Type of the engagement.'
        definition['type']['pdk_codebook_group'] = 'Passive Data Kit: External Data'
        definition['type']['pdk_codebook_order'] = 1

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

                if numeric_value < min_value:
                    min_value = numeric_value

                if numeric_value > max_value:
                    max_value = numeric_value

            del definition['duration']['observed']

            definition['duration']['range'] = [min_value, max_value]

        definition['duration']['pdk_codebook_order'] = 4
