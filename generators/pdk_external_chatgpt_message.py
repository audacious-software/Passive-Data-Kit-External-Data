def generator_name(identifier): # pylint: disable=unused-argument
    return 'Chat GPT Message'

def update_data_type_definition(definition):
    definition['pdk_description'] = 'Represents one message in a dialog with Chat GPT.'

    if 'message_id' in definition:
        definition['message_id']['pdk_variable_name'] = 'Message ID'
        definition['message_id']['pdk_variable_description'] = 'Internal ID assigned to message by Chat GPT'
        definition['message_id']['pdk_codebook_group'] = 'Passive Data Kit: Chat GPT'
        definition['message_id']['pdk_codebook_order'] = 0
        definition['message_id']['is_freetext'] = True

    if 'author' in definition:
        definition['author']['pdk_variable_name'] = 'Message Author'
        definition['author']['pdk_variable_description'] = 'Party sending the message.'
        definition['author']['pdk_codebook_group'] = 'Passive Data Kit: Chat GPT'
        definition['author']['pdk_codebook_order'] = 1

    if 'pdk_encrypted_content' in definition:
        definition['pdk_encrypted_content']['pdk_variable_name'] = 'Message Content (Encrypted)'
        definition['pdk_encrypted_content']['pdk_variable_description'] = 'Content of the message sent this turn.'
        definition['pdk_encrypted_content']['pdk_codebook_group'] = 'Passive Data Kit: Chat GPT'
        definition['pdk_encrypted_content']['pdk_codebook_order'] = 2

    if 'pdk_encrypted_conversation_title' in definition:
        definition['pdk_encrypted_conversation_title']['pdk_variable_name'] = 'Conversation Title (Encrypted)'
        definition['pdk_encrypted_conversation_title']['pdk_variable_description'] = 'Title of the conversation that this message is a part of.'
        definition['pdk_encrypted_conversation_title']['pdk_codebook_group'] = 'Passive Data Kit: Chat GPT'
        definition['pdk_encrypted_conversation_title']['pdk_codebook_order'] = 3

    if 'conversation_metadata.safe_urls' in definition:
        definition['conversation_metadata.safe_urls']['pdk_variable_name'] = 'URLs Consulted'
        definition['conversation_metadata.safe_urls']['pdk_variable_description'] = 'URLs consulted in the course of this conversation.'
        definition['conversation_metadata.safe_urls']['pdk_codebook_group'] = 'Passive Data Kit: Chat GPT'
        definition['conversation_metadata.safe_urls']['pdk_codebook_order'] = 4

    if 'conversation_metadata.create_time' in definition:
        definition['conversation_metadata.create_time']['pdk_variable_name'] = 'Conversation Creation Timestamp'
        definition['conversation_metadata.create_time']['pdk_variable_description'] = 'Creation timestamp of the conversation, expressed in seconds (Unix epoch).'
        definition['conversation_metadata.create_time']['pdk_codebook_group'] = 'Passive Data Kit: Chat GPT'
        definition['conversation_metadata.create_time']['pdk_codebook_order'] = 5

    if 'conversation_metadata.update_time' in definition:
        definition['conversation_metadata.update_time']['pdk_variable_name'] = 'Conversation Update Timestamp'
        definition['conversation_metadata.update_time']['pdk_variable_description'] = 'Update timestamp of the conversation, expressed in seconds (Unix epoch).'
        definition['conversation_metadata.update_time']['pdk_codebook_group'] = 'Passive Data Kit: Chat GPT'
        definition['conversation_metadata.update_time']['pdk_codebook_order'] = 5

    if 'pdk_length_content' in definition:
        definition['pdk_length_content']['pdk_codebook_group'] = 'Passive Data Kit: Extracted Features'
        definition['pdk_length_content']['pdk_codebook_order'] = 0

    if 'pdk_word_count_content' in definition:
        definition['pdk_word_count_content']['pdk_codebook_group'] = 'Passive Data Kit: Extracted Features'
        definition['pdk_word_count_content']['pdk_codebook_order'] = 1

    del definition['conversation_metadata']
