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
