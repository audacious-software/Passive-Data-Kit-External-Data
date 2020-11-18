
def annotate(content, field_name=None):
    annotations = {}

    annotation_field = 'pdk_length'

    if field_name is not None:
        annotation_field = 'pdk_length_' + field_name

    annotations[annotation_field] = len(content)

    return annotations
