from django import template

register = template.Library()

@register.simple_tag(takes_context=True)
def fetch_source_instructions(context, data_source):
    return data_source.instruction_content(context)
