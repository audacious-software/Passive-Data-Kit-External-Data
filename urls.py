# pylint: disable=line-too-long

from django.conf.urls import url

from .views import pdk_external_request_data, pdk_external_generate_identifier, pdk_external_upload_data, pdk_external_request_data_help

urlpatterns = [
    url(r'^identifier.json$', pdk_external_generate_identifier, name='pdk_external_generate_identifier'),
    url(r'^start$', pdk_external_request_data, name='pdk_external_request_data'),
    url(r'^help/(?P<source>.+)$', pdk_external_request_data_help, name='pdk_external_request_data_help'),
    url(r'^upload/(?P<token>.+)$', pdk_external_upload_data, name='pdk_external_upload_data'),
]
