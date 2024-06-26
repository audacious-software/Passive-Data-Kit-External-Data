# pylint: disable=line-too-long, wrong-import-position

import sys

if sys.version_info[0] > 2:
    from django.urls import re_path as url # pylint: disable=no-name-in-module
else:
    from django.conf.urls import url

from .views import pdk_external_request_data, pdk_external_generate_identifier, \
                   pdk_external_upload_data, pdk_external_request_data_help, \
                   pdk_external_request, pdk_external_email_opt_out, pdk_external_pending, \
                   pdk_external_uploads, pdk_download_upload

urlpatterns = [
    url(r'^identifier.json$', pdk_external_generate_identifier, name='pdk_external_generate_identifier'),
    url(r'^request$', pdk_external_request, name='pdk_external_request'),
    url(r'^opt-out/(?P<token>.+)$', pdk_external_email_opt_out, name='pdk_external_email_opt_out'),
    url(r'^start/(?P<token>.+)$', pdk_external_request_data, name='pdk_external_request_data_with_params'),
    url(r'^start$', pdk_external_request_data, name='pdk_external_request_data'),
    url(r'^help/(?P<source>.+)$', pdk_external_request_data_help, name='pdk_external_request_data_help'),
    url(r'^upload/(?P<token>.+)$', pdk_external_upload_data, name='pdk_external_upload_data'),
    url(r'^pending/(?P<identifier>.+).json$', pdk_external_pending, name='pdk_external_pending'),
    url(r'^uploads/(?P<identifier>.+).json$', pdk_external_uploads, name='pdk_external_uploads'),
    url(r'^download/(?P<file_id>.+)$', pdk_download_upload, name='pdk_download_upload'),
]
