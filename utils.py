#pylint: disable=line-too-long

import base64
import hashlib

from nacl.public import SealedBox, PublicKey
from nacl.secret import SecretBox

from django.conf import settings

from passive_data_kit.models import DataPoint

PENDING_POINTS_LIMIT = 1000

PENDING_POINTS = []

def hash_content(cleartext):
    sha512 = hashlib.sha512()

    sha512.update(settings.PDK_EXTERNAL_CONTENT_PUBLIC_KEY.encode('utf-8'))
    sha512.update(cleartext.encode('utf-8'))

    return sha512.hexdigest()

def encrypt_content(cleartext):
    box = SealedBox(PublicKey(base64.b64decode(settings.PDK_EXTERNAL_CONTENT_PUBLIC_KEY)))

    return base64.b64encode(box.encrypt(cleartext))

def secret_encrypt_content(cleartext):
    box = SecretBox(base64.b64decode(settings.PDK_EXTERNAL_CONTENT_SYMETRIC_KEY))

    return base64.b64encode(box.encrypt(cleartext))

def secret_decrypt_content(cleartext):
    box = SecretBox(base64.b64decode(settings.PDK_EXTERNAL_CONTENT_SYMETRIC_KEY))

    return box.decrypt(base64.b64decode(cleartext))

def create_engagement_event(source, identifier, start, outgoing_engagement=None, incoming_engagement=None, engagement_type='unknown', duration=0): # pylint: disable=too-many-arguments
    metadata = {
        'type': engagement_type,
        'duration': duration,
    }

    if outgoing_engagement is not None:
        metadata['outgoing_engagement'] = outgoing_engagement

    if incoming_engagement is not None:
        metadata['incoming_engagement'] = incoming_engagement

    point = DataPoint.objects.create_data_point('pdk-external-engagement-' + source, identifier, metadata, user_agent='Passive Data Kit External Importer', created=start, skip_save=True, skip_extract_secondary_identifier=True)

    if incoming_engagement is not None and incoming_engagement > 0:
        point.secondary_identifier = 'incoming active'
    elif outgoing_engagement is None:
        point.secondary_identifier = 'none'
    elif outgoing_engagement is not None and outgoing_engagement > 0:
        point.secondary_identifier = 'active'
    else:
        point.secondary_identifier = 'passive'

    queue_batch_insert(point)

def queue_batch_insert(data_point, force_insert=False):
    if force_insert is True or len(PENDING_POINTS) >= PENDING_POINTS_LIMIT:
        DataPoint.objects.bulk_create(PENDING_POINTS)

        del PENDING_POINTS[:]

    if data_point is not None:
        PENDING_POINTS.append(data_point)

def finish_batch_inserts():
    queue_batch_insert(None, force_insert=True)

def include_data(identifier, created_date, data_point):
    try:
        return settings.PASSIVE_DATA_KIT_EXTERNAL_INCLUDE_DATA_FUNCTION(identifier, created_date, data_point)
    except AttributeError:
        pass

    return True
