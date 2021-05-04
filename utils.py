#pylint: disable=line-too-long

import base64
import hashlib

from nacl.public import SealedBox, PublicKey
from nacl.secret import SecretBox

from django.conf import settings

from passive_data_kit.models import DataPoint

def hash_content(cleartext):
    sha512 = hashlib.sha512()

    sha512.update(settings.PDK_EXTERNAL_CONTENT_PUBLIC_KEY)
    sha512.update(cleartext)

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

def create_engagement_event(source, identifier, start, outgoing_engagement=0, incoming_engagement=0, engagement_type='unknown', duration=0): # pylint: disable=too-many-arguments
    metadata = {
        'type': engagement_type,
        'duration': duration,
        'outgoing_engagement': outgoing_engagement,
        'incoming_engagement': incoming_engagement,
    }

    point = DataPoint.objects.create_data_point('pdk-external-engagement-' + source, identifier, metadata, user_agent='Passive Data Kit External Importer', created=start)

    if outgoing_engagement > 0:
        point.secondary_identifier = 'active'
    elif incoming_engagement > 0:
        point.secondary_identifier = 'incoming active'
    else:
        point.secondary_identifier = 'passive'

    point.save()
