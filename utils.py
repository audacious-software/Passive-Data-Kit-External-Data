import base64
import hashlib

from nacl.public import SealedBox, PublicKey

from django.conf import settings

def hash_content(cleartext):
    sha512 = hashlib.sha512()

    sha512.update(settings.PDK_EXTERNAL_CONTENT_PUBLIC_KEY)
    sha512.update(cleartext)

    return sha512.hexdigest()


def encrypt_content(cleartext):
    box = SealedBox(PublicKey(base64.b64decode(settings.PDK_EXTERNAL_CONTENT_PUBLIC_KEY)))

    return base64.b64encode(box.encrypt(cleartext))
