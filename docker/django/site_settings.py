# pylint: skip-file

"""
Settings.py for Dockerized container
"""

import os
import datetime
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

SECRET_KEY = os.getenv('DJANGO_SECRET_KEY')

DEBUG = True
ADMINS = [(os.getenv('DJANGO_ADMIN_NAME'), os.getenv('DJANGO_ADMIN_EMAIL'))]

ALLOWED_HOSTS = ['*']

# Application definition

INSTALLED_APPS = (
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'django.contrib.gis',
    'prettyjson',
    'anymail',
    'docker_utils',
    'passive_data_kit',
    'passive_data_kit_codebook',
    'passive_data_kit_content_analysis',
    'passive_data_kit_external_data',
)

MIDDLEWARE = (
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
    'django.middleware.security.SecurityMiddleware',
)

ROOT_URLCONF = 'pdk_site.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

WSGI_APPLICATION = 'pdk_site.wsgi.application'

DATABASES = {
    'default': {
        'ENGINE':   'django.contrib.gis.db.backends.postgis',
        'NAME':     os.getenv('PG_DB'),
        'USER':     os.getenv('PG_USER'),
        'PASSWORD': os.getenv('PG_PASSWORD'),
        'HOST':     'db',
        'PORT':     '',
    }
}

ANYMAIL = {
    'MAILGUN_API_KEY': os.getenv('MAILGUN_API_KEY'),
    'MAILGUN_SENDER_DOMAIN': os.getenv('MAILGUN_SENDER_DOMAIN'),
}

EMAIL_BACKEND = 'anymail.backends.mailgun.EmailBackend'
DEFAULT_FROM_EMAIL = os.getenv('CRON_FROM_EMAIL')
SERVER_EMAIL = os.getenv('CRON_FROM_EMAIL')
AUTOMATED_EMAIL_FROM_ADDRESS = os.getenv('CRON_FROM_EMAIL')

# if 'test' in sys.argv or 'test_coverage' in sys.argv: #Covers regular testing and django-coverage
#    DATABASES['default']['ENGINE'] = 'django.contrib.gis.db.backends.spatialite'
#     SPATIALITE_LIBRARY_PATH = 'mod_spatialite'

# Internationalization
# https://docs.djangoproject.com/en/1.8/topics/i18n/

LANGUAGE_CODE = 'en-us'

TIME_ZONE = 'UTC'

USE_I18N = True

USE_L10N = True

USE_TZ = True

SITE_URL = 'http://localhost:%s' % os.getenv('NGINX_WEB_PORT')

# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/1.8/howto/static-files/

STATIC_URL = '/static/'
STATIC_ROOT = '/app/static/'

MEDIA_URL = '/media/'
MEDIA_ROOT = '/app/media'

SILENCED_SYSTEM_CHECKS = ['fields.W904']

PDK_EXTERNAL_CONTENT_PUBLIC_KEY = os.getenv('PDK_EXTERNAL_CONTENT_PUBLIC_KEY')
PDK_EXTERNAL_CONTENT_SYMETRIC_KEY = os.getenv('PDK_EXTERNAL_CONTENT_SYMETRIC_KEY')

PDK_EXTERNAL_CONTENT_DISABLE_ENCRYPTION = os.getenv('PDK_EXTERNAL_CONTENT_DISABLE_ENCRYPTION', False)
PDK_EXTERNAL_CONTENT_DISABLE_HASHING = os.getenv('PDK_EXTERNAL_CONTENT_DISABLE_HASHING', False)

PDK_EMAIL_REMINDER_DURATION = datetime.timedelta(seconds=60)

PDK_DASHBOARD_ENABLED = True

PDK_ENABLED_CHECKS = []
