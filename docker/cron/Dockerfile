FROM ubuntu:24.04

# Add crontab file in the cron directory
ADD ./docker/cron/crontab /etc/cron.d/django

# Give execution rights on the cron job
RUN chmod 0644 /etc/cron.d/django

# Create the log file to be able to run tail
RUN touch /var/log/cron.log

# Install Ubuntu packages

RUN apt-get update
RUN apt-get install -y apt-utils curl git libldap2-dev libsasl2-dev slapd ldap-utils cron gdal-bin ssmtp mailutils moreutils
RUN apt-get install -y python3-dev gdal-bin python3-venv

WORKDIR /app

# Set up local application environment

RUN python3 -m venv /app/venv
RUN . /app/venv/bin/activate && pip install -U wheel pip
ADD requirements.txt /app/requirements.txt

WORKDIR /app/

RUN python3 -m venv /app/venv

RUN . /app/venv/bin/activate && pip install -U wheel pip

ADD ./requirements.txt /app/requirements.txt
RUN . /app/venv/bin/activate && pip install -r /app/requirements.txt
RUN . /app/venv/bin/activate && pip install "django-anymail[mailgun]"

RUN . /app/venv/bin/activate && django-admin startproject pdk_site
RUN git clone https://github.com/bric-digital/PassiveDataKit-Django.git /app/pdk_site/passive_data_kit
RUN git clone https://github.com/bric-digital/PassiveDataKit-Codebook.git /app/pdk_site/passive_data_kit_codebook
RUN git clone https://github.com/bric-digital/PassiveDataKit-Content-Analysis.git /app/pdk_site/passive_data_kit_content_analysis
RUN git clone https://github.com/bric-digital/Docker-Utils-Django.git /app/pdk_site/docker_utils

ADD . /app/pdk_site/passive_data_kit_external_data

RUN . /app/venv/bin/activate && pip install -r /app/pdk_site/passive_data_kit/requirements.txt -r /app/pdk_site/passive_data_kit_external_data/requirements.txt -r /app/pdk_site/docker_utils/requirements.txt -r /app/pdk_site/passive_data_kit_content_analysis/requirements.txt  -r /app/pdk_site/passive_data_kit_codebook/requirements.txt
RUN . /app/venv/bin/activate && pip install "django-anymail[mailgun]"

ADD ./docker/django/site_settings.py /app/pdk_site/pdk_site/settings.py
ADD ./docker/django/site_urls.py /app/pdk_site/pdk_site/urls.py
ADD ./docker/django/wsgi.py /app/pdk_site/pdk_site/wsgi.py

RUN mv /etc/environment /etc/environment.old
ADD ./docker/.env /etc/environment
RUN cat /etc/environment.old >> /etc/environment

ADD ./docker/cron/run.sh /app/run.sh
