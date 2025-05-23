#!/bin/bash

echo Starting Passive Data Kit: External Data CRON server...
source /app/venv/bin/activate

echo "root=$CRON_MAIL_RECIPIENT" > /etc/ssmtp/ssmtp.conf
echo "mailhub=$CRON_MAIL_SERVER" >> /etc/ssmtp/ssmtp.conf
echo "rewriteDomain=$CRON_MAIL_DOMAIN" >> /etc/ssmtp/ssmtp.conf
echo "hostname=$CRON_SENDER_HOSTNAME" >> /etc/ssmtp/ssmtp.conf
echo "UseTLS=Yes" >> /etc/ssmtp/ssmtp.conf
echo "UseSTARTTLS=Yes" >> /etc/ssmtp/ssmtp.conf
echo "FromLineOverride=No" >> /etc/ssmtp/ssmtp.conf
echo "AuthUser=$CRON_MAIL_USERNAME" >> /etc/ssmtp/ssmtp.conf
echo "AuthPass=$CRON_MAIL_PASSWORD" >> /etc/ssmtp/ssmtp.conf

cd /app/pdk_site

echo "Seeding codebook data types..."
python3 manage.py pdk_seed_data_point_types
echo "Updating codebook data types..."
python3 manage.py pdk_update_data_types

cron && tail -f /var/log/cron.log
