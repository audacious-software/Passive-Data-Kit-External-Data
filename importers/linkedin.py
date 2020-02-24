# pylint: disable=line-too-long

import csv
import re
import traceback
import zipfile

import StringIO

import arrow

from django.conf import settings

from passive_data_kit.models import DataPoint

from ..utils import hash_content, encrypt_content

def process_follows(request_identifier, follows_raw):
    file_like = StringIO.StringIO(follows_raw)

    csv_reader = csv.reader(file_like)

    for row in csv_reader:
        if row[0] != 'Organization':
            follow_point = {}

            created = arrow.get(row[1], 'ddd MMM DD HH:mm:ss ZZZ YYYY').datetime

            follow_point['pdk_encrypted_organization'] = encrypt_content(row[0])
            follow_point['pdk_hashed_organization'] = hash_content(row[0])

            DataPoint.objects.create_data_point('pdk-external-linkedin-follow', request_identifier, follow_point, user_agent='Passive Data Kit External Importer', created=created)

def process_connections(request_identifier, connections_raw):
    file_like = StringIO.StringIO(connections_raw)

    csv_reader = csv.reader(file_like)

    for row in csv_reader:
        if row[0] != 'First Name':
            connection_point = {}

            created = arrow.get(row[5], 'DD MMM YYYY').to(settings.TIME_ZONE).replace(hour=12, minute=0, second=0).datetime

            connection_point['pdk_encrypted_first_name'] = encrypt_content(row[0])
            connection_point['pdk_hashed_first_name'] = hash_content(row[0])

            connection_point['pdk_encrypted_last_name'] = encrypt_content(row[1])
            connection_point['pdk_hashed_last_name'] = hash_content(row[1])

            connection_point['pdk_encrypted_email'] = encrypt_content(row[2])
            connection_point['pdk_hashed_email'] = hash_content(row[2])

            connection_point['pdk_encrypted_company'] = encrypt_content(row[3])
            connection_point['pdk_hashed_company'] = hash_content(row[3])

            connection_point['pdk_encrypted_position'] = encrypt_content(row[4])
            connection_point['pdk_hashed_position'] = hash_content(row[4])

            DataPoint.objects.create_data_point('pdk-external-linkedin-connection', request_identifier, connection_point, user_agent='Passive Data Kit External Importer', created=created)

def process_contacts(request_identifier, contacts_raw):
    file_like = StringIO.StringIO(contacts_raw)

    csv_reader = csv.reader(file_like)

    for row in csv_reader:
        if row[0] != 'Source':
            contact_point = {}

            created = arrow.get(row[7], 'M/D/YY, h:mm A').to(settings.TIME_ZONE).datetime

            contact_point['source'] = row[0]

            contact_point['pdk_encrypted_first_name'] = encrypt_content(row[1])
            contact_point['pdk_hashed_first_name'] = hash_content(row[1])

            contact_point['pdk_encrypted_last_name'] = encrypt_content(row[2])
            contact_point['pdk_hashed_last_name'] = hash_content(row[2])

            contact_point['pdk_encrypted_companies'] = encrypt_content(row[3])

            contact_point['pdk_encrypted_title'] = encrypt_content(row[4])
            contact_point['pdk_hashed_title'] = hash_content(row[4])

            contact_point['pdk_encrypted_emails'] = encrypt_content(row[5])
            contact_point['pdk_encrypted_phone_numbers'] = encrypt_content(row[6])

            row_io = StringIO.StringIO()
            row_csv = csv.writer(row_io)
            row_csv.writerow(row)

            contact_point['pdk_encrypted_row'] = encrypt_content(row_io.getvalue())

            DataPoint.objects.create_data_point('pdk-external-linkedin-contact', request_identifier, contact_point, user_agent='Passive Data Kit External Importer', created=created)

def process_email_addresses(request_identifier, emails_raw):
    file_like = StringIO.StringIO(emails_raw)

    csv_reader = csv.reader(file_like)

    for row in csv_reader:
        if row[0] != 'Email Address':
            email_point = {}

            if row[3] != 'Not Available':
                created = arrow.get(row[3], 'M/D/YY, h:mm A').to(settings.TIME_ZONE).datetime

                email_point['pdk_encrypted_email'] = encrypt_content(row[0])
                email_point['pdk_hashed_email'] = hash_content(row[0])

                email_point['pdk_confirmed'] = row[1] == 'Yes'
                email_point['pdk_is_primary'] = row[2] == 'Yes'

                DataPoint.objects.create_data_point('pdk-external-linkedin-email', request_identifier, email_point, user_agent='Passive Data Kit External Importer', created=created)

def process_groups(request_identifier, groups_raw):
    file_like = StringIO.StringIO(groups_raw)

    csv_reader = csv.reader(file_like)

    for row in csv_reader:
        if row[0] != 'Group name':
            group_point = {}

            created = arrow.get(row[3], 'YYYY/MM/DD HH:mm:ss ZZZ').datetime

            group_point['pdk_encrypted_group_name'] = encrypt_content(row[0])
            group_point['pdk_hashed_group_name'] = hash_content(row[0])

            group_point['pdk_encrypted_group_description'] = encrypt_content(row[1])
            group_point['pdk_encrypted_group_rules'] = encrypt_content(row[2])

            group_point['pdk_membership'] = row[4]

            DataPoint.objects.create_data_point('pdk-external-linkedin-membership', request_identifier, group_point, user_agent='Passive Data Kit External Importer', created=created)


def process_invitations(request_identifier, invitations_raw):
    file_like = StringIO.StringIO(invitations_raw)

    csv_reader = csv.reader(file_like)

    for row in csv_reader:
        if row[0] != 'From':
            invite_point = {}

            created = arrow.get(row[2], 'M/D/YY, h:mm A').to(settings.TIME_ZONE).datetime

            invite_point['pdk_encrypted_from'] = encrypt_content(row[0])
            invite_point['pdk_hashed_from'] = hash_content(row[0])

            invite_point['pdk_encrypted_to'] = encrypt_content(row[1])
            invite_point['pdk_hashed_to'] = hash_content(row[1])

            invite_point['pdk_encrypted_message'] = encrypt_content(row[3])
            invite_point['pdk_length_message'] = len(row[3])

            invite_point['pdk_direction'] = row[4]

            DataPoint.objects.create_data_point('pdk-external-linkedin-invitation', request_identifier, invite_point, user_agent='Passive Data Kit External Importer', created=created)

def process_messages(request_identifier, messages_raw):
    file_like = StringIO.StringIO(messages_raw)

    csv_reader = csv.reader(file_like)

    for row in csv_reader:
        if row[0] != 'FROM':
            message_point = {}

            created = arrow.get(row[2], 'YYYY-MM-DDTHH:mm:ss').to(settings.TIME_ZONE).datetime

            message_point['pdk_encrypted_from'] = encrypt_content(row[0])
            message_point['pdk_hashed_from'] = hash_content(row[0])

            message_point['pdk_encrypted_to'] = encrypt_content(row[1])
            message_point['pdk_hashed_to'] = hash_content(row[1])

            message_point['pdk_encrypted_subject'] = encrypt_content(row[3])
            message_point['pdk_length_subject'] = len(row[3])

            message_point['pdk_encrypted_content'] = encrypt_content(row[4])
            message_point['pdk_length_content'] = len(row[4])

            message_point['pdk_direction'] = row[5]
            message_point['pdk_folder'] = row[6]

            DataPoint.objects.create_data_point('pdk-external-linkedin-message', request_identifier, message_point, user_agent='Passive Data Kit External Importer', created=created)


def process_recommendations_given(request_identifier, recommendations_raw):
    file_like = StringIO.StringIO(recommendations_raw)

    csv_reader = csv.reader(file_like)

    for row in csv_reader:
        if row[0] != 'First Name':
            recommendation_point = {}

            created = arrow.get(row[5], 'M/DD/YY, h:mm A').to(settings.TIME_ZONE).datetime

            recommendation_point['pdk_encrypted_first_name'] = encrypt_content(row[0])
            recommendation_point['pdk_hashed_first_name'] = hash_content(row[0])

            recommendation_point['pdk_encrypted_last_name'] = encrypt_content(row[1])
            recommendation_point['pdk_hashed_last_name'] = hash_content(row[1])

            recommendation_point['pdk_encrypted_company'] = encrypt_content(row[2])
            recommendation_point['pdk_hashed_company'] = hash_content(row[2])

            recommendation_point['pdk_encrypted_title'] = encrypt_content(row[3])
            recommendation_point['pdk_hashed_title'] = hash_content(row[3])

            recommendation_point['pdk_encrypted_text'] = encrypt_content(row[5])
            recommendation_point['pdk_length_text'] = len(row[5])

            if row[5]:
                recommendation_point['pdk_status'] = len(row[5])

            DataPoint.objects.create_data_point('pdk-external-linkedin-recommendation-given', request_identifier, recommendation_point, user_agent='Passive Data Kit External Importer', created=created)

def process_recommendations_received(request_identifier, recommendations_raw): # pylint: disable=invalid-name
    file_like = StringIO.StringIO(recommendations_raw)

    csv_reader = csv.reader(file_like)

    for row in csv_reader:
        if row[0] != 'First Name':
            recommendation_point = {}

            created = arrow.get(row[5], 'M/DD/YY, h:mm A').to(settings.TIME_ZONE).datetime

            recommendation_point['pdk_encrypted_first_name'] = encrypt_content(row[0])
            recommendation_point['pdk_hashed_first_name'] = hash_content(row[0])

            recommendation_point['pdk_encrypted_last_name'] = encrypt_content(row[1])
            recommendation_point['pdk_hashed_last_name'] = hash_content(row[1])

            recommendation_point['pdk_encrypted_company'] = encrypt_content(row[2])
            recommendation_point['pdk_hashed_company'] = hash_content(row[2])

            recommendation_point['pdk_encrypted_title'] = encrypt_content(row[3])
            recommendation_point['pdk_hashed_title'] = hash_content(row[3])

            recommendation_point['pdk_encrypted_text'] = encrypt_content(row[5])
            recommendation_point['pdk_length_text'] = len(row[5])

            if row[5]:
                recommendation_point['pdk_status'] = len(row[5])

            DataPoint.objects.create_data_point('pdk-external-linkedin-recommendation-received', request_identifier, recommendation_point, user_agent='Passive Data Kit External Importer', created=created)

def process_registration(request_identifier, registration_raw):
    file_like = StringIO.StringIO(registration_raw)

    csv_reader = csv.reader(file_like)

    for row in csv_reader:
        if row[0] != 'Registered At':
            registration_point = {}

            created = arrow.get(row[0], 'M/DD/YY, h:mm A').to(settings.TIME_ZONE).datetime

            if row[1]:
                registration_point['pdk_encrypted_ip_address'] = encrypt_content(row[1])

            if row[2]:
                registration_point['pdk_subscription_types'] = row[2]

            DataPoint.objects.create_data_point('pdk-external-linkedin-registration', request_identifier, registration_point, user_agent='Passive Data Kit External Importer', created=created)


def import_data(request_identifier, path): # pylint: disable=too-many-branches
    content_bundle = zipfile.ZipFile(path)

    for content_file in content_bundle.namelist():
        try:
            if content_file.endswith('/'):
                pass
            elif re.match(r'^Company\ Follows\.csv', content_file):
                process_follows(request_identifier, content_bundle.open(content_file).read())
            elif re.match(r'^Connections\.csv', content_file):
                process_connections(request_identifier, content_bundle.open(content_file).read())
            elif re.match(r'^Contacts\.csv', content_file):
                process_contacts(request_identifier, content_bundle.open(content_file).read())
            elif re.match(r'^Email\ Addresses\.csv', content_file):
                process_email_addresses(request_identifier, content_bundle.open(content_file).read())
            elif re.match(r'^Groups\.csv', content_file):
                process_groups(request_identifier, content_bundle.open(content_file).read())
            elif re.match(r'^Invitations\.csv', content_file):
                process_invitations(request_identifier, content_bundle.open(content_file).read())
            elif re.match(r'^messages\.csv', content_file):
                process_messages(request_identifier, content_bundle.open(content_file).read())
            elif re.match(r'^Recommendations\ Given\.csv', content_file):
                process_recommendations_given(request_identifier, content_bundle.open(content_file).read())
            elif re.match(r'^Recommendations\ Received\.csv', content_file):
                process_recommendations_received(request_identifier, content_bundle.open(content_file).read())
            elif re.match(r'^Registration\.csv', content_file):
                process_registration(request_identifier, content_bundle.open(content_file).read())
            else:
                print '[' + request_identifier + ']: Unable to process: ' + content_file
        except: # pylint: disable=bare-except
            traceback.print_exc()
            return False

    return True
