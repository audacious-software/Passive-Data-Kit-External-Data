# pylint: disable=line-too-long

from __future__ import print_function

import csv
import re
import traceback
import zipfile

from io import BytesIO

import arrow

from django.conf import settings

from passive_data_kit.models import DataPoint

from ..utils import hash_content, encrypt_content, create_engagement_event

def process_follows(request_identifier, follows_raw):
    file_like = BytesIO(follows_raw)

    csv_reader = csv.reader(file_like)

    for row in csv_reader:
        if row[0] != 'Organization':
            follow_point = {}

            created = arrow.get(row[1], 'ddd MMM DD HH:mm:ss ZZZ YYYY').datetime

            follow_point['pdk_encrypted_organization'] = encrypt_content(row[0])
            follow_point['pdk_hashed_organization'] = hash_content(row[0])

            DataPoint.objects.create_data_point('pdk-external-linkedin-follow', request_identifier, follow_point, user_agent='Passive Data Kit External Importer', created=created)

            create_engagement_event(source='linkedin', identifier=request_identifier, engagement_level=1.0, engagement_type='follow', start=created)


def process_connections(request_identifier, connections_raw):
    file_like = BytesIO(connections_raw)

    csv_reader = csv.reader(file_like)

    for row in csv_reader:
        if len(row) >= 6:
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
    file_like = BytesIO(contacts_raw)

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

            row_io = BytesIO()
            row_csv = csv.writer(row_io)
            row_csv.writerow(row)

            contact_point['pdk_encrypted_row'] = encrypt_content(row_io.getvalue())

            DataPoint.objects.create_data_point('pdk-external-linkedin-contact', request_identifier, contact_point, user_agent='Passive Data Kit External Importer', created=created)


def process_email_addresses(request_identifier, emails_raw):
    file_like = BytesIO(emails_raw)

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
    file_like = BytesIO(groups_raw)

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
    file_like = BytesIO(invitations_raw)

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
    ''' Updated 12/21/20. Format may be fluid. '''

    file_like = BytesIO(messages_raw)

    csv_reader = csv.reader(file_like)

    for row in csv_reader:
        if row[2] != 'FROM' and row[5] != 'DIRECTION':
            message_point = {}

            created = None

            try:
                created = arrow.get(row[5]).to(settings.TIME_ZONE).datetime
            except arrow.parser.ParserError:
                try:
                    created = arrow.get(row[5], 'YYYY-MM-DD HH:mm:ss ZZZ').to(settings.TIME_ZONE).datetime
                except arrow.parser.ParserError:
                    print('Invalid LinkedIn messages file: ' + request_identifier)

                    return

            message_point['pdk_encrypted_from'] = encrypt_content(row[2])
            message_point['pdk_hashed_from'] = hash_content(row[2])

            message_point['pdk_encrypted_to'] = encrypt_content(row[4])
            message_point['pdk_hashed_to'] = hash_content(row[4])

            message_point['pdk_encrypted_subject'] = encrypt_content(row[6])
            message_point['pdk_length_subject'] = len(row[6])

            message_point['pdk_encrypted_content'] = encrypt_content(row[7])
            message_point['pdk_length_content'] = len(row[7])

            # message_point['pdk_direction'] = row[5]
            message_point['pdk_folder'] = row[8]

            DataPoint.objects.create_data_point('pdk-external-linkedin-message', request_identifier, message_point, user_agent='Passive Data Kit External Importer', created=created)

            create_engagement_event(source='linkedin', identifier=request_identifier, engagement_level=1.0, engagement_type='message', start=created)


def process_recommendations_given(request_identifier, recommendations_raw):
    file_like = BytesIO(recommendations_raw)

    csv_reader = csv.reader(file_like)

    for row in csv_reader:
        if row[0] != 'First Name':
            recommendation_point = {}

            created = arrow.get(row[5], 'M/D/YY, h:mm A').to(settings.TIME_ZONE).datetime

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

            create_engagement_event(source='linkedin', identifier=request_identifier, engagement_level=1.0, engagement_type='recommendation', start=created)


def process_recommendations_received(request_identifier, recommendations_raw): # pylint: disable=invalid-name
    file_like = BytesIO(recommendations_raw)

    csv_reader = csv.reader(file_like)

    for row in csv_reader:
        if row[0] != 'First Name':
            recommendation_point = {}

            created = arrow.get(row[5], 'M/D/YY, h:mm A').to(settings.TIME_ZONE).datetime

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
    file_like = BytesIO(registration_raw)

    csv_reader = csv.reader(file_like)

    for row in csv_reader:
        if row[0] != 'Registered At':
            registration_point = {}

            created = arrow.get(row[0], 'M/D/YY, h:mm A').to(settings.TIME_ZONE).datetime

            if row[1]:
                registration_point['pdk_encrypted_ip_address'] = encrypt_content(row[1])

            if row[2]:
                registration_point['pdk_subscription_types'] = row[2]

            DataPoint.objects.create_data_point('pdk-external-linkedin-registration', request_identifier, registration_point, user_agent='Passive Data Kit External Importer', created=created)

            create_engagement_event(source='linkedin', identifier=request_identifier, engagement_level=1.0, engagement_type='registration', start=created)


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
                print('[' + request_identifier + ']: Unable to process: ' + content_file)
        except: # pylint: disable=bare-except
            traceback.print_exc()
            return False

    return True


def external_data_metadata(generator_identifier, point):
    if generator_identifier.startswith('pdk-external-linkedin') is False:
        return None

    metadata = {}
    metadata['service'] = 'LinkedIn'
    metadata['event'] = generator_identifier

    if generator_identifier == 'pdk-external-linkedin-follow':
        metadata['event'] = 'Follow'
        metadata['direction'] = 'Outgoing'
        metadata['media_type'] = 'Relationship'
    elif generator_identifier == 'pdk-external-linkedin-connection':
        metadata['event'] = 'Connection'
        metadata['direction'] = 'Outgoing'
        metadata['media_type'] = 'Relationship'
    elif generator_identifier == 'pdk-external-linkedin-membership':
        metadata['event'] = 'Joined Group'
        metadata['direction'] = 'Outgoing'
        metadata['media_type'] = 'Relationship'
    elif generator_identifier == 'pdk-external-linkedin-invitation':
        metadata['event'] = 'Invitation'

        properties = point.fetch_properties()

        metadata['direction'] = properties['pdk_direction']
        metadata['media_type'] = 'Relationship'
    elif generator_identifier == 'pdk-external-linkedin-message':
        metadata['event'] = 'Direct Message'

        properties = point.fetch_properties()

        if 'pdk_direction' in properties:
            metadata['direction'] = properties['pdk_direction']
        else:
            metadata['direction'] = 'Unknown'

        metadata['media_type'] = 'Text'
    elif generator_identifier == 'pdk-external-linkedin-recommendation-given':
        metadata['event'] = 'Recommendation'
        metadata['direction'] = 'Outgoing'
        metadata['media_type'] = 'Relationship'
    elif generator_identifier == 'pdk-external-linkedin-recommendation-received':
        metadata['event'] = 'Recommendation'
        metadata['direction'] = 'Incoming'
        metadata['media_type'] = 'Relationship'

    return metadata
