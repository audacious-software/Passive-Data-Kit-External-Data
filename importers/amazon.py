# pylint: disable=line-too-long, no-member

from __future__ import print_function

from builtins import str # pylint: disable=redefined-builtin

import json
import math

import arrow
import chardet
import pandas
import pathlib2

from django.conf import settings

from passive_data_kit.models import DataPoint

from ..utils import hash_content, create_engagement_event

DROP_COLUMNS = (
    'Payment Instrument Type',
    'Purchase Order Number',
    'PO Line Number',
    'Ordering Customer Email',
    'Shipping Address Name',
    'Shipping Address Street 1',
    'Shipping Address Street 2',
    'Shipping Address City',
    'Shipping Address State',
    'Shipping Address Zip',
    'Carrier Name & Tracking Number',
    'Buyer Name',
    'Group Name',
    'Account Group',
    'PO Number',
    'Payment Reference ID',
    'Payment Date',
    'Payment Amount',
    'Payment Instrument Type',
    'Payment Identifier',
    'Company Compliance',
    'Receiving Status',
    'Received Quantity',
    'Received Date',
    'Receiver Name',
    'Receiver Email',
    'GL Code',
    'Department',
    'Cost Center',
    'Project Code',
    'Company Compliance',
    'Approver',
    'Order Receiving Status',
    'Order Received Quantity',
    'Account User',
    'Account User Email',
)


def process_item(request_identifier, item):
    order_date_str = item.get('Order Date', None)

    if order_date_str is not None: # pylint: disable=too-many-nested-blocks
        order_date = None

        if order_date_str != 'No data found for this time period':
            try:
                order_date = arrow.get(order_date_str, 'M/D/YY').replace(tzinfo=settings.TIME_ZONE, hour=12)
            except arrow.parser.ParserMatchError:
                order_date = arrow.get(order_date_str, 'M/D/YYYY').replace(tzinfo=settings.TIME_ZONE, hour=12)

            if order_date is not None:
                carrier_name = ''

                try:
                    carrier_name = item['Carrier Name & Tracking Number'].split('(')[0]
                except AttributeError:
                    pass
                except KeyError:
                    pass

                pdk_item = {
                    'item': item['Title'],
                    'category': item.get('Category', item.get('Segment', None)),
                    'asin': item.get('ASIN/ISBN', item.get('ASIN', None)),
                    'unspc_code': item.get('UNSPSC Code', item.get('UNSPSC', None)),
                    'website': item.get('Website', None),
                    'condition': item.get('Condition', item.get('Product Condition', None)),
                    'seller': item.get('Seller', item.get('Seller Name', None)),
                    'seller_credentials': item['Seller Credentials'],
                    'list_price': item.get('List Price Per Unit', item.get('Listed PPU', None)),
                    'purchase_price': item.get('Purchase Price Per Unit', item.get('Purchase PPU', None)),
                    'purchase_subtotal': item['Item Subtotal'],
                    'purchase_tax': item.get('Item Subtotal Tax', item.get('Item Tax', None)),
                    'purchase_total': item.get('Item Total', item.get('Item Net Total', None)),
                    'tax_exemption_applied': item['Tax Exemption Applied'],
                    'tax_exemption_type': item['Tax Exemption Type'],
                    'exemption_opt_out': item.get('Exemption Opt-Out', item.get('Tax Exemption Opt Out', None)),
                    'currency': item['Currency'],
                    'quantity': item.get('Quantity', item.get('Item Quantity', None)),
                    'zipcode': item.get('Shipping Address Zip', item.get('Seller ZipCode', '')),
                    'carrier_name': carrier_name,
                    'status': item.get('Order Status', ''),
                    'ordered': order_date.date().isoformat(),
                    'pdk_hashed_order_id': hash_content(item['Order ID']),
                }


                ship_date = item.get('Shipment Date', '')

                if isinstance(ship_date, str) and ship_date != '':
                    try:
                        ship_date = arrow.get(item['Shipment Date'], 'M/D/YY').replace(tzinfo=settings.TIME_ZONE, hour=12)
                    except arrow.parser.ParserMatchError:
                        ship_date = arrow.get(item['Shipment Date'], 'M/D/YYYY').replace(tzinfo=settings.TIME_ZONE, hour=12)

                    pdk_item['shipped'] = ship_date.date().isoformat()

                release_date = item.get('Release Date', '')

                if isinstance(release_date, str) and release_date != '':
                    release_date = arrow.get(item['Release Date']).replace(tzinfo=settings.TIME_ZONE, hour=12)

                    pdk_item['released'] = release_date.date().isoformat()

                payload_keys = list(pdk_item.keys())

                for key in payload_keys:
                    try:
                        if math.isnan(pdk_item[key]):
                            del pdk_item[key]
                    except TypeError:
                        pass

                created = order_date.datetime

                DataPoint.objects.create_data_point('pdk-external-amazon-item', request_identifier, pdk_item, user_agent='Passive Data Kit External Importer', created=created)

                create_engagement_event(source='amazon', identifier=request_identifier, outgoing_engagement=1.0, engagement_type='purchase', start=created)


def import_data(request_identifier, path): # pylint: disable=too-many-branches
    detected = chardet.detect(pathlib2.Path(path).read_bytes())

    encoding = detected.get('encoding')

    if detected.get('confidence', 0) < 0.75:
        print('Detected: %s' % json.dumps(detected, indent=2))
        print('Falling back to "unicode_escape".')

        encoding = 'unicode_escape'

    data_frame = pandas.read_csv(path, encoding=encoding)

    for index, row in data_frame.iterrows(): # pylint: disable=unused-variable
        process_item(request_identifier, row)

    data_frame = data_frame.drop([x for x in DROP_COLUMNS if x in data_frame.columns], axis=1)

    data_frame.to_csv(path, index=False)

    return True

def external_data_metadata(generator_identifier, point): # pylint: disable=unused-argument
    if generator_identifier.startswith('pdk-external-amazon') is False:
        return None

    metadata = {}
    metadata['service'] = 'Amazon'
    metadata['event'] = generator_identifier

    return metadata
