# pylint: disable=line-too-long, no-member

from __future__ import print_function

from builtins import str # pylint: disable=redefined-builtin

import math

import arrow
import pandas

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
                    'category': item['Category'],
                    'asin': item['ASIN/ISBN'],
                    'unspc_code': item['UNSPSC Code'],
                    'website': item['Website'],
                    'condition': item['Condition'],
                    'seller': item['Seller'],
                    'seller_credentials': item['Seller Credentials'],
                    'list_price': item['List Price Per Unit'],
                    'purchase_price': item['Purchase Price Per Unit'],
                    'purchase_subtotal': item['Item Subtotal'],
                    'purchase_tax': item['Item Subtotal Tax'],
                    'purchase_total': item['Item Total'],
                    'tax_exemption_applied': item['Tax Exemption Applied'],
                    'tax_exemption_type': item['Tax Exemption Type'],
                    'exemption_opt_out': item['Exemption Opt-Out'],
                    'currency': item['Currency'],
                    'quantity': item['Quantity'],
                    'zipcode': item.get('Shipping Address Zip', ''),
                    'carrier_name': carrier_name,
                    'status': item['Order Status'],
                    'ordered': order_date.date().isoformat(),
                    'pdk_hashed_order_id': hash_content(item['Order ID']),
                }


                ship_date = item['Shipment Date']

                if isinstance(ship_date, str) and ship_date != '':
                    try:
                        ship_date = arrow.get(item['Shipment Date'], 'M/D/YY').replace(tzinfo=settings.TIME_ZONE, hour=12)
                    except arrow.parser.ParserMatchError:
                        ship_date = arrow.get(item['Shipment Date'], 'M/D/YYYY').replace(tzinfo=settings.TIME_ZONE, hour=12)

                    pdk_item['shipped'] = ship_date.date().isoformat()

                release_date = item['Release Date']

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
    data_frame = pandas.read_csv(path)

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
