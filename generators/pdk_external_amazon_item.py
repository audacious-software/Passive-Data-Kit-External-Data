# pylint: disable=line-too-long, no-member

import csv
import calendar
import io
import os
import tempfile
import traceback

from zipfile import ZipFile

import arrow
import pytz

from past.utils import old_div

from django.conf import settings
from django.utils.text import slugify

from passive_data_kit.models import DataPoint, DataSourceReference, DataGeneratorDefinition


def generator_name(identifier): # pylint: disable=unused-argument
    return 'Amazon Items'

def compile_report(generator, sources, data_start=None, data_end=None, date_type='created'): # pylint: disable=too-many-locals, too-many-branches, too-many-statements
    now = arrow.get()
    filename = tempfile.gettempdir() + os.path.sep + 'pdk_export_' + str(now.timestamp) + str(old_div(now.microsecond, 1e6)) + '.zip'

    with ZipFile(filename, 'w') as export_file:
        seen_sources = []

        for source in sources:
            try:
                export_source = source

                seen_index = 1

                while slugify(export_source) in seen_sources:
                    export_source = source + '__' + str(seen_index)

                    seen_index += 1

                seen_sources.append(slugify(export_source))

                identifier = slugify(generator + '__' + export_source)

                secondary_filename = tempfile.gettempdir() + os.path.sep + identifier + '.txt'

                with io.open(secondary_filename, 'w', encoding='utf-8') as outfile:
                    writer = csv.writer(outfile, delimiter='\t')

                    columns = [
                        'Source',
                        'Created Timestamp',
                        'Created Date',
                        'Recorded Timestamp',
                        'Recorded Date',
                        'Order Date',
                        'Order ID',
                        'Title',
                        'Category',
                        'ASIN/ISBN',
                        'UNSPSC Code',
                        'Website',
                        'Release Date',
                        'Condition',
                        'Seller',
                        'Seller Credentials',
                        'List Price Per Unit',
                        'Purchase Price Per Unit',
                        'Quantity',
                        'Payment Instrument Type',
                        'Purchase Order Number',
                        'PO Line Number',
                        'Ordering Customer Email',
                        'Shipment Date',
                        'Shipping Address Name',
                        'Shipping Address Street 1',
                        'Shipping Address Street 2',
                        'Shipping Address City',
                        'Shipping Address State',
                        'Shipping Address Zip',
                        'Order Status',
                        'Carrier Name & Tracking Number',
                        'Item Subtotal',
                        'Item Subtotal Tax',
                        'Item Total',
                        'Tax Exemption Applied',
                        'Tax Exemption Type',
                        'Exemption Opt-Out',
                        'Buyer Name',
                        'Currency',
                        'Group Name',
                    ]

                    writer.writerow(columns)

                    source_reference = DataSourceReference.reference_for_source(source)
                    generator_definition = DataGeneratorDefinition.definition_for_identifier(generator)

                    points = DataPoint.objects.filter(source_reference=source_reference, generator_definition=generator_definition)

                    if data_start is not None:
                        if date_type == 'recorded':
                            points = points.filter(recorded__gte=data_start)
                        else:
                            points = points.filter(created__gte=data_start)

                    if data_end is not None:
                        if date_type == 'recorded':
                            points = points.filter(recorded__lte=data_end)
                        else:
                            points = points.filter(created__lte=data_end)

                    points = points.order_by('source', 'created')

                    for point in points:
                        properties = point.fetch_properties()

                        row = []

                        created = point.created.astimezone(pytz.timezone(settings.TIME_ZONE))
                        recorded = point.recorded.astimezone(pytz.timezone(settings.TIME_ZONE))

                        row.append(point.source)
                        row.append(calendar.timegm(point.created.utctimetuple()))
                        row.append(created.isoformat())

                        row.append(calendar.timegm(point.recorded.utctimetuple()))
                        row.append(recorded.isoformat())

                        row.append(properties['ordered'])
                        row.append(properties['pdk_hashed_order_id'])
                        row.append(properties.get('item', '(Unknown Item - ASIN Not Available)'))
                        row.append(properties.get('category', ''))
                        row.append(properties.get('asin', ''))
                        row.append(properties.get('unspc_code', ''))
                        row.append(properties.get('website', ''))
                        row.append(properties.get('released', ''))
                        row.append(properties.get('condition', ''))
                        row.append(properties.get('seller', ''))
                        row.append(properties.get('seller_credentials', ''))
                        row.append(properties.get('list_price', ''))
                        row.append(properties.get('purchase_price', ''))
                        row.append(properties.get('quantity', ''))
                        row.append('')
                        row.append('')
                        row.append('')
                        row.append('')
                        row.append(properties.get('shipped', ''))
                        row.append('')
                        row.append('')
                        row.append('')
                        row.append('')
                        row.append('')
                        row.append(properties.get('zipcode', ''))
                        row.append(properties.get('status', ''))
                        row.append(properties.get('carrier_name', ''))
                        row.append(properties.get('purchase_subtotal', ''))
                        row.append(properties.get('purchase_tax', ''))
                        row.append(properties.get('purchase_total', ''))
                        row.append(properties.get('tax_exemption_applied', ''))
                        row.append(properties.get('tax_exemption_type', ''))
                        row.append(properties.get('exemption_opt_out', ''))
                        row.append('')
                        row.append(properties.get('currency', ''))
                        row.append('')

                        writer.writerow(row)
            except:
                traceback.print_exc()

            export_file.write(secondary_filename, slugify(generator) + '/' + slugify(export_source) + '.txt')

            os.remove(secondary_filename)

    return filename
