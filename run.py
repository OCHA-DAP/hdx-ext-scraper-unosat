import argparse
import logging
from datetime import datetime, timedelta
from os.path import join

import pymysql.cursors
from dateutil.parser import parse
from hdx.facades import logging_kwargs
from hdx.utilities import get_uuid
from slugify import slugify

logging_kwargs['smtp_config_yaml'] = join('config', 'smtp_configuration.yml')

from hdx.facades.keyword_arguments import facade
from hdx.data.showcase import Showcase
from hdx.data.dataset import Dataset
from hdx.utilities.dictandlist import args_to_dict
from hdx.utilities.easy_logging import setup_logging

setup_logging()
logger = logging.getLogger(__name__)


class UNOSATError(Exception):
    pass


# Standard strings
standardEventTypesDict = {
    'AC': 'accidents - technical disasters',
    'CW': 'cold waves',
    'CE': 'complex emergency',
    'DR': 'droughts',
    'EQ': 'earthquakes',
    'EP': 'epidemics and outbreaks',
    'EC': 'cyclones - hurricanes - typhoons',
    'FR': 'urban fires',
    'FL': 'floods - storm surges',
    'FF': 'flash floods',
    'HT': 'heat waves',
    'IN': 'insect infestations',
    'LS': 'landslides - mudslides',
    'MS': 'landslides - mudslides',
    'OT': None,
    'AV': 'avalanches',
    'SS': 'floods - storm surges',
    'ST': 'wind storms - tornados - severe local storms',
    'TO': 'wind storms - tornados - severe local storms',
    'TC': 'cyclones - hurricanes - typhoons',
    'TS': 'tsunamis',
    'VW': 'wind storms - tornados - severe local storms',
    'VO': 'volcanos',
    'WF': 'wild fires'}


def make_hdx_entries(start_date, **params):
    logger.info('Adding any datasets created or updated after %s' % start_date.date().isoformat())

    # Connect to the database
    connection = pymysql.connect(**params)
    try:
        with connection.cursor() as cursor:
            # Read all countries
            sql = "SELECT * FROM `area`"
            cursor.execute(sql)
            unosatCountryCodes = dict()
            for unosatCountryCode in cursor:
                unosatCountryCodes[unosatCountryCode['id_area']] = unosatCountryCode['area_iso3']
            # Read a multiple records
            sql = "SELECT * FROM `product` WHERE NOT (GDB_Link LIKE '' AND SHP_Link LIKE '') AND (product_archived IS FALSE) AND (product_created>%s or updated>%s)"
            cursor.execute(sql, (start_date, start_date))
            if not cursor.rowcount:
                raise UNOSATError('No db results found')
            batch = get_uuid()
            for unosatDBEntry in cursor:
                if not unosatDBEntry:
                    raise UNOSATError('Empty row in db!')
                productID = str(unosatDBEntry['id_product'])
                logger.info('Processing UNOSAT product %s' % productID)
                logger.debug(unosatDBEntry)
                id_area = unosatDBEntry['id_area']
                iso3 = unosatCountryCodes[id_area]
                product_glide = unosatDBEntry['product_glide']
                # logger.info('product_glide = %s' % product_glide)
                typetag = product_glide[:2]
                product_description = unosatDBEntry['product_description']
                if '-' in product_glide:
                    glideiso3 = product_glide.split('-')[3]
                    product_description = '**Glide code: %s**  %s' % (product_glide, product_description)
                else:
                    glideiso3 = product_glide[10:13]
                    product_description = '**UNOSAT code: %s**  %s' % (product_glide, product_description)

                if iso3 != glideiso3:
                    raise UNOSATError(
                        'UNOSAT id_area=%s, area_iso3=%s does not match glide iso3=%s' % (id_area, iso3, glideiso3))

                # Dataset variables
                title = unosatDBEntry['product_title']
                slugified_name = slugify(title)
                if len(slugified_name) > 90:
                    slugified_name = slugified_name.replace('satellite-detected-', '')
                    slugified_name = slugified_name.replace('estimation-of-', '')
                    slugified_name = slugified_name.replace('geodata-of-', '')[:90]
                event_type = standardEventTypesDict[typetag]
                tags = ['geodata']
                if event_type:
                    tags.append(event_type)

                dataset = Dataset({
                    'name': slugified_name,
                    'title': title,
                    'notes': product_description})
                dataset.set_maintainer('83fa9515-3ba4-4f1d-9860-f38b20f80442')
                dataset.add_country_location(iso3)
                dataset.add_tags(tags)
                dataset.set_expected_update_frequency('Never')
                dataset.set_dataset_date_from_datetime(unosatDBEntry['product_created'])

                gdb_link = unosatDBEntry['GDB_Link']
                bitsgdb = gdb_link.split('/')
                shp_link = unosatDBEntry['SHP_Link']
                bitsshp = shp_link.split('/')

                resources = [
                    {
                        'name': bitsgdb[len(bitsgdb) - 1],
                        'format': 'zipped geodatabase',
                        'url': gdb_link,
                        'description': 'Zipped geodatabase',
                    },
                    {
                        'name': bitsshp[len(bitsshp) - 1],
                        'format': 'zipped shapefile',
                        'url': shp_link,
                        'description': 'Zipped shapefile',
                    }
                ]

                dataset.add_update_resources(resources)
                dataset.update_from_yaml()

                showcase = Showcase({
                    'name': '%s-showcase' % slugified_name,
                    'title': 'Static PDF Map',
                    'notes': 'Static viewing map for printing.',
                    'url': 'https://unosat-maps.web.cern.ch/unosat-maps/%s/%s' % (
                        unosatDBEntry['product_folder'], unosatDBEntry['product_url1']),
                    'image_url': 'https://unosat-maps.web.cern.ch/unosat-maps/%s/%s' % (
                        unosatDBEntry['product_folder'], unosatDBEntry['product_img'])
                })
                showcase.add_tags(tags)

                dataset.create_in_hdx(remove_additional_resources=True, hxl_update=False, updated_by_script='UNOSAT',
                                      batch=batch)
                showcase.create_in_hdx()
                showcase.add_dataset(dataset)

                with open('publishlog.txt', 'a+') as f:
                    f.write('%s,%s\n' % (productID, dataset.get_hdx_url()))
                    f.close()
    finally:
        connection.close()


def main(db_params, start_date, **ignore):
    params = args_to_dict(db_params)
    port = params.get('port')
    if port:
        params['port'] = int(port)
    params['charset'] = 'utf8'
    params['cursorclass'] = pymysql.cursors.DictCursor
    make_hdx_entries(start_date, **params)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='unosatdb_to_hdx')
    parser.add_argument('-hk', '--hdx_key', default=None, help='HDX api key')
    parser.add_argument('-hs', '--hdx_site', default=None, help='HDX site to use')
    parser.add_argument('-dp', '--db_params', default=None, help='Database connection parameters')
    parser.add_argument('-sd', '--start_date', default=None,
                        help='Add any datasets created or updated after this date. Defaults to one week prior to current date.')
    args = parser.parse_args()
    hdx_site = args.hdx_site
    if hdx_site is None:
        hdx_site = 'feature'
    start_date = args.start_date
    if start_date is None:
        start_date = datetime.utcnow() - timedelta(days=7)
    else:
        start_date = parse(start_date)
    facade(main, hdx_key=args.hdx_key, hdx_site=hdx_site, user_agent='UNOSAT', db_params=args.db_params,
           start_date=start_date)
