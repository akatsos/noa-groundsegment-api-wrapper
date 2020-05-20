import requests
import json
from datetime import date, datetime, timedelta

def _load_configuration():
    with open("config.json") as conf_file:
        config = json.load(conf_file)

    configuration = {
        'username': config['credentials']['username'],
        'password': config['credentials']['password'],
        'products_url': config['urls']['products'],
    }

    return configuration


def _build_products_url(reception_id, datetime_start, datetime_end, instrument, satellite, product_url):
    """ Builds the URL based on inserted parameters """
    
    if reception_id:
        return ''.join((product_url, "?reception_id={}".format(reception_id)))

    inst_str, sat_str = '', ''

    # Check datetimes
    if datetime_start:
        dt_start = datetime_start
    else:
        dt_start = date.today().strftime('%Y-%m-%dT00:00')
    
    if datetime_end:
        dt_end = datetime_end
    else:
        dt_end = date.today().strftime('%Y-%m-%dT23:59')

    # Datetime String
    if dt_start < dt_end:
        dt_str = '?sensing_start__gte={}&sensing_start__lte={}'.format(dt_start, dt_end)
    elif dt_start > dt_end:
        dt_str = '?sensing_start__gte={}&sensing_start__lte={}'.format(dt_end, dt_start)
    else:
        dt_str = '?sensing_start__gte={}&sensing_start__lte={}'.format(dt_start, dt_end)

    # Instrument & Satellite String
    if instrument is None:
        if satellite:
            sat_str = '&id__endswith={}'.format(satellite)
    else:
        inst_str = '&id__endswith={}'.format(instrument)

    return ''.join((product_url, dt_str, sat_str, inst_str))

def _build_products(products, req):
    """Handles pagination"""
    while products['next']:
        for product in products['results']:
            yield product
        
        req.get(products['next']).raise_for_status()
        products = req.get(products['next']).json()

    for product in products['results']:
            yield product

def get_products(reception_id=None, datetime_start=None, datetime_end=None, instrument=None, satellite=None, size=None):
    """
    Create a session and return the results
    """
    config = _load_configuration()

    req = requests.Session()
    req.auth = (config['username'], config['password'])
    req.headers.update({'Accept': 'application/json', 'Content-Type': 'application/json;charset=UTF-8',})

    url = _build_products_url(reception_id, datetime_start, datetime_end, instrument, satellite, config['products_url'])

    # If response status is not 4xx/5xx, the receptions are returned in JSON
    (req.get(url)).raise_for_status()

    products = (req.get(url)).json()

    return _build_products(products, req)
