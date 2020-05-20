import requests
import json
from datetime import date, datetime, timedelta

def _load_configuration():
    with open("config.json") as conf_file:
        config = json.load(conf_file)

    configuration = {
        'username': config['credentials']['username'],
        'password': config['credentials']['password'],
        'satellites': config['satellites'],
        'instruments': config['instruments'],
        'receptions_url': config['urls']['receptions']
    }

    return configuration

def _build_receptions_url(datetime_start, datetime_end, instrument, satellite, elevation, daytime, all_satellites, all_instruments, reception_url):
    """ Builds the URL based on inserted parameters """
    
    inst_str, sat_str, elev_str, daytime_str = '', '', '', ''

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
    if instrument == 'all':
        if satellite != 'all':
            sat_str = '&id__endswith={}'.format(all_satellites[satellite])
    else:
        for id in all_instruments[instrument]:
            inst_str += '&id__endswith={}'.format(id)

    # Elevation String
    if elevation:
        elev_str = '&elevation__gte={}'.format(elevation)

    # Daytime String
    if daytime is not None:
        daytime_str = '&daytime={}'.format(daytime)

    return ''.join((reception_url, dt_str, sat_str, inst_str, elev_str, daytime_str))

def _build_receptions(receptions, req):
    """Handles pagination"""
    while receptions['next']:
        for reception in receptions['results']:
            yield reception
        
        req.get(receptions['next']).raise_for_status()
        receptions = req.get(receptions['next']).json()

    for reception in receptions['results']:
            yield reception

def get_receptions(datetime_start=None, datetime_end=None, instrument='all', satellite='all', elevation=None, daytime=None):
    """
    Create a session and return the results
    """
    config = _load_configuration()

    req = requests.Session()
    req.auth = (config['username'], config['password'])
    req.headers.update({'Accept': 'application/json', 'Content-Type': 'application/json;charset=UTF-8',})

    url = _build_receptions_url(datetime_start, datetime_end, instrument, satellite, elevation, daytime, config['satellites'], config['instruments'], config['receptions_url'])

    # If response status is not 4xx/5xx, the receptions are returned in JSON
    (req.get(url)).raise_for_status()

    receptions = (req.get(url)).json()

    return _build_receptions(receptions, req)