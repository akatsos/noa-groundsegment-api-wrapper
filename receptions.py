import requests
import argparse
import json, csv
from datetime import date, datetime

def get_arguments(satellites, instruments):
    parser = argparse.ArgumentParser(description='NOA GroundSegment DataHub API Receptions Wrapper')

    parser.add_argument('--instrument', '-i',
                            choices=instruments,
                            default='all')

    parser.add_argument('--satellite', '-s',
                            choices=satellites,
                            default='all')

    parser.add_argument('--datetime_start', '-dts', 
                        type=lambda s: datetime.strptime(s, '%Y-%m-%d %H:%M'), 
                        default=date.today().strftime('%Y-%m-%d 23:59'),
                        help='Format: Y-m-d H:M | eg.\"2020-05-01 00:00\"')

    parser.add_argument('--datetime_end', '-dte', 
                        type=lambda s: datetime.strptime(s, '%Y-%m-%d %H:%M'), 
                        default=date.today().strftime('%Y-%m-%d 23:59'),
                        help='Format: Y-m-d H:M | eg.\"2020-05-01 00:00\"')

    parser.add_argument('--format', '-f',
                            choices=['json', 'csv', 'human', 'human_compact', 'id', 'total'],
                            default='total')

    args = parser.parse_args()

    return args


def load_configuration():
    with open("config.json") as conf_file:
        config = json.load(conf_file)

    username = config['credentials']['username']
    password = config['credentials']['password']

    satellites = config['satellites']
    instruments = config['instruments']

    receptions_url = config['urls']['receptions']

    return username, password, satellites, instruments, receptions_url


def build_url(args):
    """
    Build URL based on given arguements
    """
    inst_str, sat_str = '', ''

    dt_start = (args.datetime_start).strftime('%Y-%m-%dT%H:%M')
    dt_end = (args.datetime_end).strftime('%Y-%m-%dT%H:%M')

    # Datetime 
    if args.datetime_start < args.datetime_end:
        dt_str = '?sensing_start__gte={}&sensing_start__lte={}'.format(dt_start, dt_end)
    elif args.datetime_start > args.datetime_end:
        dt_str = '?sensing_start__gte={}&sensing_start__lte={}'.format(dt_end, dt_start)
    else:
        dt_str = '?sensing_start__gte={}&sensing_start__lte={}'.format((args.datetime_start).strftime('%Y-%m-%dT00:00'), (args.datetime_end).strftime('%Y-%m-%dT23:59'))

    # Instrument & Satellite
    if args.instrument == 'all':
        if args.satellite != 'all':
            sat_str = '&id__endswith={}'.format(satellites[args.satellite])
    else:
        for id in instruments[args.instrument]:
            inst_str += '&id__endswith={}'.format(id)

    return ''.join((receptions_url, dt_str, sat_str, inst_str))


def get_receptions(username, password, url, format):
    """
    Create a session and return the results
    """
    req = requests.Session()
    req.auth = (username, password)
    req.headers.update({'Accept': 'application/json', 'Content-Type': 'application/json;charset=UTF-8',})
    
    # If response status is not 4xx/5xx, the receptions are returned in JSON
    (req.get(url)).raise_for_status()

    receptions = (req.get(url)).json()

    if format == 'total':
        return receptions['count']
    else:
        return _build_receptions(receptions, req)


def _build_receptions(receptions, req):
    """Handles pagination"""
    while receptions['next']:
        for reception in receptions['results']:
            yield reception
        
        req.get(receptions['next']).raise_for_status()
        receptions = req.get(receptions['next']).json()

    for reception in receptions['results']:
            yield reception

def show_receptions(receptions, args):
    """Outputs the receptions on the desired format"""
    # Total
    if args.format == 'total':
        print("{} receptions found".format(receptions))
    # JSON
    elif args.format == 'json':
        receptions_list = []
        for reception in receptions:
            receptions_list.append(reception)
        with open('{}_{}_{}_{}.json'.format(args.datetime_start, args.datetime_end, args.satellite, args.instrument), 'w') as json_file:
            json.dump(receptions_list, json_file, sort_keys = True, indent = 4, ensure_ascii=False)
    # CSV
    elif args.format == 'csv':
        with open('{}_{}_{}_{}.csv'.format(args.datetime_start, args.datetime_end, args.satellite, args.instrument), 'w') as csv_file:
            fieldnames = ['id', 'ingestion_date', 'sensing_start', 'sensing_stop', 'orbit', 'elevation', 'direction', 'location', 'daytime', 'station', 'geometry']
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()

            for reception in receptions:
                writer.writerow({
                    'id': reception['id'], 'ingestion_date': reception['ingestion_date'], 
                    'sensing_start': reception['sensing_start'], 'sensing_stop': reception['sensing_stop'],
                    'orbit': reception['orbit'], 'elevation': reception['elevation'],
                    'direction': reception['direction'], 'location': reception['location'], 
                    'daytime': reception['daytime'], 'station': reception['station'], 'geometry': reception['geom']
                    })
    # Human Compact
    elif args.format == 'human_compact':
        for reception in receptions:
            print('---')
            for key in reception.keys():
                if key not in ('ingestion_date', 'sensing_stop', 'geom', 'orbit', 'direction', 'daytime'):
                    print('{}: {}'.format(key, reception[key]))
    # Human
    elif args.format == 'human':
        for reception in receptions:
            print('---')
            for key in reception.keys():
                print('{}: {}'.format(key, reception[key]))
    # ID
    elif args.format == 'id':
        for reception in receptions:
            print(reception['id'])


# - - - - - - - - - - - -

# Load Configuration
username, password, satellites, instruments, receptions_url = load_configuration()

# Get command line arguments
args = get_arguments(list(satellites.keys()), list(instruments.keys()))

# Build URL based on command line arguements
url = build_url(args)

# Create Session and return the results
receptions = get_receptions(username, password, url, args.format)

# Print reception info
show_receptions(receptions, args)