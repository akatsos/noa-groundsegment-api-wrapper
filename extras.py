import json, csv

def show_receptions(receptions, format):
    """Outputs the receptions on the desired format"""
    # Total
    if format == 'total':
        print("{} receptions found".format(receptions))
    # JSON
    elif format == 'json':
        receptions_list = []
        for reception in receptions:
            receptions_list.append(reception)
        with open('receptions.json', 'w') as json_file:
            json.dump(receptions_list, json_file, sort_keys = True, indent = 4, ensure_ascii=False)
    # CSV
    elif format == 'csv':
        with open('receptions.csv', 'w') as csv_file:
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
    elif format == 'human_compact':
        for reception in receptions:
            print('---')
            for key in reception.keys():
                if key not in ('ingestion_date', 'sensing_stop', 'geom', 'orbit', 'direction', 'daytime'):
                    print('{}: {}'.format(key, reception[key]))
    # Human
    elif format == 'human':
        for reception in receptions:
            print('---')
            for key in reception.keys():
                print('{}: {}'.format(key, reception[key]))
    # ID
    elif format == 'id':
        for reception in receptions:
            print(reception['id'])