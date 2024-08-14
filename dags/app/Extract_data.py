import pandas as pd
import json
import requests

def get_urls(url):
    # Mengunduh file JSON
    response = requests.get(url)
    data = response.json()

    # Fungsi untuk mengekstrak event_params dan user_properties
    def extract_params(params_list):
        params = []
        for param in params_list:
            key = param['key']
            value = param['value']
            param_data = {
                'key': key,
                'string_value': value.get('string_value'),
                'int_value': value.get('int_value'),
                'float_value': value.get('float_value'),
                'double_value': value.get('double_value')
            }
            params.append(param_data)
        return params

    # Fungsi untuk meratakan struktur JSON
    def flatten_data(data):
        events = []
        devices = []
        geos = []
        app_infos = []
        traffic_sources = []

        for event in data:
            flat_event = {
                'event_timestamp': event['event_timestamp'],
                'event_date': event['event_date'],
                'event_name': event['event_name'],
                'event_previous_timestamp': event['event_previous_timestamp'],
                'event_bundle_sequence_id': event['event_bundle_sequence_id'],
                'event_server_timestamp_offset': event['event_server_timestamp_offset'],
                'user_pseudo_id': event['user_pseudo_id'],
                'user_first_touch_timestamp': event['user_first_touch_timestamp'],
                'stream_id': event['stream_id'],
                'platform': event['platform'],
            }
            events.append(flat_event)

            device = event['device']
            device['user_pseudo_id'] = event['user_pseudo_id']
            devices.append(device)

            geo = event['geo']
            geo['user_pseudo_id'] = event['user_pseudo_id']
            geos.append(geo)

            app_info = event['app_info']
            app_info['user_pseudo_id'] = event['user_pseudo_id']
            app_infos.append(app_info)

            traffic_source = event['traffic_source']
            traffic_source['user_pseudo_id'] = event['user_pseudo_id']
            traffic_sources.append(traffic_source)

        return events, devices, geos, app_infos, traffic_sources

    # Meratakan data JSON
    events, devices, geos, app_infos, traffic_sources = flatten_data(data)

    # Konversi ke DataFrame
    df_events = pd.DataFrame(events)
    df_devices = pd.DataFrame(devices)
    df_geos = pd.DataFrame(geos)
    df_app_infos = pd.DataFrame(app_infos)
    df_traffic_sources = pd.DataFrame(traffic_sources)

    # Konversi DataFrames ke JSON
    json_output = {
        'events': df_events.to_json(orient='records'),
        'devices': df_devices.to_json(orient='records'),
        'geos': df_geos.to_json(orient='records'),
        'app_infos': df_app_infos.to_json(orient='records'),
        'traffic_sources': df_traffic_sources.to_json(orient='records')
    }

    print(f"Tabel events")
    print(df_events.head())
    print(f"\nTabel devices")
    print(df_devices.head())
    print(f"\nTabel geos")
    print(df_geos.head())
    print(f"\nTabel app_infos")
    print(df_app_infos.head())
    print(f"\nTabel traffic_sources")
    print(df_traffic_sources.head())
    """
    # Simpan DataFrame ke dalam file CSV (Opsional)
    df_events.to_csv('./Data/events.csv', index=False)
    df_devices.to_csv('./Data/devices.csv', index=False)
    df_geos.to_csv('./Data/geos.csv', index=False)
    df_app_infos.to_csv('./Data/app_infos.csv', index=False)
    df_traffic_sources.to_csv('./Data/traffic_sources.csv', index=False)
    """

    return json_output

def extract_data():

    data_extract = get_urls('https://drive.google.com/uc?export=download&id=14zGvrxQKHix2IBq1Jr3LyHXm35EwVyYG')

    return data_extract

if __name__ == "__main__":
 
    extract_data()