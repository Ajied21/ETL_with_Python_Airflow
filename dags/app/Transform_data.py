import pandas as pd
import json
from .Extract_data import extract_data

def transform_table(df, columns_to_keep):
    df = df.copy()
    df.dropna()

    for column, dtype in columns_to_keep.items():
        if dtype == 'int':
            df[column] = pd.to_numeric(df[column], errors='coerce').fillna(0).astype(int)
        elif dtype == 'float':
            df[column] = pd.to_numeric(df[column], errors='coerce').astype(float)
        elif dtype == 'datetime64[ns]':
            df[column] = pd.to_datetime(df[column], errors='coerce')

    return df

def transform_data():
    json_output = extract_data()  # Ambil data JSON dari fungsi extract_data

    # Konversi JSON ke DataFrame
    df_events = pd.read_json(json_output['events'], orient='records')
    df_devices = pd.read_json(json_output['devices'], orient='records')
    df_geos = pd.read_json(json_output['geos'], orient='records')
    df_app_infos = pd.read_json(json_output['app_infos'], orient='records')
    df_traffic_sources = pd.read_json(json_output['traffic_sources'], orient='records')

    # Definisikan kolom untuk setiap tabel
    app_infos_cols = {
        'user_pseudo_id': 'str',
        'id': 'str',
        'version': 'float',
        'firebase_app_id': 'str',
        'install_source': 'str'
    }

    devices_cols = {
        'user_pseudo_id': 'str',
        'category': 'str',
        'mobile_brand_name': 'str',
        'mobile_model_name': 'str',
        'mobile_os_hardware_model': 'str',
        'operating_system': 'str',
        'operating_system_version': 'float',
        'vendor_id': 'str',
        'advertising_id': 'str',  # Tipe 'str' untuk UUID atau kode alfanumerik
        'language': 'str',
        'is_limited_ad_tracking': 'int',
        'time_zone_offset_seconds': 'datetime64[ns]'
    }

    events_cols = {
        'user_pseudo_id': 'str',
        'event_timestamp': 'datetime64[ns]',
        'event_date': 'datetime64[ns]',
        'event_name': 'str',
        'event_bundle_sequence_id': 'int',
        'event_server_timestamp_offset': 'datetime64[ns]',
        'user_first_touch_timestamp': 'datetime64[ns]',
        'stream_id': 'int',
        'platform': 'str'
    }

    geos_cols = {
        'user_pseudo_id': 'str',
        'continent': 'str',
        'country': 'str',
        'region': 'str',
        'city': 'str',
        'sub_continent': 'str'
    }

    traffic_sources_cols = {
        'user_pseudo_id': 'str'
    }

    # Transformasi tabel
    app_infos = transform_table(df_app_infos[app_infos_cols.keys()], app_infos_cols)
    devices = transform_table(df_devices[devices_cols.keys()], devices_cols)
    events = transform_table(df_events[events_cols.keys()], events_cols)
    geos = transform_table(df_geos[geos_cols.keys()], geos_cols)
    traffic_sources = transform_table(df_traffic_sources[traffic_sources_cols.keys()], traffic_sources_cols)

    # Simpan data yang telah ditransformasi ke dalam file JSON
    output_paths = {
        'app_infos': app_infos,
        'devices': devices,
        'events': events,
        'geos': geos,
        'traffic_sources': traffic_sources
    }
    
    print(f"Tabel events")
    print(df_events.info())
    print(f"\nTabel devices")
    print(df_devices.info())
    print(f"\nTabel geos")
    print(df_geos.info())
    print(f"\nTabel app_infos")
    print(df_app_infos.info())
    print(f"\nTabel traffic_sources")
    print(df_traffic_sources.info())
    
    return output_paths

if __name__ == "__main__":

    transform_data()