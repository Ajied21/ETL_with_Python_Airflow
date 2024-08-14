import psycopg2
import pandas as pd
from .Transform_data import transform_data

def create_tables(conn):
    queries = [
        """
        CREATE TABLE IF NOT EXISTS app_infos (
            user_pseudo_id VARCHAR(255),
            id VARCHAR(255),
            version FLOAT,
            firebase_app_id VARCHAR(255),
            install_source VARCHAR(255)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS devices (
            user_pseudo_id VARCHAR(255),
            category VARCHAR(255),
            mobile_brand_name VARCHAR(255),
            mobile_model_name VARCHAR(255),
            mobile_os_hardware_model VARCHAR(255),
            operating_system VARCHAR(255),
            operating_system_version VARCHAR(255),
            vendor_id VARCHAR(255),
            advertising_id VARCHAR(255),
            language VARCHAR(255),
            is_limited_ad_tracking INT,
            time_zone_offset_seconds TIMESTAMP
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS events (
            user_pseudo_id VARCHAR(255),
            event_timestamp TIMESTAMP,
            event_date TIMESTAMP,
            event_name VARCHAR(255),
            event_bundle_sequence_id INT,
            event_server_timestamp_offset TIMESTAMP,
            user_first_touch_timestamp TIMESTAMP,
            stream_id INT,
            platform VARCHAR(255)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS geos (
            user_pseudo_id VARCHAR(255),
            continent VARCHAR(255),
            country VARCHAR(255),
            region VARCHAR(255),
            city VARCHAR(255),
            sub_continent VARCHAR(255)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS traffic_sources (
            user_pseudo_id VARCHAR(255)
        );
        """
    ]
    with conn.cursor() as cursor:
        for query in queries:
            cursor.execute(query)
        conn.commit()

def load_postgres(conn, table_name, df):
    with conn.cursor() as cursor:
        for _, row in df.iterrows():
            columns = ', '.join(row.index)
            values = ', '.join(['%s'] * len(row))
            insert_query = f'INSERT INTO {table_name} ({columns}) VALUES ({values});'
            cursor.execute(insert_query, list(row))
        conn.commit()

def load_data():
    conn = psycopg2.connect(
        host="postgres",
        database="project-dibimbing",
        user="ajied",
        password="admin"
    )

    create_tables(conn)

    # Transform data
    dataframes = transform_data()

    # Load data into PostgreSQL
    load_postgres(conn, 'app_infos', dataframes['app_infos'])
    load_postgres(conn, 'devices', dataframes['devices'])
    load_postgres(conn, 'events', dataframes['events'])
    load_postgres(conn, 'geos', dataframes['geos'])
    load_postgres(conn, 'traffic_sources', dataframes['traffic_sources'])

    conn.close()

if __name__ == "__main__":
    
    load_data()