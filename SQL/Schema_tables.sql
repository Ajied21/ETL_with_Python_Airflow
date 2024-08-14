-- create table
 CREATE TABLE IF NOT EXISTS app_infos (
            user_pseudo_id VARCHAR(255),
            id VARCHAR(255),
            version FLOAT,
            firebase_app_id VARCHAR(255),
            install_source VARCHAR(255)
        );

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
        
CREATE TABLE IF NOT EXISTS geos (
            user_pseudo_id VARCHAR(255),
            continent VARCHAR(255),
            country VARCHAR(255),
            region VARCHAR(255),
            city VARCHAR(255),
            sub_continent VARCHAR(255)
        );

CREATE TABLE IF NOT EXISTS traffic_sources (
            user_pseudo_id VARCHAR(255)
        );

--drop all table from original airflow
DO $$ DECLARE
    r RECORD;
BEGIN
    FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = current_schema()) LOOP
        EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.tablename) || ' CASCADE';
    END LOOP;
END $$;