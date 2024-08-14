#!/bin/bash

#Setel Kepemilikan dan Izin Direktori di Host
#sudo chown -R $USER:$USER ./dags
#sudo chmod -R 775 ./dags

# Menjalankan perintah SQL untuk drop semua tabel
echo "drop table in database..."
sudo docker exec -i pipeline_etl_with_airflow-postgres-1 psql -U ajied -d project-dibimbing <<EOF
DO \$\$ DECLARE
    r RECORD;
BEGIN
    FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = current_schema()) LOOP
        EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.tablename) || ' CASCADE';
    END LOOP;
END \$\$;
EOF

sleep 5

echo "Success drop tables..."
