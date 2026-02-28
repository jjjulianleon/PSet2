from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from os import path
import os

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

@data_exporter
def export_data(data, *args, **kwargs):
    schema_name = 'bronze'
    table_name = 'trips'

    #Conexion a la bdd
    db_host = os.getenv('POSTGRES_HOST')
    db_user = os.getenv('POSTGRES_USER')
    db_password = os.getenv('POSTGRES_PASSWORD')
    db_name = os.getenv('POSTGRES_DB')
    db_port = os.getenv('POSTGRES_PORT', '5432')

    connection_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

    from sqlalchemy import create_engine
    engine = create_engine(connection_string)

    # Idempotencia; borrar e insertar por mes/servicio
    combos = data[['source_month', 'service_type']].drop_duplicates()

    for _, row in combos.iterrows():
        sm = row['source_month']
        st = row['service_type']

        # DELETE existente

        try:
            with engine.connect() as conn:
                conn.execute(f"DELETE FROM {schema_name}.{table_name} WHERE source_month = '{sm}' AND service_type = '{st}'")
                conn.commit()
        except Exception:
            pass

        # INSERT de mes/servicio        

        subset = data[(data['source_month'] == sm) & (data['service_type'] == st)]
        subset.to_sql(table_name, engine, schema=schema_name, if_exists='append', index=False, chunksize=10000)
        print(f"EXPORTED: {st} {sm} -> {len(subset)} filas")
