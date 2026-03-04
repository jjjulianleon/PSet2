import pandas as pd
import os
import gc
from sqlalchemy import create_engine, text


@custom
def ingest_bronze(*args, **kwargs):
    base_url = 'https://d37ci6vzurychx.cloudfront.net/trip-data'
    service_types = ['yellow', 'green']
    years = [2022, 2023, 2024, 2025]

    # Columnas estandar para ambos servicios
    standard_cols = [
        'VendorID', 'pickup_datetime', 'dropoff_datetime',
        'store_and_fwd_flag', 'RatecodeID', 'PULocationID',
        'DOLocationID', 'passenger_count', 'trip_distance',
        'fare_amount', 'extra', 'mta_tax', 'tip_amount',
        'tolls_amount', 'improvement_surcharge', 'total_amount',
        'payment_type', 'congestion_surcharge',
        'airport_fee', 'trip_type',
        'ingest_ts', 'source_month', 'service_type'
    ]

    # Conexion a PostgreSQL
    db_host = os.getenv('POSTGRES_HOST')
    db_user = os.getenv('POSTGRES_USER')
    db_password = os.getenv('POSTGRES_PASSWORD')
    db_name = os.getenv('POSTGRES_DB')
    db_port = os.getenv('POSTGRES_PORT', '5432')
    connection_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

    # Consultar meses ya cargados para saltarlos
    engine = create_engine(connection_string)
    try:
        loaded = pd.read_sql(
            "SELECT DISTINCT source_month, service_type FROM bronze.trips",
            engine
        )
        loaded_set = set(zip(loaded['source_month'], loaded['service_type']))
        print(f"Meses ya cargados: {len(loaded_set)}")
    except Exception:
        loaded_set = set()
        print("Primera ejecucion, no hay datos previos")
    engine.dispose()

    for service in service_types:
        for year in years:
            for month in range(1, 13):
                source_month = f"{year}-{month:02d}"
                url = f"{base_url}/{service}_tripdata_{source_month}.parquet"

                # Saltar si ya esta cargado
                if (source_month, service) in loaded_set:
                    print(f"YA EXISTE: {service} {source_month}, saltando...")
                    continue

                try:
                    # 1. EXTRACT: descargar parquet
                    df = pd.read_parquet(url)

                    # 2. TRANSFORM: unificar columnas datetime
                    if 'tpep_pickup_datetime' in df.columns:
                        df = df.rename(columns={
                            'tpep_pickup_datetime': 'pickup_datetime',
                            'tpep_dropoff_datetime': 'dropoff_datetime'
                        })
                    elif 'lpep_pickup_datetime' in df.columns:
                        df = df.rename(columns={
                            'lpep_pickup_datetime': 'pickup_datetime',
                            'lpep_dropoff_datetime': 'dropoff_datetime'
                        })

                    # 3. TRANSFORM: agregar metadatos
                    df['ingest_ts'] = pd.Timestamp.now()
                    df['source_month'] = source_month
                    df['service_type'] = service

                    # 4. TRANSFORM: estandarizar columnas
                    for col in standard_cols:
                        if col not in df.columns:
                            df[col] = None
                    df = df[standard_cols]

                    # 5. LOAD: idempotencia - DELETE antes de INSERT
                    engine = create_engine(connection_string)
                    try:
                        with engine.connect() as conn:
                            conn.execute(text(
                                f"DELETE FROM bronze.trips WHERE source_month = '{source_month}' AND service_type = '{service}'"
                            ))
                            conn.commit()
                    except Exception:
                        pass

                    # 6. LOAD: insertar en bronze
                    df.to_sql('trips', engine, schema='bronze', if_exists='append', index=False, chunksize=10000)
                    print(f"OK: {service} {source_month} -> {len(df)} filas")

                    # 7. Liberar memoria
                    engine.dispose()
                    del df
                    gc.collect()

                except Exception as e:
                    print(f"SKIP: {service} {source_month} -> {e}")
