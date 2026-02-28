import pandas as pd

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data(*args, **kwargs):
    base_url = 'https://d37ci6vzurychx.cloudfront.net/trip-data'
    service_types = ['yellow', 'green']
    years = [2022, 2023, 2024, 2025]

    dfs = []

    for service in service_types:
        for year in years:
            for month in range(1, 13):
                source_month = f"{year}-{month:02d}"
                url = f"{base_url}/{service}_tripdata_{source_month}.parquet"

                try:
                    df = pd.read_parquet(url)
                    df['ingest_ts'] = pd.Timestamp.now()
                    df['source_month'] = source_month
                    df['service_type'] = service
                    dfs.append(df)
                    print(f"Ok: {service} {source_month} -> {len(df)} filas")
                except Exception as e:
                    print(f"SKIP: {service} {source_month} -> {e}")
    
    return pd.concat(dfs, ignore_index=True)