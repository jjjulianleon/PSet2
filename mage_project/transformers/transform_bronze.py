if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    df = data.copy()

    if ('tpep_pickup_datetime' in df.columns
            and 'lpep_pickup_datetime' in df.columns):
        df['pickup_datetime'] = df['tpep_pickup_datetime'].fillna(df['lpep_pickup_datetime'])
        df['dropoff_datetime'] = df['tpep_dropoff_datetime'].fillna(df['lpep_dropoff_datetime'])
    elif 'tpep_pickup_datetime' in df.columns:
        df['pickup_datetime'] = df['tpep_pickup_datetime']
        df['dropoff_datetime'] = df['tpep_dropoff_datetime']
    elif 'lpep_pickup_datetime' in df.columns:
        df['pickup_datetime'] = df['lpep_pickup_datetime']
        df['dropoff_datetime'] = df['lpep_dropoff_datetime']

    df = df.drop(columns=[
        'tpep_pickup_datetime', 'tpep_dropoff_datetime',
        'lpep_pickup_datetime', 'lpep_dropoff_datetime',
    ], errors='ignore')

    return df
