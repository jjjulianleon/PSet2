if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
import os
from sqlalchemy import create_engine, text

@custom
def run_partitioning_chain(*args, **kwargs):
    db_host = os.getenv('POSTGRES_HOST')
    db_user = os.getenv('POSTGRES_USER')
    db_password = os.getenv('POSTGRES_PASSWORD')
    db_name = os.getenv('POSTGRES_DB')
    db_port = os.getenv('POSTGRES_PORT', '5432')
    conn_str = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    engine = create_engine(conn_str)

    with open('/home/src/sql/partitioning/01_create_partitions.sql', 'r') as f:
        sql_script = f.read()

    with engine.begin() as conn:
        conn.execute(text(sql_script))

    print("Partitioning script ejecutado correctamente")
