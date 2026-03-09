if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
import os
from sqlalchemy import create_engine, text

@custom
def run_partitioning(*args, **kwargs):
    db_host = os.getenv('POSTGRES_HOST')
    db_user = os.getenv('POSTGRES_USER')
    db_password = os.getenv('POSTGRES_PASSWORD')                                          
    db_name = os.getenv('POSTGRES_DB')                                                    
    db_port = os.getenv('POSTGRES_PORT', '5432')                                          
    engine = create_engine(f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}")

    with open ('/home/src/sql/partitioning/01_create_partitions.sql', 'r') as f:
        sql_script = f.read()
    
    with engine.begin() as conn:
        conn.execute(text(sql_script))

    print("Partitioning script ejecutado correctamente")
    