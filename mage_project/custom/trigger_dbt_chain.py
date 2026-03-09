if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
import requests

@custom
def trigger_dbt_chain(*args, **kwargs):
    url = 'http://localhost:6789/api/pipeline_schedules/2/pipeline_runs/ca5d786ce38645f1874137ba3ec39bb2'
    response = requests.post(url, json={'pipeline_run': {'variables': {}}})

    if response.status_code == 200:
        print('Pipeline dbt_after_ingest disparado exitosamente')
    else:
        print(f'Error: {response.status_code} - {response.text}')
        raise Exception('No se pudo disparar dbt_after_ingest')