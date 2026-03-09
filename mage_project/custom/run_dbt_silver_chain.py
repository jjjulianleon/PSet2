if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
import subprocess

@custom
def run_dbt_silver(*args, **kwargs):
    result = subprocess.run(
        ['dbt', 'run', '--select', 'silver', '--profiles-dir', '.'],
        cwd='/home/src/dbt_project',
        capture_output=True,
        text=True
    )
    print(result.stdout)
    if result.returncode !=0:
        print(result.stderr)
        raise Exception('dbt run silver failed')