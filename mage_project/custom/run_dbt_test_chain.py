if 'custom' not in globals():                                                             
    from mage_ai.data_preparation.decorators import custom                                
import subprocess                                                                         
                                                                                            
@custom                                                                                   
def run_dbt_test_chain(*args, **kwargs):                  
    result = subprocess.run(
        ['dbt', 'test', '--profiles-dir', '.'],
        cwd='/home/src/dbt_project',                                                      
        capture_output=True,                                                              
        text=True                                                                         
    )                                                                                     
    print(result.stdout)                                  
    if result.returncode != 0:
        print(result.stderr)
        raise Exception('dbt test failed') 