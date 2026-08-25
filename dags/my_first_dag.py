from airflow.sdk import DAG 
from airflow.providers.standard.operators.bash import BashOperator 
from airflow.providers.standard.operators.python import PythonOperator 


dag = DAG(dag_id ='my_first_dag') 


def print_context(**kwargs): 
    print(kwargs) 
    print("Job Completed") 

     
copy_file = BashOperator(dag = dag,  
                         task_id = "copy_file", 
                         bash_command ="echo copying file") 

task2 = PythonOperator( 
            task_id = 'task2', 
            python_callable = print_context, 
            dag = dag 
) 
copy_file >> task2 