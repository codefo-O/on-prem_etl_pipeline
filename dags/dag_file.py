from email.mime import application
import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator


DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(0),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False
}

def retrieve_file(**kwargs):
    print(kwargs)
    input_path = kwargs['dag_run'].conf['input_path']
    kwargs['ti'].xcom_push(key = 'input_path', value = input_path)
    output_path = kwargs['dag_run'].conf['output_path']
    kwargs['ti'].xcom_push(key = 'output_path', value = output_path)
    file_type = kwargs['dag_run'].conf['file_type']
    kwargs['ti'].xcom_push(key = 'file_type', value = file_type)
    partition_column = kwargs['dag_run'].conf['partition_column']
    kwargs['ti'].xcom_push(key = 'partition_column', value = partition_column)

dag = DAG(
    dag_id= 'poc_etl_demo',
    default_args=DEFAULT_ARGS,
    schedule_interval='0 0 0 0 0'
)

parse_request = PythonOperator(task_id='parse_request',
                             provide_context=True,
                             python_callable=retrieve_file,
                             dag=dag)

bash_submit = BashOperator(
    task_id="poc_elt_demo",
    bash_command="spark-submit --master local --py-files /jars/job.zip /scripts/workflow_entry.py -p \"{'input_path': '{{ task_instance.xcom_pull('parse_request', key='input_path') }}', 'name':'poc etl demo', 'file_type': '{{ task_instance.xcom_pull('parse_request', key='file_type') }}', 'output_path': '{{ task_instance.xcom_pull('parse_request', key='output_path') }}', 'partition_column': '{{ task_instance.xcom_pull('parse_request', key='partition_column') }}'}\"",
    dag=dag,
)

bash_submit.set_upstream(parse_request)

                                          