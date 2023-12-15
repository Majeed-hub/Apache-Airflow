from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to write tasks!
from airflow.operators.bash_operator import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago

# default arguments
default_args = {
    'owner': 'Abdul Majeed',
    'start_date': days_ago(0),
    'email': ['majeed@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'etl_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(minutes=1),
)

unzip_data = BashOperator(
    task_id = 'unzip_data',
    bash_command = 'tar -xvzf home/project/airflow/dags/finalassignment/tolldata.tgz',
    dag = dag,
)

extract_data_from_csv = BashOperator(
    task_id = 'extract_data_from_csv',
    bash_command = 'cut -d"," -f1,2,3,4 home/project/airflow/dags/finalassignment/vehicle-data.csv > \
     home/project/airflow/dags/finalassignment/csv_data.csv',
    dag = dag,
)

extract_data_from_tsv = BashOperator(
    task_id = 'extract_data_from_tsv',
    bash_command = 'cut -f5,6,7 tollplaza-data.tsv | awk 'BEGIN {OFS=","} {print $1, $2, $3}' > tsv_data.csv,
    dag = dag,
)


extract_data_from_fixed_width = BashOperator(
    task_id = 'extract_data_from_fixed_width',
    bash_command = 'awk 'BEGIN {OFS=","}{print substr($0, 59, 3), substr($0, 63, 5)}' payment-data.txt > fixed_width_data.csv',
    dag = dag,
)

consolidate_data = BashOperator(
    task_id = 'consolidate_data',
    bash_command = 'paste -d',' csv_data.csv tsv_data.csv fixed_width_data.cs > extracted_data.csv',
    dag = dag,
)

transform_data = BashOperator(
    task_id = 'transform_data',
    bash_command = 'tr '[:lower:]' '[:upper:]' < extracted_data.csv > transformed_data.csv ',
    dag = dag,
)

unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_tsv >> consolidate_data >> transform_data