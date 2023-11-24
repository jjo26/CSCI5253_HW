from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
from etl_scripts.transform import transform_data
from etl_scripts.load import load_data
from etl_scripts.load import load_fact_data

SOURCE_URL = 'https://data.austintexas.gov/api/views/9t4d-g238/rows.csv'
AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', '/opt/airflow')
CSV_TARGET_DIR = AIRFLOW_HOME + '/data/{{ ds }}/downloads'
CSV_TARGET_FILE = CSV_TARGET_DIR + '/outcomes_{{ ds }}.csv'
TRANS_TARGET_DIR = AIRFLOW_HOME + '/data/{{ ds }}/processed'

with DAG(
    dag_id="outcomes_dag",
    start_date = datetime(2023,11,23),
    schedule_interval='@daily'
) as dag:

    extract = BashOperator(
        task_id="extract",
        bash_command=f"curl --create-dirs -o {CSV_TARGET_FILE} {SOURCE_URL}",
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=transform_data,
        op_kwargs = {
            'source_csv': CSV_TARGET_FILE,
            'target_dir': TRANS_TARGET_DIR
        }

    )


    load_dates_dim = PythonOperator(
        task_id="load_dates_dim",
        python_callable=load_data,
        op_kwargs = {
            'table_file': TRANS_TARGET_DIR + '/dim_processed.csv',
            'table_name': 'processed_dim',
            'key': 'processed_id'
        }

    )

    load_animals_dim = PythonOperator(
        task_id="load_animals_dim",
        python_callable=load_data,
        op_kwargs = {
            'table_file': TRANS_TARGET_DIR + '/dim_animal.csv',
            'table_name': 'animal_dim',
            'key': 'animal_id'
        }

    )

    load_outcome_type_dim = PythonOperator(
        task_id="load_outcome_type_dim",
        python_callable=load_data,
        op_kwargs = {
            'table_file': TRANS_TARGET_DIR + '/dim_outcometype.csv',
            'table_name': 'outcome_type_dim',
            'key': 'outcome_type_id'
        }

    )

    load_outcome_subtype_dim = PythonOperator(
        task_id="load_outcome_subtype_dim",
        python_callable=load_data,
        op_kwargs = {
            'table_file': TRANS_TARGET_DIR + '/dim_outcomesubtype.csv',
            'table_name': 'outcome_subtype_dim',
            'key': 'outcome_subtype_id'
        }

    )

    load_repro_dim = PythonOperator(
        task_id="load_repro_dim",
        python_callable=load_data,
        op_kwargs = {
            'table_file': TRANS_TARGET_DIR + '/dim_repro.csv',
            'table_name': 'repro_dim',
            'key': 'reproductive_status_id'
        }

    )


    load_outcomes_fct = PythonOperator(
        task_id="load_outcomes_fct",
        python_callable=load_fact_data,
        op_kwargs = {
            'table_file': TRANS_TARGET_DIR + '/fct_outcomes.csv',
            'table_name': 'visit_fact'
        }

    )

    extract >> transform >> [load_animals_dim, load_dates_dim, load_outcome_type_dim, load_outcome_subtype_dim, load_repro_dim] >> load_outcomes_fct