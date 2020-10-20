from datetime import (
    datetime
)

from airflow import (
    DAG
)

from operators.extract import (
    ExtractFromPostgresqlOperator
)
from operators.transform import (
    ProcessStoppedFoodOperator
)
from operators.load import (
    LoadToPostgresqlOperator
)


args = {
    'owner': 'Me',
    'start_date': datetime(2020, 10, 17, hour=15, minute=0)
}

TABLE_NAME = 'dashboards_productsstoppedfood'
TABLE_COLUMNS = [
    'updated_at', 'plu', 'ui2', 'ui3', 'name',
    'last_saled', 'days_of_nonsale', 'leftover', 'store_id', 'is_actual'
]
CSV_FILE_RAW_PATH = 'data/stopped_food.csv.gz'
CSV_FILE_CLEANED_PATH = 'data/stopped_food_clean.csv.gz'
CSV_FILE_PARAMS = {
    'sep': ';',
    'header': False,
    'index': False,
    'mode': 'w',
    'chunksize': 1000,
    'compression': 'gzip',
    'encoding': 'utf-8'
}


with DAG(
        'import_stopped_food',
        default_args=args,
        schedule_interval='*/50 * * * *'
) as dag:
    extract_stopped_food = ExtractFromPostgresqlOperator(
        task_id='extract_stopped_food',
        postgres_conn_id='postgres_default',
        sql='select {{ params.target_fields }} from {{ params.table_name }}',
        params={'table_name': TABLE_NAME, 'target_fields': ', '.join(TABLE_COLUMNS)},
        pandas_sql_params={'chunksize': 1000},
        csv_path=CSV_FILE_RAW_PATH,
        csv_params=CSV_FILE_PARAMS,
        depends_on_past=True,
        dag=dag)

    transform_stopped_food = ProcessStoppedFoodOperator(
        task_id='transform_stopped_food',
        csv_file_from_path=CSV_FILE_RAW_PATH,
        csv_file_to_path=CSV_FILE_CLEANED_PATH,
        csv_file_header=TABLE_COLUMNS,
        csv_file_params=CSV_FILE_PARAMS,
        dag=dag)

    load_stopped_food = LoadToPostgresqlOperator(
        task_id='load_stopped_food',
        postgres_conn_id='postgres_default',
        csv_path=CSV_FILE_CLEANED_PATH,
        destination_table=TABLE_NAME,
        target_fields=TABLE_COLUMNS
    )

    extract_stopped_food >> transform_stopped_food >> load_stopped_food
