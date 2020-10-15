from datetime import (
    datetime
)

from airflow import (
    DAG
)

from operators.extract import (
    ExtractFromPostgresOperator
)
from operators.transform import (
    TransformCSVFileOperator
)
from operators.load import (
    LoadFromCSVOperator
)


args = {
    'owner': 'Me',
    'start_date': datetime(2020, 10, 15, hour=13, minute=0)
}

TABLE_NAME = 'dashboards_recycle'
TABLE_COLUMNS = [
    'updated_at', 'id_item', 'name', 'quantity',
    'store_id', 'is_selected_for_dashboard',
    'quantity_rto_plan', 'recycle_plan', 'rto_plan'
]
CSV_FILE_RAW_PATH = 'data/recycle.csv'
CSV_FILE_CLEANED_PATH = 'data/recycle_clean.csv'
CSV_FILE_PARAMS = {
    'sep': ';',
    'header': False,
    'index': False,
    'mode': 'w',
    'encoding': 'utf-8'
}


with DAG(
        'import_recycle',
        default_args=args,
        schedule_interval='*/50 * * * *'
) as dag:
    extract_recycle = ExtractFromPostgresOperator(
        task_id='extract_recycle',
        postgres_conn_id='postgres_default',
        sql='select {{ params.target_fields }} from {{ params.table_name }}',
        params={'table_name': TABLE_NAME, 'target_fields': ', '.join(TABLE_COLUMNS)},
        pandas_sql_params={'chunksize': 100},
        csv_path=CSV_FILE_RAW_PATH,
        csv_params=CSV_FILE_PARAMS,
        depends_on_past=True,
        dag=dag)

    transform_recycle = TransformCSVFileOperator(
        task_id='transform_recycle',
        csv_file_from_path=CSV_FILE_RAW_PATH,
        csv_file_to_path=CSV_FILE_CLEANED_PATH,
        csv_file_header=TABLE_COLUMNS,
        csv_file_params=CSV_FILE_PARAMS,
        dag=dag)

    load_recycle = LoadFromCSVOperator(
        task_id='load_recycle',
        postgres_conn_id='postgres_default',
        csv_path=CSV_FILE_CLEANED_PATH,
        destination_table=TABLE_NAME,
        target_fields=TABLE_COLUMNS
    )

    extract_recycle >> transform_recycle >> load_recycle
