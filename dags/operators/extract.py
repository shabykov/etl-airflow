import logging

from airflow.hooks.postgres_hook import (
    PostgresHook
)
from airflow.operators.bash_operator import (
    BaseOperator
)
from airflow.utils.decorators import (
    apply_defaults
)


class ExtractFromPostgresqlOperator(BaseOperator):
    """
    Extract data from postgresql source data
    """

    template_fields = ('sql', 'csv_path',)
    template_ext = ('.sql',)
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
            self,
            sql,
            csv_path,
            postgres_conn_id='postgres_default',
            autocommit=False,
            pandas_sql_params=None,
            csv_params=None,
            *args, **kwargs):
        super(ExtractFromPostgresqlOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.postgres_conn_id = postgres_conn_id
        self.csv_path = csv_path
        self.autocommit = autocommit
        self.pandas_sql_params = pandas_sql_params
        self.csv_params = csv_params

    def execute(self, context):
        logging.info('Executing: ' + str(self.sql))
        source = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        pandas_df = source.get_pandas_df(self.sql, parameters=self.pandas_sql_params)
        logging.info('Saving to: ' + str(self.csv_path))
        pandas_df.to_csv(self.csv_path, **self.csv_params)
