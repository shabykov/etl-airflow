from contextlib import closing

import pandas as pd
from airflow.hooks.postgres_hook import (
    PostgresHook
)
from airflow.operators import (
    BaseOperator
)
from airflow.utils.decorators import (
    apply_defaults
)


def truncate_table(target, table):
    """
    Clear target table before load data
    :param target:
    :param table:
    :return:
    """
    with closing(target.get_conn()) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.execute("TRUNCATE {} RESTART IDENTITY;".format(table))
            conn.commit()


class LoadFromCSVOperator(BaseOperator):
    """
    Load data to target table
    """

    ui_color = '#ededed'

    @apply_defaults
    def __init__(
            self,
            csv_path,
            destination_table,
            target_fields,
            postgres_conn_id='postgres_default',
            autocommit=True,
            *args, **kwargs):
        super(LoadFromCSVOperator, self).__init__(*args, **kwargs)
        self.csv_path = csv_path
        self.destination_table = destination_table
        self.target_fields = target_fields
        self.postgres_conn_id = postgres_conn_id
        self.autocommit = autocommit

    def execute(self, context):
        # Read from CSV file
        df = pd.read_csv(self.csv_path, sep=';', header=None)
        rows = [tuple(x) for x in df.to_numpy()]
        target = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        truncate_table(target, self.destination_table)
        target.insert_rows(
            self.destination_table,
            rows=rows,
            target_fields=self.target_fields,
            commit_every=1000,
        )
