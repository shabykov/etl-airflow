import logging
from contextlib import (
    closing
)

import pandas
import psycopg2
import psycopg2.extras as extras
from airflow.hooks.postgres_hook import (
    PostgresHook
)
from airflow.operators import (
    BaseOperator
)
from airflow.utils.decorators import (
    apply_defaults
)


class PostgresqlPatchedHook(PostgresHook):
    def insert_many_rows(self,
                         table,
                         rows,
                         target_fields=None):

        with closing(self.get_conn()) as conn:
            if self.supports_autocommit:
                self.set_autocommit(conn, False)
            conn.commit()

            with closing(conn.cursor()) as cur:
                cur.execute("TRUNCATE %s RESTART IDENTITY;" % table)
                self.log.info("Table %s is cleaned", table)
                try:
                    sql = self._generate_many_insert_sql(table, target_fields)
                    extras.execute_values(cur, sql, rows)
                except (Exception, psycopg2.DatabaseError) as error:
                    conn.rollback()
                    self.log.error('Load error: {}'.format(error.__str__()))
                    raise

            conn.commit()
        self.log.info("Done loading. Loaded a total of %s rows", len(rows))

    @staticmethod
    def _generate_many_insert_sql(table, target_fields):
        if target_fields:
            target_fields_fragment = ", ".join(target_fields)
        else:
            target_fields_fragment = ''

        sql = "INSERT INTO %s (%s) VALUES %%s" % (table, target_fields_fragment)
        return sql


class LoadToPostgresqlOperator(BaseOperator):
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
        super(LoadToPostgresqlOperator, self).__init__(*args, **kwargs)
        self.csv_path = csv_path
        self.destination_table = destination_table
        self.target_fields = target_fields
        self.postgres_conn_id = postgres_conn_id
        self.autocommit = autocommit

    def execute(self, context):
        try:
            # Read from CSV file
            df = pandas.read_csv(self.csv_path, sep=';', header=None)
            rows = [tuple(x) for x in df.to_numpy()]
        except (Exception, FileNotFoundError, pandas.errors.EmptyDataError) as error:
            logging.error('Input file is empty: {}'.format(error.__str__()))
            raise

        target = PostgresqlPatchedHook(postgres_conn_id=self.postgres_conn_id)
        target.insert_many_rows(
            self.destination_table,
            rows=rows,
            target_fields=self.target_fields
        )
