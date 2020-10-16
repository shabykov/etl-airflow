from datetime import (
    datetime
)

import pandas as pd
from airflow.operators import (
    BaseOperator
)
from airflow.utils.decorators import (
    apply_defaults
)


class ProcessRecycleOperator(BaseOperator):
    """
    Transform source data to load
    """

    @apply_defaults
    def __init__(
            self,
            csv_file_from_path,
            csv_file_to_path,
            csv_file_header,
            csv_file_params,
            *args, **kwargs):

        super(ProcessRecycleOperator, self).__init__(*args, **kwargs)
        self.csv_file_from_path = csv_file_from_path
        self.csv_file_to_path = csv_file_to_path
        self.csv_file_header = csv_file_header
        self.csv_file_params = csv_file_params

    def execute(self, context):
        df = pd.read_csv(self.csv_file_from_path, sep=';', names=self.csv_file_header)
        df = df[df['store_id'].notna()]
        df['updated_at'] = [datetime.now().isoformat()] * len(df)
        df.fillna(value={
            'id_item': 0,
            'name': 'unknown',
            'quantity': 0,
            'is_selected_for_dashboard': False,
            'quantity_rto_plan': 0,
            'recycle_plan': 0,
            'rto_plan': 0
        }, inplace=True)
        df.to_csv(self.csv_file_to_path, **self.csv_file_params)
