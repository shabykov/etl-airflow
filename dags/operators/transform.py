from datetime import (
    datetime
)

import pandas
from airflow.operators import (
    BaseOperator
)
from airflow.utils.decorators import (
    apply_defaults
)


class ProcessCSVFileOperator(BaseOperator):
    """
    Process source data to load
    """

    @apply_defaults
    def __init__(
            self,
            csv_file_from_path,
            csv_file_to_path,
            csv_file_header,
            csv_file_params,
            *args, **kwargs):
        super(ProcessCSVFileOperator, self).__init__(*args, **kwargs)
        self.csv_file_from_path = csv_file_from_path
        self.csv_file_to_path = csv_file_to_path
        self.csv_file_header = csv_file_header
        self.csv_file_params = csv_file_params

    def execute(self, context):
        raise NotImplementedError()


class ProcessRecycleOperator(ProcessCSVFileOperator):
    """
    Process source Recycle data to load
    """

    def execute(self, context):
        df = pandas.read_csv(self.csv_file_from_path, sep=';', names=self.csv_file_header)
        df = df[df['store_id'].notna()]
        df['updated_at'] = datetime.now().isoformat()
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


class ProcessStoppedFoodOperator(ProcessCSVFileOperator):
    def execute(self, context):
        df = pandas.read_csv(self.csv_file_from_path, sep=';', names=self.csv_file_header)
        df = df[df['store_id'].notna()]
        df['updated_at'] = datetime.now().isoformat()
        df['is_actual'] = True
        df.fillna(value={
            'plu': 'unknown',
            'ui2': 'unknown',
            'ui3': 'unknown',
            'name': 'unknown',
            'last_saled': 0,
            'days_of_nonsale': 0,
            'leftover': 0
        }, inplace=True)
        df.to_csv(self.csv_file_to_path, **self.csv_file_params)
