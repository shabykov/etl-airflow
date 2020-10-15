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


class TransformCSVFileOperator(BaseOperator):
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

        super(TransformCSVFileOperator, self).__init__(*args, **kwargs)
        self.csv_file_from_path = csv_file_from_path
        self.csv_file_to_path = csv_file_to_path
        self.csv_file_header = csv_file_header
        self.csv_file_params = csv_file_params

    def execute(self, context):
        df = pd.read_csv(self.csv_file_from_path, sep=';', names=self.csv_file_header)
        df['updated_at'] = [datetime.now().isoformat()] * len(df)
        df.to_csv(self.csv_file_to_path, **self.csv_file_params)
