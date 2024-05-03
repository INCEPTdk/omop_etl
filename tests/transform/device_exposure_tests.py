"""Device exposure transformation tests"""

import pandas as pd
from sqlalchemy import select

from etl.models.omopcdm54.clinical import (
    Stem as OmopStem,
    DeviceExposure as OmopDeviceExposure,
)

from etl.transform.device_exposure import transform as device_exposure_transformation
from etl.util.db import make_db_session, session_context
from tests.testutils import (
    PostgresBaseTest,
    base_path,
    enforce_dtypes,
    write_to_db,
)


class DeviceExposureTest(PostgresBaseTest):

    TARGET_MODEL = [OmopStem, OmopDeviceExposure]

    INPUT_OMOP_STEM = f"{base_path()}/test_data/device_exposure/in_omop_stem.csv"
    OUTPUT_FILE = f"{base_path()}/test_data/device_exposure/out_omop_device_exposure.csv"

    def setUp(self):
        super().setUp()
        self._create_tables_and_schema(self.TARGET_MODEL, schema='omopcdm')


        self.omop_stem = pd.read_csv(self.INPUT_OMOP_STEM, index_col=False, sep=';')

        self.expected_df = pd.read_csv(self.OUTPUT_FILE, index_col=False, sep=';', parse_dates = ['device_exposure_start_date','device_exposure_end_date', 'device_exposure_start_datetime', 'device_exposure_end_datetime'])
        self.expected_cols = [getattr(self.TARGET_MODEL[1], col) for col in self.expected_df.columns.to_list()]

    def tearDown(self) -> None:
        super().tearDown()
        self._drop_tables_and_schema(self.TARGET_MODEL, schema='omopcdm')

    def _insert_test_data(self, engine):
        write_to_db(engine, self.omop_stem, OmopStem.__tablename__, schema=OmopStem.metadata.schema)

    def test_transform(self):
        self._insert_test_data(self.engine)

        with session_context(make_db_session(self.engine)) as session:
            device_exposure_transformation(session)

        result = select(self.expected_cols)
        result_df = pd.read_sql(result, self.engine)
        result_df = enforce_dtypes(self.expected_df, result_df)
        pd.testing.assert_frame_equal(result_df, self.expected_df)

__all__ = ['DeviceExposureTest']
