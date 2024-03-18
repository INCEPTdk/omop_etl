"""Care site transformation tests"""

import pathlib

import pandas as pd

from etl.models.omopcdm54.health_systems import CareSite, Location
from etl.sql.care_site import get_care_site_insert
from etl.util.db import make_db_session, session_context
from tests.testutils import PostgresBaseTest, write_to_db


class CareSiteTransformationTest(PostgresBaseTest):
    MODELS = [Location, CareSite]

    def setUp(self):
        super().setUp()
        self._drop_tables_and_schema(self.MODELS, schema="omopcdm")
        self._create_tables_and_schema(self.MODELS, schema="omopcdm")
        base_path = pathlib.Path(__file__).parent.resolve()
        self.in_empty_location = pd.read_csv(
            f"{base_path}/test_data/care_site/in_empty_location.csv"
        )
        self.in_location = pd.read_csv(
            f"{base_path}/test_data/care_site/in_location.csv"
        )

    def tearDown(self) -> None:
        super().tearDown()
        self._drop_tables_and_schema(self.MODELS, schema="omopcdm")

    def _run_care_site_transformation(self, session, shak_code):

        care_site_insert = get_care_site_insert(shak_code)
        session.execute(care_site_insert)

    def test_transform(self):

        write_to_db(
            self.engine,
            self.in_location,
            Location.__tablename__,
            schema="omopcdm",
            if_exists="replace",
        )
        with session_context(make_db_session(self.engine)) as session:
            shak_code = "1301011"
            test_care_site_name = "RH 4131 Intensiv Terapiklinik"
            test_place_of_service_concept_id = 32037
            test_place_of_service_source_value = "department_type|ICU"
            test_care_site_source_value = "department_shak_code|1301011"

            self._run_care_site_transformation(session, shak_code)
            result = session.query(CareSite).all()
            self.assertEqual(len(result), 1)
            self.assertEqual(result[0].location_id, 1)
            self.assertEqual(result[0].care_site_name, test_care_site_name)
            self.assertEqual(
                result[0].care_site_source_value, test_care_site_source_value
            )
            self.assertEqual(
                result[0].place_of_service_source_value,
                test_place_of_service_source_value,
            )
            self.assertEqual(
                result[0].place_of_service_concept_id,
                test_place_of_service_concept_id,
            )

    def test_transform_empty_shak_code(self):

        write_to_db(
            self.engine,
            self.in_empty_location,
            Location.__tablename__,
            schema="omopcdm",
            if_exists="replace",
        )

        with session_context(make_db_session(self.engine)) as session:
            shak_code = None

            test_care_site_name = None
            test_place_of_service_concept_id = None
            test_place_of_service_source_value = "department_type|"
            test_care_site_source_value = "department_shak_code|"

            self._run_care_site_transformation(session, shak_code)
            result = session.query(CareSite).all()
            self.assertEqual(len(result), 1)
            self.assertEqual(result[0].location_id, 1)
            self.assertEqual(result[0].care_site_name, test_care_site_name)
            self.assertEqual(
                result[0].care_site_source_value, test_care_site_source_value
            )
            self.assertEqual(
                result[0].place_of_service_source_value,
                test_place_of_service_source_value,
            )
            self.assertEqual(
                result[0].place_of_service_concept_id,
                test_place_of_service_concept_id,
            )


__all__ = ["CareSiteTransformationTest"]
