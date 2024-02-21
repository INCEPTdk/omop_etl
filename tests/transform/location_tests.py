"""Location transformation tests"""
import os

from etl.models.omopcdm54.health_systems import Location
from etl.util.db import make_db_session, session_context
from tests.testutils import PostgresBaseTest


class LocationTransformationTest(PostgresBaseTest):
    MODELS = [Location]

    def setUp(self):
        super().setUp()
        self._create_tables_and_schema(self.MODELS, schema='omopcdm')

    def tearDown(self) -> None:
        super().tearDown()
        self._drop_tables_and_schema(self.MODELS, schema='omopcdm')

    def _run_location_transformation(self, session):
        from etl.transform.location import transform
        transform(session)

    def test_transform(self):
        with session_context(make_db_session(self.engine)) as session:
            os.environ["HOSPITAL_SHAK_CODE"] = "1309"
            self._run_location_transformation(session)

            result = session.query(Location).all()
            self.assertEqual(len(result), 1)
            self.assertEqual(result[0].location_id, 1)
            self.assertEqual(result[0].zip, "2400")
            self.assertEqual(result[0].location_source_value, "1309")
            self.assertEqual(result[0].country_concept_id, 4330435)


__all__ = ["LocationTransformationTest"]
