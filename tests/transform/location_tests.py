"""Location transformation tests"""

from etl.models.omopcdm54.health_systems import Location
from etl.sql.location import get_location_insert
from etl.util.db import make_db_session, session_context
from tests.testutils import DuckDBBaseTest


class LocationTransformationTest(DuckDBBaseTest):
    MODELS = [Location]

    def setUp(self):
        super().setUp()
        self._create_tables_and_schema(self.MODELS, schema='omopcdm')

    def tearDown(self) -> None:
        super().tearDown()
        self._drop_tables_and_schema(self.MODELS, schema='omopcdm')

    def _run_location_transformation(self, session, shak_code):

        location_insert = get_location_insert(shak_code)
        session.execute(location_insert)

    def test_transform(self):
        with session_context(make_db_session(self.engine)) as session:
            shak_code = '1301011'
            self._run_location_transformation(session, shak_code)

            result = session.query(Location).all()
            self.assertEqual(len(result), 1)
            self.assertEqual(result[0].location_id, 1)
            self.assertEqual(result[0].zip, "2100")
            self.assertEqual(result[0].location_source_value, "department_shak_code|1301011")
            self.assertEqual(result[0].country_concept_id, 4330435)

    def test_transform_empty_shak_code(self):
        with session_context(make_db_session(self.engine)) as session:
            shak_code = None
            self._run_location_transformation(session, shak_code)

            result = session.query(Location).all()
            self.assertEqual(len(result), 1)
            self.assertEqual(result[0].location_id, 1)
            self.assertEqual(result[0].zip, None)
            self.assertEqual(result[0].location_source_value, "department_shak_code|")
            self.assertEqual(result[0].country_concept_id, 4330435)


__all__ = ["LocationTransformationTest"]
