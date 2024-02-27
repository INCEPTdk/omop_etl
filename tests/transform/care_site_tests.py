
"""Care site transformation tests"""

from etl.models.omopcdm54.health_systems import Location, CareSite
from etl.sql.care_site import get_care_site_insert 
from etl.util.db import make_db_session, session_context
from tests.testutils import PostgresBaseTest
from tests.transform.location_tests import LocationTransformationTest


class CareSiteTransformationTest(LocationTransformationTest):
    MODELS = [Location, CareSite]

    def _run_care_site_transformation(self, session, shak_code):

        care_site_insert = get_care_site_insert(shak_code)
        session.execute(care_site_insert)

    def test_transform(self):
        super().test_transform()

        with session_context(make_db_session(self.engine)) as session:
            shak_code = '1301011'

            test_care_site_name = 'RH 4131 Intensiv Terapiklinik'
            test_place_of_service_concept_id = 4148981
            test_place_of_service_source_value = 'department_type|ICU'
            test_care_site_source_value = 'department_shak_code|1301011'

            self._run_care_site_transformation(session, shak_code)
            result = session.query(CareSite).all()
            self.assertEqual(len(result), 1)
            self.assertEqual(result[0].location_id, 1)
            self.assertEqual(result[0].care_site_name, test_care_site_name)
            self.assertEqual(result[0].care_site_source_value, test_care_site_source_value)
            self.assertEqual(result[0].place_of_service_source_value, test_place_of_service_source_value)
            self.assertEqual(result[0].place_of_service_concept_id, test_place_of_service_concept_id)

    def test_transform_empty_shak_code(self):
        
        super().test_transform_empty_shak_code()

        with session_context(make_db_session(self.engine)) as session:
            shak_code = None

            test_care_site_name = None
            test_place_of_service_concept_id = None
            test_place_of_service_source_value = 'department_type|'
            test_care_site_source_value = 'department_shak_code|'
            
            self._run_care_site_transformation(session, shak_code)
            result = session.query(CareSite).all()
            self.assertEqual(len(result), 1)
            self.assertEqual(result[0].location_id, 1)
            self.assertEqual(result[0].care_site_name, test_care_site_name)
            self.assertEqual(result[0].care_site_source_value, test_care_site_source_value)
            self.assertEqual(result[0].place_of_service_source_value, test_place_of_service_source_value)
            self.assertEqual(result[0].place_of_service_concept_id, test_place_of_service_concept_id)

__all__ = ["CareSiteTransformationTest"]
