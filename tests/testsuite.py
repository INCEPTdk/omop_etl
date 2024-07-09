import os
import unittest

from tests.models.sourcetests import *
from tests.models.targettests import *
from tests.processtests import ProcessUnitTests
from tests.util.connectiontests import *
from tests.util.loggertests import *
from tests.util.sqltests import *

# only run regression tests if explicitly set ETL_RUN_INTEGRATION_TESTS variable
if os.getenv("ETL_RUN_INTEGRATION_TESTS", None) == "ON":
    from tests.processtests import ProcessDuckDBTests, RunETLDuckDBTests
    from tests.transform.care_site_tests import *
    from tests.transform.condition_era_tests import *
    from tests.transform.condition_occurrence_tests import *
    from tests.transform.create_omopcdm_tables_tests import *
    from tests.transform.death_tests import *
    from tests.transform.device_exposure_tests import *
    from tests.transform.drug_era_tests import *
    from tests.transform.drug_exposure_tests import *
    from tests.transform.location_tests import *
    from tests.transform.measurement_tests import *
    from tests.transform.merge import *
    from tests.transform.observation_period_tests import *
    from tests.transform.observation_tests import *
    from tests.transform.person_tests import *
    from tests.transform.procedure_occurrence_tests import *
    from tests.transform.specimen_tests import *
    from tests.transform.stem_tests import *
    from tests.transform.visit_occurrence_tests import *
    from tests.util.dbtests import *


def main():
    unittest.TextTestRunner(verbosity=3).run(unittest.TestSuite())


if __name__ == "__main__":
    unittest.main()
