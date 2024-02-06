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
    from tests.processtests import ProcessPostgresTests, RunETLPostgresTests
    from tests.transform.create_omopcdm_tables_tests import *
    from tests.util.dbtests import *


def main():
    unittest.TextTestRunner(verbosity=3).run(unittest.TestSuite())


if __name__ == "__main__":
    unittest.main()
