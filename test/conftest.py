import os

import pytest
from mcr_py.package.gtfs.fixtures import *
from mcr_py.package.structs.fixtures import *


@pytest.fixture(scope="session")
def testdata_path():
    return os.path.join(os.path.dirname(__file__), "testdata")
