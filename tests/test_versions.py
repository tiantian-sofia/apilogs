import apilogs.bin
import apilogs.core
import apilogs._version
from apilogs import AWSLogs


def test_versions_in_modules():
    assert apilogs.bin.__version__ == apilogs._version.__version__


def test_package_exports():
    assert AWSLogs is apilogs.core.AWSLogs
