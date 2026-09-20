import apilogs.bin
import apilogs.core
import apilogs._version


def test_versions_in_modules():
    assert apilogs.bin.__version__ == apilogs._version.__version__
