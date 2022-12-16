import logging
import os
import time

import pytest

try:
    # See https://github.com/spulec/freezegun/issues/98#issuecomment-590553475.
    import pandas  # noqa: F401
except ImportError:
    pass

# Enable debug logging.
logging.getLogger().setLevel(logging.DEBUG)
os.putenv("DATAHUB_DEBUG", "1")

# Disable telemetry
os.putenv("DATAHUB_TELEMETRY_ENABLED", "false")


@pytest.fixture
def mock_time(monkeypatch):
    def fake_time():
        #  	Fri Dec 16 2022 14:06:31 GMT+0000
        return 1671199591.0975091

    monkeypatch.setattr(time, "time", fake_time)

    yield