from importlib.metadata import version

import stratum_sdk


def test_version() -> None:
    assert stratum_sdk.__version__ == "0.0.0"


def test_runtime_version_matches_distribution_metadata() -> None:
    assert version("stratum-sdk") == stratum_sdk.__version__
