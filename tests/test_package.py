import automation_harness


def test_package_exposes_version() -> None:
    assert automation_harness.__version__
