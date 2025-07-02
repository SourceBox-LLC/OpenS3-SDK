"""
Pytest configuration for OpenS3 SDK tests
"""
import os
import pytest

# Skip example tests in CI mode
def is_ci_mode():
    return os.environ.get("OPENS3_CI_MODE", "false").lower() == "true"

def pytest_ignore_collect(collection_path):
    """Skip example and compatibility test files in CI mode"""
    if is_ci_mode() and ("example" in str(collection_path) or "compatibility" in str(collection_path)):
        return True
    return False
