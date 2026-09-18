import pytest

import uring_api


def require_uring() -> None:
    if not uring_api.is_available():
        pytest.skip("io_uring is not available")
