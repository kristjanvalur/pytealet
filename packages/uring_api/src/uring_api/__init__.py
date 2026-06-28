"""Small Python wrapper around Linux io_uring."""

from __future__ import annotations

from dataclasses import dataclass
import errno as _errno
from importlib import resources
from typing import Any

try:
    from _uring_api import C_API_ABI_VERSION as C_API_ABI_VERSION
    from _uring_api import C_API_FEATURE_CORE as C_API_FEATURE_CORE
    from _uring_api import C_API_FEATURES as C_API_FEATURES
    from _uring_api import Completion as Completion
    from _uring_api import IORING_SETUP_CLAMP as IORING_SETUP_CLAMP
    from _uring_api import IORING_SETUP_COOP_TASKRUN as IORING_SETUP_COOP_TASKRUN
    from _uring_api import IORING_SETUP_CQSIZE as IORING_SETUP_CQSIZE
    from _uring_api import IORING_SETUP_DEFER_TASKRUN as IORING_SETUP_DEFER_TASKRUN
    from _uring_api import IORING_SETUP_SINGLE_ISSUER as IORING_SETUP_SINGLE_ISSUER
    from _uring_api import IORING_SETUP_TASKRUN_FLAG as IORING_SETUP_TASKRUN_FLAG
    from _uring_api import Ring as Ring
    from _uring_api import SubmissionQueueFull as SubmissionQueueFull
    from _uring_api import __compiled_liburing_version__ as __compiled_liburing_version__
    from _uring_api import __compiled_liburing_version_info__ as __compiled_liburing_version_info__
    from _uring_api import __liburing_version__ as __liburing_version__
    from _uring_api import probe as _probe
except ImportError as exc:
    _native_import_error: ImportError | None = exc
    C_API_ABI_VERSION = 4
    C_API_FEATURE_CORE = 1 << 0
    C_API_FEATURES = 0
    IORING_SETUP_CQSIZE = 1 << 3
    IORING_SETUP_CLAMP = 1 << 4
    IORING_SETUP_COOP_TASKRUN = 1 << 8
    IORING_SETUP_TASKRUN_FLAG = 1 << 9
    IORING_SETUP_SINGLE_ISSUER = 1 << 12
    IORING_SETUP_DEFER_TASKRUN = 1 << 13
    __compiled_liburing_version__ = "unavailable"
    __compiled_liburing_version_info__ = (0, 0)
    __liburing_version__ = "unavailable"

    @dataclass(frozen=True)
    class Completion:
        user_data: object
        res: int
        flags: int
        result: object

    class Ring:  # type: ignore[no-redef]
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            raise RuntimeError("uring-api native extension is unavailable") from _native_import_error

    class SubmissionQueueFull(RuntimeError):
        """Raised when no submission queue entry is currently available."""

    def _probe(entries: int = 2, flags: int = 0) -> dict[str, Any]:
        if entries <= 0:
            raise ValueError("entries must be between 1 and UINT_MAX")
        return {
            "available": False,
            "errno": _errno.ENOSYS,
            "message": f"uring-api native extension is unavailable: {_native_import_error}",
            "features": 0,
            "requested_flags": flags,
            "active_flags": 0,
            "sq_entries": 0,
            "cq_entries": 0,
            "liburing_version": __liburing_version__,
            "compiled_liburing_version": __compiled_liburing_version__,
            "compiled_liburing_version_info": __compiled_liburing_version_info__,
        }
else:
    _native_import_error = None

DEFAULT_ENTRIES = 8
DEFAULT_FLAGS = 0


@dataclass(frozen=True)
class UringProbe:
    """Describes whether a minimal io_uring instance can be created."""

    available: bool
    errno: int | None
    message: str | None
    features: int
    requested_flags: int
    active_flags: int
    sq_entries: int
    cq_entries: int
    liburing_version: str
    compiled_liburing_version: str
    compiled_liburing_version_info: tuple[int, int]


def probe(entries: int = 2, flags: int = DEFAULT_FLAGS) -> UringProbe:
    """Returns availability information from a tiny `io_uring` initialisation attempt."""

    result: dict[str, Any] = _probe(entries, flags)
    compiled_version_info = tuple(int(part) for part in result["compiled_liburing_version_info"])
    if len(compiled_version_info) != 2:
        raise RuntimeError("compiled liburing version info must contain major and minor values")
    return UringProbe(
        available=bool(result["available"]),
        errno=result["errno"],
        message=result["message"],
        features=int(result["features"]),
        requested_flags=int(result["requested_flags"]),
        active_flags=int(result["active_flags"]),
        sq_entries=int(result["sq_entries"]),
        cq_entries=int(result["cq_entries"]),
        liburing_version=str(result["liburing_version"]),
        compiled_liburing_version=str(result["compiled_liburing_version"]),
        compiled_liburing_version_info=compiled_version_info,
    )


def is_available() -> bool:
    """Returns True if this process can create a minimal `io_uring` instance."""

    return probe().available


def get_include() -> str:
    """Returns the installed include directory for C API clients."""

    return str(resources.files("uring_api").joinpath("include"))


__all__ = [
    "DEFAULT_ENTRIES",
    "DEFAULT_FLAGS",
    "C_API_ABI_VERSION",
    "C_API_FEATURE_CORE",
    "C_API_FEATURES",
    "Completion",
    "IORING_SETUP_CLAMP",
    "IORING_SETUP_COOP_TASKRUN",
    "IORING_SETUP_CQSIZE",
    "IORING_SETUP_DEFER_TASKRUN",
    "IORING_SETUP_SINGLE_ISSUER",
    "IORING_SETUP_TASKRUN_FLAG",
    "Ring",
    "SubmissionQueueFull",
    "UringProbe",
    "__compiled_liburing_version__",
    "__compiled_liburing_version_info__",
    "__liburing_version__",
    "get_include",
    "is_available",
    "probe",
]
