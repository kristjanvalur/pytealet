"""Small Python wrapper around Linux io_uring."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from _uring_api import Ring as Ring
from _uring_api import __compiled_liburing_version__ as __compiled_liburing_version__
from _uring_api import __compiled_liburing_version_info__ as __compiled_liburing_version_info__
from _uring_api import __liburing_version__ as __liburing_version__
from _uring_api import probe as _probe

DEFAULT_ENTRIES = 8
DEFAULT_FLAGS = 0


@dataclass(frozen=True)
class UringProbe:
    """Describes whether a minimal io_uring instance can be created."""

    available: bool
    errno: int | None
    message: str | None
    features: int
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
        sq_entries=int(result["sq_entries"]),
        cq_entries=int(result["cq_entries"]),
        liburing_version=str(result["liburing_version"]),
        compiled_liburing_version=str(result["compiled_liburing_version"]),
        compiled_liburing_version_info=compiled_version_info,
    )


def is_available() -> bool:
    """Returns True if this process can create a minimal `io_uring` instance."""

    return probe().available


__all__ = [
    "DEFAULT_ENTRIES",
    "DEFAULT_FLAGS",
    "Ring",
    "UringProbe",
    "__compiled_liburing_version__",
    "__compiled_liburing_version_info__",
    "__liburing_version__",
    "is_available",
    "probe",
]
