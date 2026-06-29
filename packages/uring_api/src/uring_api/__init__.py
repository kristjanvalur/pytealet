"""Small Python wrapper around Linux io_uring."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from importlib import resources
from types import TracebackType
from typing import TYPE_CHECKING, Any

try:
    from _uring_api import C_API_ABI_VERSION as C_API_ABI_VERSION
    from _uring_api import C_API_FEATURE_CORE as C_API_FEATURE_CORE
    from _uring_api import C_API_FEATURES as C_API_FEATURES
    from _uring_api import COMPLETION_KIND_ACCEPT as COMPLETION_KIND_ACCEPT
    from _uring_api import COMPLETION_KIND_CANCEL as COMPLETION_KIND_CANCEL
    from _uring_api import COMPLETION_KIND_CONNECT as COMPLETION_KIND_CONNECT
    from _uring_api import COMPLETION_KIND_CLOSE as COMPLETION_KIND_CLOSE
    from _uring_api import COMPLETION_KIND_RECV as COMPLETION_KIND_RECV
    from _uring_api import COMPLETION_KIND_RECVMSG as COMPLETION_KIND_RECVMSG
    from _uring_api import COMPLETION_KIND_SEND as COMPLETION_KIND_SEND
    from _uring_api import COMPLETION_KIND_SENDMSG as COMPLETION_KIND_SENDMSG
    from _uring_api import COMPLETION_KIND_SENDTO as COMPLETION_KIND_SENDTO
    from _uring_api import COMPLETION_KIND_SHUTDOWN as COMPLETION_KIND_SHUTDOWN
    from _uring_api import COMPLETION_KIND_SOCKET as COMPLETION_KIND_SOCKET
    from _uring_api import COMPLETION_KIND_WAKE as COMPLETION_KIND_WAKE
    from _uring_api import Completion as Completion
    from _uring_api import IORING_CQE_F_MORE as IORING_CQE_F_MORE
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
    C_API_ABI_VERSION = 1
    C_API_FEATURE_CORE = 1 << 0
    C_API_FEATURES = 0
    COMPLETION_KIND_RECV = 1
    COMPLETION_KIND_SEND = 2
    COMPLETION_KIND_WAKE = 3
    COMPLETION_KIND_SENDTO = 4
    COMPLETION_KIND_RECVMSG = 5
    COMPLETION_KIND_ACCEPT = 6
    COMPLETION_KIND_CONNECT = 7
    COMPLETION_KIND_CANCEL = 8
    COMPLETION_KIND_SHUTDOWN = 9
    COMPLETION_KIND_CLOSE = 10
    COMPLETION_KIND_SENDMSG = 11
    COMPLETION_KIND_SOCKET = 12
    IORING_SETUP_CQSIZE = 1 << 3
    IORING_SETUP_CLAMP = 1 << 4
    IORING_SETUP_COOP_TASKRUN = 1 << 8
    IORING_SETUP_TASKRUN_FLAG = 1 << 9
    IORING_SETUP_SINGLE_ISSUER = 1 << 12
    IORING_SETUP_DEFER_TASKRUN = 1 << 13
    IORING_CQE_F_MORE = 1 << 1
    __compiled_liburing_version__ = "unavailable"
    __compiled_liburing_version_info__ = (0, 0)
    __liburing_version__ = "unavailable"

    @dataclass(frozen=True)
    class Completion:
        user_data: object
        kind: int
        res: int
        flags: int
        result: object

    class Ring:  # type: ignore[no-redef]
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            raise RuntimeError("uring-api native extension is unavailable") from _native_import_error

        @property
        def fd(self) -> int:
            raise RuntimeError("uring-api native extension is unavailable") from _native_import_error

        @property
        def features(self) -> int:
            raise RuntimeError("uring-api native extension is unavailable") from _native_import_error

        @property
        def sq_entries(self) -> int:
            raise RuntimeError("uring-api native extension is unavailable") from _native_import_error

        @property
        def cq_entries(self) -> int:
            raise RuntimeError("uring-api native extension is unavailable") from _native_import_error

        @property
        def closed(self) -> bool:
            raise RuntimeError("uring-api native extension is unavailable") from _native_import_error

        @property
        def running(self) -> bool:
            raise RuntimeError("uring-api native extension is unavailable") from _native_import_error

        @property
        def callback(self) -> Callable[[Completion], object] | None:
            raise RuntimeError("uring-api native extension is unavailable") from _native_import_error

        @callback.setter
        def callback(self, value: Callable[[Completion], object] | None) -> None:
            raise RuntimeError("uring-api native extension is unavailable") from _native_import_error

        def close(self) -> None:
            raise RuntimeError("uring-api native extension is unavailable") from _native_import_error

        def serve_completions(self) -> None:
            raise RuntimeError("uring-api native extension is unavailable") from _native_import_error

        def stop_serving(self) -> None:
            raise RuntimeError("uring-api native extension is unavailable") from _native_import_error

        def reset_serving(self) -> None:
            raise RuntimeError("uring-api native extension is unavailable") from _native_import_error

        def break_wait(self) -> None:
            raise RuntimeError("uring-api native extension is unavailable") from _native_import_error

        def submit_recv(self, fd: int, buf: Any, user_data: object = None) -> Completion:
            raise RuntimeError("uring-api native extension is unavailable") from _native_import_error

        def submit_send(self, fd: int, data: Any, user_data: object = None, flags: int = 0) -> Completion:
            raise RuntimeError("uring-api native extension is unavailable") from _native_import_error

        def submit_recvmsg(self, fd: int, buf: Any, user_data: object = None) -> Completion:
            raise RuntimeError("uring-api native extension is unavailable") from _native_import_error

        def submit_sendto(self, fd: int, data: Any, address: Any, user_data: object = None, flags: int = 0) -> Completion:
            raise RuntimeError("uring-api native extension is unavailable") from _native_import_error

        def submit_sendmsg(
            self, fd: int, data: Any, address: Any = None, user_data: object = None, flags: int = 0
        ) -> Completion:
            raise RuntimeError("uring-api native extension is unavailable") from _native_import_error

        def submit_accept(self, fd: int, user_data: object = None) -> Completion:
            raise RuntimeError("uring-api native extension is unavailable") from _native_import_error

        def submit_connect(self, fd: int, address: Any, user_data: object = None) -> Completion:
            raise RuntimeError("uring-api native extension is unavailable") from _native_import_error

        def submit_cancel(self, completion: Completion) -> Completion:
            raise RuntimeError("uring-api native extension is unavailable") from _native_import_error

        def submit_shutdown(self, fd: int, how: int, user_data: object = None) -> Completion:
            raise RuntimeError("uring-api native extension is unavailable") from _native_import_error

        def submit_close(self, fd: int, user_data: object = None) -> Completion:
            raise RuntimeError("uring-api native extension is unavailable") from _native_import_error

        def submit_socket(
            self, domain: int, type: int, protocol: int = 0, flags: int = 0, user_data: object = None
        ) -> Completion:
            raise RuntimeError("uring-api native extension is unavailable") from _native_import_error

        def wait(self, timeout: float | None = None) -> Completion | None:
            raise RuntimeError("uring-api native extension is unavailable") from _native_import_error

        def __enter__(self) -> Ring:
            raise RuntimeError("uring-api native extension is unavailable") from _native_import_error

        def __exit__(
            self,
            exc_type: type[BaseException] | None,
            exc: BaseException | None,
            traceback: TracebackType | None,
        ) -> None:
            raise RuntimeError("uring-api native extension is unavailable") from _native_import_error

    class SubmissionQueueFull(RuntimeError):
        """Raised when no submission queue entry is currently available."""

    def _probe(entries: int = 2, flags: int = 0) -> dict[str, Any]:
        if entries <= 0:
            raise ValueError("entries must be between 1 and UINT_MAX")
        return {}
else:
    _native_import_error = None

DEFAULT_ENTRIES = 8
DEFAULT_FLAGS = 0


def probe(entries: int = 2, flags: int = DEFAULT_FLAGS) -> dict[str, bool]:
    """Returns runtime availability and operation capabilities as a flat dictionary."""

    return {str(name): bool(available) for name, available in _probe(entries, flags).items()}


def is_available() -> bool:
    """Returns True if this process can create a minimal `io_uring` instance."""

    return probe().get("available", False)


def get_include() -> str:
    """Returns the installed include directory for C API clients."""

    return str(resources.files("uring_api").joinpath("include"))


__all__ = [
    "DEFAULT_ENTRIES",
    "DEFAULT_FLAGS",
    "C_API_ABI_VERSION",
    "C_API_FEATURE_CORE",
    "C_API_FEATURES",
    "COMPLETION_KIND_ACCEPT",
    "COMPLETION_KIND_CANCEL",
    "COMPLETION_KIND_CONNECT",
    "COMPLETION_KIND_CLOSE",
    "COMPLETION_KIND_RECV",
    "COMPLETION_KIND_RECVMSG",
    "COMPLETION_KIND_SEND",
    "COMPLETION_KIND_SENDMSG",
    "COMPLETION_KIND_SENDTO",
    "COMPLETION_KIND_SHUTDOWN",
    "COMPLETION_KIND_SOCKET",
    "COMPLETION_KIND_WAKE",
    "Completion",
    "IORING_CQE_F_MORE",
    "IORING_SETUP_CLAMP",
    "IORING_SETUP_COOP_TASKRUN",
    "IORING_SETUP_CQSIZE",
    "IORING_SETUP_DEFER_TASKRUN",
    "IORING_SETUP_SINGLE_ISSUER",
    "IORING_SETUP_TASKRUN_FLAG",
    "Ring",
    "SubmissionQueueFull",
    "__compiled_liburing_version__",
    "__compiled_liburing_version_info__",
    "__liburing_version__",
    "get_include",
    "is_available",
    "probe",
]

if TYPE_CHECKING:
    from _uring_api import Completion as Completion
    from _uring_api import Ring as Ring
