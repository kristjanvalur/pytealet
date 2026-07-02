"""Parse completed io_uring statx buffers into Python metadata."""

from __future__ import annotations

import os
import struct
from typing import Any

try:
    from _uring_api import STATX_BASIC_STATS as STATX_BASIC_STATS
    from _uring_api import STATX_BUFFER_SIZE as STATX_BUFFER_SIZE
    from _uring_api import STATX_SIZE as STATX_SIZE
    from _uring_api import STATX_STX_SIZE_OFFSET as STATX_STX_SIZE_OFFSET
except ImportError:
    STATX_BASIC_STATS = 0x000007FF
    STATX_SIZE = 0x00000200
    STATX_BUFFER_SIZE = 256
    STATX_STX_SIZE_OFFSET = 40

# Little-endian ``struct statx`` field offsets (Linux uapi). ``stx_size`` offset is
# checked in C via ``uring_api_statx_layout.h``.
STATX_STX_MASK_OFFSET = 0
STATX_STX_BLKSIZE_OFFSET = 4
STATX_STX_NLINK_OFFSET = 16
STATX_STX_UID_OFFSET = 20
STATX_STX_GID_OFFSET = 24
STATX_STX_MODE_OFFSET = 28
STATX_STX_INO_OFFSET = 32
STATX_STX_BLOCKS_OFFSET = 48
STATX_STX_ATIME_OFFSET = 64
STATX_STX_BTIME_OFFSET = 80
STATX_STX_CTIME_OFFSET = 96
STATX_STX_MTIME_OFFSET = 112
STATX_STX_RDEV_OFFSET = 128
STATX_STX_DEV_OFFSET = 136
_STATX_TIMESTAMP = struct.Struct("<qii")


def _view(buf: Any) -> memoryview:
    return memoryview(buf)


def _require_buffer(view: memoryview) -> None:
    if len(view) < STATX_BUFFER_SIZE:
        raise ValueError("statx buffer must be at least STATX_BUFFER_SIZE bytes")


def statx_mask(buf: Any) -> int:
    """Return the ``stx_mask`` field written by a successful statx completion."""

    view = _view(buf)
    _require_buffer(view)
    return int.from_bytes(view[STATX_STX_MASK_OFFSET : STATX_STX_MASK_OFFSET + 4], "little", signed=False)


def _require_mask_field(mask: int, field: int, name: str) -> None:
    if not (mask & field):
        raise ValueError(f"statx buffer does not contain {name} fields")


def _require_full_mask(mask: int, required: int, name: str) -> None:
    if (mask & required) != required:
        raise ValueError(f"statx buffer does not contain {name} fields")


def statx_st_size(buf: Any) -> int:
    """Read ``stx_size`` from a completed statx buffer.

    This is the usual helper for positioned file I/O (append EOF, ``SEEK_END``,
    sendfile sizing). Submit with ``mask=STATX_SIZE`` when only the byte length is
    needed.

    Call only after the statx completion reports ``res == 0``.
    """

    view = _view(buf)
    _require_buffer(view)
    mask = int.from_bytes(view[STATX_STX_MASK_OFFSET : STATX_STX_MASK_OFFSET + 4], "little", signed=False)
    _require_mask_field(mask, STATX_SIZE, "STATX_SIZE")
    return int.from_bytes(
        view[STATX_STX_SIZE_OFFSET : STATX_STX_SIZE_OFFSET + 8],
        "little",
        signed=False,
    )


def _timestamp_seconds(view: memoryview, offset: int) -> tuple[int, int]:
    sec, nsec, _reserved = _STATX_TIMESTAMP.unpack_from(view, offset)
    return sec, nsec


def statx_to_stat_result(buf: Any) -> os.stat_result:
    """Build ``os.stat_result`` from a completed statx buffer.

    Expects the fields requested by ``STATX_BASIC_STATS`` to be present in
    ``stx_mask``. Submit with ``mask=STATX_BASIC_STATS`` (or a superset).

    ``st_ctime`` is taken from ``stx_ctime`` (attribute-change time), not
    ``stx_btime`` (creation/birth time). ``os.stat_result`` has no standard birth
    time field. Timestamps use whole seconds; sub-second ``stx_*_nsec`` fields are
    not mapped.

    Call only after the statx completion reports ``res == 0``.
    """

    view = _view(buf)
    _require_buffer(view)
    mask = statx_mask(buf)
    _require_full_mask(mask, STATX_BASIC_STATS, "STATX_BASIC_STATS")

    nlink, uid, gid, mode = struct.unpack_from("<IIIH", view, STATX_STX_NLINK_OFFSET)
    ino, size, _blocks = struct.unpack_from("<QQQ", view, STATX_STX_INO_OFFSET)
    atime_sec, _atime_nsec = _timestamp_seconds(view, STATX_STX_ATIME_OFFSET)
    ctime_sec, _ctime_nsec = _timestamp_seconds(view, STATX_STX_CTIME_OFFSET)
    mtime_sec, _mtime_nsec = _timestamp_seconds(view, STATX_STX_MTIME_OFFSET)
    _rdev_major, _rdev_minor, dev_major, dev_minor = struct.unpack_from("<IIII", view, STATX_STX_RDEV_OFFSET)

    # Ten-field ``os.stat_result`` uses whole seconds. Nanoseconds are not mapped
    # because the 16-field tuple layout is platform-sensitive in CPython.
    return os.stat_result(
        (
            mode,
            ino,
            os.makedev(dev_major, dev_minor),
            nlink,
            uid,
            gid,
            size,
            atime_sec,
            mtime_sec,
            ctime_sec,
        )
    )