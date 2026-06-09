from pathlib import Path as _Path

from _tealet import *
from _tealet import __version__


def get_include():
    """Return the directory containing public tealet C API headers."""
    return str(_Path(__file__).resolve().parent / "include")
