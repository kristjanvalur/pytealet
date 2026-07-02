import os

import pytest

from tealetio.files import parse_open_mode

if hasattr(os, "O_CLOEXEC"):
    _CLOEXEC = os.O_CLOEXEC
else:
    _CLOEXEC = 0


@pytest.mark.parametrize(
    ("mode", "expected_flags"),
    [
        ("rb", os.O_RDONLY | _CLOEXEC),
        ("wb", os.O_WRONLY | os.O_CREAT | os.O_TRUNC | _CLOEXEC),
        ("ab", os.O_WRONLY | os.O_CREAT | os.O_APPEND | _CLOEXEC),
        ("r+b", os.O_RDWR | _CLOEXEC),
        ("rb+", os.O_RDWR | _CLOEXEC),
        ("w+b", os.O_RDWR | os.O_CREAT | os.O_TRUNC | _CLOEXEC),
        ("wb+", os.O_RDWR | os.O_CREAT | os.O_TRUNC | _CLOEXEC),
        ("a+b", os.O_RDWR | os.O_CREAT | os.O_APPEND | _CLOEXEC),
        ("ab+", os.O_RDWR | os.O_CREAT | os.O_APPEND | _CLOEXEC),
    ],
)
def test_parse_open_mode_maps_supported_binary_modes(mode: str, expected_flags: int) -> None:
    flags, creat_mode = parse_open_mode(mode)
    assert flags == expected_flags
    assert creat_mode == 0o666


@pytest.mark.parametrize(
    "mode",
    ["", "rt", "xb", "x+b", "u", "abr"],
)
def test_parse_open_mode_rejects_unsupported_modes(mode: str) -> None:
    with pytest.raises(ValueError):
        parse_open_mode(mode)
