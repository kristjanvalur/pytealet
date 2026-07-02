import os

import pytest

from tealetio.files import parse_open_mode


@pytest.mark.parametrize(
    ("mode", "expected_flags"),
    [
        ("rb", os.O_RDONLY),
        ("wb", os.O_WRONLY | os.O_CREAT | os.O_TRUNC),
        ("ab", os.O_WRONLY | os.O_CREAT | os.O_APPEND),
        ("r+b", os.O_RDWR),
        ("rb+", os.O_RDWR),
        ("w+b", os.O_RDWR | os.O_CREAT | os.O_TRUNC),
        ("wb+", os.O_RDWR | os.O_CREAT | os.O_TRUNC),
        ("a+b", os.O_RDWR | os.O_CREAT | os.O_APPEND),
        ("ab+", os.O_RDWR | os.O_CREAT | os.O_APPEND),
    ],
)
def test_parse_open_mode_maps_supported_binary_modes(mode: str, expected_flags: int) -> None:
    flags, creat_mode = parse_open_mode(mode)
    assert flags == expected_flags
    assert creat_mode == 0o666


@pytest.mark.parametrize(
    "mode",
    ["", "rt", "xb", "x+b", "u"],
)
def test_parse_open_mode_rejects_unsupported_modes(mode: str) -> None:
    with pytest.raises(ValueError):
        parse_open_mode(mode)