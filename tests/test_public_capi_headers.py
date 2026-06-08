from pathlib import Path

import tealet


def test_tealet_get_include_points_to_header_dir():
    include_dir = Path(tealet.get_include())
    header = include_dir / "pytealet_capi.h"

    assert include_dir.is_dir()
    assert header.is_file()


def test_internal_header_forwards_to_installed_public_header():
    repo_root = Path(__file__).resolve().parents[1]
    internal_header = repo_root / "src" / "_tealet" / "pytealet_capi.h"
    internal_text = internal_header.read_text(encoding="utf-8")

    assert '#include "../tealet/include/pytealet_capi.h"' in internal_text
