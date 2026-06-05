# _tealet C Extension

This directory contains the C extension module for tealet.

## Structure

- `pytealet.c` - main runtime implementation
- `pytealet_module.c` - CPython module lifecycle hooks
- `frame_info.c` - frame capture/restore helpers
- `tstate_state.c` - thread-state transfer helpers
- `libtealet/` - vendored **release archive** (primary path)
  - `lib/` - prebuilt libraries by ABI
  - `tealet/` - libtealet public headers
  - `stackman/` - bundled stackman headers/libs
- `libtealet-src/` - local **source checkout** override (debug workflows only, gitignored)

## Build Modes

`setup.py` supports two modes:

1. **Release mode (default)**
   - uses `src/_tealet/libtealet`
   - links against static archives from `libtealet/lib/<abi>/`
   - archive names are `libtealet.a` on Unix-like ABIs and `tealet_static.lib` on Windows ABIs
2. **Source mode (opt-in)**
   - set `BUILD_LIBTEALET_FROM_SOURCE=1`
   - expects local source checkout at `src/_tealet/libtealet-src`
   - builds `bin/libtealet.a` from source before linking

Example source-mode rebuild:

```bash
BUILD_LIBTEALET_FROM_SOURCE=1 uv sync --active --reinstall-package tealet
```

## Updating Vendored Release Archive

To refresh the primary vendored archive from libtealet releases:

```bash
cd src/_tealet
rm -rf libtealet
curl -L https://github.com/kristjanvalur/libtealet/releases/download/v0.7.5/libtealet-0.7.5.tar.gz | tar -xz
mkdir -p libtealet
mv LICENSE Makefile README.md lib stackman tealet libtealet/
```

For a different version, replace `v0.7.5` and `libtealet-0.7.5` in the URL.

## Dependencies

- **libtealet** - core stack-slicing library for coroutines
- **stackman** - platform-specific stack manipulation (bundled with libtealet)
