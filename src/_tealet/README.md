# _tealet C Extension

This directory contains the C extension module for tealet.

## Structure

- `_tealet.c` - Main C extension module implementation
- `libtealet/` - Pre-built binary distribution of libtealet (currently v0.3.2)
  - `lib/` - Pre-compiled libraries for various platforms/ABIs
  - `tealet/` - Header files for libtealet
  - `stackman/` - Header files for stackman (stack manipulation library)

## Building

The C extension is configured in `setup.py` and will be built automatically during installation. The build process:

1. Detects the platform ABI using `make -C libtealet abiname`
2. Links against the appropriate pre-built library in `libtealet/lib/<abi>/`

**Note:** The current `_tealet.c` code requires Python 3 compatibility updates before it will build.

## Updating libtealet

To update to a newer version of libtealet:

1. Download the **binary distribution** (not source) from the libtealet releases:
   ```bash
   cd src/_tealet
   rm -rf libtealet
   curl -L https://github.com/kristjanvalur/libtealet/releases/download/v0.3.2/libtealet-0.3.2.tar.gz | tar -xz
   mv libtealet-0.3.2 libtealet
   ```

2. For a different version, replace `v0.3.2` and `libtealet-0.3.2` with the desired version.

3. Check available releases at: https://github.com/kristjanvalur/libtealet/releases

4. Commit the changes:
   ```bash
   git add libtealet
   git commit -m "Update libtealet to v0.3.2"
   ```

## Dependencies

- **libtealet** - Core stack-slicing library for coroutines
- **stackman** - Platform-specific stack manipulation (bundled with libtealet)
