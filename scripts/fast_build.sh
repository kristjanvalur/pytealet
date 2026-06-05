#!/bin/bash
# Fast C extension development script for uv

set -e  # Exit on error

# Parse command line arguments
BUILD_TYPE="optimized"
LIBTEALET_MODE="release"
if [[ "$1" == "debug" ]]; then
    BUILD_TYPE="debug"
    echo "=== Fast C Extension Build (uv) - DEBUG MODE ==="
elif [[ "$1" == "optimized" || "$1" == "" ]]; then
    BUILD_TYPE="optimized"
    echo "=== Fast C Extension Build (uv) - OPTIMIZED MODE ==="
else
    echo "Usage: $0 [debug|optimized] [release|source]"
    echo "  debug     - Build with -g -O0 -DDEBUG flags"
    echo "  optimized - Build with -O3 -DNDEBUG flags (default)"
    echo "  release   - Link against vendored release archive (default)"
    echo "  source    - Build libtealet from src/_tealet/libtealet-src"
    exit 1
fi

if [[ "$2" == "source" || "$2" == "release" ]]; then
    LIBTEALET_MODE="$2"
elif [[ "$2" != "" ]]; then
    echo "Usage: $0 [debug|optimized] [release|source]"
    exit 1
fi

BUILD_LIBTEALET_FROM_SOURCE="0"
if [[ "$LIBTEALET_MODE" == "source" ]]; then
    BUILD_LIBTEALET_FROM_SOURCE="1"
fi

PYTEALET_EXT_DEBUG="0"
LIBTEALET_DEBUG="0"
if [[ "$BUILD_TYPE" == "debug" ]]; then
    PYTEALET_EXT_DEBUG="1"
    LIBTEALET_DEBUG="1"
fi

echo "libtealet mode: $LIBTEALET_MODE"

# Clean any existing build artifacts
echo "Cleaning build artifacts..."
rm -rf build/
rm -f src/_tealet*.so

# Rebuild the extension with uv
echo "Building C extension with uv..."
BUILD_LIBTEALET_FROM_SOURCE="$BUILD_LIBTEALET_FROM_SOURCE" \
LIBTEALET_DEBUG="$LIBTEALET_DEBUG" \
PYTEALET_EXT_DEBUG="$PYTEALET_EXT_DEBUG" \
uv sync --active --reinstall-package tealet

# Test if it worked
echo "Testing C extension..."
uv run --active python -c "
try:
    import _tealet
    print('✓ _tealet module loaded successfully')
    
    # Quick functional test
    t = _tealet.tealet()
    print(f'✓ Created tealet: {t}')
    print(f'✓ State: {t.state}')
    main_t = _tealet.main()
    print(f'✓ Main: {main_t}')
    print(f'✓ Main state: {main_t.state}')
    
except Exception as e:
    print(f'✗ C extension failed: {e}')
    import traceback
    traceback.print_exc()
    exit(1)
"

echo "=== Build Complete ==="
