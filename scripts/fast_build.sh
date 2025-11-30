#!/bin/bash
# Fast C extension development script for uv

set -e  # Exit on error

# Parse command line arguments
BUILD_TYPE="optimized"
if [[ "$1" == "debug" ]]; then
    BUILD_TYPE="debug"
    echo "=== Fast C Extension Build (uv) - DEBUG MODE ==="
elif [[ "$1" == "optimized" || "$1" == "" ]]; then
    BUILD_TYPE="optimized"
    echo "=== Fast C Extension Build (uv) - OPTIMIZED MODE ==="
else
    echo "Usage: $0 [debug|optimized]"
    echo "  debug     - Build with -g -O0 -DDEBUG flags"
    echo "  optimized - Build with -O3 -DNDEBUG flags (default)"
    exit 1
fi

# Clean any existing build artifacts
echo "Cleaning build artifacts..."
rm -rf build/
rm -f src/_tealet*.so

# Rebuild the extension with uv
echo "Building C extension with uv..."
uv sync --reinstall-package tealet

# Test if it worked
echo "Testing C extension..."
uv run python -c "
try:
    import _tealet
    print('✓ _tealet module loaded successfully')
    
    # Quick functional test
    t = _tealet.tealet()
    print(f'✓ Created tealet: {t}')
    print(f'✓ State: {t.state}')
    print(f'✓ Main: {t.main}')
    print(f'✓ Main state: {t.main.state}')
    
except Exception as e:
    print(f'✗ C extension failed: {e}')
    import traceback
    traceback.print_exc()
    exit(1)
"

echo "=== Build Complete ==="
