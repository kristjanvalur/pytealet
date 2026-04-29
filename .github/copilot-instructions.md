# GitHub Copilot Instructions for pytealet

## Project Overview

**pytealet** is a Python wrapper for [libtealet](https://github.com/kristjanvalur/libtealet), providing efficient coroutine and stack-slicing capabilities to Python through a C extension. This project brings the power of cooperative multitasking to Python using the modern libtealet foundation, offering a greenlet-compatible interface.

### Key Concepts
- **Stack-slicing**: Technique coined by Christian Tismer for Stackless Python where parts of the C stack are saved/restored on the heap
- **Tealets**: Coroutines that can suspend entire execution stacks without requiring `async`/`await` keywords
- **Greenlet compatibility**: Drop-in replacement for greenlet-based code with similar API
- **No compiler support needed**: Unlike async/await, works with any Python code

## Project Structure

```
pytealet/
├── src/
│   ├── tealet/              # Pure Python package
│   │   ├── __init__.py      # Main tealet module (imports from _tealet)
│   │   ├── greenlet.py      # Greenlet-compatible interface
│   │   └── tealet.py        # Tealet-specific wrapper
│   └── _tealet/             # C extension module
│       ├── pytealet.c       # Main C extension (modernized for Python 3.10+)
│       ├── libtealet/       # Pre-built libtealet v0.3.2
│       │   ├── lib/         # Platform-specific pre-built libraries
│       │   ├── tealet/      # libtealet headers
│       │   └── stackman/    # Stack manipulation headers
│       └── README.md        # C extension documentation
├── tests/                   # Test suite (pytest)
│   ├── test_tealet.py      # Tealet-specific tests (10/12 passing)
│   └── test_greenlet.py    # Greenlet compatibility tests
├── scripts/
│   └── fast_build.sh       # Rapid C extension rebuild script
├── docs/
│   ├── ARCHITECTURE.md     # Design and architecture documentation
│   └── ISSUES.md           # Current bugs and tracking
├── setup.py                # Build configuration with ABI detection
├── pyproject.toml          # Modern Python package configuration
├── CHANGELOG.md            # Version history
├── LICENSE                 # MIT License
└── README.md               # Project documentation
```

## Development Workflow

### Quick Setup
```bash
# Create virtual environment with uv
uv venv --python 3.10

# Install in development mode
uv sync --dev
```

### Building the C Extension
**Use the fast build script for development:**
```bash
./scripts/fast_build.sh          # Optimized build (default)
./scripts/fast_build.sh debug    # Debug build with -g -O0

# The script automatically:
# 1. Cleans build artifacts
# 2. Rebuilds with uv sync --reinstall-package tealet
# 3. Runs basic smoke tests
```

**Manual build (if needed):**
```bash
# Clean rebuild
rm -rf build/ src/_tealet*.so
uv sync --reinstall-package tealet
```

### Running Tests
```bash
# All tests
uv run pytest tests/

# Specific test file
uv run pytest tests/test_tealet.py -v

# Specific test class
uv run pytest tests/test_tealet.py::TestModule -v

# Current status: 10/12 tests passing
# TestRandom1 and TestRandom2 have segfaults (tracked in ISSUES.md)
```

### Current Status
- ✅ Modern project structure complete
- ✅ Pre-built libtealet integration
- ✅ Build infrastructure with ABI detection
- ✅ Python 3.10+ compatibility complete (modernized from 2013 codebase)
- ✅ P0 critical bugs fixed (segfault, exit flags)
- ✅ 10/12 tests passing
- ⚠️ 2 random stress tests have segfaults (likely missing NULL checks)

## Recent Fixes (November 2025)

### Completed
1. **File renamed**: `_tealet.c` → `pytealet.c`
2. **Python 3 API**: Updated PyInt→PyLong, exc_*→curexc_*, PyModuleDef
3. **libtealet v0.3.2 API**: Updated tealet->data → tealet->extra
4. **Type safety**: Created tealet_extra_t structure
5. **Static linking**: Changed from dynamic to static libtealet.a
6. **Critical bug #1**: Fixed pytealet_get_main() NULL pointer segfault
7. **Critical bug #2**: Fixed tealet_exit() flags (TEALET_EXIT_DELETE)
8. **Test modernization**: Converted from unittest to pytest
9. **Build tooling**: Created scripts/fast_build.sh

### Known Issues
See `docs/ISSUES.md` for current bug tracking and status.

## Coding Guidelines

### Python Code Style
- Follow **PEP 8** for Python code
- Use type hints where appropriate (Python 3.10+ syntax)
- Keep greenlet API compatibility where possible
- Document deviations from greenlet behavior

### C Extension Code
The C extension has been modernized for Python 3.10+:
- Uses Python 3 C API (PyLong_*, not PyInt_*)
- Uses Python 3.7+ exception handling (curexc_*, not exc_*)
- Uses PyModuleDef for module initialization
- Follows libtealet v0.3.2 API (tealet->extra, not tealet->data)
- Static linking against pre-built libtealet.a
- Type-safe extra data via tealet_extra_t structure

**When modifying C code:**
- Test on multiple Python versions (3.10-3.14)
- Use `./scripts/fast_build.sh` for rapid iteration
- Run tests after each change: `uv run pytest tests/`
- Check `docs/ARCHITECTURE.md` for design patterns

### libtealet Integration
- Never modify files in `src/_tealet/libtealet/` - treat as vendored dependency
- To update libtealet, follow instructions in `src/_tealet/README.md`
- Link against pre-built libraries, don't build from source
- Respect libtealet's MIT license

## Style Guide (from libtealet)

### Documentation
- Use clear, direct language
- Include code examples for all public APIs
- Explain "why" not just "what"
- Use ⚠️ for warnings, ✅ for recommendations, ❌ for anti-patterns

### Code Comments
- Explain intent and non-obvious behavior
- Document memory ownership and lifecycle
- Mark platform-specific code clearly
- Use `/* C89 style comments */`

### API Design Principles
- **Safety first**: Make dangerous operations obvious
- **Explicit over implicit**: Prefer clear APIs over magic behavior
- **Memory discipline**: Clear ownership semantics for heap vs stack
- **Platform-agnostic**: Hide platform details behind clean interfaces

## Common Patterns

### Memory Safety
Stack-allocated data becomes invalid when switching contexts:

```python
# ❌ WRONG - Stack data passed across switch
def worker(t):
    local_data = {"key": "value"}
    tealet.switch(other, local_data)  # Dangerous!

# ✅ CORRECT - Heap data or module-level
shared_data = {"key": "value"}  # Module level
def worker(t):
    tealet.switch(other, shared_data)  # Safe
```

### Greenlet Compatibility
```python
from tealet import greenlet

def worker():
    print("Hello from tealet!")
    greenlet.getcurrent().parent.switch()

g = greenlet.greenlet(worker)
g.switch()  # Should work like greenlet
```

### Tealet-Specific Usage
```python
import _tealet

def worker(current, arg):
    print(f"Received: {arg}")
    return current.main  # Return to main

main = _tealet.tealet()
t = _tealet.tealet()
t.run(worker, "hello")
```

## Development Workflow

### Testing
```bash
# Run all tests with pytest
uv run pytest tests/

# Run specific test file
uv run pytest tests/test_tealet.py -v

# Run with coverage
uv run pytest tests/ --cov=tealet --cov-report=html
```

### Building C Extension
```bash
# Fast rebuild (recommended for development)
./scripts/fast_build.sh

# Debug build with symbols
./scripts/fast_build.sh debug

# Manual clean build
rm -rf build/ src/_tealet*.so
uv sync --reinstall-package tealet
```

## Key Differences from greenlet

- Built on libtealet instead of direct stackman integration
- Does not use the external greenlet module at runtime
- Uses an internal greenlet emulation layer
- The internal greenlet emulation layer is currently not working and is disabled
- May have different performance characteristics
- Uses tealet's memory allocator interface
- Supports custom allocators via `tealet_alloc_t`

## Performance Characteristics

From libtealet (targets for Python wrapper):
- Context switch: ~100-500 CPU cycles (vs greenlet's similar performance)
- Memory per coroutine: ~2-16 KB (incremental growth)
- No kernel involvement (pure user-space)
- Manual scheduling (deterministic)

## Links and Resources

- **libtealet**: https://github.com/kristjanvalur/libtealet
  - API Reference: https://github.com/kristjanvalur/libtealet/blob/master/docs/API.md
  - Getting Started: https://github.com/kristjanvalur/libtealet/blob/master/docs/GETTING_STARTED.md
  - Architecture: https://github.com/kristjanvalur/libtealet/blob/master/docs/ARCHITECTURE.md
- **stackman**: https://github.com/stackless-dev/stackman
- **greenlet**: https://github.com/python-greenlet/greenlet (compatibility target)
- **Stackless Python**: http://www.stackless.com (historical inspiration)

## License

MIT License - Same as libtealet for consistency.

## Contributing

When contributing:
1. Maintain greenlet API compatibility where possible
2. Document any behavioral differences
3. Add tests for new functionality
4. Update CHANGELOG.md
5. Follow the coding style from libtealet project
6. Test on multiple Python versions (3.10-3.14)

## Next Steps / Current Priorities

1. **Python 3 Compatibility**: Update `_tealet.c` for Python 3.10+ C API
2. **Build Verification**: Get extension building on Linux, macOS, Windows
3. **Test Suite**: Ensure tests pass with modernized code
4. **Documentation**: Add Python API docs based on greenlet compatibility
5. **CI/CD**: Set up GitHub Actions for automated testing
