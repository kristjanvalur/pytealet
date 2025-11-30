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
│       ├── _tealet.c        # Main C extension (needs Python 3 updates)
│       ├── libtealet/       # Pre-built libtealet v0.3.2
│       │   ├── lib/         # Platform-specific pre-built libraries
│       │   ├── tealet/      # libtealet headers
│       │   └── stackman/    # Stack manipulation headers
│       └── README.md        # C extension documentation
├── tests/                   # Test suite
│   ├── test_tealet.py      # Tealet-specific tests
│   └── test_greenlet.py    # Greenlet compatibility tests
├── setup.py                # Build configuration with ABI detection
├── pyproject.toml          # Modern Python package configuration
├── CHANGELOG.md            # Version history
├── LICENSE                 # MIT License
└── README.md               # Project documentation
```

## Technical Details

### Dependencies
- **libtealet v0.3.2**: Core stack-slicing library (pre-built binaries included)
- **stackman**: Platform-specific stack operations (bundled with libtealet)
- **Python 3.10-3.14**: Supported Python versions

### Build System
- Uses **uv** for development and dependency management
- **setuptools** for C extension building
- Automatic ABI detection via `make -C libtealet abiname`
- Links against pre-built `libtealet.a` from `lib/<abi>/`

### Current Status
- ✅ Modern project structure complete
- ✅ Pre-built libtealet integration
- ✅ Build infrastructure with ABI detection
- ⚠️ `_tealet.c` requires Python 3 compatibility updates (currently Python 2.x code from 2013)

## Coding Guidelines

### Python Code Style
- Follow **PEP 8** for Python code
- Use type hints where appropriate (Python 3.10+ syntax)
- Keep greenlet API compatibility where possible
- Document deviations from greenlet behavior

### C Extension Code
When updating `_tealet.c` for Python 3 compatibility:
- Use Python 3 C API (no `PyInt_*`, use `PyLong_*`)
- Handle thread state changes (Python 3.7+ removed `exc_type/value/traceback` from thread state)
- Use `PyModuleDef` instead of `Py_InitModule3`
- Follow libtealet's coding style (see below)
- Test on multiple Python versions (3.10-3.14)

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

### Setup
```bash
# Create virtual environment
uv venv --python 3.10

# Install in development mode (when C code is fixed)
uv sync --dev
```

### Testing
```bash
# Run tests (when build works)
pytest tests/

# Run specific test file
pytest tests/test_tealet.py
```

### Building C Extension
```bash
# Build will auto-detect ABI and link against pre-built libs
uv sync

# Clean build
rm -rf build/ *.egg-info
uv sync
```

## Key Differences from greenlet

- Built on libtealet instead of direct stackman integration
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
