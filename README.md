# Tealet

A Python wrapper for the [libtealet](https://github.com/kristjanvalur/libtealet) library, providing efficient coroutine and stack-slicing capabilities to Python. This project brings the power of cooperative multitasking through stack-slicing to Python, similar to greenlet but built on the modern libtealet foundation.

## About

Tealet provides a greenlet-compatible interface for cooperative multitasking in Python. Unlike traditional coroutines that require `async`/`await` keywords throughout your code, tealet allows you to suspend and resume entire execution stacks, enabling cooperative multitasking without special language support.

Built on libtealet v0.3.2, this library offers:
- **Stack-slicing**: Efficient context switching without kernel involvement
- **Low memory overhead**: ~2-16 KB per coroutine vs 1-8 MB for OS threads
- **Fast context switches**: ~100-500 CPU cycles
- **Greenlet compatibility**: Drop-in replacement for greenlet-based code

## Project Structure

```
pytealet/
├── src/
│   ├── tealet/              # Pure Python package
│   │   ├── __init__.py
│   │   ├── greenlet.py
│   │   └── tealet.py
│   └── _tealet/             # C extension module
│       ├── _tealet.c        # Main extension code
│       └── libtealet/       # libtealet v0.3.2 from GitHub
├── tests/
│   ├── test_tealet.py
│   └── test_greenlet.py
├── pyproject.toml
└── README.md
```

## Installation

This project uses `uv` for package management. To install:

```bash
# Install in development mode
uv pip install -e .

# Install with development dependencies
uv pip install -e ".[dev]"
```

## Development

### Running Tests

```bash
pytest tests/
```

### Building the C Extension

The C extension (`_tealet`) links against pre-built libtealet libraries. It depends on:
- **[libtealet](https://github.com/kristjanvalur/libtealet) v0.3.2** - Core stack-slicing library (pre-built binaries in `src/_tealet/libtealet/`)
- **[stackman](https://github.com/stackless-dev/stackman)** - Platform-specific stack operations (bundled with libtealet)

**Build Requirements:**
- Python development headers (`python3.10-dev` or similar)
- C compiler (gcc, clang, or MSVC)
- Make (for ABI detection)

**Current Status:** The build infrastructure is complete, but `_tealet.c` requires Python 3 compatibility updates.

## Contributing

Contributions are welcome! Please feel free to submit issues or pull requests.

## Related Projects

- [libtealet](https://github.com/kristjanvalur/libtealet) - The underlying C library for stack-slicing
- [greenlet](https://github.com/python-greenlet/greenlet) - Original Python greenlet implementation
- [Stackless Python](http://www.stackless.com/) - Python with built-in microthreads

## Changelog

See [CHANGELOG.md](CHANGELOG.md) for version history and release notes.

## License

MIT License - See [LICENSE](LICENSE) file for details

## About

This is a modernized version of the pytealet project, restructured to follow modern Python packaging standards with a src-layout and using uv for dependency management.
