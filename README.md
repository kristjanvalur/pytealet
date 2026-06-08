# Tealet

A Python wrapper for the [libtealet](https://github.com/kristjanvalur/libtealet) library, providing efficient coroutine and stack-slicing capabilities to Python. This project brings the power of cooperative multitasking through stack-slicing to Python, similar to greenlet but built on the modern libtealet foundation.

## About

Tealet provides a greenlet-compatible interface for cooperative multitasking in Python. Unlike traditional coroutines that require `async`/`await` keywords throughout your code, tealet allows you to suspend and resume entire execution stacks, enabling cooperative multitasking without special language support.

Built on vendored libtealet release archives (currently v0.7.5), this library offers:
- **Stack-slicing**: Efficient context switching without kernel involvement
- **Low memory overhead**: ~2-16 KB per coroutine vs 1-8 MB for OS threads
- **Fast context switches**: ~100-500 CPU cycles
- **Greenlet compatibility**: Drop-in replacement for greenlet-based code

## Project Structure

```
pytealet/
├── docs/
│   ├── ARCHITECTURE.md
│   └── ISSUES.md
├── scripts/
│   └── fast_build.sh
├── src/
│   ├── greenlet_legacy.py   # Legacy greenlet compatibility shim (dev/test helper)
│   ├── tealet_examples.py    # Development examples (generator/scheduler/event/future)
│   ├── tealet/              # Pure Python package
│   │   ├── __init__.py
│   │   └── greenlet/
│   └── _tealet/             # C extension module
│       ├── pytealet.c       # Core runtime for tealet objects
│       ├── pytealet_module.c # CPython module lifecycle
│       ├── frame_info.c     # Frame capture/restore helpers
│       ├── tstate_state.c   # Thread-state transfer helpers
│       ├── libtealet/       # Vendored libtealet release archive (primary)
│       └── libtealet-src/   # Optional local libtealet source checkout (gitignored)
├── tests/
│   ├── test_tealet.py
│   ├── test_greenlet_legacy.py
│   └── compat_greenlet/
├── pyproject.toml
└── README.md
```

## Development

### Development Setup

This project uses uv for dependency and environment management.

```bash
# Create and activate a local virtual environment
uv venv --python 3.13
source .venv/bin/activate

# Install project and development dependencies
uv sync --dev --active
```

If you are using a custom debug CPython build, install via uv pip with an explicit interpreter:

```bash
uv venv --python /path/to/cpython-debug/python .venv-cpython313-debug
uv pip install --python .venv-cpython313-debug/bin/python -e .[dev]
```

### Running Tests

```bash
uv run --active python -m pytest tests/
```

### Example Code

The repository includes runnable development examples in `src/tealet_examples.py`.
These show:
- a simple tealet-backed generator
- a minimal cooperative scheduler and event primitive
- a minimal future implementation built on top of the scheduler/event model

Run the module from a source checkout:

```bash
uv run --active python -m tealet_examples
```

### Runtime Frame Introspection Toggle

The extension exposes a module-level runtime switch for dormant-tealet frame exposure:

```python
import _tealet

enabled = _tealet.frame_introspection()      # get current setting
_tealet.frame_introspection(False)           # disable dormant-frame capture
```

Compile-time capability is exposed as `_tealet.PYTEALET_WITH_PENDING_FRAME_INTROSPECTION`.

### Building Third-Party Extensions Against Tealet C API

The package installs a public C API header and exposes an include-path helper:

```python
import tealet

include_dir = tealet.get_include()
```

The public header is:

- `pytealet_capi.h`

Client extensions should include this header at build time and import the
runtime capsule using `PyTealetApi_Import()`.

### Building the C Extension

The C extension (`_tealet`) links against pre-built libtealet libraries from the vendored release archive. It depends on:
- **[libtealet](https://github.com/kristjanvalur/libtealet) v0.7.5** - Core stack-slicing library (pre-built binaries in `src/_tealet/libtealet/lib/`)
- **[stackman](https://github.com/stackless-dev/stackman)** - Platform-specific stack operations (bundled with libtealet)

Default build mode is release-archive based. To use a local source checkout for debugging instead, place it at `src/_tealet/libtealet-src/` and enable source mode:

```bash
BUILD_LIBTEALET_FROM_SOURCE=1 uv sync --active --reinstall-package tealet
```

**Build Requirements:**
- Python development headers (`python3.10-dev` or similar)
- C compiler (gcc, clang, or MSVC)
- Make (for ABI detection)

**Current Status:** The build infrastructure and Python 3 modernization are in place; active work is focused on runtime stability and stress-test robustness.

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
