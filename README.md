# Tealet

A Python wrapper for [libtealet](https://github.com/kristjanvalur/libtealet), giving Python code low-level stack-slicing primitives.

Normal Python functions have a nice property: they can call other functions without changing their own shape. `tealet` keeps that feeling while allowing a stack to pause and resume later, so higher-level runtimes can build cooperative scheduling without forcing `async`/`await` through every layer.

## About

`tealet` is intentionally small. It is a building-block library, not a scheduler, event loop, or complete async framework. The core package provides stack-switching primitives such as `switch`, `run`, and `throw`, plus thread ownership and runtime state checks that schedulers can use directly.

Why is that useful? Because stack slicing lets code keep its ordinary call shape. Instead of colouring every function with `async` and propagating `await` through the stack, a tealet can suspend where it is and continue from the same Python frame later.

Built on vendored libtealet release archives, currently v0.7.6, the core package provides:

- **Stack slicing**: suspend and resume Python execution stacks without kernel thread switches
- **Low memory overhead**: roughly 2-16 KB per tealet, instead of megabytes for an OS thread stack
- **Fast context switches**: stack transfers intended to be cheap enough for cooperative runtimes
- **Composable primitives**: enough machinery to build custom schedulers and runtime policies

Need a scheduler, locks, futures, selector helpers, or asyncio coexistence? Use the separate `tealetio` package in `packages/tealetio`. It depends on `tealet`, while `tealet` itself stays dependency-light and runtime-focused.

The `tealet.greenlet` compatibility layer is best viewed as a practical compatibility shim and proof of concept. It shows how richer APIs can sit on top of the primitives, but the core project does not claim broad greenlet parity.

## Development

### Development Setup

This repository is a `uv` workspace. The main package is `tealet`, and the current secondary workspace project is `tealetio` under `packages/tealetio`.

Use `uv` for dependency and environment management.

```bash
# Create and activate a local virtual environment
uv venv --python 3.13
source .venv/bin/activate

# Install project and development dependencies
uv sync --active --dev
```

Using a custom debug CPython build? Prefer `uv pip` with an explicit interpreter, so the debug ABI is unambiguous:

```bash
uv venv --python /path/to/cpython-debug/python .venv-cpython313-debug
uv pip install --python .venv-cpython313-debug/bin/python -e . --group dev
```

### Running Tests

```bash
uv run --active python -m pytest tests/
```

### Core Example Code

The repository includes runnable development examples in `src/tealet_examples.py`.
They show:

- a simple tealet-backed generator
- a deliberately minimal `tealet.simple_scheduler.SimpleScheduler` example

The core `SimpleScheduler` example demonstrates basic cooperative scheduling on top of tealet primitives only. It intentionally has no IO facilities, timers, thread-safe callbacks, futures, or asyncio interoperability. For those features, use `tealetio`.

Run the module from a source checkout:

```bash
uv run --active python -m tealet_examples
```

### Scheduler Package

Scheduler, task/future, lock, selector, and asyncio coexistence APIs live in the separate `tealetio` workspace package. `tealetio` depends on `tealet`; `tealet` has no dependency on `tealetio`.

Package-specific documentation lives under `packages/tealetio/docs/`.

Run the `tealetio` test suite from the workspace root:

```bash
uv run --active --package tealetio python -m pytest packages/tealetio/tests/
```

### Runtime Frame Introspection Toggle

Need to inspect dormant tealet frames while debugging? The extension exposes a module-level runtime switch for dormant-frame exposure:

```python
import _tealet

enabled = _tealet.frame_introspection()      # get current setting
_tealet.frame_introspection(False)           # disable dormant-frame capture
```

Compile-time capability is exposed as `_tealet.PYTEALET_WITH_PENDING_FRAME_INTROSPECTION`.

### Building Third-Party Extensions Against Tealet C API

Building a C extension against tealet? The package installs a public C API header and exposes an include-path helper:

```python
import tealet

include_dir = tealet.get_include()
```

The public header is:

- `pytealet_capi.h`

Client extensions should include this header at build time and import the runtime capsule using `PyTealetApi_Import()`.

## API Documentation

Detailed API references live in the `docs/` folder:

- [docs/PYTHON_API.md](docs/PYTHON_API.md) for the Python-level API (`tealet`, `_tealet`, and compatibility shim notes)
- [docs/C_API.md](docs/C_API.md) for the capsule-based C API (`pytealet_capi.h`)
- [packages/tealetio/docs/PYTHON_API.md](packages/tealetio/docs/PYTHON_API.md) for scheduler, task/future, lock, selector, runner, and asyncio APIs

## Supported Python and Platforms

`tealet` supports Python 3.10 through 3.15, including free-threaded Python builds where CPython provides that ABI.

`tealet` is a C extension based on libtealet and Stackman, so platform support depends on the native stack-switching code that ships in the vendored libtealet release archive. The default build can select pre-built libtealet libraries for these targets:

- **Linux / System V ABI**: x86, x86_64, ARM32, AArch64, and RISC-V 64
- **macOS / Darwin**: x86_64 and Apple Silicon
- **Windows**: x86, x64, and ARM64

Other platforms need corresponding libtealet/Stackman support and a matching build integration. In other words, `tealet` is not a pure-Python portability layer; it follows the platforms where libtealet can safely save and restore native stacks.

### Building the C Extension

The C extension, `_tealet`, links against pre-built libtealet libraries from the vendored release archive. It depends on:

- **[libtealet](https://github.com/kristjanvalur/libtealet) v0.7.6**: core stack-slicing library, with pre-built binaries in `src/_tealet/libtealet/lib/`
- **[stackman](https://github.com/stackless-dev/stackman)**: platform-specific stack operations, bundled with libtealet

Default build mode is release-archive based. To use a local source checkout for debugging instead, place it at `src/_tealet/libtealet-src/` and enable source mode:

```bash
BUILD_LIBTEALET_FROM_SOURCE=1 uv sync --active --reinstall-package tealet
```

**Build requirements:**

- Python development headers (`python3.10-dev` or similar)
- C compiler, such as gcc, clang, or MSVC
- Make (for ABI detection)

**Current status:** The build infrastructure and Python 3 modernisation work are in place. Active work is focused on runtime stability, stress-test robustness, and keeping the C extension memory-safe across supported Python versions.

## Contributing

Contributions are welcome. Issues, focused bug reports, and small pull requests are especially helpful while the runtime is still hardening.

## Project Structure

Need to find your way around the checkout? The main pieces are:

```
pytealet/
├── docs/
│   ├── ARCHITECTURE.md
│   ├── PYTHON_API.md
│   ├── C_API.md
│   └── ISSUES.md
├── scripts/
│   └── fast_build.sh
├── src/
│   ├── greenlet_legacy.py   # Legacy greenlet compatibility shim (dev/test helper)
│   ├── tealet_examples.py    # Development examples for core tealet primitives
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
├── packages/
│   └── tealetio/             # Optional scheduler/asyncio package built on tealet
│       └── docs/             # tealetio-specific API and design docs
├── pyproject.toml
└── README.md
```

## Related Projects

- [libtealet](https://github.com/kristjanvalur/libtealet) - The underlying C library for stack-slicing
- [greenlet](https://github.com/python-greenlet/greenlet) - Original Python greenlet implementation
- [Stackless Python](http://www.stackless.com/) - Python with built-in microthreads

## Changelog

See [CHANGELOG.md](CHANGELOG.md) for version history and release notes.

## License

MIT License. See [LICENSE](LICENSE) for details.
