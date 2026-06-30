# Tealet

A Python wrapper for [libtealet](https://github.com/kristjanvalur/libtealet), giving Python code low-level stack-slicing primitives.

Normal Python functions have a nice property: they can call other functions without changing their own shape. `tealet` keeps that feeling while allowing a stack to pause and resume later, so higher-level runtimes can build cooperative scheduling without forcing `async`/`await` through every layer.

## Related Workspace Projects

This repository also contains higher-level packages built on top of core `tealet`:

- [`tealetio`](packages/tealetio/): a synchronous, asyncio-like runtime for tealet, including schedulers, tasks, futures, locks, queues, selector helpers, and asyncio coexistence.
- [`tealet-greenlet`](packages/tealet-greenlet/): an experimental greenlet emulation layer via tealet, including greenlet-compatible imports and upstream-style compatibility tests.
- [`uring-api`](packages/uring_api/): a Linux `io_uring` experiment exposing native ring probing, socket send/recv submission, completion waiting, and callback-thread delivery.

## About

`tealet` is intentionally small. It is a building-block library, not a scheduler, event loop, or complete async framework. The core package provides stack-switching primitives such as `switch`, `run`, and `throw`, plus thread ownership and runtime state checks that schedulers can use directly.

Why is that useful? Because stack slicing lets code keep its ordinary call shape. Instead of colouring every function with `async` and propagating `await` through the stack, a tealet can suspend where it is and continue from the same Python frame later.

Built on vendored libtealet release archives, currently v0.7.6, the core package provides:

- **Stack slicing**: suspend and resume Python execution stacks without kernel thread switches
- **Low memory overhead**: roughly 2-16 KB per tealet, instead of megabytes for an OS thread stack
- **Fast context switches**: stack transfers intended to be cheap enough for cooperative runtimes
- **Composable primitives**: enough machinery to build custom schedulers and runtime policies

Need a scheduler, locks, futures, selector helpers, asyncio coexistence, or greenlet compatibility? Use the related workspace packages above. They depend on `tealet`, while `tealet` itself stays dependency-light and runtime-focused.

## Development

### Development Setup

This repository is a `uv` workspace. The main package is `tealet`; secondary workspace projects live under `packages/`.

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

### Quality Checks

Use the top-level Makefile for Python formatting, linting, and type checking:

```bash
make check
```

That runs the non-mutating checks:

```bash
uvx ruff format --check .
uvx ruff check .
uv run --all-packages --with ty ty check
```

Use `make fix` to apply Ruff formatting and autofixes. The `ty` configuration
checks the installable source trees and leaves test folders out, since tests
often use deliberately dynamic or type-unsafe probes.

You can also run the type checker directly:

```bash
uv run --all-packages --with ty ty check
```

### Release Process

The workspace publishes each distribution independently. Release tags are the
canonical publish trigger:

- `tealet`: `tealet-vX.Y.Z` or `vX.Y.Z`
- `tealetio`: `tealetio-vX.Y.Z`
- `tealet-greenlet`: `tealet-greenlet-vX.Y.Z`
- `uring-api`: `uring-api-vX.Y.Z`

Sibling packages should declare compatibility ranges for their dependency on the
base `tealet` package, not exact pins. For the current `0.1` line, use
`tealet>=0.1.0rc2,<0.2` unless a package needs a newer core API. Tighten the
lower bound when a release starts using new `tealet` behaviour, and move the
upper bound when the next compatible `tealet` line is known.

Before tagging a release, update the package version, package changelog, and any
dependency lower bounds that changed. Run `make check` from the workspace root,
then push the matching release tag so only the intended package publish workflow
runs.

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

### Greenlet Compatibility Package

Greenlet compatibility APIs live in the separate `tealet-greenlet` workspace package. Its canonical import is `tealet_greenlet`, with a transition wrapper available at `tealet.greenlet` when the package is installed.

Run the package tests from the workspace root:

```bash
uv run --active --package tealet-greenlet python -m pytest packages/tealet-greenlet/tests/
```

### Linux io_uring Package

The `uring-api` workspace package is a standalone Linux `io_uring` wrapper used
to develop native proactor ideas independently of `tealetio`. It links against
system `liburing` and currently exposes ring probing, socket send/recv helpers,
completion waiting, and direct callback-thread delivery.

Install the system headers before building it on Debian/Ubuntu-style systems:

```bash
sudo apt install liburing-dev
```

Run the standalone package tests from the workspace root:

```bash
uv run --active --package uring-api python -m pytest packages/uring_api/tests/
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

- [docs/PYTHON_API.md](docs/PYTHON_API.md) for the Python-level core API (`tealet` and `_tealet`)
- [docs/C_API.md](docs/C_API.md) for the capsule-based C API (`pytealet_capi.h`)
- [packages/tealetio/docs/PYTHON_API.md](packages/tealetio/docs/PYTHON_API.md) for scheduler, task/future, lock, selector, runner, and asyncio APIs
- [packages/tealet-greenlet/docs/PYTHON_API.md](packages/tealet-greenlet/docs/PYTHON_API.md) for the greenlet compatibility package
- [packages/uring_api/README.md](packages/uring_api/README.md) for the standalone Linux `io_uring` wrapper

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
│   ├── tealet_examples.py    # Development examples for core tealet primitives
│   ├── tealet/              # Pure Python package
│   │   ├── __init__.py
│   │   └── greenlet/         # Transition wrapper for tealet-greenlet
│   └── _tealet/             # C extension module
│       ├── pytealet.c       # Core runtime for tealet objects
│       ├── pytealet_module.c # CPython module lifecycle
│       ├── frame_info.c     # Frame capture/restore helpers
│       ├── tstate_state.c   # Thread-state transfer helpers
│       ├── libtealet/       # Vendored libtealet release archive (primary)
│       └── libtealet-src/   # Optional local libtealet source checkout (gitignored)
├── tests/
│   ├── test_tealet.py
│   └── test_examples.py
├── packages/
│   ├── tealetio/             # Optional scheduler/asyncio package built on tealet
│   │   └── docs/             # tealetio-specific API and design docs
│   ├── tealet-greenlet/      # Greenlet compatibility package built on tealet
│   │   ├── docs/             # tealet-greenlet-specific API and architecture docs
│   │   └── tests/            # Legacy and upstream-compat greenlet tests
│   └── uring_api/            # Standalone Linux io_uring wrapper experiment
├── pyproject.toml
└── README.md
```

## Related Projects

- [libtealet](https://github.com/kristjanvalur/libtealet) - The underlying C library for stack-slicing
- [greenlet](https://github.com/python-greenlet/greenlet) - Original Python greenlet implementation
- [Stackless Python](http://www.stackless.com/) - Python with built-in microthreads

## Changelog

See [CHANGELOG.md](CHANGELOG.md) for workspace and core `tealet` version history.
Sibling packages keep their own histories:

- [tealetio changelog](packages/tealetio/CHANGELOG.md)
- [tealet-greenlet changelog](packages/tealet-greenlet/CHANGELOG.md)

## License

MIT License. See [LICENSE](LICENSE) for details.
