# Workspace and tealet Changelog

All notable workspace and core `tealet` changes will be documented in this file.

The repository root is both the workspace home and the package home for the base
`tealet` library, so this changelog covers both roles. Package-specific histories
for sibling projects live next to those packages:

- [tealetio changelog](packages/tealetio/CHANGELOG.md)
- [tealet-greenlet changelog](packages/tealet-greenlet/CHANGELOG.md)

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased] - 2026-06-05

## [0.1.0rc2] - 2026-06-25

### Workspace
- Split greenlet compatibility into the separate `tealet-greenlet` workspace package.
- Updated sibling workspace packages to depend on `tealet==0.1.0rc2`.

### tealet
- Kept core `tealet` focused on stack-slicing primitives, C API headers, and runtime safety.

### Notes
- Pre-release candidate for publishing the split workspace package set: `tealet`, `tealetio`, and `tealet-greenlet`.

## [0.1.0rc1] - 2026-06-09

### Added
- Split pure pytealet runtime tests into domain-focused files and added tests layout documentation.

### Changed
- Consolidated obsolete planning docs into architecture guidance and removed stale docs references.
- Refined C API and public header documentation links and organization.

### Notes
- Pre-release candidate for validating the end-to-end release and publication workflow before `0.1.0`.

### Highlights (last 6 months)
- Vendored runtime updated from libtealet v0.3.2 to v0.7.2, with release archives as the primary integration path.
- Build and packaging flow streamlined around the vendored release archive, while preserving an opt-in local source override path for deep debugging.
- Significant runtime stabilization work across stack switching, frame/thread-state handling, and regression hardening in both debug and optimized builds.
- API surface streamlined and clarified, including cleaner C/Python module boundaries and improved consistency in tealet/greenlet-facing behavior.
- Greenlet proof-of-concept compatibility functionality expanded and exercised through legacy and compatibility-oriented test paths.
- Python support widened and actively exercised across 3.10 through 3.15, including free-threaded variants (3.14t/3.15t).
- Thread ownership and safety model substantially improved, with explicit cross-thread guards plus thread cleanup/reap lifecycle support.
- CI and branch workflow modernized (mainline migration to `main`, active development on `development`, broader interpreter matrix coverage).
- Overall project velocity in this period reflects extensive modernization beyond initial bootstrap scope, with focus on robustness and long-term maintainability.

## [0.1.0] - 2025-11-29

### Added
- Initial project structure with modern Python packaging (src-layout)
- Python package `tealet` with greenlet-compatible interface
- C extension module `_tealet` structure
- Integration with libtealet v0.3.2 for stack-slicing functionality
- Comprehensive test suite (`test_tealet.py`, `test_greenlet.py`)
- Modern `pyproject.toml` configuration with uv support
- MIT License
- Project documentation in README.md

### Project Structure
- `src/tealet/` - Pure Python package with greenlet compatibility layer
- `src/_tealet/` - C extension module
  - `_tealet.c` - Main extension implementation
  - `libtealet/` - libtealet v0.3.2 library (stack-slicing core)
- `tests/` - Test suite for both tealet and greenlet interfaces
- Python version support: 3.10 through 3.14

### Dependencies
- libtealet v0.3.2 - Core stack-slicing library
- stackman - Platform-specific stack operations (bundled with libtealet)

### Notes
- C extension build configuration to be completed in future release
- This is a modernization of the original pytealet project
- Based on the greenlet project by Armin Rigo and the Stackless Python work
