# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
