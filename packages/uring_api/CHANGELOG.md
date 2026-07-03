# uring-api Changelog

All notable changes to the `uring-api` package will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed
- Moved native extension sources from flat `src/uring_api_*.c/.h` files into
  `src/_uring_api/`, matching core `tealet`'s `src/_tealet/` layout.
- Split the monolithic `tests/test_uring_api.py` into focused modules with
  shared `conftest.py` and `helpers.py` (see `tests/README.md`).
- Aligned `pyproject.toml` package-data with `setup.py` so wheels ship
  `uring_api_completion_kinds.h` alongside `uring_api_capi.h`.