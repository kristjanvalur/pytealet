# tealet-greenlet Changelog

All notable changes to the `tealet-greenlet` package will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed
- Relaxed the `tealet` dependency to the compatible `0.1` range
	(`tealet>=0.1.0rc2,<0.2`) instead of pinning one release candidate exactly.

## [0.1.0rc1] - 2026-06-25

### Added
- First release candidate for `tealet-greenlet`, the experimental greenlet emulation package built on `tealet`.
- Added canonical `tealet_greenlet` imports plus drop-in `greenlet` and `greenlet._greenlet` wrappers.
- Added package-local API and architecture documentation.
- Added legacy and upstream-style compatibility tests under the package test tree.

### Changed
- Split greenlet compatibility out of the core `tealet` package.
- Moved the historical `greenlet_legacy.py` shim into tests as a comparison fixture instead of an installable module.
- Depends on the compatible `tealet` 0.1 release-candidate line for the split workspace release.

### Notes
- Pre-release candidate for tracking greenlet compatibility without making the core `tealet` wheel carry that semantic surface.