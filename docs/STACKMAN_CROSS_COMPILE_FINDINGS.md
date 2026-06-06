# Stackman cross-compilation findings and recommendations

## Goal
Document the recent ABI selection and linking failures seen in pytealet CI, clarify what came from stackman versus pytealet integration logic, and propose improvements that a stackman-focused agent can implement upstream.

## Executive summary
1. The failures are cross-compilation target-detection issues.
2. The stackman ABI detector is macro-driven and can be target-correct, but only if the correct target compiler and target flags are supplied.
3. In pytealet, some failures were from pytealet-side ABI selection logic, not from stackman Makefiles directly.
4. Upstream stackman and libtealet can be made more robust by adding explicit ABI override support and stronger target-toolchain detection paths.

## What failed in CI

### 1. macOS cross-build on macos-14
1. x86_64 wheel tests on macos-14 failed with a missing symbol at import time.
2. This matched wrong-arch library selection during cross-build.

### 2. Windows ARM64 cross-build
1. Win ARM64 job selected Win x64 prebuilt archive.
2. Link then failed with unresolved tealet symbols, consistent with wrong archive architecture.

## Key distinction: stackman detector versus pytealet setup logic
1. In pytealet setup, Windows and Darwin ABI selection has custom target-aware logic in [setup.py](../setup.py#L49).
2. Only non-Windows and non-Darwin paths should fall through to Makefile abiname probing in [setup.py](../setup.py#L103).
3. Therefore, the recent win-arm64 mis-selection was primarily pytealet detection logic and environment signal handling, not stackman Makefile execution.

## How stackman ABI detection currently works

### Makefile path
1. Stackman computes ABI via [src/_tealet/libtealet/stackman/Makefile](../src/_tealet/libtealet/stackman/Makefile#L21).
2. If PLATFORM_PREFIX is provided, CC and related tools are switched to target-prefixed toolchain in [src/_tealet/libtealet/stackman/Makefile](../src/_tealet/libtealet/stackman/Makefile#L14).

### Detection script path
1. The script preprocesses abiname.c using supplied CC and CFLAGS in [src/_tealet/libtealet/stackman/tools/abiname.sh](../src/_tealet/libtealet/stackman/tools/abiname.sh#L21).
2. It then compiles and runs the preprocessed file with host cc in [src/_tealet/libtealet/stackman/tools/abiname.sh](../src/_tealet/libtealet/stackman/tools/abiname.sh#L23).
3. The printed ABI token comes from macros in platform headers selected via stackman.h include flow.

### Macro source of ABI token
1. ABI names are determined by compiler predefined macros in [src/_tealet/libtealet/stackman/stackman/platforms/platform.h](../src/_tealet/libtealet/stackman/stackman/platforms/platform.h#L35).
2. Examples include win_x86, win_x64, win_aarch64, darwin_x86_64, darwin_arm64, sysv_amd64.

## Why cross builds can still go wrong
1. If the preprocessor phase does not receive correct target toolchain and target flags, macros reflect host defaults.
2. If wrapper projects bypass stackman Makefiles and do their own ABI resolution, they must use target-aware signals consistently.
3. On CI, host architecture indicators are common and can override target intent unless explicitly prioritized.

## Improvements recommended for stackman and libtealet upstream

### Priority 1: explicit ABI override
1. Add an explicit ABI override variable in Makefiles and tools.
2. Example behavior: if STACKMAN_ABI is set, skip auto detection and use it directly.
3. Benefit: packaging systems can pass known target ABI and avoid heuristic mistakes.

### Priority 2: stronger target-toolchain discovery
1. Add a compiler-target interrogation step before macro probing.
2. For GCC and Clang toolchains, check target triple via compiler query and map to ABI token.
3. Keep macro-based probing as fallback.

### Priority 3: make script behavior less host-sensitive
1. Keep preprocessing with target compiler and target flags.
2. Preserve current run-on-host strategy for generated C, but make diagnostics explicit when target and host disagree.
3. Emit a warning including detected compiler, target triple, and chosen ABI.

### Priority 4: consistency and naming cleanup
1. Ensure ABI naming is consistent across docs, headers, release artifacts, and packaging scripts.
2. Keep compatibility aliases if historical names must remain supported.

## Standard cross-compilation practices that can be adopted
1. Use target-prefixed compiler via CC when available.
2. Pass explicit target flags, for example arch or target options, through CFLAGS.
3. Prefer explicit ABI override for build systems that already know the target wheel architecture.
4. In CI, prefer target environment variables over host machine probes.

## Suggested validation matrix for upstream changes
1. Native Linux x86_64, aarch64.
2. Native macOS arm64 and x86_64.
3. macOS cross-build arm64 host producing x86_64 and arm64 outputs.
4. Windows cross-build from x64 host producing x86, x64, arm64.
5. Verify chosen ABI token, selected archive path, and successful link for each target.

## Practical takeaway for pytealet
1. Keep target-aware ABI selection in pytealet setup for Windows and macOS.
2. If stackman upstream adds explicit ABI override, pytealet should prefer that path for deterministic packaging.
3. This reduces dependence on host-leaning heuristics in mixed host-target CI runs.
