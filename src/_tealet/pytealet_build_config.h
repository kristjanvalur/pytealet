#ifndef PYTEALET_BUILD_CONFIG_H
#define PYTEALET_BUILD_CONFIG_H

/*
 * Centralized compile-time switches for pytealet local experiments.
 *
 * This header is force-included by setup.py for:
 *  1) libtealet static library build (when BUILD_LIBTEALET_FROM_SOURCE=1)
 *  2) _tealet extension compilation
 *
 * Edit values here to run controlled test variants without changing source files.
 */

/*
 * Preset profiles for fast switching during debug/bisect work.
 *
 * TEALET_PYTEALET_PROFILE_UPSTREAM_LIKE:
 *   Keep behavior close to upstream defaults (minimal local diagnostics).
 *
 * TEALET_PYTEALET_PROFILE_DEBUG_HEAVY:
 *   Enable pytealet local diagnostics and validation checks.
 */
#define TEALET_PYTEALET_PROFILE_UPSTREAM_LIKE 1
#define TEALET_PYTEALET_PROFILE_DEBUG_HEAVY   2

#ifndef TEALET_PYTEALET_PROFILE
#define TEALET_PYTEALET_PROFILE TEALET_PYTEALET_PROFILE_UPSTREAM_LIKE
#endif

#if TEALET_PYTEALET_PROFILE == TEALET_PYTEALET_PROFILE_UPSTREAM_LIKE

/* Upstream-like defaults */
#ifndef TEALET_PYTEALET_ENABLE_STACK_DIAGNOSTICS
#define TEALET_PYTEALET_ENABLE_STACK_DIAGNOSTICS 0
#endif

#ifndef TEALET_PYTEALET_ENABLE_MAGIC_COOKIES
#define TEALET_PYTEALET_ENABLE_MAGIC_COOKIES 0
#endif

#ifndef TEALET_PYTEALET_VALIDATE_PRE_RESTORE
#define TEALET_PYTEALET_VALIDATE_PRE_RESTORE 0
#endif

#ifndef TEALET_PYTEALET_MIN_INITIAL_SAVE
#define TEALET_PYTEALET_MIN_INITIAL_SAVE 0
#endif

#ifndef TEALET_PYTEALET_FIX_LOCAL_CFRAME_COPY
#define TEALET_PYTEALET_FIX_LOCAL_CFRAME_COPY 0
#endif

#elif TEALET_PYTEALET_PROFILE == TEALET_PYTEALET_PROFILE_DEBUG_HEAVY

/* Debug-heavy defaults */
#ifndef TEALET_PYTEALET_ENABLE_STACK_DIAGNOSTICS
#define TEALET_PYTEALET_ENABLE_STACK_DIAGNOSTICS 1
#endif

#ifndef TEALET_PYTEALET_ENABLE_MAGIC_COOKIES
#define TEALET_PYTEALET_ENABLE_MAGIC_COOKIES 1
#endif

#ifndef TEALET_PYTEALET_VALIDATE_PRE_RESTORE
#define TEALET_PYTEALET_VALIDATE_PRE_RESTORE 1
#endif

#ifndef TEALET_PYTEALET_MIN_INITIAL_SAVE
#define TEALET_PYTEALET_MIN_INITIAL_SAVE 2048
#endif

#ifndef TEALET_PYTEALET_FIX_LOCAL_CFRAME_COPY
#define TEALET_PYTEALET_FIX_LOCAL_CFRAME_COPY 0
#endif

#else
#error "Unknown TEALET_PYTEALET_PROFILE value"
#endif

#endif /* PYTEALET_BUILD_CONFIG_H */
