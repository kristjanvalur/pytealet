#ifndef PYTEALET_BUILD_CONFIG_H
#define PYTEALET_BUILD_CONFIG_H

/*
 * Centralized compile-time switches for pytealet local experiments.
 *
 * This header is force-included by setup.py for:
 *  1) libtealet static library build (when BUILD_LIBTEALET_FROM_SOURCE=1)
 *  2) _tealet extension compilation
 *
 * Edit values here to run controlled test variants without changing source
 * files.
 */

/*
 * Note: historical local instrumentation toggles were removed after
 * syncing to the current libtealet baseline.
 *
 * Keep this header in place because setup.py force-includes it for both
 * libtealet and extension builds.
 */

/* Disable dormant-tealet frame introspection/rewrite support.
 *
 * Default is enabled in pytealet_common.h; this local override forces behavior
 * that matches the 3.10-style no-pending-frame-introspection path.
 */
#define PYTEALET_WITH_PENDING_FRAME_INTROSPECTION 0

#endif /* PYTEALET_BUILD_CONFIG_H */
