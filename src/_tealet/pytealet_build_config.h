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
 * Note: historical local instrumentation toggles were removed after
 * syncing to the current libtealet baseline.
 *
 * Keep this header in place because setup.py force-includes it for both
 * libtealet and extension builds, but there are currently no active
 * pytealet-local compile-time switches defined here.
 */

#endif /* PYTEALET_BUILD_CONFIG_H */
