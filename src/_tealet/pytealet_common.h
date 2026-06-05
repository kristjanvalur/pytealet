/* pytealet_common.h - compile-time feature flags shared by extension modules.
 *
 * Contains Python-version feature macros, state constants, and compatibility
 * typedefs used across multiple internal source files.
 */

#ifndef PYTEALET_COMMON_H
#define PYTEALET_COMMON_H

#include "Python.h"

#define STATE_NEW 0
#define STATE_STUB 1
#define STATE_RUN 2
#define STATE_EXIT 3

#ifndef PYTEALET_VERSION
#define PYTEALET_VERSION "0.0.0+unknown"
#endif

/* Controls dormant-tealet frame introspection for Python versions that do not
 * expose tstate->frame directly. When set to 0, frame capture/rewriting is
 * disabled and behavior matches the 3.10-style no-pending-frame path.
 */
#ifndef PYTEALET_WITH_PENDING_FRAME_INTROSPECTION
#define PYTEALET_WITH_PENDING_FRAME_INTROSPECTION 1
#endif

#ifndef PYTEALET_DEFER_DELETE
/* Keep the exited tealet in the pytealet structure for access to the tealet api. */
#define PYTEALET_DEFER_DELETE 0
#endif

/* Python minor-version helpers for readable version-specific conditionals. */
#if PY_VERSION_HEX >= 0x030A0000 && PY_VERSION_HEX < 0x030B0000
#define PY310 1
#endif

#if PY_VERSION_HEX >= 0x030B0000
#define Py311P 1
#if PY_VERSION_HEX < 0x030C0000
#define PY311 1
#endif
#endif

#if PY_VERSION_HEX >= 0x030C0000
#define PY312P 1
#if PY_VERSION_HEX < 0x030D0000
#define PY312 1
#endif
#endif

#if PY_VERSION_HEX >= 0x030D0000
#define PY313P 1
#if PY_VERSION_HEX < 0x030E0000
#define PY313 1
#endif
#endif

#if PY_VERSION_HEX >= 0x030E0000
#define PY314P 1
#if PY_VERSION_HEX < 0x030F0000
#define PY314 1
#endif
#endif

#if PY_VERSION_HEX >= 0x030F0000
#define PY315P 1
#if PY_VERSION_HEX < 0x03100000
#define PY315 1
#endif
#endif

/* PyThreadState capability macros for clearer compatibility guards. */

#if defined(PY310) || defined(PY311)
#define PY_HAS_TSTATE_CUREXC_FIELDS
#endif

#if defined(PY310)
#define PY_HAS_TSTATE_RECURSION_DEPTH
#endif

#if defined(PY311)
#define PY_HAS_TSTATE_RECURSION_REMAINING
#endif

#if defined(PY312P)
#define PY_HAS_TSTATE_PY_RECURSION_REMAINING
#endif

#if defined(PY312) || defined(PY313)
#define PY_HAS_TSTATE_C_RECURSION_REMAINING
#endif

#if defined(PY310) || defined(PY311) || defined(PY312)
#define PY_HAS_TSTATE_TRASH_DELETE_NESTING
#endif

#if defined(PY313P)
#define PY_HAS_TSTATE_DELETE_LATER
#endif

#define PY_HAS_TSTATE_CONTEXT

#if defined(PY310) || defined(PY311) || defined(PY312)
#define PY_HAS_TSTATE_CFRAME
#endif

#if defined(PY310)
#define PY_HAS_TSTATE_FRAME
#endif

#if defined(Py311P)
#define PY_HAS_TSTATE_CURRENT_FRAME
#define PY_HAS_TSTATE_DATASTACK
#endif

#if defined(PY311)
#define PY_HAS_TSTATE_CFRAME_USE_TRACING
#endif

#if defined(PY314P)
#define PY_HAS_TSTATE_CURRENT_EXECUTOR
#endif

#if defined(PY_HAS_TSTATE_CFRAME)
#if defined(PY310)
typedef CFrame PyTealetCFrame;
#else
typedef _PyCFrame PyTealetCFrame;
#endif
#endif

#endif