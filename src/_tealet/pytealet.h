#ifndef PYTEALET_H
#define PYTEALET_H

#include "Python.h"
#include "tealet.h"

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

#if PY_VERSION_HEX >= 0x030D0000 && PY_VERSION_HEX < 0x030E0000
#define PY313 1
#endif

#if PY_VERSION_HEX >= 0x030E0000 && PY_VERSION_HEX < 0x030F0000
#define PY314 1
#endif

#if PY_VERSION_HEX >= 0x030F0000 && PY_VERSION_HEX < 0x03100000
#define PY315 1
#endif

#if defined(PY310) || defined(PY311) || defined(PY312)
#define PY_HAS_CFRAME
#endif

#if defined(PY310)
#define PY_HAS_TSTATE_FRAME
#endif

#if defined(PY_HAS_CFRAME)
#if defined(PY310)
typedef CFrame PyTealetCFrame;
#else
typedef _PyCFrame PyTealetCFrame;
#endif
#endif

/* push an object into the tealet dustbin, to be decrefed later. */
void PyTealet_dustbin_push(tealet_t *tealet, PyObject *obj);

#endif