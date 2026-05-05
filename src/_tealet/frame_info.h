/* This file defines a structure which holds information about the Python frame of
 * a suspended tealet, for introspection.
 * In some Python versions, this requires re-writing frames on the stack so that
 * the frame objects are accessible and can be safely refcounted accessed.
 */

#ifndef PYTEALET_FRAME_INFO_H
#define PYTEALET_FRAME_INFO_H

#include "Python.h"
#include "frameobject.h"

#include "pytealet_common.h"
#include "tealet.h"

#if !defined(PY_HAS_TSTATE_FRAME) && PYTEALET_WITH_PENDING_FRAME_INTROSPECTION
#define PYTEALET_HAS_PENDING_FRAME_INTROSPECTION 1
#endif

#if defined(PYTEALET_HAS_PENDING_FRAME_INTROSPECTION) && defined(PY312P)
#include "internal/pycore_frame.h"

typedef struct PyTealetFrameInfoEntry {
    _PyInterpreterFrame **location;
    _PyInterpreterFrame *old_value;
} PyTealetFrameInfoEntry;

#ifndef PYTEALET_FRAMEINFO_FIXED_ITEMS
#define PYTEALET_FRAMEINFO_FIXED_ITEMS 2
#endif
#endif

typedef struct PyTealetFrameInfo {
#if defined(PYTEALET_HAS_PENDING_FRAME_INTROSPECTION)
    /* Snapshot of the dormant frame object for tealet.frame queries. */
    PyFrameObject *frame;
#if defined(PY312P)
    /* Do not traverse/rewrite beyond this frame during chain hiding. */
    void *stop_frame;
    /* Rewrites use a small inline buffer first, then heap storage on overflow. */
    PyTealetFrameInfoEntry *items;
    Py_ssize_t size;
    Py_ssize_t capacity;
    PyTealetFrameInfoEntry fixed_items[PYTEALET_FRAMEINFO_FIXED_ITEMS];
#endif
#else
    /* C89 does not allow empty structs; keep a single placeholder byte. */
    unsigned char unused;
#endif
} PyTealetFrameInfo;

void PyTealetFrameInfo_Init(PyTealetFrameInfo *info);
void PyTealetFrameInfo_SetStopFrame(PyTealetFrameInfo *info, void *stop_frame);
void PyTealetFrameInfo_Fini(PyTealetFrameInfo *info);
int PyTealetFrameInfo_Capture(PyTealetFrameInfo *info, int rewrite_chain);
PyObject *PyTealetFrameInfo_GetFrame(const PyTealetFrameInfo *info);
void PyTealetFrameInfo_Release(PyTealetFrameInfo *info, tealet_t *dustbin_tealet);

#endif