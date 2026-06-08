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
/*
 * Hack: CPython internal frame definitions are gated behind Py_BUILD_CORE,
 * but pending frame introspection requires _PyInterpreterFrame details on
 * 3.12+ (including 3.13). Define it only around this include to minimize
 * namespace/policy bleed into the rest of the extension.
 */
#if !defined(Py_BUILD_CORE)
#define PYTEALET_DEFINED_PY_BUILD_CORE 1
#define Py_BUILD_CORE
#endif
#include "internal/pycore_frame.h"
#if defined(PY314P)
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
#endif
#include "internal/pycore_interpframe.h"
#include "internal/pycore_interpframe_structs.h"
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC diagnostic pop
#endif
#endif
#if defined(PYTEALET_DEFINED_PY_BUILD_CORE)
#undef Py_BUILD_CORE
#undef PYTEALET_DEFINED_PY_BUILD_CORE
#endif

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
void PyTealetFrameInfo_Fini(PyTealetFrameInfo *info);
int PyTealetFrameInfo_Capture(PyTealetFrameInfo *info, int rewrite_chain);
PyObject *PyTealetFrameInfo_GetFrame(const PyTealetFrameInfo *info);
void PyTealetFrameInfo_Release(PyTealetFrameInfo *info, tealet_t *dustbin_tealet);

/* GC helpers for captured dormant-frame references. */
int PyTealetFrameInfo_Visit(const PyTealetFrameInfo *info, visitproc visit, void *arg);
void PyTealetFrameInfo_ClearForGC(PyTealetFrameInfo *info);

#endif
