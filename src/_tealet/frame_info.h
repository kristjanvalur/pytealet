#ifndef PYTEALET_FRAME_INFO_H
#define PYTEALET_FRAME_INFO_H

#include "Python.h"
#include "frameobject.h"

#include "tealet.h"
#include "pytealet.h"


/* This file defines a structure which holds information about the Python frame of
 * a suspended tealet, for introspection.
 * In some Python versions, this requires re-writing frames on the stack so that
 * the frame objects are accessible and can be safely refcounted accessed.
 */

 typedef struct PyTealetFrameInfo {
#if !defined(PY_HAS_TSTATE_FRAME)
    /* Snapshot of the dormant frame object for tealet.frame queries. */
    PyFrameObject *frame;
#if defined(PY312P)
    /* Internal rewrite storage owned by frame_info.c implementation. */
    void *items;
    Py_ssize_t size;
    Py_ssize_t capacity;
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

#endif