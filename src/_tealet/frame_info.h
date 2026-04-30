#ifndef PYTEALET_FRAME_INFO_H
#define PYTEALET_FRAME_INFO_H

#include "Python.h"
#include "frameobject.h"

#include "tealet.h"
#include "pytealet.h"

#if !defined(PY_HAS_TSTATE_FRAME)
typedef struct PyTealetFrameInfo {
    /* Snapshot of the dormant frame object for tealet.frame queries. */
    PyFrameObject *frame;
#if defined(PY312P)
    /* Internal rewrite storage owned by frame_info.c implementation. */
    void *items;
    Py_ssize_t size;
    Py_ssize_t capacity;
#endif
} PyTealetFrameInfo;

void PyTealetFrameInfo_Init(PyTealetFrameInfo *info);
void PyTealetFrameInfo_Fini(PyTealetFrameInfo *info);
int PyTealetFrameInfo_Capture(PyTealetFrameInfo *info, int rewrite_chain);
void PyTealetFrameInfo_Release(PyTealetFrameInfo *info, tealet_t *dustbin_tealet);

#endif
#endif