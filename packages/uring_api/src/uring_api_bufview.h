#ifndef URING_API_BUFVIEW_H
#define URING_API_BUFVIEW_H

/* private implementation header; not part of the public C API. */

#include "uring_api_bufgroup.h"

typedef struct {
    PyObject_HEAD PyObject *buf_group;
    unsigned int buffer_id;
    unsigned int length;
    unsigned int export_count;
    Py_ssize_t export_shape;
    Py_ssize_t export_stride;
    bool leased;
    bool recycled;
} UringApiBufView;

extern PyTypeObject UringApiBufView_Type;

PyObject *UringApiBufView_create(PyObject *buf_group_obj, unsigned int buffer_id, unsigned int length);
PyObject *UringApiBufView_create_empty(PyObject *buf_group_obj);
PyObject *UringApiRing_create_buf_view(UringApiRing *self, PyObject *args, PyObject *kwargs);

#endif