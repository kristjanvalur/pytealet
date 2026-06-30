#ifndef URING_API_BUFGROUP_H
#define URING_API_BUFGROUP_H

/* private implementation header; not part of the public C API. */

#include "uring_api_common.h"

typedef struct {
    PyObject_HEAD UringApiRing *ring;
    struct io_uring_buf_ring *ring_buffer;
    unsigned char *storage;
    unsigned int buffer_size;
    unsigned int buffer_count;
    unsigned short group_id;
    int mask;
} UringApiBufGroup;

extern PyTypeObject UringApiBufGroup_Type;

PyObject *UringApiBufGroup_create(UringApiRing *ring, unsigned int buffer_size, unsigned int buffer_count);
void UringApiBufGroup_recycle(UringApiBufGroup *self, unsigned int buffer_id);
PyObject *UringApiRing_create_buf_group(UringApiRing *self, PyObject *args, PyObject *kwargs);

#endif