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
    unsigned int leased_count;
    unsigned short group_id;
    int mask;
    /* optional callable(pool) invoked by tealetio when a receive path closes */
    PyObject *release_callback;
} UringApiBufGroup;

extern PyTypeObject UringApiBufGroup_Type;

void UringApiRing_clear_free_buf_group_ids(UringApiRing *ring);
unsigned short UringApiRing_alloc_buf_group_id(UringApiRing *ring);
int UringApiRing_release_buf_group_id(UringApiRing *ring, unsigned short group_id);
PyObject *UringApiBufGroup_create(UringApiRing *ring, unsigned int buffer_size, unsigned int buffer_count);
void UringApiBufGroup_recycle(UringApiBufGroup *self, unsigned int buffer_id);
void UringApiBufGroup_note_leased(UringApiBufGroup *self);
void UringApiBufGroup_note_unleased(UringApiBufGroup *self);
PyObject *UringApiRing_create_buf_group(UringApiRing *self, PyObject *args, PyObject *kwargs);

#endif