/*
 * Reusable CQE staging buffers for batched completion draining.
 */

#include "uring_api_staging.h"
#include "uring_api_core.h"

#include <assert.h>
#include <liburing.h>

int staging_buffer_ensure_capacity(UringApiStagingBuffer *buf, size_t index) {
    size_t needed;
    UringApiStagedCQE *entries;

    needed = index + 1;
    if (needed <= buf->capacity) {
        return 0;
    }
    if (buf->capacity == 0) {
        needed = 8;
    } else {
        needed = buf->capacity;
        while (needed < index + 1) {
            needed *= 2;
        }
    }
    entries = PyMem_Realloc(buf->entries, needed * sizeof(UringApiStagedCQE));
    if (!entries) {
        PyErr_NoMemory();
        return -1;
    }
    buf->entries = entries;
    buf->capacity = needed;
    return 0;
}

void staging_buffer_clear(UringApiStagingBuffer *buf) {
    PyMem_Free(buf->entries);
    buf->entries = NULL;
    buf->capacity = 0;
    buf->count = 0;
}

void staging_buffer_reset(UringApiStagingBuffer *buf) { buf->count = 0; }

void staging_buffer_record_cqe(UringApiRing *self, UringApiStagingBuffer *buf, struct io_uring_cqe *cqe) {
    UringApiCompletion *completion;
    UringApiStagedCQE *staged;
    size_t index;

    assert(buf->count < buf->capacity);
    completion = cqe_get_completion(self, cqe);
    assert(completion != NULL);
    index = buf->count;
    staged = &buf->entries[index];
    staged->res = cqe->res;
    staged->flags = cqe->flags;
    staged->completion = completion;
    staged->leg_index = 0;
    io_uring_cqe_seen(&self->ring, cqe);
    buf->count++;
}

int staging_buffer_assign_multishot_indices(UringApiRing *self, UringApiStagingBuffer *buf) {
    size_t index;

    for (index = 0; index < buf->count; index++) {
        UringApiStagedCQE *staged = &buf->entries[index];
        if (staged->completion->multishot) {
            Py_BEGIN_CRITICAL_SECTION_MUTEX(&self->completion_mutex);
            staged->leg_index = staged->completion->sequence;
            staged->completion->sequence++;
            Py_END_CRITICAL_SECTION_MUTEX();
        }
    }
    return 0;
}