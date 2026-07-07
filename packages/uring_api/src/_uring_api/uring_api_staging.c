/*
 * Reusable CQE staging buffers for batched completion draining.
 */

#include "uring_api_staging.h"
#include "uring_api_core.h"

#include <liburing.h>

static int staging_buffer_ensure_capacity(UringApiStagingBuffer *buf, size_t index) {
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

int staging_buffer_stage_cqe(UringApiRing *self, UringApiStagingBuffer *buf, struct io_uring_cqe *cqe) {
    UringApiCompletion *completion;
    UringApiStagedCQE *staged;
    size_t index;

    completion = cqe_get_completion(self, cqe);
    if (!completion) {
        PyErr_SetString(PyExc_SystemError, "io_uring CQE is missing its completion object");
        return -1;
    }
    index = buf->count;
    if (staging_buffer_ensure_capacity(buf, index) < 0) {
        return -1;
    }
    staged = &buf->entries[index];
    staged->res = cqe->res;
    staged->flags = cqe->flags;
    staged->completion = completion;
    staged->leg_index = 0;
    if (completion->multishot) {
        Py_BEGIN_CRITICAL_SECTION_MUTEX(&self->completion_mutex);
        staged->leg_index = completion->sequence;
        completion->sequence++;
        Py_END_CRITICAL_SECTION_MUTEX();
    }
    io_uring_cqe_seen(&self->ring, cqe);
    buf->count++;
    return 0;
}