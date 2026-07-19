/*
 * Reusable CQE staging buffers for batched completion draining.
 */

#include "uring_api_staging.h"
#include "uring_api_completion.h"
#include "uring_api_core.h"

#include <assert.h>
#include <liburing.h>
#include <stdlib.h>

#define STAGING_BUFFER_INITIAL_CAPACITY 4

static int staging_buffer_grow(UringApiStagingBuffer *buf) {
    size_t new_capacity;
    UringApiStagedCQE *entries;

    if (buf->capacity == 0) {
        new_capacity = STAGING_BUFFER_INITIAL_CAPACITY;
    } else {
        new_capacity = buf->capacity * 2;
    }
    entries = realloc(buf->entries, new_capacity * sizeof(UringApiStagedCQE));
    if (!entries) {
        return -1;
    }
    buf->entries = entries;
    buf->capacity = new_capacity;
    return 0;
}

void staging_buffer_clear(UringApiStagingBuffer *buf) {
    free(buf->entries);
    buf->entries = NULL;
    buf->capacity = 0;
    buf->count = 0;
}

void staging_buffer_reset(UringApiStagingBuffer *buf) { buf->count = 0; }

int staging_buffer_record_cqe(UringApiRing *self, UringApiStagingBuffer *buf, struct io_uring_cqe *cqe) {
    UringApiCompletion *completion;
    UringApiStagedCQE *staged;
    size_t index;
    unsigned long long user_data;

    user_data = io_uring_cqe_get_data64(cqe);
    /* internal break_wait NOP: wake the reaper only; no Completion to package */
    if (user_data == URING_API_WAKE_USER_DATA) {
        io_uring_cqe_seen(&self->ring, cqe);
        return 0;
    }

    if (buf->count >= buf->capacity) {
        if (staging_buffer_grow(buf) < 0) {
            return -1;
        }
    }
    completion = (UringApiCompletion *)(uintptr_t)user_data;
    assert(completion != NULL);
    index = buf->count;
    staged = &buf->entries[index];
    staged->res = cqe->res;
    staged->flags = cqe->flags;
    staged->completion = completion;
    staged->leg_index = 0;
    if (completion->multishot) {
        staged->leg_index = completion->sequence;
        completion->sequence++;
    }
    /* track multi-step in-flight refs while the drain lock is held (no GIL). */
    completion_prep_in_flight_ref(self, completion, cqe->flags);

    /* consume the kernel CQE while draining. packaging or delivery failure later
     * (OOM, conversion error, callback error, etc.) is just failure — same as any
     * other unrecoverable error path; the ring slot cannot be un-seen. */
    io_uring_cqe_seen(&self->ring, cqe);
    buf->count++;
    return 0;
}