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

/*
 * Invoke exception_handler for an error raised by nowait_error_handler.
 * Must hold the GIL. Never fails the drain: if the exception handler is unset
 * or also raises, the exception is written as unraisable and cleared.
 */
static void report_via_exception_handler(UringApiRing *self, const char *message) {
    PyObject *handler = NULL;
    PyObject *context = NULL;
    PyObject *call_result = NULL;
    PyObject *exc_type = NULL;
    PyObject *exc_value = NULL;
    PyObject *exc_tb = NULL;
    PyObject *completions = NULL;
    PyObject *msg_obj = NULL;

    PyErr_Fetch(&exc_type, &exc_value, &exc_tb);
    PyErr_NormalizeException(&exc_type, &exc_value, &exc_tb);

    Py_BEGIN_CRITICAL_SECTION(self);
    handler = self->delivery_exception_handler;
    if (handler) {
        Py_INCREF(handler);
    }
    Py_END_CRITICAL_SECTION();

    if (!handler) {
        PyErr_Restore(exc_type, exc_value, exc_tb);
        PyErr_WriteUnraisable((PyObject *)self);
        return;
    }

    context = PyDict_New();
    if (!context) {
        goto fail;
    }
    msg_obj = PyUnicode_FromString(message);
    if (!msg_obj) {
        goto fail;
    }
    if (PyDict_SetItemString(context, "message", msg_obj) < 0) {
        goto fail;
    }
    Py_CLEAR(msg_obj);
    if (PyDict_SetItemString(context, "exception", exc_value ? exc_value : Py_None) < 0) {
        goto fail;
    }
    if (PyDict_SetItemString(context, "ring", (PyObject *)self) < 0) {
        goto fail;
    }
    /* no Completion list for nowait; keep the same key as delivery errors */
    completions = PyList_New(0);
    if (!completions) {
        goto fail;
    }
    if (PyDict_SetItemString(context, "completions", completions) < 0) {
        goto fail;
    }
    Py_CLEAR(completions);

    call_result = PyObject_CallOneArg(handler, context);
    Py_DECREF(handler);
    handler = NULL;
    Py_DECREF(context);
    context = NULL;
    Py_XDECREF(exc_type);
    Py_XDECREF(exc_value);
    Py_XDECREF(exc_tb);
    if (!call_result) {
        PyErr_WriteUnraisable((PyObject *)self);
        return;
    }
    Py_DECREF(call_result);
    return;

fail:
    Py_XDECREF(handler);
    Py_XDECREF(context);
    Py_XDECREF(msg_obj);
    Py_XDECREF(completions);
    if (!PyErr_Occurred()) {
        PyErr_Restore(exc_type, exc_value, exc_tb);
    } else {
        Py_XDECREF(exc_type);
        Py_XDECREF(exc_value);
        Py_XDECREF(exc_tb);
    }
    PyErr_WriteUnraisable((PyObject *)self);
}

/*
 * Nowait CQE with res < 0: optional nowait_error_handler under a
 * temporary GIL. Staging runs without the GIL (drain ALLOW_THREADS). Always
 * returns after cqe_seen; never fails the drain.
 */
static void report_nowait_error(UringApiRing *self, int res, unsigned int flags) {
    PyGILState_STATE gstate;
    PyObject *handler = NULL;
    PyObject *context = NULL;
    PyObject *call_result = NULL;
    PyObject *res_obj = NULL;
    PyObject *flags_obj = NULL;
    PyObject *msg_obj = NULL;

    gstate = PyGILState_Ensure();

    Py_BEGIN_CRITICAL_SECTION(self);
    handler = self->nowait_error_handler;
    if (handler) {
        Py_INCREF(handler);
    }
    Py_END_CRITICAL_SECTION();

    if (!handler) {
        /* no client interest in nowait failures */
        PyGILState_Release(gstate);
        return;
    }

    context = PyDict_New();
    if (!context) {
        goto fail;
    }
    msg_obj = PyUnicode_FromString("Nowait operation failed");
    if (!msg_obj) {
        goto fail;
    }
    if (PyDict_SetItemString(context, "message", msg_obj) < 0) {
        goto fail;
    }
    Py_CLEAR(msg_obj);
    if (PyDict_SetItemString(context, "ring", (PyObject *)self) < 0) {
        goto fail;
    }
    res_obj = PyLong_FromLong(res);
    if (!res_obj) {
        goto fail;
    }
    if (PyDict_SetItemString(context, "res", res_obj) < 0) {
        goto fail;
    }
    Py_CLEAR(res_obj);
    flags_obj = PyLong_FromUnsignedLong(flags);
    if (!flags_obj) {
        goto fail;
    }
    if (PyDict_SetItemString(context, "flags", flags_obj) < 0) {
        goto fail;
    }
    Py_CLEAR(flags_obj);
    /* reserved for later correlation; unset in this iteration */
    if (PyDict_SetItemString(context, "kind", Py_None) < 0) {
        goto fail;
    }
    if (PyDict_SetItemString(context, "fd", Py_None) < 0) {
        goto fail;
    }

    call_result = PyObject_CallOneArg(handler, context);
    Py_DECREF(handler);
    handler = NULL;
    Py_DECREF(context);
    context = NULL;
    if (!call_result) {
        report_via_exception_handler(self, "Exception in nowait_error_handler");
        PyGILState_Release(gstate);
        return;
    }
    Py_DECREF(call_result);
    PyGILState_Release(gstate);
    return;

fail:
    Py_XDECREF(handler);
    Py_XDECREF(context);
    Py_XDECREF(msg_obj);
    Py_XDECREF(res_obj);
    Py_XDECREF(flags_obj);
    if (PyErr_Occurred()) {
        report_via_exception_handler(self, "Exception building nowait_error_handler context");
    }
    PyGILState_Release(gstate);
}

int staging_buffer_record_cqe(UringApiRing *self, UringApiStagingBuffer *buf, struct io_uring_cqe *cqe) {
    UringApiCompletion *completion;
    UringApiStagedCQE *staged;
    size_t index;
    unsigned long long user_data;

    user_data = io_uring_cqe_get_data64(cqe);
    /* internal wake NOP: no Completion, never report */
    if (user_data == URING_API_WAKE_USER_DATA) {
        io_uring_cqe_seen(&self->ring, cqe);
        return 0;
    }
    /*
     * Nowait: no Completion. Without IORING_FEAT_CQE_SKIP / skip-success,
     * success CQEs still arrive (res >= 0) and are dropped silently. Only res < 0
     * invokes nowait_error_handler.
     */
    if (user_data == URING_API_NOWAIT_USER_DATA) {
        int res = cqe->res;
        unsigned int flags = cqe->flags;

        io_uring_cqe_seen(&self->ring, cqe);
        if (res < 0) {
            report_nowait_error(self, res, flags);
        }
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
