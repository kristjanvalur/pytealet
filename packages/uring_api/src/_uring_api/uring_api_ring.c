/*
 * Ring lifecycle methods for the _uring_api extension.
 */

#include "uring_api_ring.h"
#include "uring_api_bufgroup.h"
#include "uring_api_bufview.h"
#include "uring_api_core.h"
#include "uring_api_dispatch.h"
#include "uring_api_fd_table.h"
#include "uring_api_prepare.h"
#include "uring_api_staging.h"

PyObject *UringApiRing_new(PyTypeObject *type, PyObject *args, PyObject *kwargs) {
    UringApiRing *self = (UringApiRing *)type->tp_alloc(type, 0);

    (void)args;
    (void)kwargs;
    if (!self) {
        return NULL;
    }

#ifdef URING_API_USE_PYTHREAD_RING_LOCK
    self->ring_lock = PyThread_allocate_lock();
    if (!self->ring_lock) {
        PyErr_NoMemory();
        PyObject_GC_Del(self);
        return NULL;
    }
#endif
    self->cqe_drain_lock = PyThread_allocate_lock();
    if (!self->cqe_drain_lock) {
#ifdef URING_API_USE_PYTHREAD_RING_LOCK
        PyThread_free_lock(self->ring_lock);
        self->ring_lock = NULL;
#endif
        PyErr_NoMemory();
        PyObject_GC_Del(self);
        return NULL;
    }
#ifdef URING_API_USE_PYTHREAD_MUTEX
    self->refcount_mutex = PyThread_allocate_lock();
    if (!self->refcount_mutex) {
        PyThread_free_lock(self->cqe_drain_lock);
        self->cqe_drain_lock = NULL;
#ifdef URING_API_USE_PYTHREAD_RING_LOCK
        PyThread_free_lock(self->ring_lock);
        self->ring_lock = NULL;
#endif
        PyErr_NoMemory();
        PyObject_GC_Del(self);
        return NULL;
    }
#endif
    if (UringApiIdlePark_init(&self->idle) < 0) {
#ifdef URING_API_USE_PYTHREAD_MUTEX
        PyThread_free_lock(self->refcount_mutex);
        self->refcount_mutex = NULL;
#endif
        PyThread_free_lock(self->cqe_drain_lock);
        self->cqe_drain_lock = NULL;
#ifdef URING_API_USE_PYTHREAD_RING_LOCK
        PyThread_free_lock(self->ring_lock);
        self->ring_lock = NULL;
#endif
        PyObject_GC_Del(self);
        return NULL;
    }
    self->auto_submit = true;
    self->experimental_send_all_submit_next = false;
    return (PyObject *)self;
}

int UringApiRing_init(UringApiRing *self, PyObject *args, PyObject *kwargs) {
    static char *keywords[] = {"entries", "flags", "auto_submit", "experimental_send_all_submit_next", NULL};
    struct io_uring_params params;
    unsigned long entries_value = 8;
    unsigned long flags_value = 0;
    unsigned int entries;
    unsigned int flags;
    int auto_submit = 1;
    int send_all_submit_next = 0;
    int ret;
    int failed = 0;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|kkpp", keywords, &entries_value, &flags_value, &auto_submit,
                                     &send_all_submit_next)) {
        return -1;
    }
    if (entries_value == 0 || entries_value > UINT_MAX) {
        PyErr_SetString(PyExc_ValueError, "entries must be between 1 and UINT_MAX");
        return -1;
    }
    if (flags_value > UINT_MAX) {
        PyErr_SetString(PyExc_ValueError, "flags must fit in an unsigned int");
        return -1;
    }
    entries = (unsigned int)entries_value;
    flags = (unsigned int)flags_value;

    if (delivery_check_not_running(self) < 0) {
        return -1;
    }

    Py_BEGIN_CRITICAL_SECTION(self);
    if (self->initialized) {
        io_uring_queue_exit(&self->ring);
        self->initialized = false;
    }
    self->receive_state = URING_API_RECEIVE_IDLE;
    self->delivery_stop_requested = false;
    self->delivery_active_workers = 0;
    self->next_buf_group = 1;
    self->setup_flags = flags;
    self->owner_thread_id = 0;
    self->auto_submit = auto_submit != 0;
    self->experimental_send_all_submit_next = send_all_submit_next != 0;

    memset(&self->ring, 0, sizeof(self->ring));
    memset(&params, 0, sizeof(params));
    params.flags = flags;

    errno = 0;
    Py_BEGIN_ALLOW_THREADS;
    ret = io_uring_queue_init_params(entries, &self->ring, &params);
    Py_END_ALLOW_THREADS;

    if (ret < 0) {
        int errnum = normalize_ret_errno(ret);
        errno = errnum;
        PyErr_SetFromErrno(PyExc_OSError);
        failed = 1;
    } else {
        self->initialized = true;
        if (flags & (IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_DEFER_TASKRUN)) {
            self->owner_thread_id = (unsigned long long)PyThread_get_thread_ident();
        }
    }
    Py_END_CRITICAL_SECTION();

    return failed ? -1 : 0;
}

void UringApiRing_dealloc(UringApiRing *self) {
    PyObject_GC_UnTrack(self);
    (void)UringApiRing_stop_delivery(self);
    if (self->initialized) {
        io_uring_queue_exit(&self->ring);
        self->initialized = false;
    }
    (void)UringApiRing_clear(self);
    UringApiRing_clear_free_buf_group_ids(self);
    staging_buffer_clear(&self->wait_staging);
    self->c_delivery_callback = NULL;
    self->c_delivery_callback_user_data = NULL;
    if (self->cqe_drain_lock) {
        PyThread_free_lock(self->cqe_drain_lock);
        self->cqe_drain_lock = NULL;
    }
#ifdef URING_API_USE_PYTHREAD_MUTEX
    if (self->refcount_mutex) {
        PyThread_free_lock(self->refcount_mutex);
        self->refcount_mutex = NULL;
    }
#endif
    UringApiIdlePark_fini(&self->idle);
#ifdef URING_API_USE_PYTHREAD_RING_LOCK
    if (self->ring_lock) {
        PyThread_free_lock(self->ring_lock);
        self->ring_lock = NULL;
    }
#endif
    PyObject_GC_Del(self);
}

int UringApiRing_traverse(UringApiRing *self, visitproc visit, void *arg) {
    Py_VISIT(self->delivery_callback);
    Py_VISIT(self->delivery_exception_handler);
    Py_VISIT(self->nowait_error_handler);
    if (fd_table_traverse(self, visit, arg) < 0) {
        return -1;
    }
    return completion_fifo_traverse(&self->fill_wait, visit, arg);
}

int UringApiRing_clear(UringApiRing *self) {
    Py_CLEAR(self->delivery_callback);
    Py_CLEAR(self->delivery_exception_handler);
    Py_CLEAR(self->nowait_error_handler);
    send_all_clear_continuations(self);
    return 0;
}

PyObject *UringApiRing_close(UringApiRing *self, PyObject *Py_UNUSED(ignored)) {
    if (delivery_check_not_running(self) < 0) {
        return NULL;
    }
    Py_BEGIN_CRITICAL_SECTION(self);
    if (self->initialized) {
        io_uring_queue_exit(&self->ring);
        self->initialized = false;
    }
    self->receive_state = URING_API_RECEIVE_IDLE;
    self->delivery_stop_requested = false;
    self->delivery_active_workers = 0;
    self->next_buf_group = 1;
    UringApiRing_clear_free_buf_group_ids(self);
    self->setup_flags = 0;
    self->owner_thread_id = 0;
    Py_END_CRITICAL_SECTION();
    /* wake any host-side idle park after the ring is no longer open. */
    UringApiIdlePark_signal(&self->idle);
    Py_RETURN_NONE;
}

PyObject *UringApiRing_enter(UringApiRing *self, PyObject *Py_UNUSED(ignored)) {
    Py_INCREF(self);
    return (PyObject *)self;
}

PyObject *UringApiRing_exit(UringApiRing *self, PyObject *args) { return UringApiRing_close(self, NULL); }

static PyObject *UringApiRing_get_fd(UringApiRing *self, void *closure) {
    if (!self->initialized) {
        return PyLong_FromLong(-1);
    }
    return PyLong_FromLong(self->ring.ring_fd);
}

static PyObject *UringApiRing_get_features(UringApiRing *self, void *closure) {
    if (!self->initialized) {
        return PyLong_FromUnsignedLong(0);
    }
    return PyLong_FromUnsignedLong(self->ring.features);
}

static PyObject *UringApiRing_get_sq_entries(UringApiRing *self, void *closure) {
    if (!self->initialized) {
        return PyLong_FromUnsignedLong(0);
    }
    return PyLong_FromUnsignedLong(ring_sq_entries(self));
}

static PyObject *UringApiRing_get_cq_entries(UringApiRing *self, void *closure) {
    if (!self->initialized) {
        return PyLong_FromUnsignedLong(0);
    }
    return PyLong_FromUnsignedLong(ring_cq_entries(self));
}

static PyObject *UringApiRing_get_closed(UringApiRing *self, void *closure) {
    if (self->initialized) {
        Py_RETURN_FALSE;
    }
    Py_RETURN_TRUE;
}

static PyObject *UringApiRing_get_running(UringApiRing *self, void *closure) {
    if (self->receive_state == URING_API_RECEIVE_DELIVERING) {
        Py_RETURN_TRUE;
    }
    Py_RETURN_FALSE;
}

static PyObject *UringApiRing_pending_count(UringApiRing *self, PyObject *Py_UNUSED(ignored)) {
    return PyLong_FromUnsignedLong(ring_pending_count(self));
}

static PyObject *UringApiRing_get_auto_submit(UringApiRing *self, void *closure) {
    int enabled;

    (void)closure;
    Py_BEGIN_CRITICAL_SECTION(self);
    enabled = self->auto_submit;
    Py_END_CRITICAL_SECTION();
    if (enabled) {
        Py_RETURN_TRUE;
    }
    Py_RETURN_FALSE;
}

static int UringApiRing_set_auto_submit(UringApiRing *self, PyObject *value, void *closure) {
    int truth;

    (void)closure;
    if (value == NULL) {
        PyErr_SetString(PyExc_TypeError, "cannot delete auto_submit");
        return -1;
    }
    truth = PyObject_IsTrue(value);
    if (truth < 0) {
        return -1;
    }
    Py_BEGIN_CRITICAL_SECTION(self);
    self->auto_submit = truth != 0;
    Py_END_CRITICAL_SECTION();
    return 0;
}

static PyObject *UringApiRing_get_experimental_send_all_submit_next(UringApiRing *self, void *closure) {
    int enabled;

    (void)closure;
    Py_BEGIN_CRITICAL_SECTION(self);
    enabled = self->experimental_send_all_submit_next;
    Py_END_CRITICAL_SECTION();
    if (enabled) {
        Py_RETURN_TRUE;
    }
    Py_RETURN_FALSE;
}

static int UringApiRing_set_experimental_send_all_submit_next(UringApiRing *self, PyObject *value, void *closure) {
    int truth;

    (void)closure;
    if (value == NULL) {
        PyErr_SetString(PyExc_TypeError, "cannot delete experimental_send_all_submit_next");
        return -1;
    }
    truth = PyObject_IsTrue(value);
    if (truth < 0) {
        return -1;
    }
    Py_BEGIN_CRITICAL_SECTION(self);
    self->experimental_send_all_submit_next = truth != 0;
    Py_END_CRITICAL_SECTION();
    return 0;
}

static PyObject *UringApiRing_get_callback(UringApiRing *self, void *closure) {
    PyObject *callback;

    (void)closure;
    Py_BEGIN_CRITICAL_SECTION(self);
    callback = Py_XNewRef(self->delivery_callback);
    Py_END_CRITICAL_SECTION();
    if (!callback) {
        Py_RETURN_NONE;
    }
    return callback;
}

int UringApiRing_set_callback(UringApiRing *self, PyObject *value, void *closure) {
    PyObject *callback;
    PyObject *old_callback = NULL;
    int ret = 0;

    (void)closure;
    if (!value) {
        PyErr_SetString(PyExc_TypeError, "cannot delete callback");
        return -1;
    }
    if (value != Py_None && !PyCallable_Check(value)) {
        PyErr_SetString(PyExc_TypeError, "callback must be callable or None");
        return -1;
    }

    callback = value == Py_None ? NULL : Py_NewRef(value);
    Py_BEGIN_CRITICAL_SECTION(self);
    if (delivery_is_running_locked(self)) {
        PyErr_SetString(PyExc_RuntimeError, "cannot change callback while completion service is active");
        ret = -1;
    } else {
        old_callback = self->delivery_callback;
        self->delivery_callback = callback;
        callback = NULL;
    }
    Py_END_CRITICAL_SECTION();
    Py_XDECREF(callback);
    Py_XDECREF(old_callback);
    return ret;
}

static PyObject *UringApiRing_get_exception_handler(UringApiRing *self, void *closure) {
    PyObject *handler;

    (void)closure;
    Py_BEGIN_CRITICAL_SECTION(self);
    handler = Py_XNewRef(self->delivery_exception_handler);
    Py_END_CRITICAL_SECTION();
    if (!handler) {
        Py_RETURN_NONE;
    }
    return handler;
}

int UringApiRing_set_exception_handler(UringApiRing *self, PyObject *value, void *closure) {
    PyObject *handler;
    PyObject *old_handler = NULL;
    int ret = 0;

    (void)closure;
    if (!value) {
        PyErr_SetString(PyExc_TypeError, "cannot delete exception_handler");
        return -1;
    }
    if (value != Py_None && !PyCallable_Check(value)) {
        PyErr_SetString(PyExc_TypeError, "exception_handler must be callable or None");
        return -1;
    }

    handler = value == Py_None ? NULL : Py_NewRef(value);
    Py_BEGIN_CRITICAL_SECTION(self);
    old_handler = self->delivery_exception_handler;
    self->delivery_exception_handler = handler;
    handler = NULL;
    Py_END_CRITICAL_SECTION();
    Py_XDECREF(handler);
    Py_XDECREF(old_handler);
    return ret;
}

static PyObject *UringApiRing_get_nowait_error_handler(UringApiRing *self, void *closure) {
    PyObject *handler;

    (void)closure;
    Py_BEGIN_CRITICAL_SECTION(self);
    handler = Py_XNewRef(self->nowait_error_handler);
    Py_END_CRITICAL_SECTION();
    if (!handler) {
        Py_RETURN_NONE;
    }
    return handler;
}

int UringApiRing_set_nowait_error_handler(UringApiRing *self, PyObject *value, void *closure) {
    PyObject *handler;
    PyObject *old_handler = NULL;

    (void)closure;
    if (!value) {
        PyErr_SetString(PyExc_TypeError, "cannot delete nowait_error_handler");
        return -1;
    }
    if (value != Py_None && !PyCallable_Check(value)) {
        PyErr_SetString(PyExc_TypeError, "nowait_error_handler must be callable or None");
        return -1;
    }

    handler = value == Py_None ? NULL : Py_NewRef(value);
    Py_BEGIN_CRITICAL_SECTION(self);
    old_handler = self->nowait_error_handler;
    self->nowait_error_handler = handler;
    handler = NULL;
    Py_END_CRITICAL_SECTION();
    Py_XDECREF(handler);
    Py_XDECREF(old_handler);
    return 0;
}

/*
 * Flush prepared SQEs to the kernel. Returns the number of SQEs submitted
 * (may be 0). prepare_* methods only fill SQEs; call this (or wait/serve, which
 * flush first) to make work kernel-visible.
 */
PyObject *UringApiRing_submit(UringApiRing *self, PyObject *Py_UNUSED(ignored)) {
    int submitted = 0;
    int failed = 0;

    Py_BEGIN_CRITICAL_SECTION(self);
    if (ring_check_open(self) < 0) {
        failed = 1;
    } else if (ring_check_submit_thread(self, 1) < 0) {
        failed = 1;
    } else if (send_all_flush_continuations(self, 1, &submitted) < 0) {
        failed = 1;
    } else if (ring_flush_pending(self, &submitted) < 0) {
        failed = 1;
    }
    Py_END_CRITICAL_SECTION();

    if (failed) {
        return NULL;
    }
    return PyLong_FromLong(submitted);
}

static PyMethodDef UringApiRing_methods[] = {
    {"close", (PyCFunction)UringApiRing_close, METH_NOARGS, "Close the io_uring instance."},
    {"pending_count", (PyCFunction)UringApiRing_pending_count, METH_NOARGS,
     "Return the number of waitable Completions still in flight.\n\n"
     "Incremented when prepare takes the in-flight ref (SQE fill, or conflict\n"
     "FIFO enqueue); decremented when that ref is dropped (oneshot CQE packaged,\n"
     "or multishot / send_zc / send_all after the terminal CQE). Construct-only\n"
     "and ordinary nowait ops are not counted; nowait send_all is counted until\n"
     "the drain terminals. MORE shells do not add to the count."},
    {"submit", (PyCFunction)UringApiRing_submit, METH_NOARGS,
     "Flush prepared SQEs to the kernel. Returns the number of SQEs submitted "
     "(may be 0), including those flushed to make room while filling a parked "
     "send-all next-leg. prepare_* methods only fill SQEs; call submit() when "
     "you want them to run, or rely on wait()/serve_completions() which flush "
     "first when auto_submit is true (default). When auto_submit is true and "
     "the SQ is full, prepare also flushes to make room. submit() itself never "
     "raises SubmissionQueueFull: a parked send-all next-leg is filled, "
     "submitting already-prepared SQEs first if the SQ is full (SQPOLL may wait "
     "for a slot)."},
    {"serve_completions", (PyCFunction)UringApiRing_serve_completions, METH_NOARGS,
     "Serve completions until stop_serving is called."},
    {"stop_serving", (PyCFunction)UringApiRing_stop_serving, METH_NOARGS, "Ask completion workers to stop."},
    {"reset_serving", (PyCFunction)UringApiRing_reset_serving, METH_NOARGS, "Clear the completion service stop flag."},
    {"create_buf_group", _PyCFunction_CAST(UringApiRing_create_buf_group), URING_API_METH_KEYWORDS,
     "Create a provided-buffer group for multishot receive operations."},
    {"create_buf_view", _PyCFunction_CAST(UringApiRing_create_buf_view), URING_API_METH_KEYWORDS,
     "Create a read-only leased view into a buffer group slot."},
    {"construct_recv", _PyCFunction_CAST(UringApiRing_construct_recv), URING_API_METH_KEYWORDS,
     "Construct a recv Completion without reserving an SQE.\n\n"
     "Binds the buffer, fd, flags, and user_data so reverse links can be armed\n"
     "before ring.prepare(...). flags is MSG_* plus optional POLL_FIRST;\n"
     "bit 0 is also MSG_OOB and is applied as ioprio, not OOB.\n"
     "Does not make the recv kernel-visible."},
    {"prepare_recv", _PyCFunction_CAST(UringApiRing_prepare_recv), URING_API_METH_KEYWORDS,
     "Construct and prepare a recv operation (convenience for construct_recv + prepare)."},
    {"construct_recv_buf", _PyCFunction_CAST(UringApiRing_construct_recv_buf), URING_API_METH_KEYWORDS,
     "Construct a one-shot provided-buffer recv Completion without reserving an SQE."},
    {"prepare_recv_buf", _PyCFunction_CAST(UringApiRing_prepare_recv_buf), URING_API_METH_KEYWORDS,
     "Construct and prepare a provided-buffer recv (convenience for construct_recv_buf + prepare)."},
    {"construct_recv_multishot", _PyCFunction_CAST(UringApiRing_construct_recv_multishot), METH_FASTCALL,
     "Construct a multishot provided-buffer recv Completion without reserving an SQE."},
    {"prepare_recv_multishot", _PyCFunction_CAST(UringApiRing_prepare_recv_multishot), METH_FASTCALL,
     "Construct and prepare a multishot provided-buffer recv (convenience for construct_recv_multishot + prepare)."},
    {"construct_send", _PyCFunction_CAST(UringApiRing_construct_send), METH_FASTCALL,
     "Construct a send Completion without reserving an SQE.\n\n"
     "Positional only: fd, data, flags=0, user_data=None.\n"
     "Binds the buffer, fd, flags, and user_data so reverse links can be armed\n"
     "before ring.prepare(...). flags is MSG_* plus optional POLL_FIRST;\n"
     "bit 0 is also MSG_OOB and is applied as ioprio, not OOB.\n"
     "Does not make the send kernel-visible."},
    {"construct_send_all", _PyCFunction_CAST(UringApiRing_construct_send_all), METH_FASTCALL,
     "Construct a send-all Completion without reserving an SQE.\n\n"
     "Positional only: fd, data, flags=0, user_data=None.\n"
     "One waitable that drains data with repeated send SQEs until the buffer is\n"
     "exhausted. Partial CQEs are consumed internally. Success res is total bytes."},
    {"construct_send_zc", _PyCFunction_CAST(UringApiRing_construct_send_zc), METH_FASTCALL,
     "Construct a zero-copy send Completion without reserving an SQE.\n\n"
     "Positional only: fd, data, flags=0, zc_flags=0, user_data=None."},
    {"prepare", _PyCFunction_CAST(UringApiRing_prepare), METH_FASTCALL,
     "Accept constructed Completions: fill SQEs, or park on a send-all-busy\n"
     "fd's conflict FIFO.\n\n"
     "Positional only: a Completion or a sequence of Completions.\n"
     "Accepts any constructed Completion, including cancel and poll_remove.\n"
     "Returns the number accepted (SQE fills and FIFO parks). Does not submit;\n"
     "wait()/submit() flush (or get_sqe flushes when auto_submit is true and\n"
     "the SQ is full). Completion.prepared is true only after an SQE fill.\n"
     "If auto_submit is false and the SQ is full, raises SubmissionQueueFull.\n"
     "On error the prefix of the sequence may already be accepted."},
    {"prepare_send", _PyCFunction_CAST(UringApiRing_prepare_send), METH_FASTCALL,
     "Construct and prepare a send operation (convenience for construct_send + prepare)."},
    {"prepare_send_all", _PyCFunction_CAST(UringApiRing_prepare_send_all), METH_FASTCALL,
     "Construct and prepare a send-all (convenience for construct_send_all + prepare)."},
    {"prepare_send_zc", _PyCFunction_CAST(UringApiRing_prepare_send_zc), METH_FASTCALL,
     "Construct and prepare a zero-copy send (convenience for construct_send_zc + prepare)."},
    {"construct_recvmsg", _PyCFunction_CAST(UringApiRing_construct_recvmsg), URING_API_METH_KEYWORDS,
     "Construct a recvmsg Completion without reserving an SQE."},
    {"prepare_recvmsg", _PyCFunction_CAST(UringApiRing_prepare_recvmsg), URING_API_METH_KEYWORDS,
     "Construct and prepare a recvmsg (convenience for construct_recvmsg + prepare)."},
    {"construct_sendto", _PyCFunction_CAST(UringApiRing_construct_sendto), URING_API_METH_KEYWORDS,
     "Construct a sendto Completion without reserving an SQE."},
    {"prepare_sendto", _PyCFunction_CAST(UringApiRing_prepare_sendto), URING_API_METH_KEYWORDS,
     "Construct and prepare a sendto (convenience for construct_sendto + prepare)."},
    {"construct_sendmsg", _PyCFunction_CAST(UringApiRing_construct_sendmsg), URING_API_METH_KEYWORDS,
     "Construct a sendmsg Completion without reserving an SQE."},
    {"prepare_sendmsg", _PyCFunction_CAST(UringApiRing_prepare_sendmsg), URING_API_METH_KEYWORDS,
     "Construct and prepare a sendmsg (convenience for construct_sendmsg + prepare)."},
    {"construct_sendmsg_zc", _PyCFunction_CAST(UringApiRing_construct_sendmsg_zc), URING_API_METH_KEYWORDS,
     "Construct a zero-copy sendmsg Completion without reserving an SQE."},
    {"prepare_sendmsg_zc", _PyCFunction_CAST(UringApiRing_prepare_sendmsg_zc), URING_API_METH_KEYWORDS,
     "Construct and prepare a zero-copy sendmsg (convenience for construct_sendmsg_zc + prepare)."},
    {"construct_accept", _PyCFunction_CAST(UringApiRing_construct_accept), METH_FASTCALL,
     "Construct an accept Completion without reserving an SQE. Positional only: fd, flags=0, user_data=None."},
    {"prepare_accept", _PyCFunction_CAST(UringApiRing_prepare_accept), METH_FASTCALL,
     "Construct and prepare an accept (convenience for construct_accept + prepare)."},
    {"construct_accept_multishot", _PyCFunction_CAST(UringApiRing_construct_accept_multishot), METH_FASTCALL,
     "Construct a multishot accept Completion without reserving an SQE."},
    {"prepare_accept_multishot", _PyCFunction_CAST(UringApiRing_prepare_accept_multishot), METH_FASTCALL,
     "Construct and prepare a multishot accept (convenience for construct_accept_multishot + prepare).\n\n"
     "Positional args: fd, flags=0, user_data=None.\n"
     "Seed completion.sequence after construct if the first leg is not 0."},
    {"construct_connect", _PyCFunction_CAST(UringApiRing_construct_connect), URING_API_METH_KEYWORDS,
     "Construct a connect Completion without reserving an SQE."},
    {"prepare_connect", _PyCFunction_CAST(UringApiRing_prepare_connect), URING_API_METH_KEYWORDS,
     "Construct and prepare a connect (convenience for construct_connect + prepare)."},
    {"construct_poll", _PyCFunction_CAST(UringApiRing_construct_poll), METH_FASTCALL,
     "Construct a one-shot poll Completion without reserving an SQE. Positional only: fd, mask, user_data=None."},
    {"prepare_poll", _PyCFunction_CAST(UringApiRing_prepare_poll), METH_FASTCALL,
     "Construct and prepare a one-shot poll (convenience for construct_poll + prepare)."},
    {"construct_poll_multishot", _PyCFunction_CAST(UringApiRing_construct_poll_multishot), METH_FASTCALL,
     "Construct a multishot poll Completion without reserving an SQE. Positional only: fd, mask, user_data=None."},
    {"prepare_poll_multishot", _PyCFunction_CAST(UringApiRing_prepare_poll_multishot), METH_FASTCALL,
     "Construct and prepare a multishot poll (convenience for construct_poll_multishot + prepare)."},
    {"construct_poll_remove", _PyCFunction_CAST(UringApiRing_construct_poll_remove), METH_FASTCALL,
     "Construct a poll_remove Completion without reserving an SQE.\n\n"
     "Positional only: completion, user_data=None. The target poll identity\n"
     "need not be prepared yet; prepare the poll first if you want one flush\n"
     "to publish both in order."},
    {"prepare_poll_remove", _PyCFunction_CAST(UringApiRing_prepare_poll_remove), METH_FASTCALL,
     "Construct and prepare a poll_remove (convenience for construct_poll_remove + prepare)."},
    {"construct_poll_remove_nowait", _PyCFunction_CAST(UringApiRing_construct_poll_remove_nowait), METH_FASTCALL,
     "Construct a nowait poll_remove Completion (temporary hold; prepare stamps a tagged SQE)."},
    {"prepare_poll_remove_nowait", _PyCFunction_CAST(UringApiRing_prepare_poll_remove_nowait), METH_FASTCALL,
     "Construct, prepare, and drop a nowait poll_remove. Returns None. Positional only."},
    {"construct_cancel", _PyCFunction_CAST(UringApiRing_construct_cancel), METH_FASTCALL,
     "Construct a cancel Completion without reserving an SQE.\n\n"
     "Positional only: completion, user_data=None. The target identity is the\n"
     "constructed Completion; it need not be prepared or kernel-visible yet."},
    {"prepare_cancel", _PyCFunction_CAST(UringApiRing_prepare_cancel), METH_FASTCALL,
     "Construct and prepare a cancel (convenience for construct_cancel + prepare)."},
    {"construct_cancel_nowait", _PyCFunction_CAST(UringApiRing_construct_cancel_nowait), METH_FASTCALL,
     "Construct a nowait cancel Completion (temporary hold; prepare stamps a tagged SQE)."},
    {"prepare_cancel_nowait", _PyCFunction_CAST(UringApiRing_prepare_cancel_nowait), METH_FASTCALL,
     "Construct, prepare, and drop a nowait cancel. Returns None. Positional only. "
     "Lost-race acks (-ENOENT/-EALREADY) are silent; other res < 0 invoke nowait_error_handler."},
    {"construct_shutdown", _PyCFunction_CAST(UringApiRing_construct_shutdown), METH_FASTCALL,
     "Construct a shutdown Completion without reserving an SQE. Positional only: fd, how, user_data=None."},
    {"prepare_shutdown", _PyCFunction_CAST(UringApiRing_prepare_shutdown), METH_FASTCALL,
     "Construct and prepare a shutdown (convenience for construct_shutdown + prepare)."},
    {"construct_shutdown_nowait", _PyCFunction_CAST(UringApiRing_construct_shutdown_nowait), METH_FASTCALL,
     "Construct a nowait shutdown Completion (temporary hold; prepare stamps a tagged SQE)."},
    {"prepare_shutdown_nowait", _PyCFunction_CAST(UringApiRing_prepare_shutdown_nowait), METH_FASTCALL,
     "Construct, prepare, and drop a nowait shutdown. Returns None. Positional only."},
    {"construct_close", _PyCFunction_CAST(UringApiRing_construct_close), METH_FASTCALL,
     "Construct a close Completion without reserving an SQE. Positional only: fd, user_data=None."},
    {"prepare_close", _PyCFunction_CAST(UringApiRing_prepare_close), METH_FASTCALL,
     "Construct and prepare a close (convenience for construct_close + prepare)."},
    {"construct_close_nowait", _PyCFunction_CAST(UringApiRing_construct_close_nowait), METH_FASTCALL,
     "Construct a nowait close Completion (temporary hold; prepare stamps a tagged SQE)."},
    {"prepare_close_nowait", _PyCFunction_CAST(UringApiRing_prepare_close_nowait), METH_FASTCALL,
     "Construct, prepare, and drop a nowait close. Returns None. "
     "Positional only. When IORING_FEAT_CQE_SKIP is available, sets IOSQE_CQE_SKIP_SUCCESS so successful "
     "closes post no CQE."},
    {"construct_read", _PyCFunction_CAST(UringApiRing_construct_read), URING_API_METH_KEYWORDS,
     "Construct a file read Completion without reserving an SQE."},
    {"prepare_read", _PyCFunction_CAST(UringApiRing_prepare_read), URING_API_METH_KEYWORDS,
     "Construct and prepare a file read (convenience for construct_read + prepare)."},
    {"construct_write", _PyCFunction_CAST(UringApiRing_construct_write), URING_API_METH_KEYWORDS,
     "Construct a file write Completion without reserving an SQE."},
    {"prepare_write", _PyCFunction_CAST(UringApiRing_prepare_write), URING_API_METH_KEYWORDS,
     "Construct and prepare a file write (convenience for construct_write + prepare)."},
    {"construct_openat", _PyCFunction_CAST(UringApiRing_construct_openat), URING_API_METH_KEYWORDS,
     "Construct an openat Completion without reserving an SQE."},
    {"prepare_openat", _PyCFunction_CAST(UringApiRing_prepare_openat), URING_API_METH_KEYWORDS,
     "Construct and prepare an openat (convenience for construct_openat + prepare)."},
    {"construct_statx", _PyCFunction_CAST(UringApiRing_construct_statx), URING_API_METH_KEYWORDS,
     "Construct a statx Completion without reserving an SQE."},
    {"prepare_statx", _PyCFunction_CAST(UringApiRing_prepare_statx), URING_API_METH_KEYWORDS,
     "Construct and prepare a statx (convenience for construct_statx + prepare)."},
    {"construct_statx_fdsize", _PyCFunction_CAST(UringApiRing_construct_statx_fdsize), URING_API_METH_KEYWORDS,
     "Construct an fd-only statx Completion without reserving an SQE."},
    {"prepare_statx_fdsize", _PyCFunction_CAST(UringApiRing_prepare_statx_fdsize), URING_API_METH_KEYWORDS,
     "Construct and prepare fd-only statx (convenience for construct_statx_fdsize + prepare)."},
    {"construct_socket", _PyCFunction_CAST(UringApiRing_construct_socket), URING_API_METH_KEYWORDS,
     "Construct a socket-creation Completion without reserving an SQE."},
    {"prepare_socket", _PyCFunction_CAST(UringApiRing_prepare_socket), URING_API_METH_KEYWORDS,
     "Construct and prepare a socket creation (convenience for construct_socket + prepare)."},
    {"break_wait", (PyCFunction)UringApiRing_break_wait, METH_NOARGS,
     "Open the wait_idle park immediately. When completion service is idle, also best-effort submit one internal NOP "
     "(no Completion object; tagged wake user_data) to wake wait() on an empty CQ (skipped while serve workers own "
     "reaping). NOP failure still succeeds after signalling."},
    {"wait_idle", _PyCFunction_CAST(UringApiRing_wait_idle), URING_API_METH_KEYWORDS,
     "Host-side park until break_wait/close or timeout. Returns True if signalled, False on timeout. "
     "At most one concurrent waiter; many break_wait callers may signal the same park."},
    {"wait", _PyCFunction_CAST(UringApiRing_wait), URING_API_METH_KEYWORDS,
     "If auto_submit is on, flush prepared SQEs when this thread may submit "
     "(no-op if SQ empty), then wait for ready "
     "completions with the given timeout. With no callback, returns a list (possibly empty on "
     "timeout/break_wait). With a delivery callback, invokes it for non-empty user batches and "
     "returns None; empty batches skip the callback."},
    {"__enter__", (PyCFunction)UringApiRing_enter, METH_NOARGS, NULL},
    {"__exit__", (PyCFunction)UringApiRing_exit, METH_VARARGS, NULL},
    {NULL, NULL, 0, NULL}};

static PyGetSetDef UringApiRing_getset[] = {
    {"fd", (getter)UringApiRing_get_fd, NULL, NULL, NULL},
    {"features", (getter)UringApiRing_get_features, NULL, NULL, NULL},
    {"sq_entries", (getter)UringApiRing_get_sq_entries, NULL, NULL, NULL},
    {"cq_entries", (getter)UringApiRing_get_cq_entries, NULL, NULL, NULL},
    {"closed", (getter)UringApiRing_get_closed, NULL, NULL, NULL},
    {"running", (getter)UringApiRing_get_running, NULL, NULL, NULL},
    {"auto_submit", (getter)UringApiRing_get_auto_submit, (setter)UringApiRing_set_auto_submit,
     "If true (default), prepare flushes when the SQ is full, and wait() / "
     "serve_completions() flush prepared SQEs before waiting. If false, a full "
     "SQ raises SubmissionQueueFull and wait/serve do not submit.",
     NULL},
    {"experimental_send_all_submit_next", (getter)UringApiRing_get_experimental_send_all_submit_next,
     (setter)UringApiRing_set_experimental_send_all_submit_next,
     "Experimental. If true, a send_all next-leg SQE is io_uring_submit'd as soon as it is filled "
     "(when this thread may submit and auto_submit is on). If false (default), the SQE stays in "
     "the SQ until wait/submit or SQ-full, like ordinary prepare. For measuring delayed vs "
     "immediate next-leg enter cost.",
     NULL},
    {"callback", (getter)UringApiRing_get_callback, (setter)UringApiRing_set_callback, NULL, NULL},
    {"exception_handler", (getter)UringApiRing_get_exception_handler, (setter)UringApiRing_set_exception_handler, NULL,
     NULL},
    {"nowait_error_handler", (getter)UringApiRing_get_nowait_error_handler,
     (setter)UringApiRing_set_nowait_error_handler,
     "Optional hook(context) when a nowait CQE fails (res < 0). Cancel -ENOENT/-EALREADY are not "
     "failures. Context keys: message, ring, res, flags, kind (COMPLETION_KIND_*), fd (advisory int or "
     "None). Invoked after CQ drain (not under the drain lock). Must not re-enter ring wait/serve. If "
     "the hook raises, exception_handler is invoked.",
     NULL},
    {NULL, NULL, NULL, NULL, NULL}};

PyTypeObject UringApiRing_Type = {
    PyVarObject_HEAD_INIT(NULL, 0).tp_name = "_uring_api.Ring",
    .tp_basicsize = sizeof(UringApiRing),
    .tp_dealloc = (destructor)UringApiRing_dealloc,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,
    .tp_traverse = (traverseproc)UringApiRing_traverse,
    .tp_clear = (inquiry)UringApiRing_clear,
    .tp_doc = "io_uring ring",
    .tp_methods = UringApiRing_methods,
    .tp_getset = UringApiRing_getset,
    .tp_init = (initproc)UringApiRing_init,
    .tp_new = UringApiRing_new,
};
