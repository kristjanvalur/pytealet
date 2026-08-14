/*
 * Shared private helpers for the _uring_api extension.
 */

#include "uring_api_core.h"

#include <assert.h>
#include <time.h>
#include <unistd.h>

#include "uring_api_statx_layout.h"

/* if SQPOLL never frees a slot (poller stuck/dead), fail rather than hang forever */
#define URING_API_SQE_WAIT_TIMEOUT_SEC 5
/* backoff when sqring_wait is unavailable (EINVAL) so we do not busy-spin */
#define URING_API_SQE_WAIT_EINVAL_BACKOFF_US 1000

int ring_type_check(PyObject *ring) {
    if (!PyObject_TypeCheck(ring, &UringApiRing_Type)) {
        PyErr_SetString(PyExc_TypeError, "ring must be an _uring_api.Ring instance");
        return 0;
    }
    return 1;
}

int normalize_ret_errno(int ret) {
    if (ret < 0) {
        return -ret;
    }
    if (errno) {
        return errno;
    }
    return EINVAL;
}

PyObject *liburing_version_string(void) {
    return PyUnicode_FromFormat("%d.%d", IO_URING_VERSION_MAJOR, IO_URING_VERSION_MINOR);
}

PyObject *liburing_version_info(void) { return Py_BuildValue("(ii)", IO_URING_VERSION_MAJOR, IO_URING_VERSION_MINOR); }

int module_add_uint64_constant(PyObject *module, const char *name, unsigned long long value) {
    PyObject *value_obj = PyLong_FromUnsignedLongLong(value);
    if (!value_obj) {
        return -1;
    }
    if (PyModule_AddObject(module, name, value_obj) < 0) {
        Py_DECREF(value_obj);
        return -1;
    }
    return 0;
}

int module_add_setup_flag_constants(PyObject *module) {
    if (module_add_uint64_constant(module, "IORING_SETUP_SQPOLL", IORING_SETUP_SQPOLL) < 0 ||
        module_add_uint64_constant(module, "IORING_SETUP_CQSIZE", IORING_SETUP_CQSIZE) < 0 ||
        module_add_uint64_constant(module, "IORING_SETUP_CLAMP", IORING_SETUP_CLAMP) < 0 ||
        module_add_uint64_constant(module, "IORING_SETUP_COOP_TASKRUN", IORING_SETUP_COOP_TASKRUN) < 0 ||
        module_add_uint64_constant(module, "IORING_SETUP_TASKRUN_FLAG", IORING_SETUP_TASKRUN_FLAG) < 0 ||
        module_add_uint64_constant(module, "IORING_SETUP_SINGLE_ISSUER", IORING_SETUP_SINGLE_ISSUER) < 0 ||
        module_add_uint64_constant(module, "IORING_SETUP_DEFER_TASKRUN", IORING_SETUP_DEFER_TASKRUN) < 0) {
        return -1;
    }
    return 0;
}

int module_add_cqe_flag_constants(PyObject *module) {
    if (module_add_uint64_constant(module, "IORING_CQE_F_MORE", IORING_CQE_F_MORE) < 0 ||
        module_add_uint64_constant(module, "IORING_CQE_F_NOTIF", IORING_CQE_F_NOTIF) < 0) {
        return -1;
    }
    return 0;
}

int module_add_recvsend_flag_constants(PyObject *module) {
    if (module_add_uint64_constant(module, "IORING_SEND_ZC_REPORT_USAGE", IORING_SEND_ZC_REPORT_USAGE) < 0 ||
        module_add_uint64_constant(module, "IORING_NOTIF_USAGE_ZC_COPIED", IORING_NOTIF_USAGE_ZC_COPIED) < 0) {
        return -1;
    }
    return 0;
}

int module_add_completion_kind_constants(PyObject *module) {
    if (PyModule_AddIntConstant(module, "COMPLETION_KIND_RECV", URING_API_PENDING_RECV) < 0 ||
        PyModule_AddIntConstant(module, "COMPLETION_KIND_RECV_MULTISHOT", URING_API_PENDING_RECV_MULTISHOT) < 0 ||
        PyModule_AddIntConstant(module, "COMPLETION_KIND_SEND", URING_API_PENDING_SEND) < 0 ||
        PyModule_AddIntConstant(module, "COMPLETION_KIND_WAKE", URING_API_PENDING_WAKE) < 0 ||
        PyModule_AddIntConstant(module, "COMPLETION_KIND_SENDTO", URING_API_PENDING_SENDTO) < 0 ||
        PyModule_AddIntConstant(module, "COMPLETION_KIND_RECVMSG", URING_API_PENDING_RECVMSG) < 0 ||
        PyModule_AddIntConstant(module, "COMPLETION_KIND_ACCEPT", URING_API_PENDING_ACCEPT) < 0 ||
        PyModule_AddIntConstant(module, "COMPLETION_KIND_CONNECT", URING_API_PENDING_CONNECT) < 0 ||
        PyModule_AddIntConstant(module, "COMPLETION_KIND_CANCEL", URING_API_PENDING_CANCEL) < 0 ||
        PyModule_AddIntConstant(module, "COMPLETION_KIND_SHUTDOWN", URING_API_PENDING_SHUTDOWN) < 0 ||
        PyModule_AddIntConstant(module, "COMPLETION_KIND_CLOSE", URING_API_PENDING_CLOSE) < 0 ||
        PyModule_AddIntConstant(module, "COMPLETION_KIND_SENDMSG", URING_API_PENDING_SENDMSG) < 0 ||
        PyModule_AddIntConstant(module, "COMPLETION_KIND_SOCKET", URING_API_PENDING_SOCKET) < 0 ||
        PyModule_AddIntConstant(module, "COMPLETION_KIND_SEND_ZC", URING_API_PENDING_SEND_ZC) < 0 ||
        PyModule_AddIntConstant(module, "COMPLETION_KIND_SENDMSG_ZC", URING_API_PENDING_SENDMSG_ZC) < 0 ||
        PyModule_AddIntConstant(module, "COMPLETION_KIND_RECV_BUF", URING_API_PENDING_RECV_BUF) < 0 ||
        PyModule_AddIntConstant(module, "COMPLETION_KIND_POLL", URING_API_PENDING_POLL) < 0 ||
        PyModule_AddIntConstant(module, "COMPLETION_KIND_POLL_MULTISHOT", URING_API_PENDING_POLL_MULTISHOT) < 0 ||
        PyModule_AddIntConstant(module, "COMPLETION_KIND_POLL_REMOVE", URING_API_PENDING_POLL_REMOVE) < 0 ||
        PyModule_AddIntConstant(module, "COMPLETION_KIND_READ", URING_API_PENDING_READ) < 0 ||
        PyModule_AddIntConstant(module, "COMPLETION_KIND_WRITE", URING_API_PENDING_WRITE) < 0 ||
        PyModule_AddIntConstant(module, "COMPLETION_KIND_OPENAT", URING_API_PENDING_OPENAT) < 0 ||
        PyModule_AddIntConstant(module, "COMPLETION_KIND_STATX", URING_API_PENDING_STATX) < 0 ||
        PyModule_AddIntConstant(module, "COMPLETION_KIND_STATX_FDSIZE", URING_API_PENDING_STATX_FDSIZE) < 0) {
        return -1;
    }
    return 0;
}

int module_add_statx_constants(PyObject *module) {
    if (PyModule_AddIntConstant(module, "AT_FDCWD", -100) < 0 ||
        PyModule_AddIntConstant(module, "AT_EMPTY_PATH", 0x1000) < 0 ||
        PyModule_AddIntConstant(module, "STATX_BASIC_STATS", 0x000007ff) < 0 ||
        PyModule_AddIntConstant(module, "STATX_SIZE", 0x00000200) < 0 ||
        PyModule_AddIntConstant(module, "STATX_BUFFER_SIZE", 256) < 0 ||
        PyModule_AddIntConstant(module, "STATX_STX_SIZE_OFFSET", URING_API_STATX_STX_SIZE_OFFSET) < 0) {
        return -1;
    }
    return 0;
}

void sqe_set_completion(UringApiRing *self, struct io_uring_sqe *sqe, PyObject *completion) {
    uintptr_t ptr = (uintptr_t)completion;

    (void)self;
    /* Completion* must keep bits 1:0 clear so it cannot collide with special tags. */
    assert((ptr & (uintptr_t)URING_API_UD_TAG_MASK) == 0);
    io_uring_sqe_set_data64(sqe, (unsigned long long)ptr);
    ((UringApiCompletion *)completion)->prepared = true;
}

UringApiCompletion *cqe_get_completion(UringApiRing *self, struct io_uring_cqe *cqe) {
    unsigned long long user_data = io_uring_cqe_get_data64(cqe);

    (void)self;
    assert(!uring_api_ud_is_special(user_data));
    return (UringApiCompletion *)(uintptr_t)user_data;
}

unsigned int ring_sq_entries(UringApiRing *self) { return self->ring.sq.ring_entries; }

unsigned int ring_cq_entries(UringApiRing *self) { return self->ring.cq.ring_entries; }

static int dict_set_owned(PyObject *dict, const char *key, PyObject *value) {
    int ret;
    if (!value) {
        return -1;
    }
    ret = PyDict_SetItemString(dict, key, value);
    Py_DECREF(value);
    return ret;
}

int parse_entries_flags(PyObject *args, PyObject *kwargs, unsigned int default_entries, unsigned int *entries,
                        unsigned int *flags) {
    static char *keywords[] = {"entries", "flags", NULL};
    unsigned long entries_value = default_entries;
    unsigned long flags_value = 0;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|kk", keywords, &entries_value, &flags_value)) {
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
    *entries = (unsigned int)entries_value;
    *flags = (unsigned int)flags_value;
    return 0;
}

static int fd_socket_family(int fd, int *family) {
    struct sockaddr_storage storage;
    socklen_t storage_len = sizeof(storage);

    memset(&storage, 0, sizeof(storage));
    if (getsockname(fd, (struct sockaddr *)&storage, &storage_len) < 0) {
        PyErr_SetFromErrno(PyExc_OSError);
        return -1;
    }
    *family = storage.ss_family;
    return 0;
}

static int parse_port(PyObject *value, in_port_t *port) {
    long port_value = PyLong_AsLong(value);
    if (port_value == -1 && PyErr_Occurred()) {
        return -1;
    }
    if (port_value < 0 || port_value > 65535) {
        PyErr_SetString(PyExc_ValueError, "port must be between 0 and 65535");
        return -1;
    }
    *port = htons((in_port_t)port_value);
    return 0;
}

int parse_numeric_sockaddr(int fd, PyObject *address, struct sockaddr_storage *storage, socklen_t *addrlen) {
    int family;
    PyObject *host_obj;
    PyObject *port_obj;
    const char *host;

    if (fd_socket_family(fd, &family) < 0) {
        return -1;
    }
    memset(storage, 0, sizeof(*storage));

    if (family == AF_INET) {
        struct sockaddr_in *addr = (struct sockaddr_in *)storage;
        if (!PyTuple_Check(address) || PyTuple_GET_SIZE(address) != 2) {
            PyErr_SetString(PyExc_TypeError, "AF_INET address must be a (host, port) tuple");
            return -1;
        }
        host_obj = PyTuple_GET_ITEM(address, 0);
        port_obj = PyTuple_GET_ITEM(address, 1);
        host = PyUnicode_AsUTF8(host_obj);
        if (!host) {
            return -1;
        }
        addr->sin_family = AF_INET;
        if (parse_port(port_obj, &addr->sin_port) < 0) {
            return -1;
        }
        if (inet_pton(AF_INET, host, &addr->sin_addr) != 1) {
            PyErr_SetString(PyExc_ValueError, "AF_INET host must be a numeric address");
            return -1;
        }
        *addrlen = sizeof(*addr);
        return 0;
    }

    if (family == AF_INET6) {
        struct sockaddr_in6 *addr = (struct sockaddr_in6 *)storage;
        Py_ssize_t tuple_size;
        unsigned long flowinfo = 0;
        unsigned long scope_id = 0;
        if (!PyTuple_Check(address)) {
            PyErr_SetString(PyExc_TypeError, "AF_INET6 address must be a (host, port[, flowinfo[, scope_id]]) tuple");
            return -1;
        }
        tuple_size = PyTuple_GET_SIZE(address);
        if (tuple_size < 2 || tuple_size > 4) {
            PyErr_SetString(PyExc_TypeError, "AF_INET6 address must be a (host, port[, flowinfo[, scope_id]]) tuple");
            return -1;
        }
        host_obj = PyTuple_GET_ITEM(address, 0);
        port_obj = PyTuple_GET_ITEM(address, 1);
        host = PyUnicode_AsUTF8(host_obj);
        if (!host) {
            return -1;
        }
        if (tuple_size >= 3) {
            flowinfo = PyLong_AsUnsignedLong(PyTuple_GET_ITEM(address, 2));
            if (flowinfo == (unsigned long)-1 && PyErr_Occurred()) {
                return -1;
            }
        }
        if (tuple_size >= 4) {
            scope_id = PyLong_AsUnsignedLong(PyTuple_GET_ITEM(address, 3));
            if (scope_id == (unsigned long)-1 && PyErr_Occurred()) {
                return -1;
            }
        }
        if (flowinfo > UINT32_MAX || scope_id > UINT32_MAX) {
            PyErr_SetString(PyExc_ValueError, "flowinfo and scope_id must fit in uint32_t");
            return -1;
        }
        addr->sin6_family = AF_INET6;
        if (parse_port(port_obj, &addr->sin6_port) < 0) {
            return -1;
        }
        addr->sin6_flowinfo = htonl((uint32_t)flowinfo);
        addr->sin6_scope_id = (uint32_t)scope_id;
        if (inet_pton(AF_INET6, host, &addr->sin6_addr) != 1) {
            PyErr_SetString(PyExc_ValueError, "AF_INET6 host must be a numeric address");
            return -1;
        }
        *addrlen = sizeof(*addr);
        return 0;
    }

    PyErr_SetString(PyExc_NotImplementedError, "only AF_INET and AF_INET6 socket addresses are supported");
    return -1;
}

int ring_check_open(UringApiRing *self) {
    if (!self->initialized) {
        PyErr_SetString(PyExc_RuntimeError, "ring is closed");
        return -1;
    }
    return 0;
}

static unsigned long long ring_current_thread_id(void) { return (unsigned long long)PyThread_get_thread_ident(); }

static int ring_check_owner_thread(UringApiRing *self, const char *error_message, int raise_on_error) {
    unsigned long long current_thread_id;
    unsigned long long stored;

    current_thread_id = ring_current_thread_id();
    stored = self->owner_thread_id;
    if (stored == 0) {
        /*
         * Quiet probes (raise_on_error == 0) must not claim ownership — a waiter
         * or completion worker probing "may I flush?" would latch SINGLE_ISSUER /
         * DEFER_TASKRUN and steal the ring from the real issuer. Only real
         * prepare/submit paths (raise_on_error != 0) establish the owner.
         */
        if (!raise_on_error) {
            return -1;
        }
        /* races on the first assignment are acceptable; later calls still catch misuse. */
        self->owner_thread_id = current_thread_id;
        return 0;
    }
    if (stored != current_thread_id) {
        if (raise_on_error) {
            PyErr_SetString(PyExc_RuntimeError, error_message);
        }
        return -1;
    }
    return 0;
}

int ring_check_submit_thread(UringApiRing *self, int raise_on_error) {
    if (self->setup_flags & IORING_SETUP_DEFER_TASKRUN) {
        return ring_check_owner_thread(
            self, "ring was created with IORING_SETUP_DEFER_TASKRUN; submissions and completions must run on one "
                  "thread",
            raise_on_error);
    }
    if (self->setup_flags & IORING_SETUP_SINGLE_ISSUER) {
        return ring_check_owner_thread(
            self, "ring was created with IORING_SETUP_SINGLE_ISSUER; submissions must come from one thread",
            raise_on_error);
    }
    return 0;
}

int ring_check_client_thread(UringApiRing *self) {
    if (!(self->setup_flags & IORING_SETUP_DEFER_TASKRUN)) {
        return 0;
    }
    return ring_check_owner_thread(
        self, "ring was created with IORING_SETUP_DEFER_TASKRUN; submissions and completions must run on one thread",
        1);
}

int ring_flush_pending(UringApiRing *self, int *submitted_out) {
    int ret;

    /* avoid io_uring_enter when there is nothing prepared */
    if (io_uring_sq_ready(&self->ring) == 0) {
        if (submitted_out) {
            *submitted_out = 0;
        }
        return 0;
    }

    errno = 0;
    ret = io_uring_submit(&self->ring);

    if (ret < 0) {
        int errnum = normalize_ret_errno(ret);
        errno = errnum;
        PyErr_SetFromErrno(PyExc_OSError);
        return -1;
    }
    if (submitted_out) {
        *submitted_out = ret;
    }
    return 0;
}

int submit_one(UringApiRing *self) {
    int submitted = 0;

    if (ring_flush_pending(self, &submitted) < 0) {
        return -1;
    }
    if (submitted == 0) {
        PyErr_SetString(PyExc_RuntimeError, "io_uring_submit submitted no operations");
        return -1;
    }
    return 0;
}

int receive_wait_begin(UringApiRing *self, bool from_delivery_thread) {
    int ret = 0;

    Py_BEGIN_CRITICAL_SECTION(self);
    if (from_delivery_thread) {
        if (self->receive_state != URING_API_RECEIVE_DELIVERING) {
            PyErr_SetString(PyExc_RuntimeError, "completion service is not active");
            ret = -1;
        }
    } else if (self->receive_state == URING_API_RECEIVE_DELIVERING) {
        PyErr_SetString(PyExc_RuntimeError, "completion service is active");
        ret = -1;
    } else if (self->receive_state != URING_API_RECEIVE_IDLE) {
        PyErr_SetString(PyExc_RuntimeError, "another wait is already active");
        ret = -1;
    } else {
        self->receive_state = URING_API_RECEIVE_WAITING;
    }
    Py_END_CRITICAL_SECTION();
    return ret;
}

void receive_wait_end(UringApiRing *self, bool from_delivery_thread) {
    if (from_delivery_thread) {
        return;
    }

    Py_BEGIN_CRITICAL_SECTION(self);
    self->receive_state = URING_API_RECEIVE_IDLE;
    Py_END_CRITICAL_SECTION();
}

bool delivery_is_running_locked(UringApiRing *self) { return self->receive_state == URING_API_RECEIVE_DELIVERING; }

int delivery_check_not_running(UringApiRing *self) {
    int ret = 0;

    Py_BEGIN_CRITICAL_SECTION(self);
    if (delivery_is_running_locked(self)) {
        PyErr_SetString(PyExc_RuntimeError, "completion service is active");
        ret = -1;
    }
    Py_END_CRITICAL_SECTION();
    return ret;
}

void delivery_mark_exited(UringApiRing *self) {
    Py_BEGIN_CRITICAL_SECTION(self);
    if (self->delivery_active_workers > 0) {
        self->delivery_active_workers--;
    }
    if (self->delivery_active_workers == 0 && self->receive_state == URING_API_RECEIVE_DELIVERING) {
        self->receive_state = URING_API_RECEIVE_IDLE;
    }
    Py_END_CRITICAL_SECTION();
}

static int64_t monotonic_ms(void) {
    struct timespec ts;

    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
        return -1;
    }
    return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

static void set_sqe_slot_stuck_error(void) {
    PyErr_SetString(PyExc_RuntimeError, "failed to obtain an io_uring SQE slot after flushing "
                                        "(submission queue stuck; with IORING_SETUP_SQPOLL the "
                                        "poller may be dead or hung)");
}

/*
 * Reserve an SQE. If the SQ is full of prepared / not-yet-consumed entries,
 * flush then retry. With SQPOLL the poller may lag: after the second flush still
 * fails to free a slot, wait for SQ space (io_uring_sqring_wait) and retry until
 * a slot appears or URING_API_SQE_WAIT_TIMEOUT_SEC elapses. Non-SQPOLL must free
 * a slot after a successful flush; if not, raise the same RuntimeError (fatal
 * invariant failure — not recoverable backpressure).
 *
 * Callers hold the ring critical section for exclusive prep. SQPOLL wait
 * therefore keeps that CS for the wait window (GIL is released). Intended for
 * SINGLE_ISSUER-style exclusive submit; multi-issuer + SQPOLL serialises on CS.
 */
struct io_uring_sqe *get_sqe(UringApiRing *self) {
    struct io_uring_sqe *sqe;
    int flush_rounds = 0;
    int wait_ret;
    int errnum;
    int sqpoll = (self->setup_flags & IORING_SETUP_SQPOLL) != 0;
    int64_t wait_deadline_ms = -1;
    int64_t now_ms;

    if (ring_check_submit_thread(self, 1) < 0) {
        return NULL;
    }

    for (;;) {
        sqe = io_uring_get_sqe(&self->ring);
        if (sqe) {
            return sqe;
        }

        if (ring_flush_pending(self, NULL) < 0) {
            return NULL;
        }
        flush_rounds++;

        sqe = io_uring_get_sqe(&self->ring);
        if (sqe) {
            return sqe;
        }

        /*
         * Still full after flush. Non-SQPOLL: submit should have freed a slot —
         * treat as fatal. SQPOLL: wait for the poller (from the second flush
         * onward), with a wall-clock timeout so a dead poller does not hang us.
         */
        if (!sqpoll) {
            set_sqe_slot_stuck_error();
            return NULL;
        }

        if (flush_rounds < 2) {
            continue;
        }

        if (wait_deadline_ms < 0) {
            now_ms = monotonic_ms();
            if (now_ms < 0) {
                PyErr_SetFromErrno(PyExc_OSError);
                return NULL;
            }
            wait_deadline_ms = now_ms + (int64_t)URING_API_SQE_WAIT_TIMEOUT_SEC * 1000;
        } else {
            now_ms = monotonic_ms();
            if (now_ms < 0) {
                PyErr_SetFromErrno(PyExc_OSError);
                return NULL;
            }
            if (now_ms >= wait_deadline_ms) {
                set_sqe_slot_stuck_error();
                return NULL;
            }
        }

        /*
         * Callers hold the ring critical section (SQE prep is exclusive). Drop
         * the GIL during the kernel wait so other Python threads can run; the
         * ring CS still serialises get_sqe/flush (typical SINGLE_ISSUER use).
         */
        Py_BEGIN_ALLOW_THREADS;
        wait_ret = io_uring_sqring_wait(&self->ring);
        Py_END_ALLOW_THREADS;
        if (wait_ret < 0) {
            errnum = normalize_ret_errno(wait_ret);
            if (errnum == EINTR) {
                continue;
            }
            if (errnum == EINVAL) {
                /* no sqring_wait support: back off instead of tight-spinning under the CS/GIL */
                Py_BEGIN_ALLOW_THREADS;
                (void)usleep(URING_API_SQE_WAIT_EINVAL_BACKOFF_US);
                Py_END_ALLOW_THREADS;
                continue;
            }
            errno = errnum;
            PyErr_SetFromErrno(PyExc_OSError);
            return NULL;
        }
    }
}
