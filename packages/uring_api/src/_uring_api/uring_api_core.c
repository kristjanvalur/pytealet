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
/* sleep when sqring_wait cannot block: unsupported (EINVAL), or any slot is
 * already free so a multi-slot wait would return immediately. */
#define URING_API_SQE_WAIT_BACKOFF_US 1000

int ring_type_check(PyObject *ring) {
    if (!PyObject_TypeCheck(ring, &UringApiRing_Type)) {
        PyErr_SetString(PyExc_TypeError, "ring must be an _uring_api.Ring instance");
        return 0;
    }
    return 1;
}

void ring_pending_inc(UringApiRing *self) {
    uring_api_refcount_mutex_lock(&self->refcount_mutex);
    self->pending_count++;
    uring_api_refcount_mutex_unlock(&self->refcount_mutex);
}

void ring_pending_dec(UringApiRing *self) {
    uring_api_refcount_mutex_lock(&self->refcount_mutex);
    assert(self->pending_count > 0);
    self->pending_count--;
    uring_api_refcount_mutex_unlock(&self->refcount_mutex);
}

unsigned int ring_pending_count(UringApiRing *self) {
    unsigned int count;

    uring_api_refcount_mutex_lock(&self->refcount_mutex);
    count = self->pending_count;
    uring_api_refcount_mutex_unlock(&self->refcount_mutex);
    return count;
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

#ifndef IORING_CQE_F_SOCK_NONEMPTY
#define IORING_CQE_F_SOCK_NONEMPTY (1U << 2)
#endif
#ifndef IORING_RECVSEND_POLL_FIRST
#define IORING_RECVSEND_POLL_FIRST (1U << 0)
#endif

int module_add_cqe_flag_constants(PyObject *module) {
    if (module_add_uint64_constant(module, "IORING_CQE_F_MORE", IORING_CQE_F_MORE) < 0 ||
        module_add_uint64_constant(module, "IORING_CQE_F_SOCK_NONEMPTY", IORING_CQE_F_SOCK_NONEMPTY) < 0 ||
        module_add_uint64_constant(module, "IORING_CQE_F_NOTIF", IORING_CQE_F_NOTIF) < 0) {
        return -1;
    }
    return 0;
}

int module_add_recvsend_flag_constants(PyObject *module) {
    if (module_add_uint64_constant(module, "IORING_RECVSEND_POLL_FIRST", IORING_RECVSEND_POLL_FIRST) < 0 ||
        module_add_uint64_constant(module, "IORING_SEND_ZC_REPORT_USAGE", IORING_SEND_ZC_REPORT_USAGE) < 0 ||
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
        PyModule_AddIntConstant(module, "COMPLETION_KIND_STATX_FDSIZE", URING_API_PENDING_STATX_FDSIZE) < 0 ||
        PyModule_AddIntConstant(module, "COMPLETION_KIND_SEND_ALL", URING_API_PENDING_SEND_ALL) < 0) {
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

    /* Completion* must keep bits 1:0 clear so it cannot collide with special tags. */
    assert((ptr & (uintptr_t)URING_API_UD_TAG_MASK) == 0);
    io_uring_sqe_set_data64(sqe, (unsigned long long)ptr);
    completion_set_bit((UringApiCompletion *)completion, URING_API_C_PREPARED);
    /* caller holds the ring CS. nowait SQEs use a tagged user_data instead.
     * a Completion* fill is waitable, so the thread count moves with the bit. */
    self->sq_waitable = true;
    ring_note_thread_sqe(self, 1);
}

/* Ever-increasing so a nested handler can snapshot without clearing the
 * outer count. Resetting to 0 at each callback would do that. Shared by
 * every ring this thread touches; the snapshot delta is still this handler.
 * A plain ring moves it for a waitable SQE. SQPOLL also moves it for a
 * direct nowait. */
static _Thread_local uint64_t thread_sqes_prepared;
static _Thread_local uint64_t thread_internal_sqes;

void ring_note_thread_internal(void) { thread_internal_sqes++; }

uint64_t ring_thread_internal_sqes(void) { return thread_internal_sqes; }

void ring_note_thread_sqe(UringApiRing *self, int waitable) {
    if (!waitable && (self->setup_flags & IORING_SETUP_SQPOLL) == 0) {
        return;
    }
    thread_sqes_prepared++;
}

uint64_t ring_thread_sqes_prepared(void) { return thread_sqes_prepared; }

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

int parse_entries_flags(URING_API_PARSE_ARGS, unsigned int default_entries, unsigned int *entries,
                        unsigned int *flags) {
    static char *keywords[] = {"entries", "flags", NULL};
    unsigned long entries_value = default_entries;
    unsigned long flags_value = 0;

    if (!URING_API_PARSE_KEYWORDS("|kk", keywords, &entries_value, &flags_value)) {
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
    unsigned long long stored = self->owner_thread_id;

    /* 0 = unset (closed). creator latches at queue_init. */
    if (stored == 0 || stored != ring_current_thread_id()) {
        if (raise_on_error) {
            PyErr_SetString(PyExc_RuntimeError, error_message);
        }
        return -1;
    }
    return 0;
}

static int ring_on_owner_thread(UringApiRing *self) {
    unsigned long long owner = self->owner_thread_id;

    return owner != 0 && owner == ring_current_thread_id();
}

static int ring_policy_pick(UringApiRing *self, bool issuer_flag, bool worker_flag) {
    if (ring_on_owner_thread(self)) {
        return issuer_flag;
    }
    return worker_flag;
}

void ring_submission_policy_apply(UringApiRing *self, int auto_submit) {
    UringApiSubmissionPolicy *policy = &self->submission_policy;
    int enabled = auto_submit != 0;
    int sqpoll = (self->setup_flags & IORING_SETUP_SQPOLL) != 0;
    int defer = (self->setup_flags & IORING_SETUP_DEFER_TASKRUN) != 0;
    int single = (self->setup_flags & IORING_SETUP_SINGLE_ISSUER) != 0;
    /* DEFER_TASKRUN pins the owner. SINGLE_ISSUER does too, unless SQPOLL
     * is set: the kernel thread submits, so a worker may publish. the
     * kernel effect of that combination is not modelled here.
     * auto_submit does not change this bit. */
    int workers_may_submit = !defer && (sqpoll || !single);
    int workers = enabled && workers_may_submit;
    /* same publish points with or without SQPOLL. under SQPOLL,
     * io_uring_submit copies the tail and the kernel thread issues.
     * END_OF_TURN also counts a direct nowait and skips the waiter
     * and host-park gates. */
    int end_of_turn = enabled && sqpoll;

    policy->auto_submit = enabled;
    policy->workers_may_submit = workers_may_submit;
    policy->issuer_on_sq_full = enabled;
    policy->worker_on_sq_full = workers;
    policy->issuer_before_reap = enabled;
    policy->worker_before_reap = workers;
    policy->issuer_callback_sqes = enabled;
    policy->worker_callback_sqes = workers;
    /* the owner can always publish a next leg or a submit-owed copy.
     * a worker can when it may submit. auto_submit does not apply:
     * the application never sees these SQEs. */
    policy->issuer_housekeeping = 1;
    policy->worker_housekeeping = workers_may_submit;
    if (!enabled) {
        policy->callback_submit = URING_API_CALLBACK_SUBMIT_OFF;
    } else if (end_of_turn) {
        policy->callback_submit = URING_API_CALLBACK_SUBMIT_END_OF_TURN;
    } else {
        policy->callback_submit = URING_API_CALLBACK_SUBMIT_WHEN_WAITER_HELD;
    }
}

int ring_policy_on_sq_full(UringApiRing *self) {
    return ring_policy_pick(self, self->submission_policy.issuer_on_sq_full, self->submission_policy.worker_on_sq_full);
}

int ring_policy_before_reap(UringApiRing *self) {
    return ring_policy_pick(self, self->submission_policy.issuer_before_reap,
                            self->submission_policy.worker_before_reap);
}

int ring_policy_callback_sqes(UringApiRing *self) {
    return ring_policy_pick(self, self->submission_policy.issuer_callback_sqes,
                            self->submission_policy.worker_callback_sqes);
}

int ring_policy_housekeeping(UringApiRing *self) {
    return ring_policy_pick(self, self->submission_policy.issuer_housekeeping,
                            self->submission_policy.worker_housekeeping);
}

int ring_check_submit_thread(UringApiRing *self, int raise_on_error) {
    const char *message;

    /* the bit is the decision. the message still names which flag pinned it. */
    if (self->submission_policy.workers_may_submit) {
        return 0;
    }
    assert((self->setup_flags & (IORING_SETUP_DEFER_TASKRUN | IORING_SETUP_SINGLE_ISSUER)) != 0);
    if (self->setup_flags & IORING_SETUP_DEFER_TASKRUN) {
        message = "ring was created with IORING_SETUP_DEFER_TASKRUN; submissions and completions must run on the "
                  "thread that created the ring";
    } else {
        message = "ring was created with IORING_SETUP_SINGLE_ISSUER; submissions must come from the thread that "
                  "created the ring";
    }
    return ring_check_owner_thread(self, message, raise_on_error);
}

int ring_check_client_thread(UringApiRing *self) {
    if (!(self->setup_flags & IORING_SETUP_DEFER_TASKRUN)) {
        return 0;
    }
    return ring_check_owner_thread(
        self,
        "ring was created with IORING_SETUP_DEFER_TASKRUN; submissions and completions must run on the thread that "
        "created the ring",
        1);
}

/* SQEs liburing will copy on the next submit. sqe_head is the last tail it
 * published. caller holds the ring CS. this does not read ktail or khead. */
static unsigned ring_sq_to_publish(UringApiRing *self) {
    unsigned count = self->ring.sq.sqe_tail - self->ring.sq.sqe_head;

    assert(count <= self->ring.sq.ring_entries);
    return count;
}

int ring_flush_pending(UringApiRing *self, int *submitted_out) {
    int ret;
    int sqpoll = (self->setup_flags & IORING_SETUP_SQPOLL) != 0;
    unsigned to_publish = 0;

    /* a normal ring skips the enter when nothing is unsubmitted.
     * SQPOLL always calls io_uring_submit. liburing stores the tail only
     * when sqe_head is behind, and sq_ring_needs_enter is false when that
     * flush reports nothing pending. the return value in that mode is
     * unconsumed (tail - khead), so the batch we record is to_publish. */
    if (sqpoll) {
        to_publish = ring_sq_to_publish(self);
    } else if (io_uring_sq_ready(&self->ring) == 0) {
        /* an empty SQ cannot still be waitable. */
        self->sq_waitable = false;
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
    if (sqpoll) {
        ret = (int)to_publish;
    }
    if (submitted_out) {
        *submitted_out += ret;
    }
    /* a partial submit can leave a later waitable SQE, so keep the bit.
     * SQPOLL copies the whole tail or leaves it. a zero batch is not a
     * publish worth counting. */
    if (sqpoll || io_uring_sq_ready(&self->ring) == 0) {
        self->sq_waitable = false;
    }
    ring_note_submit(self, ret);
    return 0;
}

void ring_read_stats(UringApiRing *self, UringApiRingStats *out) {
    Py_BEGIN_CRITICAL_SECTION(self);
    out->sqe = self->stats.sqe;
    out->cqe = atomic_load_explicit(&self->stats.cqe, memory_order_relaxed);
    out->sq_full = self->stats.sq_full;
    out->next_leg = self->stats.next_leg;
    out->next_leg_park = self->stats.next_leg_park;
    out->submit_main_events = self->stats.submit_events[URING_API_SUBMIT_MAIN];
    out->submit_main_sqes = self->stats.submit_sqes[URING_API_SUBMIT_MAIN];
    out->submit_worker_events = self->stats.submit_events[URING_API_SUBMIT_WORKER];
    out->submit_worker_sqes = self->stats.submit_sqes[URING_API_SUBMIT_WORKER];
    out->submit_sq_full_events = self->stats.submit_events[URING_API_SUBMIT_SQ_FULL];
    out->submit_sq_full_sqes = self->stats.submit_sqes[URING_API_SUBMIT_SQ_FULL];
    out->wait_calls = atomic_load_explicit(&self->stats.wait_calls, memory_order_relaxed);
    out->wait_front_events = atomic_load_explicit(&self->stats.wait_front_events, memory_order_relaxed);
    out->wait_front_cqes = atomic_load_explicit(&self->stats.wait_front_cqes, memory_order_relaxed);
    out->wait_back_events = atomic_load_explicit(&self->stats.wait_back_events, memory_order_relaxed);
    out->wait_back_cqes = atomic_load_explicit(&self->stats.wait_back_cqes, memory_order_relaxed);
    out->cq_overflow = 0;
    /* kernel counter. close() unmaps the ring under this same critical section. */
    if (self->initialized && self->ring.cq.koverflow != NULL) {
        out->cq_overflow = io_uring_smp_load_acquire(self->ring.cq.koverflow);
    }
    Py_END_CRITICAL_SECTION();
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

int receive_wait_begin(UringApiRing *self) {
    int ret = 0;

    Py_BEGIN_CRITICAL_SECTION(self);
    if (self->receive_state == URING_API_RECEIVE_DELIVERING) {
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

void receive_wait_end(UringApiRing *self) {
    Py_BEGIN_CRITICAL_SECTION(self);
    self->receive_state = URING_API_RECEIVE_IDLE;
    Py_END_CRITICAL_SECTION();
}

int cqe_unique_waiter_active(UringApiRing *self) {
    int waiting;

    pthread_mutex_lock(&self->cqe_mu);
    waiting = self->cqe_waiting;
    pthread_mutex_unlock(&self->cqe_mu);
    return waiting;
}

int ring_should_we_submit(UringApiRing *self, uint64_t prepared_at_entry) {
    if (!ring_policy_callback_sqes(self)) {
        return 0;
    }
    if (self->submission_policy.callback_submit == URING_API_CALLBACK_SUBMIT_OFF) {
        return 0;
    }
    /* nothing new this thread cares about. a plain ring's count moves
     * for a waitable SQE. SQPOLL's count also moves for a direct nowait.
     * leave another thread's SQEs for its own turn, the harvest flush,
     * or submit(). */
    if (ring_thread_sqes_prepared() == prepared_at_entry) {
        return 0;
    }
    if (self->submission_policy.callback_submit == URING_API_CALLBACK_SUBMIT_END_OF_TURN) {
        return 1;
    }
    if (self->submission_policy.callback_submit != URING_API_CALLBACK_SUBMIT_WHEN_WAITER_HELD) {
        return 0;
    }
    /* after the host has blocked once, it submits on the way into the
     * next wait. while it is outside that park, leave the queue for that
     * submit. until the first blocking wait, an external poller may never
     * park, so the sleep flag stays out of the decision. */
    if (UringApiIdlePark_has_blocked(&self->idle) && !UringApiIdlePark_is_waiting(&self->idle)) {
        return 0;
    }
    /* cqe_waiting is set before the waiter's own flush, and it stays set
     * across io_uring_wait_cqe. a handler that returns while it is set
     * has already missed that flush. */
    if (!cqe_unique_waiter_active(self)) {
        return 0;
    }
    return 1;
}

int ring_publish_if_waiter_active(UringApiRing *self, uint64_t prepared_at_entry) {
    unsigned char saved_kind;
    unsigned char kind;
    int flush_ret;

    /* the decision is the cared count. when it is true the whole
     * ordered queue is entered, so a non-waitable already queued rides
     * along. an empty queue is a no-op. */
    if (!ring_should_we_submit(self, prepared_at_entry)) {
        return 0;
    }
    if (self->owner_thread_id != 0 && self->owner_thread_id == (unsigned long long)PyThread_get_thread_ident()) {
        kind = URING_API_SUBMIT_MAIN;
    } else {
        kind = URING_API_SUBMIT_WORKER;
    }
    saved_kind = ring_submit_kind_push(self, kind);
    flush_ret = ring_flush_pending(self, NULL);
    ring_submit_kind_pop(self, saved_kind);
    return flush_ret;
}

int ring_flush_internal_since(UringApiRing *self, uint64_t internal_at) {
    unsigned char saved_kind;
    unsigned char kind;
    int flush_ret;

    if (ring_thread_internal_sqes() == internal_at) {
        return 0;
    }
    /* auto_submit does not cover these. the housekeeping flag is the
     * owner, or a worker that may submit. anyone else leaves them. */
    if (!ring_policy_housekeeping(self)) {
        return 0;
    }
    if (self->owner_thread_id != 0 && self->owner_thread_id == (unsigned long long)PyThread_get_thread_ident()) {
        kind = URING_API_SUBMIT_MAIN;
    } else {
        kind = URING_API_SUBMIT_WORKER;
    }
    saved_kind = ring_submit_kind_push(self, kind);
    flush_ret = ring_flush_pending(self, NULL);
    ring_submit_kind_pop(self, saved_kind);
    return flush_ret;
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

/* published tail the poller has not consumed. sqe_tail ahead of sqe_head
 * is unpublished and is not included: this does not read that cursor.
 * khead is store-released by the kernel after it issues. */
static unsigned ring_sq_published_unconsumed(const struct io_uring *ring) {
    const struct io_uring_sq *sq = &ring->sq;
    unsigned head = io_uring_smp_load_acquire(sq->khead);
    unsigned tail = IO_URING_READ_ONCE(*sq->ktail);
    unsigned pending = tail - head;

    assert(pending <= sq->ring_entries);
    return pending;
}

int ring_sqpoll_drain_published(UringApiRing *self) {
    struct io_uring_sq *sq;
    int64_t deadline_ms = -1;

    if ((self->setup_flags & IORING_SETUP_SQPOLL) == 0) {
        return 0;
    }
    sq = &self->ring.sq;
    for (;;) {
        int64_t now_ms;
        unsigned flags;

        if (ring_sq_published_unconsumed(&self->ring) == 0) {
            return 0;
        }
        if (deadline_ms < 0) {
            now_ms = monotonic_ms();
            if (now_ms < 0) {
                PyErr_SetFromErrno(PyExc_OSError);
                return -1;
            }
            deadline_ms = now_ms + (int64_t)URING_API_SQE_WAIT_TIMEOUT_SEC * 1000;
        } else {
            now_ms = monotonic_ms();
            if (now_ms < 0) {
                PyErr_SetFromErrno(PyExc_OSError);
                return -1;
            }
            if (now_ms >= deadline_ms) {
                PyErr_SetString(PyExc_RuntimeError, "SQPOLL thread did not consume published SQEs "
                                                    "(poller may be dead or hung)");
                return -1;
            }
        }
        /* sample before dropping the GIL. a flag that appears while we
         * sleep is picked up on the next pass. io_uring_submit would also
         * publish sqe_tail, and close still drops that unpublished tail. */
        flags = IO_URING_READ_ONCE(*sq->kflags);
        Py_BEGIN_ALLOW_THREADS;
        if (flags & IORING_SQ_NEED_WAKEUP) {
            (void)io_uring_enter((unsigned int)self->ring.ring_fd, 0, 0, IORING_ENTER_SQ_WAKEUP, NULL);
        } else {
            (void)usleep(URING_API_SQE_WAIT_BACKOFF_US);
        }
        Py_END_ALLOW_THREADS;
    }
}

/* 0 if the ring can hold need slots. -1 if it cannot, before any enter. */
int sq_check_need(UringApiRing *self, unsigned int need) {
    unsigned int entries = ring_sq_entries(self);

    if (entries >= need) {
        return 0;
    }
    PyErr_Format(PyExc_RuntimeError, "requested %u free submission queue slots, but sq_entries is %u", need, entries);
    return -1;
}

/*
 * Make room for need free SQ slots. Caller holds the ring CS and has already
 * decided this thread may enter. Does not take a slot, so a caller that needs
 * two cannot be left holding one.
 *
 * A ring smaller than need fails immediately. Flushing cannot help, and the
 * error is not the stuck-poller RuntimeError.
 *
 * Flush, then retry. With SQPOLL the poller may lag: after the second flush
 * still fails to free the slots, wait until they appear or
 * URING_API_SQE_WAIT_TIMEOUT_SEC elapses. io_uring_sqring_wait returns as
 * soon as any slot is free, so it is used only when the SQ is completely
 * full. A partly full queue sleeps instead; otherwise need > 1 tight-spins
 * under the ring CS. Non-SQPOLL must free the slots after a successful
 * flush; if not, raise RuntimeError (stuck queue / dead poller — not
 * recoverable backpressure). SubmissionQueueFull is the caller's job, for
 * the case where this thread was not allowed to enter.
 *
 * The SQPOLL wait keeps the ring CS (GIL is released). Intended for
 * SINGLE_ISSUER-style exclusive submit; multi-issuer + SQPOLL serialises on CS.
 */
int sq_ensure_space(UringApiRing *self, unsigned int need, int *submitted_out) {
    int flush_rounds = 0;
    int wait_ret;
    int errnum;
    int sqpoll = (self->setup_flags & IORING_SETUP_SQPOLL) != 0;
    int64_t wait_deadline_ms = -1;
    int64_t now_ms;

    if (sq_check_need(self, need) < 0) {
        return -1;
    }

    for (;;) {
        if (io_uring_sq_space_left(&self->ring) >= need) {
            return 1;
        }
        {
            /* make-room enter, not the bucket the caller may already have pushed. */
            unsigned char saved_kind = ring_submit_kind_push(self, URING_API_SUBMIT_SQ_FULL);
            int flush_ret = ring_flush_pending(self, submitted_out);

            ring_submit_kind_pop(self, saved_kind);
            if (flush_ret < 0) {
                return -1;
            }
        }
        flush_rounds++;
        if (io_uring_sq_space_left(&self->ring) >= need) {
            return 1;
        }
        /* non-SQPOLL: submit should have freed a slot. not backpressure. */
        if (!sqpoll) {
            set_sqe_slot_stuck_error();
            return -1;
        }
        if (flush_rounds < 2) {
            continue;
        }
        if (wait_deadline_ms < 0) {
            now_ms = monotonic_ms();
            if (now_ms < 0) {
                PyErr_SetFromErrno(PyExc_OSError);
                return -1;
            }
            wait_deadline_ms = now_ms + (int64_t)URING_API_SQE_WAIT_TIMEOUT_SEC * 1000;
        } else {
            now_ms = monotonic_ms();
            if (now_ms < 0) {
                PyErr_SetFromErrno(PyExc_OSError);
                return -1;
            }
            if (now_ms >= wait_deadline_ms) {
                set_sqe_slot_stuck_error();
                return -1;
            }
        }
        /* drop the GIL so other Python threads can run; the ring CS stays.
         * sqring_wait cannot block unless the SQ is completely full. */
        {
            unsigned int space = io_uring_sq_space_left(&self->ring);

            Py_BEGIN_ALLOW_THREADS;
            if (space == 0) {
                wait_ret = io_uring_sqring_wait(&self->ring);
            } else {
                (void)usleep(URING_API_SQE_WAIT_BACKOFF_US);
                wait_ret = 0;
            }
            Py_END_ALLOW_THREADS;
        }
        if (wait_ret < 0) {
            errnum = normalize_ret_errno(wait_ret);
            if (errnum == EINTR) {
                continue;
            }
            if (errnum == EINVAL) {
                /* no sqring_wait support: back off instead of tight-spinning */
                Py_BEGIN_ALLOW_THREADS;
                (void)usleep(URING_API_SQE_WAIT_BACKOFF_US);
                Py_END_ALLOW_THREADS;
                continue;
            }
            errno = errnum;
            PyErr_SetFromErrno(PyExc_OSError);
            return -1;
        }
    }
}

/* note_full is 0 when the caller already counted this fill attempt's first
 * miss. This call must not bump sq_full again. */
static struct io_uring_sqe *get_sqe_loop(UringApiRing *self, int flush_if_full, int *submitted_out, int note_full) {
    struct io_uring_sqe *sqe;

    if (ring_check_submit_thread(self, 1) < 0) {
        return NULL;
    }

    sqe = io_uring_get_sqe(&self->ring);
    if (sqe) {
        ring_note_sqe(self);
        return sqe;
    }
    if (note_full) {
        ring_note_sq_full(self);
    }
    /* on_sq_full off and not flush_if_full: the caller submits. */
    if (!flush_if_full && !ring_policy_on_sq_full(self)) {
        PyErr_SetString(UringApiSubmissionQueueFullError, "no submission queue entries available");
        return NULL;
    }
    if (sq_ensure_space(self, 1, submitted_out) < 0) {
        return NULL;
    }
    sqe = io_uring_get_sqe(&self->ring);
    /* ensure left a free slot and this thread holds the ring CS. */
    assert(sqe != NULL);
    if (sqe == NULL) {
        set_sqe_slot_stuck_error();
        return NULL;
    }
    ring_note_sqe(self);
    return sqe;
}

struct io_uring_sqe *get_sqe(UringApiRing *self) {
    return get_sqe_loop(self, 0, NULL, 1);
}

int get_sqe_try(UringApiRing *self, int flush_if_full, int *submitted_out, struct io_uring_sqe **sqe_out) {
    struct io_uring_sqe *sqe = io_uring_get_sqe(&self->ring);

    assert(sqe_out != NULL);
    *sqe_out = NULL;
    if (sqe) {
        ring_note_sqe(self);
        *sqe_out = sqe;
        return 1;
    }
    /* first peek of this attempt found no slot. get_sqe_loop must not count again. */
    ring_note_sq_full(self);
    if (flush_if_full) {
        if (ring_check_submit_thread(self, 0) < 0) {
            return 0;
        }
        sqe = get_sqe_loop(self, 1, submitted_out, 0);
        if (!sqe) {
            return -1;
        }
        *sqe_out = sqe;
        return 1;
    }
    if (!ring_policy_on_sq_full(self)) {
        return 0;
    }
    sqe = get_sqe_loop(self, 0, submitted_out, 0);
    if (!sqe) {
        return -1;
    }
    *sqe_out = sqe;
    return 1;
}
