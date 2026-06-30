/*
 * Shared private helpers for the _uring_api extension.
 */

#include "uring_api_core.h"

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
    if (module_add_uint64_constant(module, "IORING_SETUP_CQSIZE", IORING_SETUP_CQSIZE) < 0 ||
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
        PyModule_AddIntConstant(module, "COMPLETION_KIND_WRITE", URING_API_PENDING_WRITE) < 0) {
        return -1;
    }
    return 0;
}

void sqe_set_completion(UringApiRing *self, struct io_uring_sqe *sqe, PyObject *completion) {
    (void)self;
    io_uring_sqe_set_data64(sqe, (unsigned long long)(uintptr_t)completion);
}

UringApiCompletion *cqe_get_completion(UringApiRing *self, struct io_uring_cqe *cqe) {
    (void)self;
    return (UringApiCompletion *)(uintptr_t)io_uring_cqe_get_data64(cqe);
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

PyObject *build_feature_probe_result(bool available, int errnum, const char *message) {
    PyObject *result = PyDict_New();
    if (!result) {
        return NULL;
    }
    if (PyDict_SetItemString(result, "available", available ? Py_True : Py_False) < 0 ||
        dict_set_owned(result, "errno", errnum ? PyLong_FromLong(errnum) : Py_NewRef(Py_None)) < 0 ||
        dict_set_owned(result, "message", message ? PyUnicode_FromString(message) : Py_NewRef(Py_None)) < 0) {
        Py_DECREF(result);
        return NULL;
    }
    return result;
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

int submit_one(UringApiRing *self) {
    int ret;

    errno = 0;
    ret = io_uring_submit(&self->ring);

    if (ret < 0) {
        int errnum = normalize_ret_errno(ret);
        errno = errnum;
        PyErr_SetFromErrno(PyExc_OSError);
        return -1;
    }
    if (ret == 0) {
        PyErr_SetString(PyExc_RuntimeError, "io_uring_submit submitted no operations");
        return -1;
    }
    return 0;
}

int receive_wait_begin(UringApiRing *self, bool from_delivery_thread) {
    int ret = 0;

    Py_BEGIN_CRITICAL_SECTION_MUTEX(&self->receive_mutex);
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
    Py_END_CRITICAL_SECTION_MUTEX();
    return ret;
}

void receive_wait_end(UringApiRing *self, bool from_delivery_thread) {
    if (from_delivery_thread) {
        return;
    }

    Py_BEGIN_CRITICAL_SECTION_MUTEX(&self->receive_mutex);
    self->receive_state = URING_API_RECEIVE_IDLE;
    Py_END_CRITICAL_SECTION_MUTEX();
}

bool delivery_is_running_locked(UringApiRing *self) { return self->receive_state == URING_API_RECEIVE_DELIVERING; }

int delivery_check_not_running(UringApiRing *self) {
    int ret = 0;

    Py_BEGIN_CRITICAL_SECTION_MUTEX(&self->receive_mutex);
    if (delivery_is_running_locked(self)) {
        PyErr_SetString(PyExc_RuntimeError, "completion service is active");
        ret = -1;
    }
    Py_END_CRITICAL_SECTION_MUTEX();
    return ret;
}

void delivery_mark_exited(UringApiRing *self) {
    Py_BEGIN_CRITICAL_SECTION_MUTEX(&self->receive_mutex);
    if (self->delivery_active_workers > 0) {
        self->delivery_active_workers--;
    }
    if (self->delivery_active_workers == 0 && self->receive_state == URING_API_RECEIVE_DELIVERING) {
        self->receive_state = URING_API_RECEIVE_IDLE;
    }
    Py_END_CRITICAL_SECTION_MUTEX();
}

struct io_uring_sqe *get_sqe(UringApiRing *self) {
    struct io_uring_sqe *sqe = io_uring_get_sqe(&self->ring);
    int ret;

    if (sqe) {
        return sqe;
    }

    errno = 0;
    ret = io_uring_submit(&self->ring);
    if (ret < 0) {
        int errnum = normalize_ret_errno(ret);
        errno = errnum;
        PyErr_SetFromErrno(PyExc_OSError);
        return NULL;
    }
    sqe = io_uring_get_sqe(&self->ring);
    if (!sqe) {
        PyErr_SetString(UringApiSubmissionQueueFullError, "no submission queue entries available");
        return NULL;
    }
    return sqe;
}
