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

int completion_type_check(PyObject *completion) {
    if (!PyObject_TypeCheck(completion, &UringApiCompletion_Type)) {
        PyErr_SetString(PyExc_TypeError, "completion must be an _uring_api.Completion instance");
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
        PyModule_AddIntConstant(module, "COMPLETION_KIND_SENDMSG_ZC", URING_API_PENDING_SENDMSG_ZC) < 0) {
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

static PyObject *sockaddr_to_object(struct sockaddr_storage *storage, socklen_t addrlen) {
    char host[INET6_ADDRSTRLEN];

    (void)addrlen;
    if (storage->ss_family == AF_INET) {
        struct sockaddr_in *addr = (struct sockaddr_in *)storage;
        if (!inet_ntop(AF_INET, &addr->sin_addr, host, sizeof(host))) {
            PyErr_SetFromErrno(PyExc_OSError);
            return NULL;
        }
        return Py_BuildValue("si", host, (int)ntohs(addr->sin_port));
    }
    if (storage->ss_family == AF_INET6) {
        struct sockaddr_in6 *addr = (struct sockaddr_in6 *)storage;
        if (!inet_ntop(AF_INET6, &addr->sin6_addr, host, sizeof(host))) {
            PyErr_SetFromErrno(PyExc_OSError);
            return NULL;
        }
        return Py_BuildValue("sIII", host, (unsigned int)ntohs(addr->sin6_port),
                             (unsigned int)ntohl(addr->sin6_flowinfo), (unsigned int)addr->sin6_scope_id);
    }
    Py_RETURN_NONE;
}

int ring_check_open(UringApiRing *self) {
    if (!self->initialized) {
        PyErr_SetString(PyExc_RuntimeError, "ring is closed");
        return -1;
    }
    return 0;
}

static bool is_power_of_two(unsigned long value) { return value != 0 && (value & (value - 1)) == 0; }

static void UringApiRecvBufferPool_free(UringApiRecvBufferPool *pool) {
    if (!pool) {
        return;
    }
    if (pool->ring_buffer && pool->ring && pool->ring->initialized) {
        (void)io_uring_free_buf_ring(&pool->ring->ring, pool->ring_buffer, pool->buffer_count, pool->group_id);
    }
    PyMem_Free(pool->storage);
    Py_XDECREF((PyObject *)pool->ring);
    PyMem_Free(pool);
}

UringApiRecvBufferPool *UringApiRecvBufferPool_new(UringApiRing *ring, unsigned int buffer_size,
                                                   unsigned int buffer_count) {
    UringApiRecvBufferPool *pool;
    size_t total_size;
    int ret = 0;
    unsigned int index;

    if (buffer_size == 0) {
        PyErr_SetString(PyExc_ValueError, "buffer_size must be positive");
        return NULL;
    }
    if (!is_power_of_two(buffer_count) || buffer_count > USHRT_MAX + 1U) {
        PyErr_SetString(PyExc_ValueError, "buffer_count must be a power of two no larger than 65536");
        return NULL;
    }
    if ((size_t)buffer_count > SIZE_MAX / (size_t)buffer_size) {
        PyErr_SetString(PyExc_ValueError, "buffer pool is too large");
        return NULL;
    }

    pool = PyMem_Calloc(1, sizeof(*pool));
    if (!pool) {
        PyErr_NoMemory();
        return NULL;
    }
    total_size = (size_t)buffer_count * (size_t)buffer_size;
    pool->storage = PyMem_Malloc(total_size);
    if (!pool->storage) {
        UringApiRecvBufferPool_free(pool);
        PyErr_NoMemory();
        return NULL;
    }

    pool->ring = ring;
    Py_INCREF((PyObject *)ring);
    pool->buffer_size = buffer_size;
    pool->buffer_count = buffer_count;
    pool->group_id = ring->next_buf_group++;
    pool->mask = io_uring_buf_ring_mask(buffer_count);
    pool->ring_buffer = io_uring_setup_buf_ring(&ring->ring, buffer_count, pool->group_id, 0, &ret);
    if (!pool->ring_buffer) {
        int errnum = normalize_ret_errno(ret);
        UringApiRecvBufferPool_free(pool);
        errno = errnum;
        PyErr_SetFromErrno(PyExc_OSError);
        return NULL;
    }

    for (index = 0; index < buffer_count; index++) {
        io_uring_buf_ring_add(pool->ring_buffer, pool->storage + ((size_t)index * buffer_size), buffer_size,
                              (unsigned short)index, pool->mask, (int)index);
    }
    io_uring_buf_ring_advance(pool->ring_buffer, (int)buffer_count);
    return pool;
}

static void UringApiRecvBufferPool_recycle(UringApiRecvBufferPool *pool, unsigned int buffer_id) {
    io_uring_buf_ring_add(pool->ring_buffer, pool->storage + ((size_t)buffer_id * pool->buffer_size), pool->buffer_size,
                          (unsigned short)buffer_id, pool->mask, 0);
    io_uring_buf_ring_advance(pool->ring_buffer, 1);
}

void UringApiCompletion_dealloc(UringApiCompletion *self) {
    PyObject_GC_UnTrack(self);
    (void)UringApiCompletion_clear(self);
    PyObject_GC_Del(self);
}

int UringApiCompletion_traverse(UringApiCompletion *self, visitproc visit, void *arg) {
    Py_VISIT(self->buffer);
    if (self->recv_pool) {
        Py_VISIT(self->recv_pool->ring);
    }
    Py_VISIT(self->user_data);
    Py_VISIT(self->result);
    return 0;
}

int UringApiCompletion_clear(UringApiCompletion *self) {
    if (self->has_view) {
        PyBuffer_Release(&self->view);
        self->has_view = false;
    }
    Py_CLEAR(self->buffer);
    UringApiRecvBufferPool_free(self->recv_pool);
    self->recv_pool = NULL;
    Py_CLEAR(self->user_data);
    Py_CLEAR(self->result);
    return 0;
}

PyObject *UringApiCompletion_new_pending(UringApiPendingKind kind, PyObject *user_data, PyObject *buffer) {
    UringApiCompletion *completion = PyObject_GC_New(UringApiCompletion, &UringApiCompletion_Type);
    if (!completion) {
        return NULL;
    }
    completion->kind = kind;
    completion->user_data = Py_NewRef(user_data);
    completion->res = 0;
    completion->flags = 0;
    completion->result = NULL;
    completion->buffer = Py_XNewRef(buffer);
    completion->recv_pool = NULL;
    completion->sequence = 0;
    completion->has_view = false;
    completion->has_msghdr = false;
    PyObject_GC_Track(completion);
    return (PyObject *)completion;
}

PyObject *UringApiCompletion_new_pending_view(UringApiPendingKind kind, PyObject *user_data, Py_buffer *view) {
    UringApiCompletion *completion = (UringApiCompletion *)UringApiCompletion_new_pending(kind, user_data, NULL);
    if (!completion) {
        return NULL;
    }
    completion->view = *view;
    completion->has_view = true;
    return (PyObject *)completion;
}

PyObject *UringApiCompletion_new_pending_recvmsg(UringApiPendingKind kind, PyObject *user_data, Py_buffer *view) {
    UringApiCompletion *completion = (UringApiCompletion *)UringApiCompletion_new_pending_view(kind, user_data, view);
    if (!completion) {
        return NULL;
    }
    memset(&completion->iov, 0, sizeof(completion->iov));
    memset(&completion->msg, 0, sizeof(completion->msg));
    memset(&completion->addr, 0, sizeof(completion->addr));
    completion->addrlen = sizeof(completion->addr);
    completion->iov.iov_base = view->buf;
    completion->iov.iov_len = (size_t)view->len;
    completion->msg.msg_name = &completion->addr;
    completion->msg.msg_namelen = completion->addrlen;
    completion->msg.msg_iov = &completion->iov;
    completion->msg.msg_iovlen = 1;
    completion->has_msghdr = true;
    return (PyObject *)completion;
}

PyObject *UringApiCompletion_new_pending_sendmsg(UringApiPendingKind kind, PyObject *user_data, Py_buffer *view) {
    UringApiCompletion *completion = (UringApiCompletion *)UringApiCompletion_new_pending_view(kind, user_data, view);
    if (!completion) {
        return NULL;
    }
    memset(&completion->iov, 0, sizeof(completion->iov));
    memset(&completion->msg, 0, sizeof(completion->msg));
    memset(&completion->addr, 0, sizeof(completion->addr));
    completion->addrlen = 0;
    completion->iov.iov_base = view->buf;
    completion->iov.iov_len = (size_t)view->len;
    completion->msg.msg_iov = &completion->iov;
    completion->msg.msg_iovlen = 1;
    completion->has_msghdr = true;
    return (PyObject *)completion;
}

bool is_zero_copy_send_kind(UringApiPendingKind kind) {
    return kind == URING_API_PENDING_SEND_ZC || kind == URING_API_PENDING_SENDMSG_ZC;
}

PyObject *UringApiCompletion_new_pending_accept(PyObject *user_data) {
    UringApiCompletion *completion =
        (UringApiCompletion *)UringApiCompletion_new_pending(URING_API_PENDING_ACCEPT, user_data, NULL);
    if (!completion) {
        return NULL;
    }
    memset(&completion->addr, 0, sizeof(completion->addr));
    completion->addrlen = sizeof(completion->addr);
    return (PyObject *)completion;
}

PyObject *UringApiCompletion_new_delivered_copy(UringApiCompletion *source) {
    UringApiCompletion *completion = PyObject_GC_New(UringApiCompletion, &UringApiCompletion_Type);
    if (!completion) {
        return NULL;
    }
    completion->kind = source->kind;
    completion->user_data = Py_NewRef(source->user_data);
    completion->res = source->res;
    completion->flags = source->flags;
    completion->result = source->result;
    source->result = NULL;
    completion->buffer = NULL;
    completion->recv_pool = NULL;
    completion->sequence = source->sequence;
    source->sequence++;
    memset(&completion->view, 0, sizeof(completion->view));
    memset(&completion->iov, 0, sizeof(completion->iov));
    memset(&completion->msg, 0, sizeof(completion->msg));
    memset(&completion->addr, 0, sizeof(completion->addr));
    completion->addrlen = 0;
    completion->has_view = false;
    completion->has_msghdr = false;
    PyObject_GC_Track(completion);
    return (PyObject *)completion;
}

void UringApiCompletion_clear_pending_state(UringApiCompletion *self) {
    if (self->has_view) {
        PyBuffer_Release(&self->view);
        self->has_view = false;
    }
    Py_CLEAR(self->buffer);
    UringApiRecvBufferPool_free(self->recv_pool);
    self->recv_pool = NULL;
}

static bool UringApiCompletion_should_clear_pending_state(UringApiCompletion *self, int res, unsigned int flags) {
    /* send_zc success has two CQEs: the operation result, then a NOTIF CQE that closes the buffer lifetime window. */
    if (is_zero_copy_send_kind(self->kind) && res >= 0 && !(flags & IORING_CQE_F_NOTIF)) {
        return false;
    }
    return !(flags & IORING_CQE_F_MORE);
}

static PyObject *UringApiCompletion_recv_multishot_payload(UringApiCompletion *self, int res, unsigned int flags) {
    unsigned int buffer_id;
    PyObject *payload;

    if (res < 0) {
        Py_RETURN_NONE;
    }
    if (res == 0 && !(flags & IORING_CQE_F_BUFFER)) {
        return PyBytes_FromStringAndSize("", 0);
    }
    if (!(flags & IORING_CQE_F_BUFFER)) {
        PyErr_SetString(PyExc_RuntimeError, "recv multishot completion did not select a buffer");
        return NULL;
    }
    buffer_id = flags >> IORING_CQE_BUFFER_SHIFT;
    if (!self->recv_pool || buffer_id >= self->recv_pool->buffer_count) {
        PyErr_SetString(PyExc_RuntimeError, "recv multishot completion selected an invalid buffer");
        return NULL;
    }
    if ((unsigned int)res > self->recv_pool->buffer_size) {
        PyErr_SetString(PyExc_RuntimeError, "recv multishot completion exceeds selected buffer size");
        return NULL;
    }

    payload = PyBytes_FromStringAndSize(
        (const char *)self->recv_pool->storage + ((size_t)buffer_id * self->recv_pool->buffer_size), (Py_ssize_t)res);
    if (!payload) {
        return NULL;
    }
    if (flags & IORING_CQE_F_MORE) {
        UringApiRecvBufferPool_recycle(self->recv_pool, buffer_id);
    }
    return payload;
}

int UringApiCompletion_complete(UringApiCompletion *self, int res, unsigned int flags) {
    PyObject *payload;

    self->res = res;
    self->flags = flags;
    if (self->kind == URING_API_PENDING_WAKE) {
        UringApiCompletion_clear_pending_state(self);
        return 1;
    }
    if (self->kind == URING_API_PENDING_RECV_MULTISHOT) {
        payload = UringApiCompletion_recv_multishot_payload(self, res, flags);
    } else if (res >= 0 && (self->kind == URING_API_PENDING_RECV || self->kind == URING_API_PENDING_SEND ||
                            is_zero_copy_send_kind(self->kind) || self->kind == URING_API_PENDING_SENDTO ||
                            self->kind == URING_API_PENDING_SENDMSG || self->kind == URING_API_PENDING_SOCKET)) {
        payload = PyLong_FromLong(res);
    } else if (res >= 0 && self->kind == URING_API_PENDING_RECVMSG) {
        self->addrlen = self->msg.msg_namelen;
        payload = sockaddr_to_object(&self->addr, self->addrlen);
    } else if (res >= 0 && self->kind == URING_API_PENDING_ACCEPT) {
        payload = sockaddr_to_object(&self->addr, self->addrlen);
        if (payload) {
            payload = Py_BuildValue("iN", res, payload);
        }
    } else if (res >= 0 && self->kind == URING_API_PENDING_CONNECT) {
        payload = Py_NewRef(Py_None);
    } else {
        payload = Py_NewRef(Py_None);
    }
    if (!payload) {
        return -1;
    }
    Py_XSETREF(self->result, payload);
    if (UringApiCompletion_should_clear_pending_state(self, res, flags)) {
        UringApiCompletion_clear_pending_state(self);
    }
    return 0;
}

PyObject *UringApiCompletion_get_user_data(UringApiCompletion *self, void *closure) {
    return Py_NewRef(self->user_data);
}

PyObject *UringApiCompletion_get_kind(UringApiCompletion *self, void *closure) {
    return PyLong_FromLong((long)self->kind);
}

PyObject *UringApiCompletion_get_res(UringApiCompletion *self, void *closure) { return PyLong_FromLong(self->res); }

PyObject *UringApiCompletion_get_flags(UringApiCompletion *self, void *closure) {
    return PyLong_FromUnsignedLong(self->flags);
}

PyObject *UringApiCompletion_get_result(UringApiCompletion *self, void *closure) {
    if (!self->result) {
        Py_RETURN_NONE;
    }
    return Py_NewRef(self->result);
}

PyObject *UringApiCompletion_get_sequence(UringApiCompletion *self, void *closure) {
    return PyLong_FromUnsignedLongLong(self->sequence);
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
