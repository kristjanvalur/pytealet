/*
 * Submission methods for the _uring_api Ring type.
 */

#include "uring_api_submit.h"
#include "uring_api_bufgroup.h"
#include "uring_api_completion.h"
#include "uring_api_core.h"
#include "uring_api_statx.h"

static int parse_socket_fd(PyObject *obj, int *fd_out) {
    long value = PyLong_AsLong(obj);

    if (value == -1 && PyErr_Occurred()) {
        return -1;
    }
    if (value < 0) {
        PyErr_SetString(PyExc_ValueError, "fd must be non-negative");
        return -1;
    }
    if (value > INT_MAX) {
        PyErr_SetString(PyExc_OverflowError, "fd out of range");
        return -1;
    }
    *fd_out = (int)value;
    return 0;
}

/* signed int for how/flags-like args (SHUT_RD etc.); no non-negative check */
static int parse_int_arg(PyObject *obj, int *value_out) {
    long value = PyLong_AsLong(obj);

    if (value == -1 && PyErr_Occurred()) {
        return -1;
    }
    if (value < INT_MIN || value > INT_MAX) {
        PyErr_SetString(PyExc_OverflowError, "integer out of range");
        return -1;
    }
    *value_out = (int)value;
    return 0;
}

static int parse_uint_arg(PyObject *obj, unsigned int *value_out) {
    unsigned long value = PyLong_AsUnsignedLong(obj);

    if (value == (unsigned long)-1 && PyErr_Occurred()) {
        return -1;
    }
    if (value > UINT_MAX) {
        PyErr_SetString(PyExc_OverflowError, "integer out of range");
        return -1;
    }
    *value_out = (unsigned int)value;
    return 0;
}

static int parse_ull_arg(PyObject *obj, unsigned long long *value_out) {
    unsigned long long value = PyLong_AsUnsignedLongLong(obj);

    if (value == (unsigned long long)-1 && PyErr_Occurred()) {
        return -1;
    }
    *value_out = value;
    return 0;
}

static int parse_recv_multishot_args(const char *name, PyObject *const *args, Py_ssize_t nargs, int *fd_out,
                                     PyObject **buf_group_out, PyObject **user_data_out, unsigned int *flags_out,
                                     unsigned long long *base_sequence_out) {
    Py_ssize_t positional_optional_count;

    if (nargs < 2) {
        PyErr_Format(PyExc_TypeError, "%s() missing required arguments 'fd' and 'buf_group'", name);
        return -1;
    }
    if (nargs > 5) {
        PyErr_Format(PyExc_TypeError, "%s() takes at most 5 positional arguments (%zd given)", name, nargs);
        return -1;
    }

    if (parse_socket_fd(args[0], fd_out) < 0) {
        return -1;
    }
    if (!PyObject_TypeCheck(args[1], &UringApiBufGroup_Type)) {
        PyErr_SetString(PyExc_TypeError, "buf_group must be a BufGroup");
        return -1;
    }
    *buf_group_out = args[1];

    positional_optional_count = nargs - 2;
    if (positional_optional_count > 0) {
        *user_data_out = args[2];
    }
    if (positional_optional_count > 1) {
        if (parse_uint_arg(args[3], flags_out) < 0) {
            return -1;
        }
    }
    if (positional_optional_count > 2) {
        if (parse_ull_arg(args[4], base_sequence_out) < 0) {
            return -1;
        }
    }
    return 0;
}

static int parse_send_args(const char *name, PyObject *const *args, Py_ssize_t nargs, Py_ssize_t max_nargs, int *fd_out,
                           Py_buffer *view_out, PyObject **user_data_out, unsigned int *flags_out,
                           unsigned int *zc_flags_out, int parse_zc_flags) {
    if (nargs < 2) {
        PyErr_Format(PyExc_TypeError, "%s() missing required arguments 'fd' and 'data'", name);
        return -1;
    }
    if (nargs > max_nargs) {
        PyErr_Format(PyExc_TypeError, "%s() takes at most %zd positional arguments (%zd given)", name, max_nargs,
                     nargs);
        return -1;
    }
    if (parse_socket_fd(args[0], fd_out) < 0) {
        return -1;
    }
    if (PyObject_GetBuffer(args[1], view_out, PyBUF_STRIDED_RO) < 0) {
        return -1;
    }
    if (nargs > 2) {
        *user_data_out = args[2];
    }
    if (nargs > 3) {
        if (parse_uint_arg(args[3], flags_out) < 0) {
            PyBuffer_Release(view_out);
            return -1;
        }
    }
    if (parse_zc_flags && nargs > 4) {
        if (parse_uint_arg(args[4], zc_flags_out) < 0) {
            PyBuffer_Release(view_out);
            return -1;
        }
    }
    return 0;
}

static int parse_accept_listener_args(const char *name, PyObject *const *args, Py_ssize_t nargs, int *fd_out,
                                      PyObject **user_data_out, unsigned int *flags_out,
                                      unsigned long long *base_sequence_out) {
    Py_ssize_t max_args = base_sequence_out ? 4 : 3;

    if (nargs < 1) {
        PyErr_Format(PyExc_TypeError, "%s() missing required argument 'fd'", name);
        return -1;
    }
    if (nargs > max_args) {
        PyErr_Format(PyExc_TypeError, "%s() takes at most %zd positional arguments (%zd given)", name, max_args, nargs);
        return -1;
    }
    if (parse_socket_fd(args[0], fd_out) < 0) {
        return -1;
    }
    if (nargs > 1) {
        *user_data_out = args[1];
    }
    if (nargs > 2) {
        if (parse_uint_arg(args[2], flags_out) < 0) {
            return -1;
        }
    }
    if (base_sequence_out && nargs > 3) {
        if (parse_ull_arg(args[3], base_sequence_out) < 0) {
            return -1;
        }
    }
    return 0;
}

static int validate_file_io_buffer_length(Py_buffer *view) {
    if (view->len < 0 || (unsigned long long)view->len > UINT_MAX) {
        PyErr_SetString(PyExc_ValueError, "buffer length must fit in uint32_t");
        return -1;
    }
    return 0;
}

static int validate_statx_buffer(Py_buffer *view) {
    if (view->len < URING_API_STATX_BUFFER_SIZE) {
        PyErr_SetString(PyExc_ValueError, "statx buffer must be at least 256 bytes");
        return -1;
    }
    return 0;
}

static PyObject *submit_after_construct(UringApiRing *self, PyObject *completion);

PyObject *UringApiRing_submit_recv_impl(UringApiRing *self, int fd, Py_buffer *view, PyObject *user_data) {
    return submit_after_construct(self, UringApiRing_construct_recv_impl(self, fd, view, user_data));
}

static PyObject *construct_pending_buf_group(UringApiRing *self, UringApiPendingKind kind, int fd,
                                             PyObject *buf_group_obj, unsigned int flags, PyObject *user_data,
                                             unsigned long long base_sequence, int multishot) {
    UringApiBufGroup *buf_group;
    PyObject *completion;
    UringApiCompletionBufGroupState *buf_group_state;

    if (!buf_group_obj || !PyObject_TypeCheck(buf_group_obj, &UringApiBufGroup_Type)) {
        PyErr_SetString(PyExc_TypeError, "buf_group must be a BufGroup");
        return NULL;
    }
    buf_group = (UringApiBufGroup *)buf_group_obj;
    if (buf_group->ring != self) {
        PyErr_SetString(PyExc_ValueError, "buf_group was not created by this ring");
        return NULL;
    }
    if (ring_check_open(self) < 0) {
        return NULL;
    }
    completion = UringApiCompletion_new_pending_buf_group(kind, user_data, buf_group_obj);
    if (!completion) {
        return NULL;
    }
    if (multishot) {
        ((UringApiCompletion *)completion)->multishot = true;
        ((UringApiCompletion *)completion)->sequence = base_sequence;
    }
    buf_group_state = UringApiCompletion_get_buf_group_state((UringApiCompletion *)completion);
    assert(buf_group_state != NULL);
    buf_group_state->fd = fd;
    buf_group_state->flags = flags;
    return completion;
}

PyObject *UringApiRing_construct_recv_buf_impl(UringApiRing *self, int fd, PyObject *buf_group_obj, unsigned int flags,
                                               PyObject *user_data) {
    return construct_pending_buf_group(self, URING_API_PENDING_RECV_BUF, fd, buf_group_obj, flags, user_data, 0, 0);
}

PyObject *UringApiRing_construct_recv_multishot_impl(UringApiRing *self, int fd, PyObject *buf_group_obj,
                                                     unsigned int flags, PyObject *user_data,
                                                     unsigned long long base_sequence) {
    return construct_pending_buf_group(self, URING_API_PENDING_RECV_MULTISHOT, fd, buf_group_obj, flags, user_data,
                                       base_sequence, 1);
}

PyObject *UringApiRing_submit_recv_buf_impl(UringApiRing *self, int fd, PyObject *buf_group_obj, unsigned int flags,
                                            PyObject *user_data) {
    return submit_after_construct(self,
                                  UringApiRing_construct_recv_buf_impl(self, fd, buf_group_obj, flags, user_data));
}

PyObject *UringApiRing_submit_recv_buf(UringApiRing *self, PyObject *args, PyObject *kwargs) {
    static char *keywords[] = {"fd", "buf_group", "user_data", "flags", NULL};
    int fd;
    unsigned int flags = 0;
    PyObject *user_data = Py_None;
    PyObject *buf_group_obj;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "iO!|OI", keywords, &fd, &UringApiBufGroup_Type, &buf_group_obj,
                                     &user_data, &flags)) {
        return NULL;
    }
    return UringApiRing_submit_recv_buf_impl(self, fd, buf_group_obj, flags, user_data);
}

PyObject *UringApiRing_submit_recv_multishot_impl(UringApiRing *self, int fd, PyObject *buf_group_obj,
                                                  unsigned int flags, PyObject *user_data,
                                                  unsigned long long base_sequence) {
    return submit_after_construct(
        self, UringApiRing_construct_recv_multishot_impl(self, fd, buf_group_obj, flags, user_data, base_sequence));
}

static PyObject *construct_pending_view(UringApiRing *self, UringApiPendingKind kind, int fd, Py_buffer *view,
                                        unsigned int flags, unsigned int zc_flags, unsigned long long offset,
                                        PyObject *user_data) {
    PyObject *completion;
    UringApiCompletionViewState *view_state;

    if (ring_check_open(self) < 0) {
        PyBuffer_Release(view);
        return NULL;
    }
    completion = UringApiCompletion_new_pending_view(kind, user_data, view);
    if (!completion) {
        return NULL;
    }
    view_state = UringApiCompletion_get_view_state((UringApiCompletion *)completion);
    assert(view_state != NULL);
    view_state->fd = fd;
    view_state->flags = flags;
    view_state->zc_flags = zc_flags;
    view_state->offset = offset;
    return completion;
}

PyObject *UringApiRing_construct_send_impl(UringApiRing *self, int fd, Py_buffer *view, unsigned int flags,
                                           PyObject *user_data) {
    return construct_pending_view(self, URING_API_PENDING_SEND, fd, view, flags, 0, 0, user_data);
}

PyObject *UringApiRing_construct_send_zc_impl(UringApiRing *self, int fd, Py_buffer *view, unsigned int flags,
                                              unsigned int zc_flags, PyObject *user_data) {
    return construct_pending_view(self, URING_API_PENDING_SEND_ZC, fd, view, flags, zc_flags, 0, user_data);
}

PyObject *UringApiRing_construct_recv_impl(UringApiRing *self, int fd, Py_buffer *view, PyObject *user_data) {
    return construct_pending_view(self, URING_API_PENDING_RECV, fd, view, 0, 0, 0, user_data);
}

PyObject *UringApiRing_construct_read_impl(UringApiRing *self, int fd, Py_buffer *view, unsigned long long offset,
                                           PyObject *user_data) {
    if (validate_file_io_buffer_length(view) < 0) {
        PyBuffer_Release(view);
        return NULL;
    }
    return construct_pending_view(self, URING_API_PENDING_READ, fd, view, 0, 0, offset, user_data);
}

PyObject *UringApiRing_construct_write_impl(UringApiRing *self, int fd, Py_buffer *view, unsigned long long offset,
                                            PyObject *user_data) {
    if (validate_file_io_buffer_length(view) < 0) {
        PyBuffer_Release(view);
        return NULL;
    }
    return construct_pending_view(self, URING_API_PENDING_WRITE, fd, view, 0, 0, offset, user_data);
}

PyObject *UringApiRing_construct_sendto_impl(UringApiRing *self, int fd, Py_buffer *view, PyObject *address,
                                             unsigned int flags, PyObject *user_data) {
    PyObject *completion;
    UringApiCompletionViewSockaddrState *sendto_state;

    if (ring_check_open(self) < 0) {
        PyBuffer_Release(view);
        return NULL;
    }
    completion = UringApiCompletion_new_pending_view_sockaddr(URING_API_PENDING_SENDTO, user_data, view);
    if (!completion) {
        return NULL;
    }
    sendto_state = UringApiCompletion_get_view_sockaddr_state((UringApiCompletion *)completion);
    assert(sendto_state != NULL);
    if (parse_numeric_sockaddr(fd, address, &sendto_state->addr, &sendto_state->addrlen) < 0) {
        Py_DECREF(completion);
        return NULL;
    }
    sendto_state->fd = fd;
    sendto_state->flags = flags;
    return completion;
}

static PyObject *construct_pending_msg(UringApiRing *self, UringApiPendingKind kind, int fd, Py_buffer *view,
                                       PyObject *address, unsigned int flags, PyObject *user_data) {
    PyObject *completion;
    UringApiCompletionMsgState *msg_state;

    if (ring_check_open(self) < 0) {
        PyBuffer_Release(view);
        return NULL;
    }
    if (kind == URING_API_PENDING_RECVMSG) {
        completion = UringApiCompletion_new_pending_recvmsg(kind, user_data, view);
    } else {
        completion = UringApiCompletion_new_pending_sendmsg(kind, user_data, view);
    }
    if (!completion) {
        return NULL;
    }
    msg_state = UringApiCompletion_get_msg_state((UringApiCompletion *)completion);
    assert(msg_state != NULL);
    if (address != NULL && address != Py_None) {
        if (parse_numeric_sockaddr(fd, address, &msg_state->addr, &msg_state->addrlen) < 0) {
            Py_DECREF(completion);
            return NULL;
        }
        msg_state->msg.msg_name = &msg_state->addr;
        msg_state->msg.msg_namelen = msg_state->addrlen;
    }
    msg_state->fd = fd;
    msg_state->flags = flags;
    return completion;
}

PyObject *UringApiRing_construct_recvmsg_impl(UringApiRing *self, int fd, Py_buffer *view, PyObject *user_data) {
    return construct_pending_msg(self, URING_API_PENDING_RECVMSG, fd, view, NULL, 0, user_data);
}

PyObject *UringApiRing_construct_sendmsg_impl(UringApiRing *self, int fd, Py_buffer *view, PyObject *address,
                                              unsigned int flags, PyObject *user_data) {
    return construct_pending_msg(self, URING_API_PENDING_SENDMSG, fd, view, address, flags, user_data);
}

PyObject *UringApiRing_construct_sendmsg_zc_impl(UringApiRing *self, int fd, Py_buffer *view, PyObject *address,
                                                 unsigned int flags, PyObject *user_data) {
    return construct_pending_msg(self, URING_API_PENDING_SENDMSG_ZC, fd, view, address, flags, user_data);
}

PyObject *UringApiRing_construct_connect_impl(UringApiRing *self, int fd, PyObject *address, PyObject *user_data) {
    PyObject *completion;
    UringApiCompletionSockaddrState *sockaddr_state;

    if (ring_check_open(self) < 0) {
        return NULL;
    }
    completion = UringApiCompletion_new_pending_sockaddr(URING_API_PENDING_CONNECT, user_data);
    if (!completion) {
        return NULL;
    }
    sockaddr_state = UringApiCompletion_get_sockaddr_state((UringApiCompletion *)completion);
    assert(sockaddr_state != NULL);
    if (parse_numeric_sockaddr(fd, address, &sockaddr_state->addr, &sockaddr_state->addrlen) < 0) {
        Py_DECREF(completion);
        return NULL;
    }
    sockaddr_state->fd = fd;
    return completion;
}

static int constructed_kind_ready(UringApiCompletion *completion) {
    UringApiCompletionViewState *view_state;
    UringApiCompletionViewSockaddrState *view_sockaddr_state;
    UringApiCompletionMsgState *msg_state;
    UringApiCompletionSockaddrState *sockaddr_state;

    switch (completion->kind) {
    case URING_API_PENDING_SEND:
    case URING_API_PENDING_SEND_ZC:
    case URING_API_PENDING_RECV:
    case URING_API_PENDING_READ:
    case URING_API_PENDING_WRITE:
        view_state = UringApiCompletion_get_view_state(completion);
        return view_state != NULL && view_state->fd >= 0;
    case URING_API_PENDING_SENDTO:
        view_sockaddr_state = UringApiCompletion_get_view_sockaddr_state(completion);
        return view_sockaddr_state != NULL && view_sockaddr_state->fd >= 0;
    case URING_API_PENDING_RECVMSG:
    case URING_API_PENDING_SENDMSG:
    case URING_API_PENDING_SENDMSG_ZC:
        msg_state = UringApiCompletion_get_msg_state(completion);
        return msg_state != NULL && msg_state->fd >= 0;
    case URING_API_PENDING_CONNECT:
        sockaddr_state = UringApiCompletion_get_sockaddr_state(completion);
        return sockaddr_state != NULL && sockaddr_state->fd >= 0;
    case URING_API_PENDING_RECV_BUF:
    case URING_API_PENDING_RECV_MULTISHOT: {
        UringApiCompletionBufGroupState *buf_group_state = UringApiCompletion_get_buf_group_state(completion);
        return buf_group_state != NULL && buf_group_state->fd >= 0;
    }
    case URING_API_PENDING_OPENAT: {
        UringApiCompletionPathState *path_state = UringApiCompletion_get_path_state(completion);
        return path_state != NULL && path_state->constructed;
    }
    case URING_API_PENDING_STATX: {
        UringApiCompletionStatxState *statx_state = UringApiCompletion_get_statx_state(completion);
        return statx_state != NULL && statx_state->constructed;
    }
    case URING_API_PENDING_STATX_FDSIZE: {
        UringApiCompletionStatxFdsizeState *statx_fdsize_state = UringApiCompletion_get_statx_fdsize_state(completion);
        return statx_fdsize_state != NULL && statx_fdsize_state->constructed;
    }
    case URING_API_PENDING_ACCEPT:
    case URING_API_PENDING_POLL:
    case URING_API_PENDING_POLL_MULTISHOT:
    case URING_API_PENDING_CLOSE:
    case URING_API_PENDING_SHUTDOWN:
    case URING_API_PENDING_SOCKET: {
        UringApiCompletionScalarState *scalar_state = UringApiCompletion_get_scalar_state(completion);
        return scalar_state != NULL && scalar_state->constructed;
    }
    case URING_API_PENDING_CANCEL:
    case URING_API_PENDING_POLL_REMOVE:
        return completion->cancel_target != NULL;
    default:
        return 0;
    }
}

/* Nowait SQE identity: tagged token, optional CQE_SKIP_SUCCESS. No Completion*. */
static int submit_prepared_nowait(UringApiRing *self, struct io_uring_sqe *sqe, unsigned int kind, int fd) {
    io_uring_sqe_set_data64(sqe, uring_api_make_nowait_user_data(kind, fd));
    if (self->ring.features & IORING_FEAT_CQE_SKIP) {
        sqe->flags |= IOSQE_CQE_SKIP_SUCCESS;
    }
    return 0;
}

static int nowait_kind_ok(UringApiPendingKind kind) {
    return kind == URING_API_PENDING_CLOSE || kind == URING_API_PENDING_SHUTDOWN || kind == URING_API_PENDING_CANCEL ||
           kind == URING_API_PENDING_POLL_REMOVE;
}

static int nowait_advisory_fd(UringApiCompletion *completion) {
    UringApiCompletionScalarState *scalar_state;

    if (completion->kind != URING_API_PENDING_CLOSE && completion->kind != URING_API_PENDING_SHUTDOWN) {
        return -1;
    }
    scalar_state = UringApiCompletion_get_scalar_state(completion);
    assert(scalar_state != NULL);
    return scalar_state->fd;
}

/* Caller holds the ring critical section. On success the completion is prepared.
 * Waitable ops hold an in-flight ref until CQE delivery. Nowait ops stamp a
 * tagged SQE and do not retain the Completion. Kind is checked before prepared
 * so a non-constructed handle reports "not constructed", not "already prepared". */
static int prepare_one_constructed(UringApiRing *self, UringApiCompletion *completion) {
    UringApiCompletionViewState *view_state;
    UringApiCompletionViewSockaddrState *view_sockaddr_state;
    UringApiCompletionMsgState *msg_state;
    UringApiCompletionSockaddrState *sockaddr_state;
    struct io_uring_sqe *sqe;

    if (!constructed_kind_ready(completion)) {
        PyErr_SetString(PyExc_ValueError, "prepare() only accepts constructed completions");
        return -1;
    }
    if (completion->prepared) {
        PyErr_SetString(PyExc_ValueError, "completion is already prepared");
        return -1;
    }
    if (completion->nowait && !nowait_kind_ok(completion->kind)) {
        PyErr_SetString(PyExc_ValueError, "nowait is only valid for close, shutdown, cancel, and poll_remove");
        return -1;
    }

    sqe = get_sqe(self);
    if (!sqe) {
        return -1;
    }
    switch (completion->kind) {
    case URING_API_PENDING_SEND:
        view_state = UringApiCompletion_get_view_state(completion);
        assert(view_state != NULL && view_state->has_view);
        io_uring_prep_send(sqe, view_state->fd, view_state->view.buf, (size_t)view_state->view.len,
                           (int)view_state->flags);
        break;
    case URING_API_PENDING_SEND_ZC:
        view_state = UringApiCompletion_get_view_state(completion);
        assert(view_state != NULL && view_state->has_view);
        io_uring_prep_send_zc(sqe, view_state->fd, view_state->view.buf, (size_t)view_state->view.len,
                              (int)view_state->flags, view_state->zc_flags);
        break;
    case URING_API_PENDING_RECV:
        view_state = UringApiCompletion_get_view_state(completion);
        assert(view_state != NULL && view_state->has_view);
        io_uring_prep_recv(sqe, view_state->fd, view_state->view.buf, (size_t)view_state->view.len, 0);
        break;
    case URING_API_PENDING_READ:
        view_state = UringApiCompletion_get_view_state(completion);
        assert(view_state != NULL && view_state->has_view);
        io_uring_prep_read(sqe, view_state->fd, view_state->view.buf, (unsigned)view_state->view.len,
                           (__u64)view_state->offset);
        break;
    case URING_API_PENDING_WRITE:
        view_state = UringApiCompletion_get_view_state(completion);
        assert(view_state != NULL && view_state->has_view);
        io_uring_prep_write(sqe, view_state->fd, view_state->view.buf, (unsigned)view_state->view.len,
                            (__u64)view_state->offset);
        break;
    case URING_API_PENDING_SENDTO:
        view_sockaddr_state = UringApiCompletion_get_view_sockaddr_state(completion);
        assert(view_sockaddr_state != NULL && view_sockaddr_state->has_view);
        io_uring_prep_sendto(sqe, view_sockaddr_state->fd, view_sockaddr_state->view.buf,
                             (size_t)view_sockaddr_state->view.len, (int)view_sockaddr_state->flags,
                             (struct sockaddr *)&view_sockaddr_state->addr, view_sockaddr_state->addrlen);
        break;
    case URING_API_PENDING_RECVMSG:
        msg_state = UringApiCompletion_get_msg_state(completion);
        assert(msg_state != NULL && msg_state->has_view);
        io_uring_prep_recvmsg(sqe, msg_state->fd, &msg_state->msg, (int)msg_state->flags);
        break;
    case URING_API_PENDING_SENDMSG:
        msg_state = UringApiCompletion_get_msg_state(completion);
        assert(msg_state != NULL && msg_state->has_view);
        io_uring_prep_sendmsg(sqe, msg_state->fd, &msg_state->msg, msg_state->flags);
        break;
    case URING_API_PENDING_SENDMSG_ZC:
        msg_state = UringApiCompletion_get_msg_state(completion);
        assert(msg_state != NULL && msg_state->has_view);
        io_uring_prep_sendmsg_zc(sqe, msg_state->fd, &msg_state->msg, msg_state->flags);
        break;
    case URING_API_PENDING_CONNECT:
        sockaddr_state = UringApiCompletion_get_sockaddr_state(completion);
        assert(sockaddr_state != NULL);
        io_uring_prep_connect(sqe, sockaddr_state->fd, (struct sockaddr *)&sockaddr_state->addr,
                              sockaddr_state->addrlen);
        break;
    case URING_API_PENDING_RECV_BUF: {
        UringApiCompletionBufGroupState *buf_group_state = UringApiCompletion_get_buf_group_state(completion);
        UringApiBufGroup *buf_group;

        assert(buf_group_state != NULL && buf_group_state->buf_group != NULL);
        buf_group = (UringApiBufGroup *)buf_group_state->buf_group;
        io_uring_prep_recv(sqe, buf_group_state->fd, NULL, (size_t)buf_group->buffer_size, (int)buf_group_state->flags);
        sqe->flags |= IOSQE_BUFFER_SELECT;
        sqe->buf_group = buf_group->group_id;
        break;
    }
    case URING_API_PENDING_RECV_MULTISHOT: {
        UringApiCompletionBufGroupState *buf_group_state = UringApiCompletion_get_buf_group_state(completion);
        UringApiBufGroup *buf_group;

        assert(buf_group_state != NULL && buf_group_state->buf_group != NULL);
        buf_group = (UringApiBufGroup *)buf_group_state->buf_group;
        io_uring_prep_recv_multishot(sqe, buf_group_state->fd, NULL, 0, (int)buf_group_state->flags);
        sqe->flags |= IOSQE_BUFFER_SELECT;
        sqe->buf_group = buf_group->group_id;
        break;
    }
    case URING_API_PENDING_OPENAT: {
        UringApiCompletionPathState *path_state = UringApiCompletion_get_path_state(completion);

        assert(path_state != NULL && path_state->path != NULL);
        io_uring_prep_openat(sqe, path_state->dfd, path_state->path, path_state->flags, path_state->mode);
        break;
    }
    case URING_API_PENDING_STATX: {
        UringApiCompletionStatxState *statx_state = UringApiCompletion_get_statx_state(completion);

        assert(statx_state != NULL && statx_state->path != NULL && statx_state->has_view);
        io_uring_prep_statx(sqe, statx_state->dfd, statx_state->path, statx_state->flags, statx_state->mask,
                            (struct statx *)statx_state->view.buf);
        break;
    }
    case URING_API_PENDING_STATX_FDSIZE: {
        UringApiCompletionStatxFdsizeState *statx_fdsize_state = UringApiCompletion_get_statx_fdsize_state(completion);

        assert(statx_fdsize_state != NULL);
        io_uring_prep_statx(sqe, statx_fdsize_state->fd, "", URING_API_AT_EMPTY_PATH, URING_API_STATX_SIZE_MASK,
                            (struct statx *)statx_fdsize_state->buf);
        break;
    }
    case URING_API_PENDING_ACCEPT: {
        UringApiCompletionScalarState *scalar_state = UringApiCompletion_get_scalar_state(completion);

        assert(scalar_state != NULL);
        if (completion->multishot) {
            io_uring_prep_multishot_accept(sqe, scalar_state->fd, NULL, NULL, scalar_state->flags);
        } else {
            io_uring_prep_accept(sqe, scalar_state->fd, NULL, NULL, scalar_state->flags);
        }
        break;
    }
    case URING_API_PENDING_POLL: {
        UringApiCompletionScalarState *scalar_state = UringApiCompletion_get_scalar_state(completion);

        assert(scalar_state != NULL);
        io_uring_prep_poll_add(sqe, scalar_state->fd, scalar_state->poll_mask);
        break;
    }
    case URING_API_PENDING_POLL_MULTISHOT: {
        UringApiCompletionScalarState *scalar_state = UringApiCompletion_get_scalar_state(completion);

        assert(scalar_state != NULL);
        io_uring_prep_poll_multishot(sqe, scalar_state->fd, scalar_state->poll_mask);
        break;
    }
    case URING_API_PENDING_CLOSE: {
        UringApiCompletionScalarState *scalar_state = UringApiCompletion_get_scalar_state(completion);

        assert(scalar_state != NULL);
        io_uring_prep_close(sqe, scalar_state->fd);
        break;
    }
    case URING_API_PENDING_SHUTDOWN: {
        UringApiCompletionScalarState *scalar_state = UringApiCompletion_get_scalar_state(completion);

        assert(scalar_state != NULL);
        io_uring_prep_shutdown(sqe, scalar_state->fd, scalar_state->how);
        break;
    }
    case URING_API_PENDING_SOCKET: {
        UringApiCompletionScalarState *scalar_state = UringApiCompletion_get_scalar_state(completion);

        assert(scalar_state != NULL);
        io_uring_prep_socket(sqe, scalar_state->domain, scalar_state->type, scalar_state->protocol,
                             scalar_state->flags);
        break;
    }
    case URING_API_PENDING_CANCEL:
        assert(completion->cancel_target != NULL);
        io_uring_prep_cancel(sqe, completion->cancel_target, 0);
        break;
    case URING_API_PENDING_POLL_REMOVE:
        assert(completion->cancel_target != NULL);
        io_uring_prep_poll_remove(sqe, (unsigned long long)(uintptr_t)completion->cancel_target);
        break;
    default:
        /* kind already validated */
        break;
    }
    if (completion->nowait) {
        if (submit_prepared_nowait(self, sqe, (unsigned int)completion->kind, nowait_advisory_fd(completion)) < 0) {
            return -1;
        }
        completion->prepared = true;
        return 0;
    }
    sqe_set_completion(self, sqe, (PyObject *)completion);
    /* in-flight ref: matches the leftover alloc ref on submit_* paths */
    Py_INCREF(completion);
    return 0;
}

static PyObject *submit_after_construct(UringApiRing *self, PyObject *completion) {
    int failed = 0;

    if (!completion) {
        return NULL;
    }
    Py_BEGIN_CRITICAL_SECTION(self);
    if (ring_check_open(self) < 0) {
        failed = 1;
    } else if (prepare_one_constructed(self, (UringApiCompletion *)completion) < 0) {
        failed = 1;
    }
    Py_END_CRITICAL_SECTION();
    if (failed) {
        Py_DECREF(completion);
        return NULL;
    }
    return completion;
}

static int prepare_constructed(UringApiRing *self, PyObject *const *items, Py_ssize_t count) {
    Py_ssize_t i;

    for (i = 0; i < count; i++) {
        PyObject *item = items[i];

        if (!PyObject_TypeCheck(item, &UringApiCompletion_Type)) {
            PyErr_SetString(PyExc_TypeError, "prepare() items must be Completion objects");
            return -1;
        }
        if (prepare_one_constructed(self, (UringApiCompletion *)item) < 0) {
            return -1;
        }
    }
    return 0;
}

int UringApiRing_prepare_impl(UringApiRing *self, PyObject *completions, int *prepared_out) {
    PyObject *seq = NULL;
    PyObject *single[1];
    PyObject *const *items;
    Py_ssize_t count;
    int failed = 0;
    int prepared = 0;

    if (PyObject_TypeCheck(completions, &UringApiCompletion_Type)) {
        single[0] = completions;
        items = single;
        count = 1;
    } else {
        seq = PySequence_Fast(completions, "completions must be a Completion or a sequence of Completions");
        if (!seq) {
            return -1;
        }
        items = PySequence_Fast_ITEMS(seq);
        count = PySequence_Fast_GET_SIZE(seq);
    }

    Py_BEGIN_CRITICAL_SECTION(self);
    if (ring_check_open(self) < 0) {
        failed = 1;
    } else if (prepare_constructed(self, items, count) < 0) {
        failed = 1;
        /* count how many in the prefix are now prepared */
        {
            Py_ssize_t i;
            for (i = 0; i < count; i++) {
                if (PyObject_TypeCheck(items[i], &UringApiCompletion_Type) &&
                    ((UringApiCompletion *)items[i])->prepared) {
                    prepared++;
                }
            }
        }
    } else {
        prepared = (int)count;
    }
    Py_END_CRITICAL_SECTION();

    Py_XDECREF(seq);
    if (prepared_out) {
        *prepared_out = prepared;
    }
    return failed ? -1 : 0;
}

PyObject *UringApiRing_submit_send_impl(UringApiRing *self, int fd, Py_buffer *view, unsigned int flags,
                                        PyObject *user_data) {
    return submit_after_construct(self, UringApiRing_construct_send_impl(self, fd, view, flags, user_data));
}

PyObject *UringApiRing_submit_read_impl(UringApiRing *self, int fd, Py_buffer *view, unsigned long long offset,
                                        PyObject *user_data) {
    return submit_after_construct(self, UringApiRing_construct_read_impl(self, fd, view, offset, user_data));
}

PyObject *UringApiRing_submit_write_impl(UringApiRing *self, int fd, Py_buffer *view, unsigned long long offset,
                                         PyObject *user_data) {
    return submit_after_construct(self, UringApiRing_construct_write_impl(self, fd, view, offset, user_data));
}

PyObject *UringApiRing_construct_openat_impl(UringApiRing *self, int dfd, PyObject *path, int flags, unsigned int mode,
                                             PyObject *user_data) {
    PyObject *completion;
    UringApiCompletionPathState *path_state;

    if (ring_check_open(self) < 0) {
        return NULL;
    }
    completion = UringApiCompletion_new_pending_path(URING_API_PENDING_OPENAT, user_data, path);
    if (!completion) {
        return NULL;
    }
    path_state = UringApiCompletion_get_path_state((UringApiCompletion *)completion);
    assert(path_state != NULL && path_state->path != NULL);
    path_state->dfd = dfd;
    path_state->flags = flags;
    path_state->mode = mode;
    path_state->constructed = true;
    return completion;
}

PyObject *UringApiRing_construct_statx_impl(UringApiRing *self, int dfd, PyObject *path, int flags, unsigned int mask,
                                            Py_buffer *view, PyObject *user_data) {
    PyObject *completion;
    UringApiCompletionStatxState *statx_state;

    if (validate_statx_buffer(view) < 0) {
        PyBuffer_Release(view);
        return NULL;
    }
    if (ring_check_open(self) < 0) {
        PyBuffer_Release(view);
        return NULL;
    }
    completion = UringApiCompletion_new_pending_statx(URING_API_PENDING_STATX, user_data, path, view);
    if (!completion) {
        return NULL;
    }
    statx_state = UringApiCompletion_get_statx_state((UringApiCompletion *)completion);
    assert(statx_state != NULL && statx_state->path != NULL);
    statx_state->dfd = dfd;
    statx_state->flags = flags;
    statx_state->mask = mask;
    statx_state->constructed = true;
    return completion;
}

PyObject *UringApiRing_construct_statx_fdsize_impl(UringApiRing *self, int fd, PyObject *user_data) {
    PyObject *completion;
    UringApiCompletionStatxFdsizeState *statx_fdsize_state;

    if (ring_check_open(self) < 0) {
        return NULL;
    }
    completion = UringApiCompletion_new_pending_statx_fdsize(user_data);
    if (!completion) {
        return NULL;
    }
    statx_fdsize_state = UringApiCompletion_get_statx_fdsize_state((UringApiCompletion *)completion);
    assert(statx_fdsize_state != NULL);
    statx_fdsize_state->fd = fd;
    statx_fdsize_state->constructed = true;
    return completion;
}

PyObject *UringApiRing_submit_openat_impl(UringApiRing *self, int dfd, PyObject *path, int flags, unsigned int mode,
                                          PyObject *user_data) {
    return submit_after_construct(self, UringApiRing_construct_openat_impl(self, dfd, path, flags, mode, user_data));
}

PyObject *UringApiRing_submit_statx_impl(UringApiRing *self, int dfd, PyObject *path, int flags, unsigned int mask,
                                         Py_buffer *view, PyObject *user_data) {
    return submit_after_construct(self,
                                  UringApiRing_construct_statx_impl(self, dfd, path, flags, mask, view, user_data));
}

PyObject *UringApiRing_submit_send_zc_impl(UringApiRing *self, int fd, Py_buffer *view, unsigned int flags,
                                           unsigned int zc_flags, PyObject *user_data) {
    return submit_after_construct(self,
                                  UringApiRing_construct_send_zc_impl(self, fd, view, flags, zc_flags, user_data));
}

PyObject *UringApiRing_submit_sendto_impl(UringApiRing *self, int fd, Py_buffer *view, PyObject *address,
                                          unsigned int flags, PyObject *user_data) {
    return submit_after_construct(self, UringApiRing_construct_sendto_impl(self, fd, view, address, flags, user_data));
}

PyObject *UringApiRing_submit_recvmsg_impl(UringApiRing *self, int fd, Py_buffer *view, PyObject *user_data) {
    return submit_after_construct(self, UringApiRing_construct_recvmsg_impl(self, fd, view, user_data));
}

PyObject *UringApiRing_submit_sendmsg_impl(UringApiRing *self, int fd, Py_buffer *view, PyObject *address,
                                           unsigned int flags, PyObject *user_data) {
    return submit_after_construct(self, UringApiRing_construct_sendmsg_impl(self, fd, view, address, flags, user_data));
}

PyObject *UringApiRing_submit_sendmsg_zc_impl(UringApiRing *self, int fd, Py_buffer *view, PyObject *address,
                                              unsigned int flags, PyObject *user_data) {
    return submit_after_construct(self,
                                  UringApiRing_construct_sendmsg_zc_impl(self, fd, view, address, flags, user_data));
}

static PyObject *construct_pending_scalar(UringApiRing *self, UringApiPendingKind kind, PyObject *user_data,
                                          int multishot, unsigned long long base_sequence) {
    PyObject *completion;
    UringApiCompletionScalarState *scalar_state;

    if (ring_check_open(self) < 0) {
        return NULL;
    }
    completion = UringApiCompletion_new_pending_scalar(kind, user_data);
    if (!completion) {
        return NULL;
    }
    if (multishot) {
        ((UringApiCompletion *)completion)->multishot = true;
        ((UringApiCompletion *)completion)->sequence = base_sequence;
    }
    scalar_state = UringApiCompletion_get_scalar_state((UringApiCompletion *)completion);
    assert(scalar_state != NULL);
    scalar_state->constructed = true;
    return completion;
}

PyObject *UringApiRing_construct_accept_impl(UringApiRing *self, int fd, unsigned int flags, PyObject *user_data) {
    PyObject *completion = construct_pending_scalar(self, URING_API_PENDING_ACCEPT, user_data, 0, 0);
    UringApiCompletionScalarState *scalar_state;

    if (!completion) {
        return NULL;
    }
    scalar_state = UringApiCompletion_get_scalar_state((UringApiCompletion *)completion);
    scalar_state->fd = fd;
    scalar_state->flags = flags;
    return completion;
}

PyObject *UringApiRing_construct_accept_multishot_impl(UringApiRing *self, int fd, unsigned int flags,
                                                       PyObject *user_data, unsigned long long base_sequence) {
    PyObject *completion = construct_pending_scalar(self, URING_API_PENDING_ACCEPT, user_data, 1, base_sequence);
    UringApiCompletionScalarState *scalar_state;

    if (!completion) {
        return NULL;
    }
    scalar_state = UringApiCompletion_get_scalar_state((UringApiCompletion *)completion);
    scalar_state->fd = fd;
    scalar_state->flags = flags;
    return completion;
}

PyObject *UringApiRing_construct_poll_impl(UringApiRing *self, int fd, unsigned int poll_mask, PyObject *user_data) {
    PyObject *completion = construct_pending_scalar(self, URING_API_PENDING_POLL, user_data, 0, 0);
    UringApiCompletionScalarState *scalar_state;

    if (!completion) {
        return NULL;
    }
    scalar_state = UringApiCompletion_get_scalar_state((UringApiCompletion *)completion);
    scalar_state->fd = fd;
    scalar_state->poll_mask = poll_mask;
    return completion;
}

PyObject *UringApiRing_construct_poll_multishot_impl(UringApiRing *self, int fd, unsigned int poll_mask,
                                                     PyObject *user_data) {
    PyObject *completion = construct_pending_scalar(self, URING_API_PENDING_POLL_MULTISHOT, user_data, 1, 0);
    UringApiCompletionScalarState *scalar_state;

    if (!completion) {
        return NULL;
    }
    scalar_state = UringApiCompletion_get_scalar_state((UringApiCompletion *)completion);
    scalar_state->fd = fd;
    scalar_state->poll_mask = poll_mask;
    return completion;
}

PyObject *UringApiRing_construct_shutdown_impl(UringApiRing *self, int fd, int how, PyObject *user_data) {
    PyObject *completion = construct_pending_scalar(self, URING_API_PENDING_SHUTDOWN, user_data, 0, 0);
    UringApiCompletionScalarState *scalar_state;

    if (!completion) {
        return NULL;
    }
    scalar_state = UringApiCompletion_get_scalar_state((UringApiCompletion *)completion);
    scalar_state->fd = fd;
    scalar_state->how = how;
    return completion;
}

PyObject *UringApiRing_construct_close_impl(UringApiRing *self, int fd, PyObject *user_data) {
    PyObject *completion = construct_pending_scalar(self, URING_API_PENDING_CLOSE, user_data, 0, 0);
    UringApiCompletionScalarState *scalar_state;

    if (!completion) {
        return NULL;
    }
    scalar_state = UringApiCompletion_get_scalar_state((UringApiCompletion *)completion);
    scalar_state->fd = fd;
    return completion;
}

PyObject *UringApiRing_construct_socket_impl(UringApiRing *self, int domain, int type, int protocol, unsigned int flags,
                                             PyObject *user_data) {
    PyObject *completion = construct_pending_scalar(self, URING_API_PENDING_SOCKET, user_data, 0, 0);
    UringApiCompletionScalarState *scalar_state;

    if (!completion) {
        return NULL;
    }
    scalar_state = UringApiCompletion_get_scalar_state((UringApiCompletion *)completion);
    scalar_state->domain = domain;
    scalar_state->type = type;
    scalar_state->protocol = protocol;
    scalar_state->flags = flags;
    return completion;
}

PyObject *UringApiRing_submit_accept_impl(UringApiRing *self, int fd, unsigned int flags, PyObject *user_data) {
    return submit_after_construct(self, UringApiRing_construct_accept_impl(self, fd, flags, user_data));
}

PyObject *UringApiRing_submit_accept_multishot_impl(UringApiRing *self, int fd, unsigned int flags, PyObject *user_data,
                                                    unsigned long long base_sequence) {
    return submit_after_construct(
        self, UringApiRing_construct_accept_multishot_impl(self, fd, flags, user_data, base_sequence));
}

PyObject *UringApiRing_submit_connect_impl(UringApiRing *self, int fd, PyObject *address, PyObject *user_data) {
    return submit_after_construct(self, UringApiRing_construct_connect_impl(self, fd, address, user_data));
}

PyObject *UringApiRing_submit_poll_impl(UringApiRing *self, int fd, unsigned int poll_mask, PyObject *user_data) {
    return submit_after_construct(self, UringApiRing_construct_poll_impl(self, fd, poll_mask, user_data));
}

PyObject *UringApiRing_submit_poll_multishot_impl(UringApiRing *self, int fd, unsigned int poll_mask,
                                                  PyObject *user_data) {
    return submit_after_construct(self, UringApiRing_construct_poll_multishot_impl(self, fd, poll_mask, user_data));
}

static int poll_remove_target_is_valid(UringApiCompletion *target) {
    if (target->kind != URING_API_PENDING_POLL && target->kind != URING_API_PENDING_POLL_MULTISHOT) {
        PyErr_SetString(PyExc_ValueError,
                        "poll_remove target must be a pending poll or poll_multishot completion handle");
        return 0;
    }
    if (target->result != NULL) {
        PyErr_SetString(PyExc_ValueError,
                        "poll_remove target must be the original submit handle, not a delivered completion");
        return 0;
    }
    return 1;
}

static PyObject *construct_pending_cancel(UringApiRing *self, UringApiPendingKind kind, PyObject *target_completion,
                                          PyObject *user_data) {
    UringApiCompletion *completion;

    if (!PyObject_TypeCheck(target_completion, &UringApiCompletion_Type)) {
        PyErr_SetString(PyExc_TypeError, "completion must be a Completion");
        return NULL;
    }
    if (kind == URING_API_PENDING_POLL_REMOVE &&
        !poll_remove_target_is_valid((UringApiCompletion *)target_completion)) {
        return NULL;
    }
    if (ring_check_open(self) < 0) {
        return NULL;
    }
    if (user_data == NULL || user_data == Py_None) {
        user_data = target_completion;
    }
    completion = (UringApiCompletion *)UringApiCompletion_new_pending(kind, user_data);
    if (!completion) {
        return NULL;
    }
    completion->cancel_target = Py_NewRef(target_completion);
    return (PyObject *)completion;
}

PyObject *UringApiRing_construct_poll_remove_impl(UringApiRing *self, PyObject *target_completion,
                                                  PyObject *user_data) {
    return construct_pending_cancel(self, URING_API_PENDING_POLL_REMOVE, target_completion, user_data);
}

PyObject *UringApiRing_construct_cancel_impl(UringApiRing *self, PyObject *target_completion, PyObject *user_data) {
    return construct_pending_cancel(self, URING_API_PENDING_CANCEL, target_completion, user_data);
}

PyObject *UringApiRing_submit_poll_remove_impl(UringApiRing *self, PyObject *target_completion, PyObject *user_data) {
    return submit_after_construct(self, UringApiRing_construct_poll_remove_impl(self, target_completion, user_data));
}

PyObject *UringApiRing_submit_cancel_impl(UringApiRing *self, PyObject *target_completion, PyObject *user_data) {
    return submit_after_construct(self, UringApiRing_construct_cancel_impl(self, target_completion, user_data));
}

PyObject *UringApiRing_submit_shutdown_impl(UringApiRing *self, int fd, int how, PyObject *user_data) {
    return submit_after_construct(self, UringApiRing_construct_shutdown_impl(self, fd, how, user_data));
}

PyObject *UringApiRing_submit_close_impl(UringApiRing *self, int fd, PyObject *user_data) {
    return submit_after_construct(self, UringApiRing_construct_close_impl(self, fd, user_data));
}

static PyObject *mark_constructed_nowait(PyObject *completion) {
    if (!completion) {
        return NULL;
    }
    if (UringApiCompletion_set_nowait_flag((UringApiCompletion *)completion, 1) < 0) {
        Py_DECREF(completion);
        return NULL;
    }
    return completion;
}

static PyObject *submit_nowait_after_construct(UringApiRing *self, PyObject *completion) {
    completion = submit_after_construct(self, completion);
    if (!completion) {
        return NULL;
    }
    Py_DECREF(completion);
    Py_RETURN_NONE;
}

PyObject *UringApiRing_construct_close_nowait_impl(UringApiRing *self, int fd) {
    return mark_constructed_nowait(UringApiRing_construct_close_impl(self, fd, Py_None));
}

PyObject *UringApiRing_construct_shutdown_nowait_impl(UringApiRing *self, int fd, int how) {
    return mark_constructed_nowait(UringApiRing_construct_shutdown_impl(self, fd, how, Py_None));
}

PyObject *UringApiRing_construct_cancel_nowait_impl(UringApiRing *self, PyObject *target_completion) {
    return mark_constructed_nowait(UringApiRing_construct_cancel_impl(self, target_completion, Py_None));
}

PyObject *UringApiRing_construct_poll_remove_nowait_impl(UringApiRing *self, PyObject *target_completion) {
    return mark_constructed_nowait(UringApiRing_construct_poll_remove_impl(self, target_completion, Py_None));
}

PyObject *UringApiRing_submit_close_nowait_impl(UringApiRing *self, int fd) {
    return submit_nowait_after_construct(self, UringApiRing_construct_close_nowait_impl(self, fd));
}

PyObject *UringApiRing_submit_shutdown_nowait_impl(UringApiRing *self, int fd, int how) {
    return submit_nowait_after_construct(self, UringApiRing_construct_shutdown_nowait_impl(self, fd, how));
}

PyObject *UringApiRing_submit_cancel_nowait_impl(UringApiRing *self, PyObject *target_completion) {
    return submit_nowait_after_construct(self, UringApiRing_construct_cancel_nowait_impl(self, target_completion));
}

PyObject *UringApiRing_submit_poll_remove_nowait_impl(UringApiRing *self, PyObject *target_completion) {
    return submit_nowait_after_construct(self, UringApiRing_construct_poll_remove_nowait_impl(self, target_completion));
}

PyObject *UringApiRing_submit_socket_impl(UringApiRing *self, int domain, int type, int protocol, unsigned int flags,
                                          PyObject *user_data) {
    return submit_after_construct(self,
                                  UringApiRing_construct_socket_impl(self, domain, type, protocol, flags, user_data));
}

PyObject *UringApiRing_submit_read(UringApiRing *self, PyObject *args, PyObject *kwargs) {
    static char *keywords[] = {"fd", "buf", "offset", "user_data", NULL};
    Py_buffer view;
    int fd;
    long long offset;
    PyObject *user_data = Py_None;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "iw*L|O", keywords, &fd, &view, &offset, &user_data)) {
        return NULL;
    }
    if (offset < 0) {
        PyBuffer_Release(&view);
        PyErr_SetString(PyExc_ValueError, "offset must be non-negative");
        return NULL;
    }
    return UringApiRing_submit_read_impl(self, fd, &view, (unsigned long long)offset, user_data);
}

PyObject *UringApiRing_submit_write(UringApiRing *self, PyObject *args, PyObject *kwargs) {
    static char *keywords[] = {"fd", "data", "offset", "user_data", NULL};
    Py_buffer view;
    int fd;
    long long offset;
    PyObject *user_data = Py_None;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "iy*L|O", keywords, &fd, &view, &offset, &user_data)) {
        return NULL;
    }
    if (offset < 0) {
        PyBuffer_Release(&view);
        PyErr_SetString(PyExc_ValueError, "offset must be non-negative");
        return NULL;
    }
    return UringApiRing_submit_write_impl(self, fd, &view, (unsigned long long)offset, user_data);
}

PyObject *UringApiRing_submit_openat(UringApiRing *self, PyObject *args, PyObject *kwargs) {
    static char *keywords[] = {"path", "flags", "mode", "user_data", "dfd", NULL};
    PyObject *path;
    int flags;
    unsigned int mode = 0;
    int dfd = -100; /* AT_FDCWD */
    PyObject *user_data = Py_None;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "Oi|IOi", keywords, &path, &flags, &mode, &user_data, &dfd)) {
        return NULL;
    }
    return UringApiRing_submit_openat_impl(self, dfd, path, flags, mode, user_data);
}

PyObject *UringApiRing_submit_statx(UringApiRing *self, PyObject *args, PyObject *kwargs) {
    static char *keywords[] = {"dfd", "path", "flags", "mask", "buf", "user_data", NULL};
    Py_buffer view;
    PyObject *path;
    int dfd;
    int flags;
    unsigned int mask;
    PyObject *user_data = Py_None;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "iOIIw*|O", keywords, &dfd, &path, &flags, &mask, &view,
                                     &user_data)) {
        return NULL;
    }
    return UringApiRing_submit_statx_impl(self, dfd, path, flags, mask, &view, user_data);
}

PyObject *UringApiRing_submit_statx_fdsize_impl(UringApiRing *self, int fd, PyObject *user_data) {
    return submit_after_construct(self, UringApiRing_construct_statx_fdsize_impl(self, fd, user_data));
}

PyObject *UringApiRing_submit_statx_fdsize(UringApiRing *self, PyObject *args, PyObject *kwargs) {
    static char *keywords[] = {"fd", "user_data", NULL};
    int fd;
    PyObject *user_data = Py_None;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "i|O", keywords, &fd, &user_data)) {
        return NULL;
    }
    return UringApiRing_submit_statx_fdsize_impl(self, fd, user_data);
}

PyObject *UringApiRing_submit_recv(UringApiRing *self, PyObject *args, PyObject *kwargs) {
    static char *keywords[] = {"fd", "buf", "user_data", NULL};
    Py_buffer view;
    int fd;
    PyObject *user_data = Py_None;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "iw*|O", keywords, &fd, &view, &user_data)) {
        return NULL;
    }
    return UringApiRing_submit_recv_impl(self, fd, &view, user_data);
}

PyObject *UringApiRing_submit_recv_multishot(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    int fd;
    unsigned int flags = 0;
    unsigned long long base_sequence = 0;
    PyObject *user_data = Py_None;
    PyObject *buf_group_obj;

    if (parse_recv_multishot_args("submit_recv_multishot", args, nargs, &fd, &buf_group_obj, &user_data, &flags,
                                  &base_sequence) < 0) {
        return NULL;
    }

    return UringApiRing_submit_recv_multishot_impl(self, fd, buf_group_obj, flags, user_data, base_sequence);
}

PyObject *UringApiRing_construct_send(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    int fd = -1;
    Py_buffer view;
    PyObject *user_data = Py_None;
    unsigned int flags = 0;

    if (parse_send_args("construct_send", args, nargs, 4, &fd, &view, &user_data, &flags, NULL, 0) < 0) {
        return NULL;
    }
    return UringApiRing_construct_send_impl(self, fd, &view, flags, user_data);
}

PyObject *UringApiRing_construct_send_zc(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    int fd = -1;
    Py_buffer view;
    PyObject *user_data = Py_None;
    unsigned int flags = 0;
    unsigned int zc_flags = 0;

    if (parse_send_args("construct_send_zc", args, nargs, 5, &fd, &view, &user_data, &flags, &zc_flags, 1) < 0) {
        return NULL;
    }
    return UringApiRing_construct_send_zc_impl(self, fd, &view, flags, zc_flags, user_data);
}

PyObject *UringApiRing_construct_recv(UringApiRing *self, PyObject *args, PyObject *kwargs) {
    static char *keywords[] = {"fd", "buf", "user_data", NULL};
    Py_buffer view;
    int fd;
    PyObject *user_data = Py_None;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "iw*|O", keywords, &fd, &view, &user_data)) {
        return NULL;
    }
    return UringApiRing_construct_recv_impl(self, fd, &view, user_data);
}

PyObject *UringApiRing_construct_recv_buf(UringApiRing *self, PyObject *args, PyObject *kwargs) {
    static char *keywords[] = {"fd", "buf_group", "user_data", "flags", NULL};
    int fd;
    unsigned int flags = 0;
    PyObject *user_data = Py_None;
    PyObject *buf_group_obj;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "iO!|OI", keywords, &fd, &UringApiBufGroup_Type, &buf_group_obj,
                                     &user_data, &flags)) {
        return NULL;
    }
    return UringApiRing_construct_recv_buf_impl(self, fd, buf_group_obj, flags, user_data);
}

PyObject *UringApiRing_construct_recv_multishot(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    int fd;
    unsigned int flags = 0;
    unsigned long long base_sequence = 0;
    PyObject *user_data = Py_None;
    PyObject *buf_group_obj;

    if (parse_recv_multishot_args("construct_recv_multishot", args, nargs, &fd, &buf_group_obj, &user_data, &flags,
                                  &base_sequence) < 0) {
        return NULL;
    }
    return UringApiRing_construct_recv_multishot_impl(self, fd, buf_group_obj, flags, user_data, base_sequence);
}

PyObject *UringApiRing_construct_read(UringApiRing *self, PyObject *args, PyObject *kwargs) {
    static char *keywords[] = {"fd", "buf", "offset", "user_data", NULL};
    Py_buffer view;
    int fd;
    long long offset;
    PyObject *user_data = Py_None;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "iw*L|O", keywords, &fd, &view, &offset, &user_data)) {
        return NULL;
    }
    if (offset < 0) {
        PyBuffer_Release(&view);
        PyErr_SetString(PyExc_ValueError, "offset must be non-negative");
        return NULL;
    }
    return UringApiRing_construct_read_impl(self, fd, &view, (unsigned long long)offset, user_data);
}

PyObject *UringApiRing_construct_write(UringApiRing *self, PyObject *args, PyObject *kwargs) {
    static char *keywords[] = {"fd", "data", "offset", "user_data", NULL};
    Py_buffer view;
    int fd;
    long long offset;
    PyObject *user_data = Py_None;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "iy*L|O", keywords, &fd, &view, &offset, &user_data)) {
        return NULL;
    }
    if (offset < 0) {
        PyBuffer_Release(&view);
        PyErr_SetString(PyExc_ValueError, "offset must be non-negative");
        return NULL;
    }
    return UringApiRing_construct_write_impl(self, fd, &view, (unsigned long long)offset, user_data);
}

PyObject *UringApiRing_construct_openat(UringApiRing *self, PyObject *args, PyObject *kwargs) {
    static char *keywords[] = {"path", "flags", "mode", "user_data", "dfd", NULL};
    PyObject *path;
    int flags;
    unsigned int mode = 0;
    int dfd = -100; /* AT_FDCWD */
    PyObject *user_data = Py_None;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "Oi|IOi", keywords, &path, &flags, &mode, &user_data, &dfd)) {
        return NULL;
    }
    return UringApiRing_construct_openat_impl(self, dfd, path, flags, mode, user_data);
}

PyObject *UringApiRing_construct_statx(UringApiRing *self, PyObject *args, PyObject *kwargs) {
    static char *keywords[] = {"dfd", "path", "flags", "mask", "buf", "user_data", NULL};
    Py_buffer view;
    PyObject *path;
    int dfd;
    int flags;
    unsigned int mask;
    PyObject *user_data = Py_None;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "iOIIw*|O", keywords, &dfd, &path, &flags, &mask, &view,
                                     &user_data)) {
        return NULL;
    }
    return UringApiRing_construct_statx_impl(self, dfd, path, flags, mask, &view, user_data);
}

PyObject *UringApiRing_construct_statx_fdsize(UringApiRing *self, PyObject *args, PyObject *kwargs) {
    static char *keywords[] = {"fd", "user_data", NULL};
    int fd;
    PyObject *user_data = Py_None;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "i|O", keywords, &fd, &user_data)) {
        return NULL;
    }
    return UringApiRing_construct_statx_fdsize_impl(self, fd, user_data);
}

PyObject *UringApiRing_construct_sendto(UringApiRing *self, PyObject *args, PyObject *kwargs) {
    static char *keywords[] = {"fd", "data", "address", "user_data", "flags", NULL};
    Py_buffer view;
    int fd;
    unsigned int flags = 0;
    PyObject *address;
    PyObject *user_data = Py_None;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "iy*O|OI", keywords, &fd, &view, &address, &user_data, &flags)) {
        return NULL;
    }
    return UringApiRing_construct_sendto_impl(self, fd, &view, address, flags, user_data);
}

PyObject *UringApiRing_construct_recvmsg(UringApiRing *self, PyObject *args, PyObject *kwargs) {
    static char *keywords[] = {"fd", "buf", "user_data", NULL};
    Py_buffer view;
    int fd;
    PyObject *user_data = Py_None;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "iw*|O", keywords, &fd, &view, &user_data)) {
        return NULL;
    }
    return UringApiRing_construct_recvmsg_impl(self, fd, &view, user_data);
}

PyObject *UringApiRing_construct_sendmsg(UringApiRing *self, PyObject *args, PyObject *kwargs) {
    static char *keywords[] = {"fd", "data", "address", "user_data", "flags", NULL};
    Py_buffer view;
    int fd;
    unsigned int flags = 0;
    PyObject *address = Py_None;
    PyObject *user_data = Py_None;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "iy*|OOI", keywords, &fd, &view, &address, &user_data, &flags)) {
        return NULL;
    }
    return UringApiRing_construct_sendmsg_impl(self, fd, &view, address, flags, user_data);
}

PyObject *UringApiRing_construct_sendmsg_zc(UringApiRing *self, PyObject *args, PyObject *kwargs) {
    static char *keywords[] = {"fd", "data", "address", "user_data", "flags", NULL};
    Py_buffer view;
    int fd;
    unsigned int flags = 0;
    PyObject *address = Py_None;
    PyObject *user_data = Py_None;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "iy*|OOI", keywords, &fd, &view, &address, &user_data, &flags)) {
        return NULL;
    }
    return UringApiRing_construct_sendmsg_zc_impl(self, fd, &view, address, flags, user_data);
}

PyObject *UringApiRing_construct_connect(UringApiRing *self, PyObject *args, PyObject *kwargs) {
    static char *keywords[] = {"fd", "address", "user_data", NULL};
    int fd;
    PyObject *address;
    PyObject *user_data = Py_None;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "iO|O", keywords, &fd, &address, &user_data)) {
        return NULL;
    }
    return UringApiRing_construct_connect_impl(self, fd, address, user_data);
}

PyObject *UringApiRing_construct_accept(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    int fd;
    unsigned int flags = 0;
    PyObject *user_data = Py_None;

    if (parse_accept_listener_args("construct_accept", args, nargs, &fd, &user_data, &flags, NULL) < 0) {
        return NULL;
    }
    return UringApiRing_construct_accept_impl(self, fd, flags, user_data);
}

PyObject *UringApiRing_construct_accept_multishot(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    int fd;
    unsigned int flags = 0;
    unsigned long long base_sequence = 0;
    PyObject *user_data = Py_None;

    if (parse_accept_listener_args("construct_accept_multishot", args, nargs, &fd, &user_data, &flags, &base_sequence) <
        0) {
        return NULL;
    }
    return UringApiRing_construct_accept_multishot_impl(self, fd, flags, user_data, base_sequence);
}

PyObject *UringApiRing_construct_poll(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    int fd;
    unsigned int poll_mask;
    PyObject *user_data = Py_None;

    if (nargs < 2) {
        PyErr_SetString(PyExc_TypeError, "construct_poll() missing required arguments 'fd' and 'mask'");
        return NULL;
    }
    if (nargs > 3) {
        PyErr_Format(PyExc_TypeError, "construct_poll() takes at most 3 positional arguments (%zd given)", nargs);
        return NULL;
    }
    if (parse_socket_fd(args[0], &fd) < 0) {
        return NULL;
    }
    if (parse_uint_arg(args[1], &poll_mask) < 0) {
        return NULL;
    }
    if (nargs > 2) {
        user_data = args[2];
    }
    return UringApiRing_construct_poll_impl(self, fd, poll_mask, user_data);
}

PyObject *UringApiRing_construct_poll_multishot(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    int fd;
    unsigned int poll_mask;
    PyObject *user_data = Py_None;

    if (nargs < 2) {
        PyErr_SetString(PyExc_TypeError, "construct_poll_multishot() missing required arguments 'fd' and 'mask'");
        return NULL;
    }
    if (nargs > 3) {
        PyErr_Format(PyExc_TypeError, "construct_poll_multishot() takes at most 3 positional arguments (%zd given)",
                     nargs);
        return NULL;
    }
    if (parse_socket_fd(args[0], &fd) < 0) {
        return NULL;
    }
    if (parse_uint_arg(args[1], &poll_mask) < 0) {
        return NULL;
    }
    if (nargs > 2) {
        user_data = args[2];
    }
    return UringApiRing_construct_poll_multishot_impl(self, fd, poll_mask, user_data);
}

PyObject *UringApiRing_construct_shutdown(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    int fd;
    int how;
    PyObject *user_data = Py_None;

    if (nargs < 2) {
        PyErr_SetString(PyExc_TypeError, "construct_shutdown() missing required arguments 'fd' and 'how'");
        return NULL;
    }
    if (nargs > 3) {
        PyErr_Format(PyExc_TypeError, "construct_shutdown() takes at most 3 positional arguments (%zd given)", nargs);
        return NULL;
    }
    if (parse_socket_fd(args[0], &fd) < 0) {
        return NULL;
    }
    if (parse_int_arg(args[1], &how) < 0) {
        return NULL;
    }
    if (nargs > 2) {
        user_data = args[2];
    }
    return UringApiRing_construct_shutdown_impl(self, fd, how, user_data);
}

PyObject *UringApiRing_construct_close(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    int fd;
    PyObject *user_data = Py_None;

    if (nargs < 1) {
        PyErr_SetString(PyExc_TypeError, "construct_close() missing required argument 'fd'");
        return NULL;
    }
    if (nargs > 2) {
        PyErr_Format(PyExc_TypeError, "construct_close() takes at most 2 positional arguments (%zd given)", nargs);
        return NULL;
    }
    if (parse_socket_fd(args[0], &fd) < 0) {
        return NULL;
    }
    if (nargs > 1) {
        user_data = args[1];
    }
    return UringApiRing_construct_close_impl(self, fd, user_data);
}

PyObject *UringApiRing_construct_socket(UringApiRing *self, PyObject *args, PyObject *kwargs) {
    static char *keywords[] = {"domain", "type", "protocol", "flags", "user_data", NULL};
    int domain;
    int type;
    int protocol = 0;
    unsigned int flags = 0;
    PyObject *user_data = Py_None;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "ii|iIO", keywords, &domain, &type, &protocol, &flags, &user_data)) {
        return NULL;
    }
    return UringApiRing_construct_socket_impl(self, domain, type, protocol, flags, user_data);
}

PyObject *UringApiRing_construct_poll_remove(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    PyObject *user_data = Py_None;

    if (nargs < 1) {
        PyErr_SetString(PyExc_TypeError, "construct_poll_remove() missing required argument 'completion'");
        return NULL;
    }
    if (nargs > 2) {
        PyErr_Format(PyExc_TypeError, "construct_poll_remove() takes at most 2 positional arguments (%zd given)",
                     nargs);
        return NULL;
    }
    if (nargs > 1) {
        user_data = args[1];
    }
    return UringApiRing_construct_poll_remove_impl(self, args[0], user_data);
}

PyObject *UringApiRing_construct_cancel(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    PyObject *user_data = Py_None;

    if (nargs < 1) {
        PyErr_SetString(PyExc_TypeError, "construct_cancel() missing required argument 'completion'");
        return NULL;
    }
    if (nargs > 2) {
        PyErr_Format(PyExc_TypeError, "construct_cancel() takes at most 2 positional arguments (%zd given)", nargs);
        return NULL;
    }
    if (nargs > 1) {
        user_data = args[1];
    }
    return UringApiRing_construct_cancel_impl(self, args[0], user_data);
}

PyObject *UringApiRing_construct_close_nowait(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    int fd;

    if (nargs != 1) {
        PyErr_SetString(PyExc_TypeError, "construct_close_nowait() takes exactly 1 positional argument");
        return NULL;
    }
    if (parse_socket_fd(args[0], &fd) < 0) {
        return NULL;
    }
    return UringApiRing_construct_close_nowait_impl(self, fd);
}

PyObject *UringApiRing_construct_shutdown_nowait(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    int fd;
    int how;

    if (nargs != 2) {
        PyErr_SetString(PyExc_TypeError, "construct_shutdown_nowait() takes exactly 2 positional arguments");
        return NULL;
    }
    if (parse_socket_fd(args[0], &fd) < 0) {
        return NULL;
    }
    if (parse_int_arg(args[1], &how) < 0) {
        return NULL;
    }
    return UringApiRing_construct_shutdown_nowait_impl(self, fd, how);
}

PyObject *UringApiRing_construct_cancel_nowait(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    if (nargs != 1) {
        PyErr_SetString(PyExc_TypeError, "construct_cancel_nowait() takes exactly 1 positional argument");
        return NULL;
    }
    return UringApiRing_construct_cancel_nowait_impl(self, args[0]);
}

PyObject *UringApiRing_construct_poll_remove_nowait(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    if (nargs != 1) {
        PyErr_SetString(PyExc_TypeError, "construct_poll_remove_nowait() takes exactly 1 positional argument");
        return NULL;
    }
    return UringApiRing_construct_poll_remove_nowait_impl(self, args[0]);
}

PyObject *UringApiRing_prepare(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    int prepared = 0;

    if (nargs != 1) {
        PyErr_SetString(PyExc_TypeError, "prepare() takes exactly 1 positional argument");
        return NULL;
    }
    if (UringApiRing_prepare_impl(self, args[0], &prepared) < 0) {
        return NULL;
    }
    return PyLong_FromLong(prepared);
}

PyObject *UringApiRing_submit_send(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    Py_buffer view;
    int fd;
    unsigned int flags = 0;
    PyObject *user_data = Py_None;

    if (parse_send_args("submit_send", args, nargs, 4, &fd, &view, &user_data, &flags, NULL, 0) < 0) {
        return NULL;
    }
    return UringApiRing_submit_send_impl(self, fd, &view, flags, user_data);
}

PyObject *UringApiRing_submit_send_zc(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    Py_buffer view;
    int fd;
    unsigned int flags = 0;
    unsigned int zc_flags = 0;
    PyObject *user_data = Py_None;

    if (parse_send_args("submit_send_zc", args, nargs, 5, &fd, &view, &user_data, &flags, &zc_flags, 1) < 0) {
        return NULL;
    }
    return UringApiRing_submit_send_zc_impl(self, fd, &view, flags, zc_flags, user_data);
}

PyObject *UringApiRing_submit_sendto(UringApiRing *self, PyObject *args, PyObject *kwargs) {
    static char *keywords[] = {"fd", "data", "address", "user_data", "flags", NULL};
    Py_buffer view;
    int fd;
    unsigned int flags = 0;
    PyObject *address;
    PyObject *user_data = Py_None;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "iy*O|OI", keywords, &fd, &view, &address, &user_data, &flags)) {
        return NULL;
    }
    return UringApiRing_submit_sendto_impl(self, fd, &view, address, flags, user_data);
}

PyObject *UringApiRing_submit_recvmsg(UringApiRing *self, PyObject *args, PyObject *kwargs) {
    static char *keywords[] = {"fd", "buf", "user_data", NULL};
    Py_buffer view;
    int fd;
    PyObject *user_data = Py_None;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "iw*|O", keywords, &fd, &view, &user_data)) {
        return NULL;
    }
    return UringApiRing_submit_recvmsg_impl(self, fd, &view, user_data);
}

PyObject *UringApiRing_submit_sendmsg(UringApiRing *self, PyObject *args, PyObject *kwargs) {
    static char *keywords[] = {"fd", "data", "address", "user_data", "flags", NULL};
    Py_buffer view;
    int fd;
    unsigned int flags = 0;
    PyObject *address = Py_None;
    PyObject *user_data = Py_None;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "iy*|OOI", keywords, &fd, &view, &address, &user_data, &flags)) {
        return NULL;
    }
    return UringApiRing_submit_sendmsg_impl(self, fd, &view, address, flags, user_data);
}

PyObject *UringApiRing_submit_sendmsg_zc(UringApiRing *self, PyObject *args, PyObject *kwargs) {
    static char *keywords[] = {"fd", "data", "address", "user_data", "flags", NULL};
    Py_buffer view;
    int fd;
    unsigned int flags = 0;
    PyObject *address = Py_None;
    PyObject *user_data = Py_None;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "iy*|OOI", keywords, &fd, &view, &address, &user_data, &flags)) {
        return NULL;
    }
    return UringApiRing_submit_sendmsg_zc_impl(self, fd, &view, address, flags, user_data);
}

PyObject *UringApiRing_submit_accept(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    int fd;
    unsigned int flags = 0;
    PyObject *user_data = Py_None;

    if (parse_accept_listener_args("submit_accept", args, nargs, &fd, &user_data, &flags, NULL) < 0) {
        return NULL;
    }
    return UringApiRing_submit_accept_impl(self, fd, flags, user_data);
}

PyObject *UringApiRing_submit_accept_multishot(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    int fd;
    unsigned int flags = 0;
    unsigned long long base_sequence = 0;
    PyObject *user_data = Py_None;

    if (parse_accept_listener_args("submit_accept_multishot", args, nargs, &fd, &user_data, &flags, &base_sequence) <
        0) {
        return NULL;
    }
    return UringApiRing_submit_accept_multishot_impl(self, fd, flags, user_data, base_sequence);
}

PyObject *UringApiRing_submit_connect(UringApiRing *self, PyObject *args, PyObject *kwargs) {
    static char *keywords[] = {"fd", "address", "user_data", NULL};
    int fd;
    PyObject *address;
    PyObject *user_data = Py_None;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "iO|O", keywords, &fd, &address, &user_data)) {
        return NULL;
    }
    return UringApiRing_submit_connect_impl(self, fd, address, user_data);
}

PyObject *UringApiRing_submit_poll(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    int fd;
    unsigned int poll_mask;
    PyObject *user_data = Py_None;

    if (nargs < 2) {
        PyErr_SetString(PyExc_TypeError, "submit_poll() missing required arguments 'fd' and 'mask'");
        return NULL;
    }
    if (nargs > 3) {
        PyErr_Format(PyExc_TypeError, "submit_poll() takes at most 3 positional arguments (%zd given)", nargs);
        return NULL;
    }
    if (parse_socket_fd(args[0], &fd) < 0) {
        return NULL;
    }
    if (parse_uint_arg(args[1], &poll_mask) < 0) {
        return NULL;
    }
    if (nargs > 2) {
        user_data = args[2];
    }
    return UringApiRing_submit_poll_impl(self, fd, poll_mask, user_data);
}

PyObject *UringApiRing_submit_poll_multishot(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    int fd;
    unsigned int poll_mask;
    PyObject *user_data = Py_None;

    if (nargs < 2) {
        PyErr_SetString(PyExc_TypeError, "submit_poll_multishot() missing required arguments 'fd' and 'mask'");
        return NULL;
    }
    if (nargs > 3) {
        PyErr_Format(PyExc_TypeError, "submit_poll_multishot() takes at most 3 positional arguments (%zd given)",
                     nargs);
        return NULL;
    }
    if (parse_socket_fd(args[0], &fd) < 0) {
        return NULL;
    }
    if (parse_uint_arg(args[1], &poll_mask) < 0) {
        return NULL;
    }
    if (nargs > 2) {
        user_data = args[2];
    }
    return UringApiRing_submit_poll_multishot_impl(self, fd, poll_mask, user_data);
}

PyObject *UringApiRing_submit_poll_remove(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    PyObject *user_data = Py_None;

    if (nargs < 1) {
        PyErr_SetString(PyExc_TypeError, "submit_poll_remove() missing required argument 'completion'");
        return NULL;
    }
    if (nargs > 2) {
        PyErr_Format(PyExc_TypeError, "submit_poll_remove() takes at most 2 positional arguments (%zd given)", nargs);
        return NULL;
    }
    if (!PyObject_TypeCheck(args[0], &UringApiCompletion_Type)) {
        PyErr_SetString(PyExc_TypeError, "completion must be a Completion");
        return NULL;
    }
    if (nargs > 1) {
        user_data = args[1];
    }
    return UringApiRing_submit_poll_remove_impl(self, args[0], user_data);
}

PyObject *UringApiRing_submit_poll_remove_nowait(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    if (nargs != 1) {
        PyErr_SetString(PyExc_TypeError, "submit_poll_remove_nowait() takes exactly 1 positional argument");
        return NULL;
    }
    if (!PyObject_TypeCheck(args[0], &UringApiCompletion_Type)) {
        PyErr_SetString(PyExc_TypeError, "completion must be a Completion");
        return NULL;
    }
    return UringApiRing_submit_poll_remove_nowait_impl(self, args[0]);
}

PyObject *UringApiRing_submit_cancel(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    PyObject *user_data = Py_None;

    if (nargs < 1) {
        PyErr_SetString(PyExc_TypeError, "submit_cancel() missing required argument 'completion'");
        return NULL;
    }
    if (nargs > 2) {
        PyErr_Format(PyExc_TypeError, "submit_cancel() takes at most 2 positional arguments (%zd given)", nargs);
        return NULL;
    }
    if (!PyObject_TypeCheck(args[0], &UringApiCompletion_Type)) {
        PyErr_SetString(PyExc_TypeError, "completion must be a Completion");
        return NULL;
    }
    if (nargs > 1) {
        user_data = args[1];
    }
    return UringApiRing_submit_cancel_impl(self, args[0], user_data);
}

PyObject *UringApiRing_submit_cancel_nowait(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    if (nargs != 1) {
        PyErr_SetString(PyExc_TypeError, "submit_cancel_nowait() takes exactly 1 positional argument");
        return NULL;
    }
    if (!PyObject_TypeCheck(args[0], &UringApiCompletion_Type)) {
        PyErr_SetString(PyExc_TypeError, "completion must be a Completion");
        return NULL;
    }
    return UringApiRing_submit_cancel_nowait_impl(self, args[0]);
}

PyObject *UringApiRing_submit_shutdown(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    int fd;
    int how;
    PyObject *user_data = Py_None;

    if (nargs < 2) {
        PyErr_SetString(PyExc_TypeError, "submit_shutdown() missing required arguments 'fd' and 'how'");
        return NULL;
    }
    if (nargs > 3) {
        PyErr_Format(PyExc_TypeError, "submit_shutdown() takes at most 3 positional arguments (%zd given)", nargs);
        return NULL;
    }
    if (parse_socket_fd(args[0], &fd) < 0) {
        return NULL;
    }
    if (parse_int_arg(args[1], &how) < 0) {
        return NULL;
    }
    if (nargs > 2) {
        user_data = args[2];
    }
    return UringApiRing_submit_shutdown_impl(self, fd, how, user_data);
}

PyObject *UringApiRing_submit_shutdown_nowait(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    int fd;
    int how;

    if (nargs != 2) {
        PyErr_SetString(PyExc_TypeError, "submit_shutdown_nowait() takes exactly 2 positional arguments");
        return NULL;
    }
    if (parse_socket_fd(args[0], &fd) < 0) {
        return NULL;
    }
    if (parse_int_arg(args[1], &how) < 0) {
        return NULL;
    }
    return UringApiRing_submit_shutdown_nowait_impl(self, fd, how);
}

PyObject *UringApiRing_submit_close(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    int fd;
    PyObject *user_data = Py_None;

    if (nargs < 1) {
        PyErr_SetString(PyExc_TypeError, "submit_close() missing required argument 'fd'");
        return NULL;
    }
    if (nargs > 2) {
        PyErr_Format(PyExc_TypeError, "submit_close() takes at most 2 positional arguments (%zd given)", nargs);
        return NULL;
    }
    if (parse_socket_fd(args[0], &fd) < 0) {
        return NULL;
    }
    if (nargs > 1) {
        user_data = args[1];
    }
    return UringApiRing_submit_close_impl(self, fd, user_data);
}

PyObject *UringApiRing_submit_close_nowait(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    int fd;

    if (nargs != 1) {
        PyErr_SetString(PyExc_TypeError, "submit_close_nowait() takes exactly 1 positional argument");
        return NULL;
    }
    if (parse_socket_fd(args[0], &fd) < 0) {
        return NULL;
    }
    return UringApiRing_submit_close_nowait_impl(self, fd);
}

PyObject *UringApiRing_submit_socket(UringApiRing *self, PyObject *args, PyObject *kwargs) {
    static char *keywords[] = {"domain", "type", "protocol", "flags", "user_data", NULL};
    int domain;
    int type;
    int protocol = 0;
    unsigned int flags = 0;
    PyObject *user_data = Py_None;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "ii|iIO", keywords, &domain, &type, &protocol, &flags, &user_data)) {
        return NULL;
    }
    return UringApiRing_submit_socket_impl(self, domain, type, protocol, flags, user_data);
}
