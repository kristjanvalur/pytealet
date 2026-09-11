/*
 * Construct factories and prepare_* sugar for the _uring_api Ring type.
 */

#include "uring_api_construct.h"
#include "uring_api_bufgroup.h"
#include "uring_api_completion.h"
#include "uring_api_core.h"
#include "uring_api_prepare.h"
#include "uring_api_probe.h"
#include "uring_api_statx.h"

static PyObject *prepare_after_construct(UringApiRing *self, PyObject *completion);

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

/* first-leg index is Completion.sequence, not SQE cargo. seed before prepare. */
static PyObject *seed_multishot_sequence(PyObject *completion, unsigned long long base_sequence) {
    if (completion != NULL) {
        ((UringApiCompletion *)completion)->sequence = base_sequence;
    }
    return completion;
}

static int parse_recv_multishot_args(const char *name, PyObject *const *args, Py_ssize_t nargs, int *fd_out,
                                     PyObject **buf_group_out, unsigned int *flags_out, PyObject **user_data_out,
                                     unsigned long long *sequence_out) {
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
    if (nargs > 2) {
        if (parse_uint_arg(args[2], flags_out) < 0) {
            return -1;
        }
    }
    if (nargs > 3) {
        *user_data_out = args[3];
    }
    if (nargs > 4) {
        if (parse_ull_arg(args[4], sequence_out) < 0) {
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
    /* fd, data, [flags], [zc_flags], [user_data] */
    if (nargs > 2) {
        if (parse_uint_arg(args[2], flags_out) < 0) {
            PyBuffer_Release(view_out);
            return -1;
        }
    }
    if (parse_zc_flags) {
        if (nargs > 3) {
            if (parse_uint_arg(args[3], zc_flags_out) < 0) {
                PyBuffer_Release(view_out);
                return -1;
            }
        }
        if (nargs > 4) {
            *user_data_out = args[4];
        }
    } else if (nargs > 3) {
        *user_data_out = args[3];
    }
    return 0;
}

static int parse_accept_listener_args(const char *name, PyObject *const *args, Py_ssize_t nargs, int *fd_out,
                                      unsigned int *flags_out, PyObject **user_data_out,
                                      unsigned long long *sequence_out) {
    Py_ssize_t max_nargs = sequence_out != NULL ? 4 : 3;

    if (nargs < 1) {
        PyErr_Format(PyExc_TypeError, "%s() missing required argument 'fd'", name);
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
    if (nargs > 1) {
        if (parse_uint_arg(args[1], flags_out) < 0) {
            return -1;
        }
    }
    if (nargs > 2) {
        *user_data_out = args[2];
    }
    if (sequence_out != NULL && nargs > 3) {
        if (parse_ull_arg(args[3], sequence_out) < 0) {
            return -1;
        }
    }
    return 0;
}

static int parse_poll_multishot_args(const char *name, PyObject *const *args, Py_ssize_t nargs, int *fd_out,
                                     unsigned int *mask_out, PyObject **user_data_out,
                                     unsigned long long *sequence_out) {
    if (nargs < 2) {
        PyErr_Format(PyExc_TypeError, "%s() missing required arguments 'fd' and 'mask'", name);
        return -1;
    }
    if (nargs > 4) {
        PyErr_Format(PyExc_TypeError, "%s() takes at most 4 positional arguments (%zd given)", name, nargs);
        return -1;
    }
    if (parse_socket_fd(args[0], fd_out) < 0) {
        return -1;
    }
    if (parse_uint_arg(args[1], mask_out) < 0) {
        return -1;
    }
    if (nargs > 2) {
        *user_data_out = args[2];
    }
    if (nargs > 3) {
        if (parse_ull_arg(args[3], sequence_out) < 0) {
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

PyObject *UringApiRing_prepare_recv_impl(UringApiRing *self, int fd, Py_buffer *view, unsigned int flags,
                                         PyObject *user_data) {
    return prepare_after_construct(self, UringApiRing_construct_recv_impl(self, fd, view, flags, user_data));
}

static PyObject *construct_pending_buf_group(UringApiRing *self, UringApiPendingKind kind, int fd,
                                             PyObject *buf_group_obj, unsigned int flags, PyObject *user_data,
                                             int multishot) {
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
        completion_set_bit((UringApiCompletion *)completion, URING_API_C_MULTISHOT);
    }
    buf_group_state = UringApiCompletion_get_buf_group_state((UringApiCompletion *)completion);
    assert(buf_group_state != NULL);
    buf_group_state->fd = fd;
    buf_group_state->flags = flags;
    return completion;
}

PyObject *UringApiRing_construct_recv_buf_impl(UringApiRing *self, int fd, PyObject *buf_group_obj, unsigned int flags,
                                               PyObject *user_data) {
    return construct_pending_buf_group(self, URING_API_PENDING_RECV_BUF, fd, buf_group_obj, flags, user_data, 0);
}

PyObject *UringApiRing_construct_recv_multishot_impl(UringApiRing *self, int fd, PyObject *buf_group_obj,
                                                     unsigned int flags, PyObject *user_data) {
    return construct_pending_buf_group(self, URING_API_PENDING_RECV_MULTISHOT, fd, buf_group_obj, flags, user_data, 1);
}

PyObject *UringApiRing_prepare_recv_buf_impl(UringApiRing *self, int fd, PyObject *buf_group_obj, unsigned int flags,
                                             PyObject *user_data) {
    return prepare_after_construct(self,
                                   UringApiRing_construct_recv_buf_impl(self, fd, buf_group_obj, flags, user_data));
}

PyObject *UringApiRing_prepare_recv_buf(UringApiRing *self, URING_API_PARSE_ARGS) {
    static char *keywords[] = {"fd", "buf_group", "flags", "user_data", NULL};
    int fd;
    unsigned int flags = 0;
    PyObject *user_data = Py_None;
    PyObject *buf_group_obj;

    if (!URING_API_PARSE_KEYWORDS("iO!|IO", keywords, &fd, &UringApiBufGroup_Type, &buf_group_obj, &flags,
                                  &user_data)) {
        return NULL;
    }
    return UringApiRing_prepare_recv_buf_impl(self, fd, buf_group_obj, flags, user_data);
}

PyObject *UringApiRing_prepare_recv_multishot_impl(UringApiRing *self, int fd, PyObject *buf_group_obj,
                                                   unsigned int flags, PyObject *user_data,
                                                   unsigned long long base_sequence) {
    return prepare_after_construct(self, seed_multishot_sequence(UringApiRing_construct_recv_multishot_impl(
                                                                     self, fd, buf_group_obj, flags, user_data),
                                                                 base_sequence));
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

PyObject *UringApiRing_construct_send_all_impl(UringApiRing *self, int fd, Py_buffer *view, unsigned int flags,
                                               PyObject *user_data) {
    return construct_pending_view(self, URING_API_PENDING_SEND_ALL, fd, view, flags, 0, 0, user_data);
}

PyObject *UringApiRing_construct_send_zc_impl(UringApiRing *self, int fd, Py_buffer *view, unsigned int flags,
                                              unsigned int zc_flags, PyObject *user_data) {
    return construct_pending_view(self, URING_API_PENDING_SEND_ZC, fd, view, flags, zc_flags, 0, user_data);
}

PyObject *UringApiRing_construct_recv_impl(UringApiRing *self, int fd, Py_buffer *view, unsigned int flags,
                                           PyObject *user_data) {
    return construct_pending_view(self, URING_API_PENDING_RECV, fd, view, flags, 0, 0, user_data);
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

PyObject *UringApiRing_construct_recvmsg_impl(UringApiRing *self, int fd, Py_buffer *view, unsigned int flags,
                                              PyObject *user_data) {
    return construct_pending_msg(self, URING_API_PENDING_RECVMSG, fd, view, NULL, flags, user_data);
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

static PyObject *prepare_after_construct(UringApiRing *self, PyObject *completion) {
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

PyObject *UringApiRing_prepare_send_impl(UringApiRing *self, int fd, Py_buffer *view, unsigned int flags,
                                         PyObject *user_data) {
    return prepare_after_construct(self, UringApiRing_construct_send_impl(self, fd, view, flags, user_data));
}

PyObject *UringApiRing_prepare_send_all_impl(UringApiRing *self, int fd, Py_buffer *view, unsigned int flags,
                                             PyObject *user_data) {
    return prepare_after_construct(self, UringApiRing_construct_send_all_impl(self, fd, view, flags, user_data));
}

PyObject *UringApiRing_prepare_read_impl(UringApiRing *self, int fd, Py_buffer *view, unsigned long long offset,
                                         PyObject *user_data) {
    return prepare_after_construct(self, UringApiRing_construct_read_impl(self, fd, view, offset, user_data));
}

PyObject *UringApiRing_prepare_write_impl(UringApiRing *self, int fd, Py_buffer *view, unsigned long long offset,
                                          PyObject *user_data) {
    return prepare_after_construct(self, UringApiRing_construct_write_impl(self, fd, view, offset, user_data));
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

PyObject *UringApiRing_prepare_openat_impl(UringApiRing *self, int dfd, PyObject *path, int flags, unsigned int mode,
                                           PyObject *user_data) {
    return prepare_after_construct(self, UringApiRing_construct_openat_impl(self, dfd, path, flags, mode, user_data));
}

PyObject *UringApiRing_prepare_statx_impl(UringApiRing *self, int dfd, PyObject *path, int flags, unsigned int mask,
                                          Py_buffer *view, PyObject *user_data) {
    return prepare_after_construct(self,
                                   UringApiRing_construct_statx_impl(self, dfd, path, flags, mask, view, user_data));
}

PyObject *UringApiRing_prepare_send_zc_impl(UringApiRing *self, int fd, Py_buffer *view, unsigned int flags,
                                            unsigned int zc_flags, PyObject *user_data) {
    return prepare_after_construct(self,
                                   UringApiRing_construct_send_zc_impl(self, fd, view, flags, zc_flags, user_data));
}

PyObject *UringApiRing_prepare_sendto_impl(UringApiRing *self, int fd, Py_buffer *view, PyObject *address,
                                           unsigned int flags, PyObject *user_data) {
    return prepare_after_construct(self, UringApiRing_construct_sendto_impl(self, fd, view, address, flags, user_data));
}

PyObject *UringApiRing_prepare_recvmsg_impl(UringApiRing *self, int fd, Py_buffer *view, unsigned int flags,
                                            PyObject *user_data) {
    return prepare_after_construct(self, UringApiRing_construct_recvmsg_impl(self, fd, view, flags, user_data));
}

PyObject *UringApiRing_prepare_sendmsg_impl(UringApiRing *self, int fd, Py_buffer *view, PyObject *address,
                                            unsigned int flags, PyObject *user_data) {
    return prepare_after_construct(self,
                                   UringApiRing_construct_sendmsg_impl(self, fd, view, address, flags, user_data));
}

PyObject *UringApiRing_prepare_sendmsg_zc_impl(UringApiRing *self, int fd, Py_buffer *view, PyObject *address,
                                               unsigned int flags, PyObject *user_data) {
    return prepare_after_construct(self,
                                   UringApiRing_construct_sendmsg_zc_impl(self, fd, view, address, flags, user_data));
}

static PyObject *construct_pending_scalar(UringApiRing *self, UringApiPendingKind kind, PyObject *user_data,
                                          int multishot) {
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
        completion_set_bit((UringApiCompletion *)completion, URING_API_C_MULTISHOT);
    }
    scalar_state = UringApiCompletion_get_scalar_state((UringApiCompletion *)completion);
    assert(scalar_state != NULL);
    scalar_state->constructed = true;
    return completion;
}

PyObject *UringApiRing_construct_accept_impl(UringApiRing *self, int fd, unsigned int flags, PyObject *user_data) {
    PyObject *completion = construct_pending_scalar(self, URING_API_PENDING_ACCEPT, user_data, 0);
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
                                                       PyObject *user_data) {
    PyObject *completion = construct_pending_scalar(self, URING_API_PENDING_ACCEPT, user_data, 1);
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
    PyObject *completion = construct_pending_scalar(self, URING_API_PENDING_POLL, user_data, 0);
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
    PyObject *completion = construct_pending_scalar(self, URING_API_PENDING_POLL_MULTISHOT, user_data, 1);
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
    PyObject *completion = construct_pending_scalar(self, URING_API_PENDING_SHUTDOWN, user_data, 0);
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
    PyObject *completion = construct_pending_scalar(self, URING_API_PENDING_CLOSE, user_data, 0);
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
    PyObject *completion = construct_pending_scalar(self, URING_API_PENDING_SOCKET, user_data, 0);
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

PyObject *UringApiRing_prepare_accept_impl(UringApiRing *self, int fd, unsigned int flags, PyObject *user_data) {
    return prepare_after_construct(self, UringApiRing_construct_accept_impl(self, fd, flags, user_data));
}

PyObject *UringApiRing_prepare_accept_multishot_impl(UringApiRing *self, int fd, unsigned int flags,
                                                     PyObject *user_data, unsigned long long base_sequence) {
    return prepare_after_construct(
        self, seed_multishot_sequence(UringApiRing_construct_accept_multishot_impl(self, fd, flags, user_data),
                                      base_sequence));
}

PyObject *UringApiRing_prepare_connect_impl(UringApiRing *self, int fd, PyObject *address, PyObject *user_data) {
    return prepare_after_construct(self, UringApiRing_construct_connect_impl(self, fd, address, user_data));
}

PyObject *UringApiRing_prepare_poll_impl(UringApiRing *self, int fd, unsigned int poll_mask, PyObject *user_data) {
    return prepare_after_construct(self, UringApiRing_construct_poll_impl(self, fd, poll_mask, user_data));
}

PyObject *UringApiRing_prepare_poll_multishot_impl(UringApiRing *self, int fd, unsigned int poll_mask,
                                                   PyObject *user_data, unsigned long long base_sequence) {
    return prepare_after_construct(
        self, seed_multishot_sequence(UringApiRing_construct_poll_multishot_impl(self, fd, poll_mask, user_data),
                                      base_sequence));
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

PyObject *UringApiRing_prepare_poll_remove_impl(UringApiRing *self, PyObject *target_completion, PyObject *user_data) {
    return prepare_after_construct(self, UringApiRing_construct_poll_remove_impl(self, target_completion, user_data));
}

PyObject *UringApiRing_prepare_cancel_impl(UringApiRing *self, PyObject *target_completion, PyObject *user_data) {
    return prepare_after_construct(self, UringApiRing_construct_cancel_impl(self, target_completion, user_data));
}

PyObject *UringApiRing_prepare_shutdown_impl(UringApiRing *self, int fd, int how, PyObject *user_data) {
    return prepare_after_construct(self, UringApiRing_construct_shutdown_impl(self, fd, how, user_data));
}

PyObject *UringApiRing_prepare_close_impl(UringApiRing *self, int fd, PyObject *user_data) {
    return prepare_after_construct(self, UringApiRing_construct_close_impl(self, fd, user_data));
}

static PyObject *mark_constructed_nowait(PyObject *completion) {
    if (!completion) {
        return NULL;
    }
    if (UringApiCompletion_set_skip_all_flag((UringApiCompletion *)completion, 1) < 0) {
        Py_DECREF(completion);
        return NULL;
    }
    return completion;
}

static PyObject *prepare_nowait_after_construct(UringApiRing *self, PyObject *completion) {
    completion = prepare_after_construct(self, completion);
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

PyObject *UringApiRing_prepare_close_nowait_impl(UringApiRing *self, int fd) {
    return prepare_nowait_after_construct(self, UringApiRing_construct_close_nowait_impl(self, fd));
}

PyObject *UringApiRing_prepare_shutdown_nowait_impl(UringApiRing *self, int fd, int how) {
    return prepare_nowait_after_construct(self, UringApiRing_construct_shutdown_nowait_impl(self, fd, how));
}

PyObject *UringApiRing_prepare_cancel_nowait_impl(UringApiRing *self, PyObject *target_completion) {
    return prepare_nowait_after_construct(self, UringApiRing_construct_cancel_nowait_impl(self, target_completion));
}

PyObject *UringApiRing_prepare_poll_remove_nowait_impl(UringApiRing *self, PyObject *target_completion) {
    return prepare_nowait_after_construct(self,
                                          UringApiRing_construct_poll_remove_nowait_impl(self, target_completion));
}

PyObject *UringApiRing_prepare_socket_impl(UringApiRing *self, int domain, int type, int protocol, unsigned int flags,
                                           PyObject *user_data) {
    return prepare_after_construct(self,
                                   UringApiRing_construct_socket_impl(self, domain, type, protocol, flags, user_data));
}

PyObject *UringApiRing_prepare_read(UringApiRing *self, URING_API_PARSE_ARGS) {
    static char *keywords[] = {"fd", "buf", "offset", "user_data", NULL};
    Py_buffer view;
    int fd;
    long long offset;
    PyObject *user_data = Py_None;

    if (!URING_API_PARSE_KEYWORDS("iw*L|O", keywords, &fd, &view, &offset, &user_data)) {
        return NULL;
    }
    if (offset < 0) {
        PyBuffer_Release(&view);
        PyErr_SetString(PyExc_ValueError, "offset must be non-negative");
        return NULL;
    }
    return UringApiRing_prepare_read_impl(self, fd, &view, (unsigned long long)offset, user_data);
}

PyObject *UringApiRing_prepare_write(UringApiRing *self, URING_API_PARSE_ARGS) {
    static char *keywords[] = {"fd", "data", "offset", "user_data", NULL};
    Py_buffer view;
    int fd;
    long long offset;
    PyObject *user_data = Py_None;

    if (!URING_API_PARSE_KEYWORDS("iy*L|O", keywords, &fd, &view, &offset, &user_data)) {
        return NULL;
    }
    if (offset < 0) {
        PyBuffer_Release(&view);
        PyErr_SetString(PyExc_ValueError, "offset must be non-negative");
        return NULL;
    }
    return UringApiRing_prepare_write_impl(self, fd, &view, (unsigned long long)offset, user_data);
}

PyObject *UringApiRing_prepare_openat(UringApiRing *self, URING_API_PARSE_ARGS) {
    static char *keywords[] = {"dfd", "path", "flags", "mode", "user_data", NULL};
    PyObject *path;
    int flags;
    unsigned int mode = 0;
    int dfd;
    PyObject *user_data = Py_None;

    if (!URING_API_PARSE_KEYWORDS("iOi|IO", keywords, &dfd, &path, &flags, &mode, &user_data)) {
        return NULL;
    }
    return UringApiRing_prepare_openat_impl(self, dfd, path, flags, mode, user_data);
}

PyObject *UringApiRing_prepare_statx(UringApiRing *self, URING_API_PARSE_ARGS) {
    static char *keywords[] = {"dfd", "path", "flags", "mask", "buf", "user_data", NULL};
    Py_buffer view;
    PyObject *path;
    int dfd;
    int flags;
    unsigned int mask;
    PyObject *user_data = Py_None;

    if (!URING_API_PARSE_KEYWORDS("iOIIw*|O", keywords, &dfd, &path, &flags, &mask, &view, &user_data)) {
        return NULL;
    }
    return UringApiRing_prepare_statx_impl(self, dfd, path, flags, mask, &view, user_data);
}

PyObject *UringApiRing_prepare_statx_fdsize_impl(UringApiRing *self, int fd, PyObject *user_data) {
    return prepare_after_construct(self, UringApiRing_construct_statx_fdsize_impl(self, fd, user_data));
}

PyObject *UringApiRing_prepare_statx_fdsize(UringApiRing *self, URING_API_PARSE_ARGS) {
    static char *keywords[] = {"fd", "user_data", NULL};
    int fd;
    PyObject *user_data = Py_None;

    if (!URING_API_PARSE_KEYWORDS("i|O", keywords, &fd, &user_data)) {
        return NULL;
    }
    return UringApiRing_prepare_statx_fdsize_impl(self, fd, user_data);
}

PyObject *UringApiRing_prepare_recv(UringApiRing *self, URING_API_PARSE_ARGS) {
    static char *keywords[] = {"fd", "buf", "flags", "user_data", NULL};
    Py_buffer view;
    int fd;
    unsigned int flags = 0;
    PyObject *user_data = Py_None;

    if (!URING_API_PARSE_KEYWORDS("iw*|IO", keywords, &fd, &view, &flags, &user_data)) {
        return NULL;
    }
    return UringApiRing_prepare_recv_impl(self, fd, &view, flags, user_data);
}

PyObject *UringApiRing_prepare_recv_multishot(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    int fd;
    unsigned int flags = 0;
    unsigned long long base_sequence = 0;
    PyObject *user_data = Py_None;
    PyObject *buf_group_obj;

    if (parse_recv_multishot_args("prepare_recv_multishot", args, nargs, &fd, &buf_group_obj, &flags, &user_data,
                                  &base_sequence) < 0) {
        return NULL;
    }

    return UringApiRing_prepare_recv_multishot_impl(self, fd, buf_group_obj, flags, user_data, base_sequence);
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

PyObject *UringApiRing_construct_send_all(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    int fd = -1;
    Py_buffer view;
    PyObject *user_data = Py_None;
    unsigned int flags = 0;

    if (parse_send_args("construct_send_all", args, nargs, 4, &fd, &view, &user_data, &flags, NULL, 0) < 0) {
        return NULL;
    }
    return UringApiRing_construct_send_all_impl(self, fd, &view, flags, user_data);
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

PyObject *UringApiRing_construct_recv(UringApiRing *self, URING_API_PARSE_ARGS) {
    static char *keywords[] = {"fd", "buf", "flags", "user_data", NULL};
    Py_buffer view;
    int fd;
    unsigned int flags = 0;
    PyObject *user_data = Py_None;

    if (!URING_API_PARSE_KEYWORDS("iw*|IO", keywords, &fd, &view, &flags, &user_data)) {
        return NULL;
    }
    return UringApiRing_construct_recv_impl(self, fd, &view, flags, user_data);
}

PyObject *UringApiRing_construct_recv_buf(UringApiRing *self, URING_API_PARSE_ARGS) {
    static char *keywords[] = {"fd", "buf_group", "flags", "user_data", NULL};
    int fd;
    unsigned int flags = 0;
    PyObject *user_data = Py_None;
    PyObject *buf_group_obj;

    if (!URING_API_PARSE_KEYWORDS("iO!|IO", keywords, &fd, &UringApiBufGroup_Type, &buf_group_obj, &flags,
                                  &user_data)) {
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

    if (parse_recv_multishot_args("construct_recv_multishot", args, nargs, &fd, &buf_group_obj, &flags, &user_data,
                                  &base_sequence) < 0) {
        return NULL;
    }
    return seed_multishot_sequence(
        UringApiRing_construct_recv_multishot_impl(self, fd, buf_group_obj, flags, user_data), base_sequence);
}

PyObject *UringApiRing_construct_read(UringApiRing *self, URING_API_PARSE_ARGS) {
    static char *keywords[] = {"fd", "buf", "offset", "user_data", NULL};
    Py_buffer view;
    int fd;
    long long offset;
    PyObject *user_data = Py_None;

    if (!URING_API_PARSE_KEYWORDS("iw*L|O", keywords, &fd, &view, &offset, &user_data)) {
        return NULL;
    }
    if (offset < 0) {
        PyBuffer_Release(&view);
        PyErr_SetString(PyExc_ValueError, "offset must be non-negative");
        return NULL;
    }
    return UringApiRing_construct_read_impl(self, fd, &view, (unsigned long long)offset, user_data);
}

PyObject *UringApiRing_construct_write(UringApiRing *self, URING_API_PARSE_ARGS) {
    static char *keywords[] = {"fd", "data", "offset", "user_data", NULL};
    Py_buffer view;
    int fd;
    long long offset;
    PyObject *user_data = Py_None;

    if (!URING_API_PARSE_KEYWORDS("iy*L|O", keywords, &fd, &view, &offset, &user_data)) {
        return NULL;
    }
    if (offset < 0) {
        PyBuffer_Release(&view);
        PyErr_SetString(PyExc_ValueError, "offset must be non-negative");
        return NULL;
    }
    return UringApiRing_construct_write_impl(self, fd, &view, (unsigned long long)offset, user_data);
}

PyObject *UringApiRing_construct_openat(UringApiRing *self, URING_API_PARSE_ARGS) {
    static char *keywords[] = {"dfd", "path", "flags", "mode", "user_data", NULL};
    PyObject *path;
    int flags;
    unsigned int mode = 0;
    int dfd;
    PyObject *user_data = Py_None;

    if (!URING_API_PARSE_KEYWORDS("iOi|IO", keywords, &dfd, &path, &flags, &mode, &user_data)) {
        return NULL;
    }
    return UringApiRing_construct_openat_impl(self, dfd, path, flags, mode, user_data);
}

PyObject *UringApiRing_construct_statx(UringApiRing *self, URING_API_PARSE_ARGS) {
    static char *keywords[] = {"dfd", "path", "flags", "mask", "buf", "user_data", NULL};
    Py_buffer view;
    PyObject *path;
    int dfd;
    int flags;
    unsigned int mask;
    PyObject *user_data = Py_None;

    if (!URING_API_PARSE_KEYWORDS("iOIIw*|O", keywords, &dfd, &path, &flags, &mask, &view, &user_data)) {
        return NULL;
    }
    return UringApiRing_construct_statx_impl(self, dfd, path, flags, mask, &view, user_data);
}

PyObject *UringApiRing_construct_statx_fdsize(UringApiRing *self, URING_API_PARSE_ARGS) {
    static char *keywords[] = {"fd", "user_data", NULL};
    int fd;
    PyObject *user_data = Py_None;

    if (!URING_API_PARSE_KEYWORDS("i|O", keywords, &fd, &user_data)) {
        return NULL;
    }
    return UringApiRing_construct_statx_fdsize_impl(self, fd, user_data);
}

PyObject *UringApiRing_construct_sendto(UringApiRing *self, URING_API_PARSE_ARGS) {
    static char *keywords[] = {"fd", "data", "address", "flags", "user_data", NULL};
    Py_buffer view;
    int fd;
    unsigned int flags = 0;
    PyObject *address;
    PyObject *user_data = Py_None;

    if (!URING_API_PARSE_KEYWORDS("iy*O|IO", keywords, &fd, &view, &address, &flags, &user_data)) {
        return NULL;
    }
    return UringApiRing_construct_sendto_impl(self, fd, &view, address, flags, user_data);
}

PyObject *UringApiRing_construct_recvmsg(UringApiRing *self, URING_API_PARSE_ARGS) {
    static char *keywords[] = {"fd", "buf", "flags", "user_data", NULL};
    Py_buffer view;
    int fd;
    unsigned int flags = 0;
    PyObject *user_data = Py_None;

    if (!URING_API_PARSE_KEYWORDS("iw*|IO", keywords, &fd, &view, &flags, &user_data)) {
        return NULL;
    }
    return UringApiRing_construct_recvmsg_impl(self, fd, &view, flags, user_data);
}

PyObject *UringApiRing_construct_sendmsg(UringApiRing *self, URING_API_PARSE_ARGS) {
    static char *keywords[] = {"fd", "data", "address", "flags", "user_data", NULL};
    Py_buffer view;
    int fd;
    unsigned int flags = 0;
    PyObject *address = Py_None;
    PyObject *user_data = Py_None;

    if (!URING_API_PARSE_KEYWORDS("iy*|OIO", keywords, &fd, &view, &address, &flags, &user_data)) {
        return NULL;
    }
    return UringApiRing_construct_sendmsg_impl(self, fd, &view, address, flags, user_data);
}

PyObject *UringApiRing_construct_sendmsg_zc(UringApiRing *self, URING_API_PARSE_ARGS) {
    static char *keywords[] = {"fd", "data", "address", "flags", "user_data", NULL};
    Py_buffer view;
    int fd;
    unsigned int flags = 0;
    PyObject *address = Py_None;
    PyObject *user_data = Py_None;

    if (!URING_API_PARSE_KEYWORDS("iy*|OIO", keywords, &fd, &view, &address, &flags, &user_data)) {
        return NULL;
    }
    return UringApiRing_construct_sendmsg_zc_impl(self, fd, &view, address, flags, user_data);
}

PyObject *UringApiRing_construct_connect(UringApiRing *self, URING_API_PARSE_ARGS) {
    static char *keywords[] = {"fd", "address", "user_data", NULL};
    int fd;
    PyObject *address;
    PyObject *user_data = Py_None;

    if (!URING_API_PARSE_KEYWORDS("iO|O", keywords, &fd, &address, &user_data)) {
        return NULL;
    }
    return UringApiRing_construct_connect_impl(self, fd, address, user_data);
}

PyObject *UringApiRing_construct_accept(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    int fd;
    unsigned int flags = 0;
    PyObject *user_data = Py_None;

    if (parse_accept_listener_args("construct_accept", args, nargs, &fd, &flags, &user_data, NULL) < 0) {
        return NULL;
    }
    return UringApiRing_construct_accept_impl(self, fd, flags, user_data);
}

PyObject *UringApiRing_construct_accept_multishot(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    int fd;
    unsigned int flags = 0;
    unsigned long long base_sequence = 0;
    PyObject *user_data = Py_None;

    if (parse_accept_listener_args("construct_accept_multishot", args, nargs, &fd, &flags, &user_data, &base_sequence) <
        0) {
        return NULL;
    }
    return seed_multishot_sequence(UringApiRing_construct_accept_multishot_impl(self, fd, flags, user_data),
                                   base_sequence);
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
    unsigned long long base_sequence = 0;
    PyObject *user_data = Py_None;

    if (parse_poll_multishot_args("construct_poll_multishot", args, nargs, &fd, &poll_mask, &user_data,
                                  &base_sequence) < 0) {
        return NULL;
    }
    return seed_multishot_sequence(UringApiRing_construct_poll_multishot_impl(self, fd, poll_mask, user_data),
                                   base_sequence);
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

PyObject *UringApiRing_construct_socket(UringApiRing *self, URING_API_PARSE_ARGS) {
    static char *keywords[] = {"domain", "type", "protocol", "flags", "user_data", NULL};
    int domain;
    int type;
    int protocol = 0;
    unsigned int flags = 0;
    PyObject *user_data = Py_None;

    if (!URING_API_PARSE_KEYWORDS("ii|iIO", keywords, &domain, &type, &protocol, &flags, &user_data)) {
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

PyObject *UringApiRing_prepare_send(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    Py_buffer view;
    int fd;
    unsigned int flags = 0;
    PyObject *user_data = Py_None;

    if (parse_send_args("prepare_send", args, nargs, 4, &fd, &view, &user_data, &flags, NULL, 0) < 0) {
        return NULL;
    }
    return UringApiRing_prepare_send_impl(self, fd, &view, flags, user_data);
}

PyObject *UringApiRing_prepare_send_all(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    Py_buffer view;
    int fd;
    unsigned int flags = 0;
    PyObject *user_data = Py_None;

    if (parse_send_args("prepare_send_all", args, nargs, 4, &fd, &view, &user_data, &flags, NULL, 0) < 0) {
        return NULL;
    }
    return UringApiRing_prepare_send_all_impl(self, fd, &view, flags, user_data);
}

PyObject *UringApiRing_prepare_send_zc(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    Py_buffer view;
    int fd;
    unsigned int flags = 0;
    unsigned int zc_flags = 0;
    PyObject *user_data = Py_None;

    if (parse_send_args("prepare_send_zc", args, nargs, 5, &fd, &view, &user_data, &flags, &zc_flags, 1) < 0) {
        return NULL;
    }
    return UringApiRing_prepare_send_zc_impl(self, fd, &view, flags, zc_flags, user_data);
}

PyObject *UringApiRing_prepare_sendto(UringApiRing *self, URING_API_PARSE_ARGS) {
    static char *keywords[] = {"fd", "data", "address", "flags", "user_data", NULL};
    Py_buffer view;
    int fd;
    unsigned int flags = 0;
    PyObject *address;
    PyObject *user_data = Py_None;

    if (!URING_API_PARSE_KEYWORDS("iy*O|IO", keywords, &fd, &view, &address, &flags, &user_data)) {
        return NULL;
    }
    return UringApiRing_prepare_sendto_impl(self, fd, &view, address, flags, user_data);
}

PyObject *UringApiRing_prepare_recvmsg(UringApiRing *self, URING_API_PARSE_ARGS) {
    static char *keywords[] = {"fd", "buf", "flags", "user_data", NULL};
    Py_buffer view;
    int fd;
    unsigned int flags = 0;
    PyObject *user_data = Py_None;

    if (!URING_API_PARSE_KEYWORDS("iw*|IO", keywords, &fd, &view, &flags, &user_data)) {
        return NULL;
    }
    return UringApiRing_prepare_recvmsg_impl(self, fd, &view, flags, user_data);
}

PyObject *UringApiRing_prepare_sendmsg(UringApiRing *self, URING_API_PARSE_ARGS) {
    static char *keywords[] = {"fd", "data", "address", "flags", "user_data", NULL};
    Py_buffer view;
    int fd;
    unsigned int flags = 0;
    PyObject *address = Py_None;
    PyObject *user_data = Py_None;

    if (!URING_API_PARSE_KEYWORDS("iy*|OIO", keywords, &fd, &view, &address, &flags, &user_data)) {
        return NULL;
    }
    return UringApiRing_prepare_sendmsg_impl(self, fd, &view, address, flags, user_data);
}

PyObject *UringApiRing_prepare_sendmsg_zc(UringApiRing *self, URING_API_PARSE_ARGS) {
    static char *keywords[] = {"fd", "data", "address", "flags", "user_data", NULL};
    Py_buffer view;
    int fd;
    unsigned int flags = 0;
    PyObject *address = Py_None;
    PyObject *user_data = Py_None;

    if (!URING_API_PARSE_KEYWORDS("iy*|OIO", keywords, &fd, &view, &address, &flags, &user_data)) {
        return NULL;
    }
    return UringApiRing_prepare_sendmsg_zc_impl(self, fd, &view, address, flags, user_data);
}

PyObject *UringApiRing_prepare_accept(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    int fd;
    unsigned int flags = 0;
    PyObject *user_data = Py_None;

    if (parse_accept_listener_args("prepare_accept", args, nargs, &fd, &flags, &user_data, NULL) < 0) {
        return NULL;
    }
    return UringApiRing_prepare_accept_impl(self, fd, flags, user_data);
}

PyObject *UringApiRing_prepare_accept_multishot(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    int fd;
    unsigned int flags = 0;
    unsigned long long base_sequence = 0;
    PyObject *user_data = Py_None;

    if (parse_accept_listener_args("prepare_accept_multishot", args, nargs, &fd, &flags, &user_data, &base_sequence) <
        0) {
        return NULL;
    }
    return UringApiRing_prepare_accept_multishot_impl(self, fd, flags, user_data, base_sequence);
}

PyObject *UringApiRing_prepare_connect(UringApiRing *self, URING_API_PARSE_ARGS) {
    static char *keywords[] = {"fd", "address", "user_data", NULL};
    int fd;
    PyObject *address;
    PyObject *user_data = Py_None;

    if (!URING_API_PARSE_KEYWORDS("iO|O", keywords, &fd, &address, &user_data)) {
        return NULL;
    }
    return UringApiRing_prepare_connect_impl(self, fd, address, user_data);
}

PyObject *UringApiRing_prepare_poll(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    int fd;
    unsigned int poll_mask;
    PyObject *user_data = Py_None;

    if (nargs < 2) {
        PyErr_SetString(PyExc_TypeError, "prepare_poll() missing required arguments 'fd' and 'mask'");
        return NULL;
    }
    if (nargs > 3) {
        PyErr_Format(PyExc_TypeError, "prepare_poll() takes at most 3 positional arguments (%zd given)", nargs);
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
    return UringApiRing_prepare_poll_impl(self, fd, poll_mask, user_data);
}

PyObject *UringApiRing_prepare_poll_multishot(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    int fd;
    unsigned int poll_mask;
    unsigned long long base_sequence = 0;
    PyObject *user_data = Py_None;

    if (parse_poll_multishot_args("prepare_poll_multishot", args, nargs, &fd, &poll_mask, &user_data, &base_sequence) <
        0) {
        return NULL;
    }
    return UringApiRing_prepare_poll_multishot_impl(self, fd, poll_mask, user_data, base_sequence);
}

PyObject *UringApiRing_prepare_poll_remove(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    PyObject *user_data = Py_None;

    if (nargs < 1) {
        PyErr_SetString(PyExc_TypeError, "prepare_poll_remove() missing required argument 'completion'");
        return NULL;
    }
    if (nargs > 2) {
        PyErr_Format(PyExc_TypeError, "prepare_poll_remove() takes at most 2 positional arguments (%zd given)", nargs);
        return NULL;
    }
    if (!PyObject_TypeCheck(args[0], &UringApiCompletion_Type)) {
        PyErr_SetString(PyExc_TypeError, "completion must be a Completion");
        return NULL;
    }
    if (nargs > 1) {
        user_data = args[1];
    }
    return UringApiRing_prepare_poll_remove_impl(self, args[0], user_data);
}

PyObject *UringApiRing_prepare_poll_remove_nowait(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    if (nargs != 1) {
        PyErr_SetString(PyExc_TypeError, "prepare_poll_remove_nowait() takes exactly 1 positional argument");
        return NULL;
    }
    if (!PyObject_TypeCheck(args[0], &UringApiCompletion_Type)) {
        PyErr_SetString(PyExc_TypeError, "completion must be a Completion");
        return NULL;
    }
    return UringApiRing_prepare_poll_remove_nowait_impl(self, args[0]);
}

PyObject *UringApiRing_prepare_cancel(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    PyObject *user_data = Py_None;

    if (nargs < 1) {
        PyErr_SetString(PyExc_TypeError, "prepare_cancel() missing required argument 'completion'");
        return NULL;
    }
    if (nargs > 2) {
        PyErr_Format(PyExc_TypeError, "prepare_cancel() takes at most 2 positional arguments (%zd given)", nargs);
        return NULL;
    }
    if (!PyObject_TypeCheck(args[0], &UringApiCompletion_Type)) {
        PyErr_SetString(PyExc_TypeError, "completion must be a Completion");
        return NULL;
    }
    if (nargs > 1) {
        user_data = args[1];
    }
    return UringApiRing_prepare_cancel_impl(self, args[0], user_data);
}

PyObject *UringApiRing_prepare_cancel_nowait(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    if (nargs != 1) {
        PyErr_SetString(PyExc_TypeError, "prepare_cancel_nowait() takes exactly 1 positional argument");
        return NULL;
    }
    if (!PyObject_TypeCheck(args[0], &UringApiCompletion_Type)) {
        PyErr_SetString(PyExc_TypeError, "completion must be a Completion");
        return NULL;
    }
    return UringApiRing_prepare_cancel_nowait_impl(self, args[0]);
}

PyObject *UringApiRing_prepare_shutdown(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    int fd;
    int how;
    PyObject *user_data = Py_None;

    if (nargs < 2) {
        PyErr_SetString(PyExc_TypeError, "prepare_shutdown() missing required arguments 'fd' and 'how'");
        return NULL;
    }
    if (nargs > 3) {
        PyErr_Format(PyExc_TypeError, "prepare_shutdown() takes at most 3 positional arguments (%zd given)", nargs);
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
    return UringApiRing_prepare_shutdown_impl(self, fd, how, user_data);
}

PyObject *UringApiRing_prepare_shutdown_nowait(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    int fd;
    int how;

    if (nargs != 2) {
        PyErr_SetString(PyExc_TypeError, "prepare_shutdown_nowait() takes exactly 2 positional arguments");
        return NULL;
    }
    if (parse_socket_fd(args[0], &fd) < 0) {
        return NULL;
    }
    if (parse_int_arg(args[1], &how) < 0) {
        return NULL;
    }
    return UringApiRing_prepare_shutdown_nowait_impl(self, fd, how);
}

PyObject *UringApiRing_prepare_close(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    int fd;
    PyObject *user_data = Py_None;

    if (nargs < 1) {
        PyErr_SetString(PyExc_TypeError, "prepare_close() missing required argument 'fd'");
        return NULL;
    }
    if (nargs > 2) {
        PyErr_Format(PyExc_TypeError, "prepare_close() takes at most 2 positional arguments (%zd given)", nargs);
        return NULL;
    }
    if (parse_socket_fd(args[0], &fd) < 0) {
        return NULL;
    }
    if (nargs > 1) {
        user_data = args[1];
    }
    return UringApiRing_prepare_close_impl(self, fd, user_data);
}

PyObject *UringApiRing_prepare_close_nowait(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs) {
    int fd;

    if (nargs != 1) {
        PyErr_SetString(PyExc_TypeError, "prepare_close_nowait() takes exactly 1 positional argument");
        return NULL;
    }
    if (parse_socket_fd(args[0], &fd) < 0) {
        return NULL;
    }
    return UringApiRing_prepare_close_nowait_impl(self, fd);
}

PyObject *UringApiRing_prepare_socket(UringApiRing *self, URING_API_PARSE_ARGS) {
    static char *keywords[] = {"domain", "type", "protocol", "flags", "user_data", NULL};
    int domain;
    int type;
    int protocol = 0;
    unsigned int flags = 0;
    PyObject *user_data = Py_None;

    if (!URING_API_PARSE_KEYWORDS("ii|iIO", keywords, &domain, &type, &protocol, &flags, &user_data)) {
        return NULL;
    }
    return UringApiRing_prepare_socket_impl(self, domain, type, protocol, flags, user_data);
}
