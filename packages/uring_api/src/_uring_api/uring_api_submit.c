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

static int parse_recv_multishot_args(PyObject *const *args, Py_ssize_t nargs, int *fd_out, PyObject **buf_group_out,
                                     PyObject **user_data_out, unsigned int *flags_out,
                                     unsigned long long *base_sequence_out) {
    Py_ssize_t positional_optional_count;

    if (nargs < 2) {
        PyErr_SetString(PyExc_TypeError, "submit_recv_multishot() missing required arguments 'fd' and 'buf_group'");
        return -1;
    }
    if (nargs > 5) {
        PyErr_Format(PyExc_TypeError, "submit_recv_multishot() takes at most 5 positional arguments (%zd given)",
                     nargs);
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

PyObject *UringApiRing_submit_recv_impl(UringApiRing *self, int fd, Py_buffer *view, PyObject *user_data) {
    struct io_uring_sqe *sqe;
    PyObject *completion = NULL;
    int failed = 0;

    completion = UringApiCompletion_new_pending_view(URING_API_PENDING_RECV, user_data, view);
    if (!completion) {
        return NULL;
    }

    Py_BEGIN_CRITICAL_SECTION(self);
    if (ring_check_open(self) < 0) {
        failed = 1;
    } else {
        sqe = get_sqe(self);
        if (!sqe) {
            failed = 1;
        } else {
            io_uring_prep_recv(sqe, fd, view->buf, (size_t)view->len, 0);
            sqe_set_completion(self, sqe, completion);
            if (submit_one_completion(self, sqe, (PyObject *)completion) < 0) {
                failed = 1;
            }
        }
    }
    Py_END_CRITICAL_SECTION();

    if (failed) {
        Py_DECREF(completion);
        return NULL;
    }
    return Py_NewRef(completion);
}

PyObject *UringApiRing_submit_recv_buf_impl(UringApiRing *self, int fd, PyObject *buf_group_obj, unsigned int flags,
                                            PyObject *user_data) {
    struct io_uring_sqe *sqe;
    UringApiBufGroup *buf_group;
    PyObject *completion = NULL;
    int failed = 0;

    if (!buf_group_obj || !PyObject_TypeCheck(buf_group_obj, &UringApiBufGroup_Type)) {
        PyErr_SetString(PyExc_TypeError, "buf_group must be a BufGroup");
        return NULL;
    }
    buf_group = (UringApiBufGroup *)buf_group_obj;
    if (buf_group->ring != self) {
        PyErr_SetString(PyExc_ValueError, "buf_group was not created by this ring");
        return NULL;
    }

    completion = UringApiCompletion_new_pending_buf_group(URING_API_PENDING_RECV_BUF, user_data, buf_group_obj);
    if (!completion) {
        return NULL;
    }

    Py_BEGIN_CRITICAL_SECTION(self);
    if (ring_check_open(self) < 0) {
        failed = 1;
    } else {
        sqe = get_sqe(self);
        if (!sqe) {
            failed = 1;
        } else {
            io_uring_prep_recv(sqe, fd, NULL, (size_t)buf_group->buffer_size, (int)flags);
            sqe->flags |= IOSQE_BUFFER_SELECT;
            sqe->buf_group = buf_group->group_id;
            sqe_set_completion(self, sqe, completion);
            if (submit_one_completion(self, sqe, (PyObject *)completion) < 0) {
                failed = 1;
            }
        }
    }
    Py_END_CRITICAL_SECTION();

    if (failed) {
        Py_DECREF(completion);
        return NULL;
    }
    return Py_NewRef(completion);
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
    struct io_uring_sqe *sqe;
    UringApiBufGroup *buf_group;
    PyObject *completion = NULL;
    int failed = 0;

    if (!buf_group_obj || !PyObject_TypeCheck(buf_group_obj, &UringApiBufGroup_Type)) {
        PyErr_SetString(PyExc_TypeError, "buf_group must be a BufGroup");
        return NULL;
    }
    buf_group = (UringApiBufGroup *)buf_group_obj;
    if (buf_group->ring != self) {
        PyErr_SetString(PyExc_ValueError, "buf_group was not created by this ring");
        return NULL;
    }

    completion = UringApiCompletion_new_pending_buf_group(URING_API_PENDING_RECV_MULTISHOT, user_data, buf_group_obj);
    if (!completion) {
        return NULL;
    }
    ((UringApiCompletion *)completion)->multishot = true;
    ((UringApiCompletion *)completion)->sequence = base_sequence;

    Py_BEGIN_CRITICAL_SECTION(self);
    if (ring_check_open(self) < 0) {
        failed = 1;
    } else {
        sqe = get_sqe(self);
        if (!sqe) {
            failed = 1;
        } else {
            io_uring_prep_recv_multishot(sqe, fd, NULL, 0, (int)flags);
            sqe->flags |= IOSQE_BUFFER_SELECT;
            sqe->buf_group = buf_group->group_id;
            sqe_set_completion(self, sqe, completion);
            if (submit_one_completion(self, sqe, (PyObject *)completion) < 0) {
                failed = 1;
            }
        }
    }
    Py_END_CRITICAL_SECTION();

    if (failed) {
        Py_DECREF(completion);
        return NULL;
    }
    return Py_NewRef(completion);
}

PyObject *UringApiRing_submit_send_impl(UringApiRing *self, int fd, Py_buffer *view, unsigned int flags,
                                        PyObject *user_data) {
    struct io_uring_sqe *sqe;
    PyObject *completion = NULL;
    int failed = 0;

    completion = UringApiCompletion_new_pending_view(URING_API_PENDING_SEND, user_data, view);
    if (!completion) {
        return NULL;
    }
    Py_BEGIN_CRITICAL_SECTION(self);
    if (ring_check_open(self) < 0) {
        failed = 1;
    } else {
        sqe = get_sqe(self);
        if (!sqe) {
            failed = 1;
        } else {
            io_uring_prep_send(sqe, fd, view->buf, (size_t)view->len, (int)flags);
            sqe_set_completion(self, sqe, completion);
            if (submit_one_completion(self, sqe, (PyObject *)completion) < 0) {
                failed = 1;
            }
        }
    }
    Py_END_CRITICAL_SECTION();

    if (failed) {
        Py_DECREF(completion);
        return NULL;
    }
    return Py_NewRef(completion);
}

PyObject *UringApiRing_submit_read_impl(UringApiRing *self, int fd, Py_buffer *view, unsigned long long offset,
                                        PyObject *user_data) {
    struct io_uring_sqe *sqe;
    PyObject *completion = NULL;
    int failed = 0;

    if (validate_file_io_buffer_length(view) < 0) {
        PyBuffer_Release(view);
        return NULL;
    }

    completion = UringApiCompletion_new_pending_view(URING_API_PENDING_READ, user_data, view);
    if (!completion) {
        return NULL;
    }

    Py_BEGIN_CRITICAL_SECTION(self);
    if (ring_check_open(self) < 0) {
        failed = 1;
    } else {
        sqe = get_sqe(self);
        if (!sqe) {
            failed = 1;
        } else {
            io_uring_prep_read(sqe, fd, view->buf, (unsigned)view->len, (__u64)offset);
            sqe_set_completion(self, sqe, completion);
            if (submit_one_completion(self, sqe, (PyObject *)completion) < 0) {
                failed = 1;
            }
        }
    }
    Py_END_CRITICAL_SECTION();

    if (failed) {
        Py_DECREF(completion);
        return NULL;
    }
    return Py_NewRef(completion);
}

PyObject *UringApiRing_submit_write_impl(UringApiRing *self, int fd, Py_buffer *view, unsigned long long offset,
                                         PyObject *user_data) {
    struct io_uring_sqe *sqe;
    PyObject *completion = NULL;
    int failed = 0;

    if (validate_file_io_buffer_length(view) < 0) {
        PyBuffer_Release(view);
        return NULL;
    }

    completion = UringApiCompletion_new_pending_view(URING_API_PENDING_WRITE, user_data, view);
    if (!completion) {
        return NULL;
    }

    Py_BEGIN_CRITICAL_SECTION(self);
    if (ring_check_open(self) < 0) {
        failed = 1;
    } else {
        sqe = get_sqe(self);
        if (!sqe) {
            failed = 1;
        } else {
            io_uring_prep_write(sqe, fd, view->buf, (unsigned)view->len, (__u64)offset);
            sqe_set_completion(self, sqe, completion);
            if (submit_one_completion(self, sqe, (PyObject *)completion) < 0) {
                failed = 1;
            }
        }
    }
    Py_END_CRITICAL_SECTION();

    if (failed) {
        Py_DECREF(completion);
        return NULL;
    }
    return Py_NewRef(completion);
}

PyObject *UringApiRing_submit_openat_impl(UringApiRing *self, int dfd, PyObject *path, int flags, unsigned int mode,
                                          PyObject *user_data) {
    struct io_uring_sqe *sqe;
    UringApiCompletionPathState *path_state;
    PyObject *completion = NULL;
    int failed = 0;

    completion = UringApiCompletion_new_pending_path(URING_API_PENDING_OPENAT, user_data, path);
    if (!completion) {
        return NULL;
    }

    Py_BEGIN_CRITICAL_SECTION(self);
    if (ring_check_open(self) < 0) {
        failed = 1;
    } else {
        path_state = UringApiCompletion_get_path_state((UringApiCompletion *)completion);
        if (!path_state || !path_state->path) {
            PyErr_SetString(PyExc_RuntimeError, "openat completion is missing path state");
            failed = 1;
        } else {
            sqe = get_sqe(self);
            if (!sqe) {
                failed = 1;
            } else {
                io_uring_prep_openat(sqe, dfd, path_state->path, flags, mode);
                sqe_set_completion(self, sqe, completion);
                if (submit_one_completion(self, sqe, (PyObject *)completion) < 0) {
                    failed = 1;
                }
            }
        }
    }
    Py_END_CRITICAL_SECTION();

    if (failed) {
        Py_DECREF(completion);
        return NULL;
    }
    return Py_NewRef(completion);
}

PyObject *UringApiRing_submit_statx_impl(UringApiRing *self, int dfd, PyObject *path, int flags, unsigned int mask,
                                         Py_buffer *view, PyObject *user_data) {
    struct io_uring_sqe *sqe;
    UringApiCompletionStatxState *statx_state;
    PyObject *completion = NULL;
    int failed = 0;

    if (validate_statx_buffer(view) < 0) {
        PyBuffer_Release(view);
        return NULL;
    }

    completion = UringApiCompletion_new_pending_statx(URING_API_PENDING_STATX, user_data, path, view);
    if (!completion) {
        return NULL;
    }

    Py_BEGIN_CRITICAL_SECTION(self);
    if (ring_check_open(self) < 0) {
        failed = 1;
    } else {
        statx_state = UringApiCompletion_get_statx_state((UringApiCompletion *)completion);
        if (!statx_state || !statx_state->path) {
            PyErr_SetString(PyExc_RuntimeError, "statx completion is missing path state");
            failed = 1;
        } else {
            sqe = get_sqe(self);
            if (!sqe) {
                failed = 1;
            } else {
                io_uring_prep_statx(sqe, dfd, statx_state->path, flags, mask, (struct statx *)statx_state->view.buf);
                sqe_set_completion(self, sqe, completion);
                if (submit_one_completion(self, sqe, (PyObject *)completion) < 0) {
                    failed = 1;
                }
            }
        }
    }
    Py_END_CRITICAL_SECTION();

    if (failed) {
        Py_DECREF(completion);
        return NULL;
    }
    return Py_NewRef(completion);
}

PyObject *UringApiRing_submit_send_zc_impl(UringApiRing *self, int fd, Py_buffer *view, unsigned int flags,
                                           unsigned int zc_flags, PyObject *user_data) {
    struct io_uring_sqe *sqe;
    PyObject *completion = NULL;
    int failed = 0;

    completion = UringApiCompletion_new_pending_view(URING_API_PENDING_SEND_ZC, user_data, view);
    if (!completion) {
        return NULL;
    }
    Py_BEGIN_CRITICAL_SECTION(self);
    if (ring_check_open(self) < 0) {
        failed = 1;
    } else {
        sqe = get_sqe(self);
        if (!sqe) {
            failed = 1;
        } else {
            io_uring_prep_send_zc(sqe, fd, view->buf, (size_t)view->len, (int)flags, zc_flags);
            sqe_set_completion(self, sqe, completion);
            if (submit_one_completion(self, sqe, (PyObject *)completion) < 0) {
                failed = 1;
            }
        }
    }
    Py_END_CRITICAL_SECTION();

    if (failed) {
        Py_DECREF(completion);
        return NULL;
    }
    return Py_NewRef(completion);
}

PyObject *UringApiRing_submit_sendto_impl(UringApiRing *self, int fd, Py_buffer *view, PyObject *address,
                                          unsigned int flags, PyObject *user_data) {
    struct io_uring_sqe *sqe;
    UringApiCompletionViewSockaddrState *sendto_state;
    PyObject *completion = NULL;
    int failed = 0;

    completion = UringApiCompletion_new_pending_view_sockaddr(URING_API_PENDING_SENDTO, user_data, view);
    if (!completion) {
        return NULL;
    }
    sendto_state = UringApiCompletion_get_view_sockaddr_state((UringApiCompletion *)completion);
    if (!sendto_state) {
        Py_DECREF(completion);
        PyErr_SetString(PyExc_RuntimeError, "sendto completion is missing view/sockaddr state");
        return NULL;
    }
    if (parse_numeric_sockaddr(fd, address, &sendto_state->addr, &sendto_state->addrlen) < 0) {
        Py_DECREF(completion);
        return NULL;
    }
    Py_BEGIN_CRITICAL_SECTION(self);
    if (ring_check_open(self) < 0) {
        failed = 1;
    } else {
        sqe = get_sqe(self);
        if (!sqe) {
            failed = 1;
        } else {
            io_uring_prep_sendto(sqe, fd, sendto_state->view.buf, (size_t)sendto_state->view.len, (int)flags,
                                 (struct sockaddr *)&sendto_state->addr, sendto_state->addrlen);
            sqe_set_completion(self, sqe, completion);
            if (submit_one_completion(self, sqe, (PyObject *)completion) < 0) {
                failed = 1;
            }
        }
    }
    Py_END_CRITICAL_SECTION();

    if (failed) {
        Py_DECREF(completion);
        return NULL;
    }
    return Py_NewRef(completion);
}

PyObject *UringApiRing_submit_recvmsg_impl(UringApiRing *self, int fd, Py_buffer *view, PyObject *user_data) {
    struct io_uring_sqe *sqe;
    UringApiCompletionMsgState *msg_state;
    PyObject *completion = NULL;
    int failed = 0;

    completion = UringApiCompletion_new_pending_recvmsg(URING_API_PENDING_RECVMSG, user_data, view);
    if (!completion) {
        return NULL;
    }
    msg_state = UringApiCompletion_get_msg_state((UringApiCompletion *)completion);
    if (!msg_state) {
        Py_DECREF(completion);
        PyErr_SetString(PyExc_RuntimeError, "recvmsg completion is missing message state");
        return NULL;
    }

    Py_BEGIN_CRITICAL_SECTION(self);
    if (ring_check_open(self) < 0) {
        failed = 1;
    } else {
        sqe = get_sqe(self);
        if (!sqe) {
            failed = 1;
        } else {
            io_uring_prep_recvmsg(sqe, fd, &msg_state->msg, 0);
            sqe_set_completion(self, sqe, completion);
            if (submit_one_completion(self, sqe, (PyObject *)completion) < 0) {
                failed = 1;
            }
        }
    }
    Py_END_CRITICAL_SECTION();

    if (failed) {
        Py_DECREF(completion);
        return NULL;
    }
    return Py_NewRef(completion);
}

PyObject *UringApiRing_submit_sendmsg_impl(UringApiRing *self, int fd, Py_buffer *view, PyObject *address,
                                           unsigned int flags, PyObject *user_data) {
    struct io_uring_sqe *sqe;
    UringApiCompletionMsgState *msg_state;
    PyObject *completion = NULL;
    int failed = 0;

    completion = UringApiCompletion_new_pending_sendmsg(URING_API_PENDING_SENDMSG, user_data, view);
    if (!completion) {
        return NULL;
    }
    msg_state = UringApiCompletion_get_msg_state((UringApiCompletion *)completion);
    if (!msg_state) {
        Py_DECREF(completion);
        PyErr_SetString(PyExc_RuntimeError, "sendmsg completion is missing message state");
        return NULL;
    }
    if (address != Py_None) {
        if (parse_numeric_sockaddr(fd, address, &msg_state->addr, &msg_state->addrlen) < 0) {
            Py_DECREF(completion);
            return NULL;
        }
        msg_state->msg.msg_name = &msg_state->addr;
        msg_state->msg.msg_namelen = msg_state->addrlen;
    }

    Py_BEGIN_CRITICAL_SECTION(self);
    if (ring_check_open(self) < 0) {
        failed = 1;
    } else {
        sqe = get_sqe(self);
        if (!sqe) {
            failed = 1;
        } else {
            io_uring_prep_sendmsg(sqe, fd, &msg_state->msg, flags);
            sqe_set_completion(self, sqe, completion);
            if (submit_one_completion(self, sqe, (PyObject *)completion) < 0) {
                failed = 1;
            }
        }
    }
    Py_END_CRITICAL_SECTION();

    if (failed) {
        Py_DECREF(completion);
        return NULL;
    }
    return Py_NewRef(completion);
}

PyObject *UringApiRing_submit_sendmsg_zc_impl(UringApiRing *self, int fd, Py_buffer *view, PyObject *address,
                                              unsigned int flags, PyObject *user_data) {
    struct io_uring_sqe *sqe;
    UringApiCompletionMsgState *msg_state;
    PyObject *completion = NULL;
    int failed = 0;

    completion = UringApiCompletion_new_pending_sendmsg(URING_API_PENDING_SENDMSG_ZC, user_data, view);
    if (!completion) {
        return NULL;
    }
    msg_state = UringApiCompletion_get_msg_state((UringApiCompletion *)completion);
    if (!msg_state) {
        Py_DECREF(completion);
        PyErr_SetString(PyExc_RuntimeError, "sendmsg completion is missing message state");
        return NULL;
    }
    if (address != Py_None) {
        if (parse_numeric_sockaddr(fd, address, &msg_state->addr, &msg_state->addrlen) < 0) {
            Py_DECREF(completion);
            return NULL;
        }
        msg_state->msg.msg_name = &msg_state->addr;
        msg_state->msg.msg_namelen = msg_state->addrlen;
    }

    Py_BEGIN_CRITICAL_SECTION(self);
    if (ring_check_open(self) < 0) {
        failed = 1;
    } else {
        sqe = get_sqe(self);
        if (!sqe) {
            failed = 1;
        } else {
            io_uring_prep_sendmsg_zc(sqe, fd, &msg_state->msg, flags);
            sqe_set_completion(self, sqe, completion);
            if (submit_one_completion(self, sqe, (PyObject *)completion) < 0) {
                failed = 1;
            }
        }
    }
    Py_END_CRITICAL_SECTION();

    if (failed) {
        Py_DECREF(completion);
        return NULL;
    }
    return Py_NewRef(completion);
}

PyObject *UringApiRing_submit_accept_impl(UringApiRing *self, int fd, unsigned int flags, PyObject *user_data) {
    struct io_uring_sqe *sqe;
    PyObject *completion = NULL;
    int failed = 0;

    completion = UringApiCompletion_new_pending(URING_API_PENDING_ACCEPT, user_data);
    if (!completion) {
        return NULL;
    }

    Py_BEGIN_CRITICAL_SECTION(self);
    if (ring_check_open(self) < 0) {
        failed = 1;
    } else {
        sqe = get_sqe(self);
        if (!sqe) {
            failed = 1;
        } else {
            io_uring_prep_accept(sqe, fd, NULL, NULL, flags);
            sqe_set_completion(self, sqe, completion);
            if (submit_one_completion(self, sqe, (PyObject *)completion) < 0) {
                failed = 1;
            }
        }
    }
    Py_END_CRITICAL_SECTION();

    if (failed) {
        Py_DECREF(completion);
        return NULL;
    }
    return Py_NewRef(completion);
}

PyObject *UringApiRing_submit_accept_multishot_impl(UringApiRing *self, int fd, unsigned int flags, PyObject *user_data,
                                                    unsigned long long base_sequence) {
    struct io_uring_sqe *sqe;
    PyObject *completion = NULL;
    UringApiCompletion *pending;
    int failed = 0;

    completion = UringApiCompletion_new_pending(URING_API_PENDING_ACCEPT, user_data);
    if (!completion) {
        return NULL;
    }
    pending = (UringApiCompletion *)completion;
    pending->multishot = true;
    pending->sequence = base_sequence;

    Py_BEGIN_CRITICAL_SECTION(self);
    if (ring_check_open(self) < 0) {
        failed = 1;
    } else {
        sqe = get_sqe(self);
        if (!sqe) {
            failed = 1;
        } else {
            /* multishot accept shares one addr buffer across legs; pass NULL and
             * let callers use getpeername() on the accepted fd when needed. */
            io_uring_prep_multishot_accept(sqe, fd, NULL, NULL, flags);
            sqe_set_completion(self, sqe, completion);
            if (submit_one_completion(self, sqe, (PyObject *)completion) < 0) {
                failed = 1;
            }
        }
    }
    Py_END_CRITICAL_SECTION();

    if (failed) {
        Py_DECREF(completion);
        return NULL;
    }
    return Py_NewRef(completion);
}

PyObject *UringApiRing_submit_connect_impl(UringApiRing *self, int fd, PyObject *address, PyObject *user_data) {
    struct io_uring_sqe *sqe;
    UringApiCompletionSockaddrState *sockaddr_state;
    PyObject *completion = NULL;
    int failed = 0;

    completion = UringApiCompletion_new_pending_sockaddr(URING_API_PENDING_CONNECT, user_data);
    if (!completion) {
        return NULL;
    }
    sockaddr_state = UringApiCompletion_get_sockaddr_state((UringApiCompletion *)completion);
    if (!sockaddr_state) {
        Py_DECREF(completion);
        PyErr_SetString(PyExc_RuntimeError, "connect completion is missing sockaddr state");
        return NULL;
    }
    if (parse_numeric_sockaddr(fd, address, &sockaddr_state->addr, &sockaddr_state->addrlen) < 0) {
        Py_DECREF(completion);
        return NULL;
    }

    Py_BEGIN_CRITICAL_SECTION(self);
    if (ring_check_open(self) < 0) {
        failed = 1;
    } else {
        sqe = get_sqe(self);
        if (!sqe) {
            failed = 1;
        } else {
            io_uring_prep_connect(sqe, fd, (struct sockaddr *)&sockaddr_state->addr, sockaddr_state->addrlen);
            sqe_set_completion(self, sqe, completion);
            if (submit_one_completion(self, sqe, (PyObject *)completion) < 0) {
                failed = 1;
            }
        }
    }
    Py_END_CRITICAL_SECTION();

    if (failed) {
        Py_DECREF(completion);
        return NULL;
    }
    return Py_NewRef(completion);
}

PyObject *UringApiRing_submit_poll_impl(UringApiRing *self, int fd, unsigned int poll_mask, PyObject *user_data) {
    struct io_uring_sqe *sqe;
    PyObject *completion = NULL;
    int failed = 0;

    completion = UringApiCompletion_new_pending(URING_API_PENDING_POLL, user_data);
    if (!completion) {
        return NULL;
    }

    Py_BEGIN_CRITICAL_SECTION(self);
    if (ring_check_open(self) < 0) {
        failed = 1;
    } else {
        sqe = get_sqe(self);
        if (!sqe) {
            failed = 1;
        } else {
            io_uring_prep_poll_add(sqe, fd, poll_mask);
            sqe_set_completion(self, sqe, completion);
            if (submit_one_completion(self, sqe, (PyObject *)completion) < 0) {
                failed = 1;
            }
        }
    }
    Py_END_CRITICAL_SECTION();

    if (failed) {
        Py_DECREF(completion);
        return NULL;
    }
    return Py_NewRef(completion);
}

PyObject *UringApiRing_submit_poll_multishot_impl(UringApiRing *self, int fd, unsigned int poll_mask,
                                                  PyObject *user_data) {
    struct io_uring_sqe *sqe;
    PyObject *completion = NULL;
    int failed = 0;

    completion = UringApiCompletion_new_pending(URING_API_PENDING_POLL_MULTISHOT, user_data);
    if (!completion) {
        return NULL;
    }
    ((UringApiCompletion *)completion)->multishot = true;

    Py_BEGIN_CRITICAL_SECTION(self);
    if (ring_check_open(self) < 0) {
        failed = 1;
    } else {
        sqe = get_sqe(self);
        if (!sqe) {
            failed = 1;
        } else {
            io_uring_prep_poll_multishot(sqe, fd, poll_mask);
            sqe_set_completion(self, sqe, completion);
            if (submit_one_completion(self, sqe, (PyObject *)completion) < 0) {
                failed = 1;
            }
        }
    }
    Py_END_CRITICAL_SECTION();

    if (failed) {
        Py_DECREF(completion);
        return NULL;
    }
    return Py_NewRef(completion);
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

PyObject *UringApiRing_submit_poll_remove_impl(UringApiRing *self, PyObject *target_completion, PyObject *user_data) {
    struct io_uring_sqe *sqe;
    UringApiCompletion *completion = NULL;
    int failed = 0;

    if (!poll_remove_target_is_valid((UringApiCompletion *)target_completion)) {
        return NULL;
    }

    if (user_data == NULL || user_data == Py_None) {
        user_data = target_completion;
    }
    completion = (UringApiCompletion *)UringApiCompletion_new_pending(URING_API_PENDING_POLL_REMOVE, user_data);
    if (!completion) {
        return NULL;
    }
    completion->cancel_target = Py_NewRef(target_completion);

    Py_BEGIN_CRITICAL_SECTION(self);
    if (ring_check_open(self) < 0) {
        failed = 1;
    } else {
        sqe = get_sqe(self);
        if (!sqe) {
            failed = 1;
        } else {
            io_uring_prep_poll_remove(sqe, (unsigned long long)(uintptr_t)target_completion);
            sqe_set_completion(self, sqe, (PyObject *)completion);
            if (submit_one_completion(self, sqe, (PyObject *)completion) < 0) {
                failed = 1;
            }
        }
    }
    Py_END_CRITICAL_SECTION();

    if (failed) {
        Py_DECREF((PyObject *)completion);
        return NULL;
    }
    return Py_NewRef((PyObject *)completion);
}

PyObject *UringApiRing_submit_cancel_impl(UringApiRing *self, PyObject *target_completion, PyObject *user_data) {
    struct io_uring_sqe *sqe;
    UringApiCompletion *completion = NULL;
    int failed = 0;

    if (user_data == NULL || user_data == Py_None) {
        user_data = target_completion;
    }
    completion = (UringApiCompletion *)UringApiCompletion_new_pending(URING_API_PENDING_CANCEL, user_data);
    if (!completion) {
        return NULL;
    }
    completion->cancel_target = Py_NewRef(target_completion);

    Py_BEGIN_CRITICAL_SECTION(self);
    if (ring_check_open(self) < 0) {
        failed = 1;
    } else {
        sqe = get_sqe(self);
        if (!sqe) {
            failed = 1;
        } else {
            io_uring_prep_cancel(sqe, target_completion, 0);
            sqe_set_completion(self, sqe, (PyObject *)completion);
            if (submit_one_completion(self, sqe, (PyObject *)completion) < 0) {
                failed = 1;
            }
        }
    }
    Py_END_CRITICAL_SECTION();

    if (failed) {
        Py_DECREF((PyObject *)completion);
        return NULL;
    }
    return Py_NewRef((PyObject *)completion);
}

PyObject *UringApiRing_submit_shutdown_impl(UringApiRing *self, int fd, int how, PyObject *user_data) {
    struct io_uring_sqe *sqe;
    PyObject *completion = NULL;
    int failed = 0;

    completion = UringApiCompletion_new_pending(URING_API_PENDING_SHUTDOWN, user_data);
    if (!completion) {
        return NULL;
    }

    Py_BEGIN_CRITICAL_SECTION(self);
    if (ring_check_open(self) < 0) {
        failed = 1;
    } else {
        sqe = get_sqe(self);
        if (!sqe) {
            failed = 1;
        } else {
            io_uring_prep_shutdown(sqe, fd, how);
            sqe_set_completion(self, sqe, completion);
            if (submit_one_completion(self, sqe, (PyObject *)completion) < 0) {
                failed = 1;
            }
        }
    }
    Py_END_CRITICAL_SECTION();

    if (failed) {
        Py_DECREF(completion);
        return NULL;
    }
    return Py_NewRef(completion);
}

PyObject *UringApiRing_submit_close_impl(UringApiRing *self, int fd, PyObject *user_data) {
    struct io_uring_sqe *sqe;
    PyObject *completion = NULL;
    int failed = 0;

    completion = UringApiCompletion_new_pending(URING_API_PENDING_CLOSE, user_data);
    if (!completion) {
        return NULL;
    }

    Py_BEGIN_CRITICAL_SECTION(self);
    if (ring_check_open(self) < 0) {
        failed = 1;
    } else {
        sqe = get_sqe(self);
        if (!sqe) {
            failed = 1;
        } else {
            io_uring_prep_close(sqe, fd);
            sqe_set_completion(self, sqe, completion);
            if (submit_one_completion(self, sqe, (PyObject *)completion) < 0) {
                failed = 1;
            }
        }
    }
    Py_END_CRITICAL_SECTION();

    if (failed) {
        Py_DECREF(completion);
        return NULL;
    }
    return Py_NewRef(completion);
}

PyObject *UringApiRing_submit_socket_impl(UringApiRing *self, int domain, int type, int protocol, unsigned int flags,
                                          PyObject *user_data) {
    struct io_uring_sqe *sqe;
    PyObject *completion = NULL;
    int failed = 0;

    completion = UringApiCompletion_new_pending(URING_API_PENDING_SOCKET, user_data);
    if (!completion) {
        return NULL;
    }

    Py_BEGIN_CRITICAL_SECTION(self);
    if (ring_check_open(self) < 0) {
        failed = 1;
    } else {
        sqe = get_sqe(self);
        if (!sqe) {
            failed = 1;
        } else {
            io_uring_prep_socket(sqe, domain, type, protocol, flags);
            sqe_set_completion(self, sqe, completion);
            if (submit_one_completion(self, sqe, (PyObject *)completion) < 0) {
                failed = 1;
            }
        }
    }
    Py_END_CRITICAL_SECTION();

    if (failed) {
        Py_DECREF(completion);
        return NULL;
    }
    return Py_NewRef(completion);
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
    struct io_uring_sqe *sqe;
    UringApiCompletionStatxFdsizeState *statx_fdsize_state;
    PyObject *completion = NULL;
    int failed = 0;

    completion = UringApiCompletion_new_pending_statx_fdsize(user_data);
    if (!completion) {
        return NULL;
    }

    Py_BEGIN_CRITICAL_SECTION(self);
    if (ring_check_open(self) < 0) {
        failed = 1;
    } else {
        statx_fdsize_state = UringApiCompletion_get_statx_fdsize_state((UringApiCompletion *)completion);
        if (!statx_fdsize_state) {
            PyErr_SetString(PyExc_RuntimeError, "statx_fdsize completion is missing buffer state");
            failed = 1;
        } else {
            sqe = get_sqe(self);
            if (!sqe) {
                failed = 1;
            } else {
                io_uring_prep_statx(sqe, fd, "", URING_API_AT_EMPTY_PATH, URING_API_STATX_SIZE_MASK,
                                    (struct statx *)statx_fdsize_state->buf);
                sqe_set_completion(self, sqe, completion);
                if (submit_one_completion(self, sqe, (PyObject *)completion) < 0) {
                    failed = 1;
                }
            }
        }
    }
    Py_END_CRITICAL_SECTION();

    if (failed) {
        Py_DECREF(completion);
        return NULL;
    }
    return Py_NewRef(completion);
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

    if (parse_recv_multishot_args(args, nargs, &fd, &buf_group_obj, &user_data, &flags, &base_sequence) < 0) {
        return NULL;
    }

    return UringApiRing_submit_recv_multishot_impl(self, fd, buf_group_obj, flags, user_data, base_sequence);
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

PyObject *UringApiRing_submit_accept(UringApiRing *self, PyObject *args, PyObject *kwargs) {
    static char *keywords[] = {"fd", "user_data", "flags", NULL};
    int fd;
    unsigned int flags = 0;
    PyObject *user_data = Py_None;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "i|OI", keywords, &fd, &user_data, &flags)) {
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

PyObject *UringApiRing_submit_poll(UringApiRing *self, PyObject *args, PyObject *kwargs) {
    static char *keywords[] = {"fd", "mask", "user_data", NULL};
    int fd;
    unsigned int poll_mask;
    PyObject *user_data = Py_None;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "iI|O", keywords, &fd, &poll_mask, &user_data)) {
        return NULL;
    }
    return UringApiRing_submit_poll_impl(self, fd, poll_mask, user_data);
}

PyObject *UringApiRing_submit_poll_multishot(UringApiRing *self, PyObject *args, PyObject *kwargs) {
    static char *keywords[] = {"fd", "mask", "user_data", NULL};
    int fd;
    unsigned int poll_mask;
    PyObject *user_data = Py_None;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "iI|O", keywords, &fd, &poll_mask, &user_data)) {
        return NULL;
    }
    return UringApiRing_submit_poll_multishot_impl(self, fd, poll_mask, user_data);
}

PyObject *UringApiRing_submit_poll_remove(UringApiRing *self, PyObject *args, PyObject *kwargs) {
    static char *keywords[] = {"completion", "user_data", NULL};
    PyObject *target_completion;
    PyObject *user_data = Py_None;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O!|O", keywords, &UringApiCompletion_Type, &target_completion,
                                     &user_data)) {
        return NULL;
    }
    return UringApiRing_submit_poll_remove_impl(self, target_completion, user_data);
}

PyObject *UringApiRing_submit_cancel(UringApiRing *self, PyObject *args, PyObject *kwargs) {
    static char *keywords[] = {"completion", "user_data", NULL};
    PyObject *target_completion;
    PyObject *user_data = Py_None;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O!|O", keywords, &UringApiCompletion_Type, &target_completion,
                                     &user_data)) {
        return NULL;
    }
    return UringApiRing_submit_cancel_impl(self, target_completion, user_data);
}

PyObject *UringApiRing_submit_shutdown(UringApiRing *self, PyObject *args, PyObject *kwargs) {
    static char *keywords[] = {"fd", "how", "user_data", NULL};
    int fd;
    int how;
    PyObject *user_data = Py_None;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "ii|O", keywords, &fd, &how, &user_data)) {
        return NULL;
    }
    return UringApiRing_submit_shutdown_impl(self, fd, how, user_data);
}

PyObject *UringApiRing_submit_close(UringApiRing *self, PyObject *args, PyObject *kwargs) {
    static char *keywords[] = {"fd", "user_data", NULL};
    int fd;
    PyObject *user_data = Py_None;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "i|O", keywords, &fd, &user_data)) {
        return NULL;
    }
    return UringApiRing_submit_close_impl(self, fd, user_data);
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