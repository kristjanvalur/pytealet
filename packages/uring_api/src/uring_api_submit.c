/*
 * Submission methods for the _uring_api Ring type.
 */

#include "uring_api_submit.h"
#include "uring_api_bufgroup.h"
#include "uring_api_completion.h"
#include "uring_api_core.h"

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
            if (submit_one(self) < 0) {
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
    struct io_uring_sqe *sqe;
    UringApiBufGroup *buf_group;
    int fd;
    unsigned int flags = 0;
    PyObject *user_data = Py_None;
    PyObject *buf_group_obj;
    PyObject *completion = NULL;
    int failed = 0;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "iO!|OI", keywords, &fd, &UringApiBufGroup_Type, &buf_group_obj,
                                     &user_data, &flags)) {
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
            if (submit_one(self) < 0) {
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

PyObject *UringApiRing_submit_recv_multishot_impl(UringApiRing *self, int fd, PyObject *buf_group_obj,
                                                  unsigned int flags, PyObject *user_data) {
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
            if (submit_one(self) < 0) {
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
            if (submit_one(self) < 0) {
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
            if (submit_one(self) < 0) {
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
            if (submit_one(self) < 0) {
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
            if (submit_one(self) < 0) {
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
            if (submit_one(self) < 0) {
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
            if (submit_one(self) < 0) {
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
    UringApiCompletionSockaddrState *sockaddr_state;
    PyObject *completion = NULL;
    int failed = 0;

    completion = UringApiCompletion_new_pending_accept(user_data);
    if (!completion) {
        return NULL;
    }
    sockaddr_state = UringApiCompletion_get_sockaddr_state((UringApiCompletion *)completion);
    if (!sockaddr_state) {
        Py_DECREF(completion);
        PyErr_SetString(PyExc_RuntimeError, "accept completion is missing sockaddr state");
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
            io_uring_prep_accept(sqe, fd, (struct sockaddr *)&sockaddr_state->addr, &sockaddr_state->addrlen, flags);
            sqe_set_completion(self, sqe, completion);
            if (submit_one(self) < 0) {
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

PyObject *UringApiRing_submit_accept_multishot_impl(UringApiRing *self, int fd, unsigned int flags,
                                                    PyObject *user_data) {
    struct io_uring_sqe *sqe;
    UringApiCompletionSockaddrState *sockaddr_state;
    PyObject *completion = NULL;
    UringApiCompletion *pending;
    int failed = 0;

    completion = UringApiCompletion_new_pending_accept(user_data);
    if (!completion) {
        return NULL;
    }
    pending = (UringApiCompletion *)completion;
    pending->multishot = true;
    sockaddr_state = UringApiCompletion_get_sockaddr_state(pending);
    if (!sockaddr_state) {
        Py_DECREF(completion);
        PyErr_SetString(PyExc_RuntimeError, "accept completion is missing sockaddr state");
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
            io_uring_prep_multishot_accept(sqe, fd, (struct sockaddr *)&sockaddr_state->addr, &sockaddr_state->addrlen,
                                           flags);
            sqe_set_completion(self, sqe, completion);
            if (submit_one(self) < 0) {
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
            if (submit_one(self) < 0) {
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
            if (submit_one(self) < 0) {
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
            if (submit_one(self) < 0) {
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

PyObject *UringApiRing_submit_poll_remove_impl(UringApiRing *self, PyObject *target_completion) {
    struct io_uring_sqe *sqe;
    PyObject *completion = NULL;
    int failed = 0;

    completion = UringApiCompletion_new_pending(URING_API_PENDING_POLL_REMOVE, target_completion);
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
            io_uring_prep_poll_remove(sqe, (unsigned long long)(uintptr_t)target_completion);
            sqe_set_completion(self, sqe, completion);
            if (submit_one(self) < 0) {
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

PyObject *UringApiRing_submit_cancel_impl(UringApiRing *self, PyObject *target_completion) {
    struct io_uring_sqe *sqe;
    PyObject *completion = NULL;
    int failed = 0;

    completion = UringApiCompletion_new_pending(URING_API_PENDING_CANCEL, target_completion);
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
            io_uring_prep_cancel(sqe, target_completion, 0);
            sqe_set_completion(self, sqe, completion);
            if (submit_one(self) < 0) {
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
            if (submit_one(self) < 0) {
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
            if (submit_one(self) < 0) {
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
            if (submit_one(self) < 0) {
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

PyObject *UringApiRing_submit_recv_multishot(UringApiRing *self, PyObject *args, PyObject *kwargs) {
    static char *keywords[] = {"fd", "buf_group", "user_data", "flags", NULL};
    int fd;
    unsigned int flags = 0;
    PyObject *user_data = Py_None;
    PyObject *buf_group_obj;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "iO!|OI", keywords, &fd, &UringApiBufGroup_Type, &buf_group_obj,
                                     &user_data, &flags)) {
        return NULL;
    }
    return UringApiRing_submit_recv_multishot_impl(self, fd, buf_group_obj, flags, user_data);
}

PyObject *UringApiRing_submit_send(UringApiRing *self, PyObject *args, PyObject *kwargs) {
    static char *keywords[] = {"fd", "data", "user_data", "flags", NULL};
    Py_buffer view;
    int fd;
    unsigned int flags = 0;
    PyObject *user_data = Py_None;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "iy*|OI", keywords, &fd, &view, &user_data, &flags)) {
        return NULL;
    }
    return UringApiRing_submit_send_impl(self, fd, &view, flags, user_data);
}

PyObject *UringApiRing_submit_send_zc(UringApiRing *self, PyObject *args, PyObject *kwargs) {
    static char *keywords[] = {"fd", "data", "user_data", "flags", "zc_flags", NULL};
    Py_buffer view;
    int fd;
    unsigned int flags = 0;
    unsigned int zc_flags = 0;
    PyObject *user_data = Py_None;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "iy*|OII", keywords, &fd, &view, &user_data, &flags, &zc_flags)) {
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

PyObject *UringApiRing_submit_accept_multishot(UringApiRing *self, PyObject *args, PyObject *kwargs) {
    static char *keywords[] = {"fd", "user_data", "flags", NULL};
    int fd;
    unsigned int flags = 0;
    PyObject *user_data = Py_None;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "i|OI", keywords, &fd, &user_data, &flags)) {
        return NULL;
    }
    return UringApiRing_submit_accept_multishot_impl(self, fd, flags, user_data);
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
    static char *keywords[] = {"completion", NULL};
    PyObject *target_completion;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O!", keywords, &UringApiCompletion_Type, &target_completion)) {
        return NULL;
    }
    return UringApiRing_submit_poll_remove_impl(self, target_completion);
}

PyObject *UringApiRing_submit_cancel(UringApiRing *self, PyObject *args, PyObject *kwargs) {
    static char *keywords[] = {"completion", NULL};
    PyObject *target_completion;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O!", keywords, &UringApiCompletion_Type, &target_completion)) {
        return NULL;
    }
    return UringApiRing_submit_cancel_impl(self, target_completion);
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