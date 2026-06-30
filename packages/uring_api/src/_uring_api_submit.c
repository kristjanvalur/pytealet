/*
 * Submission methods for the _uring_api Ring type.
 *
 * This file contains the Python-visible submit_* wrappers that prepare SQEs,
 * attach Completion objects, and submit work to the ring. It is included by
 * _uring_api.c as part of the single extension translation unit.
 */

static PyObject *UringApiRing_submit_recv(UringApiRing *self, PyObject *args, PyObject *kwargs) {
    static char *keywords[] = {"fd", "buf", "user_data", NULL};
    struct io_uring_sqe *sqe;
    Py_buffer view;
    long fd;
    PyObject *user_data = Py_None;
    PyObject *completion = NULL;
    int failed = 0;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "lw*|O", keywords, &fd, &view, &user_data)) {
        return NULL;
    }
    if (fd < 0 || fd > INT_MAX) {
        PyBuffer_Release(&view);
        PyErr_SetString(PyExc_ValueError, "fd must fit in a non-negative int");
        return NULL;
    }

    completion = UringApiCompletion_new_pending_view(URING_API_PENDING_RECV, user_data, &view);
    if (!completion) {
        PyBuffer_Release(&view);
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
            io_uring_prep_recv(sqe, (int)fd, view.buf, (size_t)view.len, 0);
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

static PyObject *UringApiRing_submit_recv_multishot(UringApiRing *self, PyObject *args, PyObject *kwargs) {
    static char *keywords[] = {"fd", "buf_group", "user_data", "flags", NULL};
    struct io_uring_sqe *sqe;
    UringApiBufGroup *buf_group;
    long fd;
    unsigned int flags = 0;
    PyObject *user_data = Py_None;
    PyObject *buf_group_obj;
    PyObject *completion = NULL;
    UringApiCompletion *pending;
    int failed = 0;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "lO!|OI", keywords, &fd, &UringApiBufGroup_Type, &buf_group_obj,
                                     &user_data, &flags)) {
        return NULL;
    }
    if (fd < 0 || fd > INT_MAX) {
        PyErr_SetString(PyExc_ValueError, "fd must fit in a non-negative int");
        return NULL;
    }
    buf_group = (UringApiBufGroup *)buf_group_obj;
    if (buf_group->ring != self) {
        PyErr_SetString(PyExc_ValueError, "buf_group was not created by this ring");
        return NULL;
    }

    completion = UringApiCompletion_new_pending(URING_API_PENDING_RECV_MULTISHOT, user_data, NULL);
    if (!completion) {
        return NULL;
    }
    pending = (UringApiCompletion *)completion;
    pending->buf_group = Py_NewRef(buf_group_obj);

    Py_BEGIN_CRITICAL_SECTION(self);
    if (ring_check_open(self) < 0) {
        failed = 1;
    } else {
        sqe = get_sqe(self);
        if (!sqe) {
            failed = 1;
        } else {
            io_uring_prep_recv_multishot(sqe, (int)fd, NULL, 0, (int)flags);
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

static PyObject *UringApiRing_submit_recv_multishot_zc(UringApiRing *self, PyObject *args, PyObject *kwargs) {
    static char *keywords[] = {"fd", "buf_group", "user_data", "flags", NULL};
    struct io_uring_sqe *sqe;
    UringApiBufGroup *buf_group;
    long fd;
    unsigned int flags = 0;
    PyObject *user_data = Py_None;
    PyObject *buf_group_obj;
    PyObject *completion = NULL;
    UringApiCompletion *pending;
    int failed = 0;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "lO!|OI", keywords, &fd, &UringApiBufGroup_Type, &buf_group_obj,
                                     &user_data, &flags)) {
        return NULL;
    }
    if (fd < 0 || fd > INT_MAX) {
        PyErr_SetString(PyExc_ValueError, "fd must fit in a non-negative int");
        return NULL;
    }
    buf_group = (UringApiBufGroup *)buf_group_obj;
    if (buf_group->ring != self) {
        PyErr_SetString(PyExc_ValueError, "buf_group was not created by this ring");
        return NULL;
    }

    completion = UringApiCompletion_new_pending(URING_API_PENDING_RECV_MULTISHOT_ZC, user_data, NULL);
    if (!completion) {
        return NULL;
    }
    pending = (UringApiCompletion *)completion;
    pending->buf_group = Py_NewRef(buf_group_obj);

    Py_BEGIN_CRITICAL_SECTION(self);
    if (ring_check_open(self) < 0) {
        failed = 1;
    } else {
        sqe = get_sqe(self);
        if (!sqe) {
            failed = 1;
        } else {
            io_uring_prep_recv_multishot(sqe, (int)fd, NULL, 0, (int)flags);
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

static PyObject *UringApiRing_submit_send(UringApiRing *self, PyObject *args, PyObject *kwargs) {
    static char *keywords[] = {"fd", "data", "user_data", "flags", NULL};
    struct io_uring_sqe *sqe;
    Py_buffer view;
    long fd;
    unsigned int flags = 0;
    PyObject *user_data = Py_None;
    PyObject *completion = NULL;
    int failed = 0;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "ly*|OI", keywords, &fd, &view, &user_data, &flags)) {
        return NULL;
    }
    if (fd < 0 || fd > INT_MAX) {
        PyBuffer_Release(&view);
        PyErr_SetString(PyExc_ValueError, "fd must fit in a non-negative int");
        return NULL;
    }

    completion = UringApiCompletion_new_pending_view(URING_API_PENDING_SEND, user_data, &view);
    if (!completion) {
        PyBuffer_Release(&view);
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
            io_uring_prep_send(sqe, (int)fd, view.buf, (size_t)view.len, (int)flags);
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

static PyObject *UringApiRing_submit_send_zc(UringApiRing *self, PyObject *args, PyObject *kwargs) {
    static char *keywords[] = {"fd", "data", "user_data", "flags", "zc_flags", NULL};
    struct io_uring_sqe *sqe;
    Py_buffer view;
    long fd;
    unsigned int flags = 0;
    unsigned int zc_flags = 0;
    PyObject *user_data = Py_None;
    PyObject *completion = NULL;
    int failed = 0;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "ly*|OII", keywords, &fd, &view, &user_data, &flags, &zc_flags)) {
        return NULL;
    }
    if (fd < 0 || fd > INT_MAX) {
        PyBuffer_Release(&view);
        PyErr_SetString(PyExc_ValueError, "fd must fit in a non-negative int");
        return NULL;
    }

    completion = UringApiCompletion_new_pending_view(URING_API_PENDING_SEND_ZC, user_data, &view);
    if (!completion) {
        PyBuffer_Release(&view);
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
            io_uring_prep_send_zc(sqe, (int)fd, view.buf, (size_t)view.len, (int)flags, zc_flags);
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

static PyObject *UringApiRing_submit_sendto(UringApiRing *self, PyObject *args, PyObject *kwargs) {
    static char *keywords[] = {"fd", "data", "address", "user_data", "flags", NULL};
    struct io_uring_sqe *sqe;
    Py_buffer view;
    long fd;
    unsigned int flags = 0;
    PyObject *address;
    PyObject *user_data = Py_None;
    PyObject *completion = NULL;
    UringApiCompletion *pending;
    int failed = 0;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "ly*O|OI", keywords, &fd, &view, &address, &user_data, &flags)) {
        return NULL;
    }
    if (fd < 0 || fd > INT_MAX) {
        PyBuffer_Release(&view);
        PyErr_SetString(PyExc_ValueError, "fd must fit in a non-negative int");
        return NULL;
    }

    completion = UringApiCompletion_new_pending_view(URING_API_PENDING_SENDTO, user_data, &view);
    if (!completion) {
        PyBuffer_Release(&view);
        return NULL;
    }
    pending = (UringApiCompletion *)completion;
    if (parse_numeric_sockaddr((int)fd, address, &pending->addr, &pending->addrlen) < 0) {
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
            io_uring_prep_sendto(sqe, (int)fd, view.buf, (size_t)view.len, (int)flags,
                                 (struct sockaddr *)&pending->addr, pending->addrlen);
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

static PyObject *UringApiRing_submit_recvmsg(UringApiRing *self, PyObject *args, PyObject *kwargs) {
    static char *keywords[] = {"fd", "buf", "user_data", NULL};
    struct io_uring_sqe *sqe;
    Py_buffer view;
    long fd;
    PyObject *user_data = Py_None;
    PyObject *completion = NULL;
    int failed = 0;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "lw*|O", keywords, &fd, &view, &user_data)) {
        return NULL;
    }
    if (fd < 0 || fd > INT_MAX) {
        PyBuffer_Release(&view);
        PyErr_SetString(PyExc_ValueError, "fd must fit in a non-negative int");
        return NULL;
    }

    completion = UringApiCompletion_new_pending_recvmsg(URING_API_PENDING_RECVMSG, user_data, &view);
    if (!completion) {
        PyBuffer_Release(&view);
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
            io_uring_prep_recvmsg(sqe, (int)fd, &((UringApiCompletion *)completion)->msg, 0);
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

static PyObject *UringApiRing_submit_sendmsg(UringApiRing *self, PyObject *args, PyObject *kwargs) {
    static char *keywords[] = {"fd", "data", "address", "user_data", "flags", NULL};
    struct io_uring_sqe *sqe;
    Py_buffer view;
    long fd;
    unsigned int flags = 0;
    PyObject *address = Py_None;
    PyObject *user_data = Py_None;
    PyObject *completion = NULL;
    UringApiCompletion *pending;
    int failed = 0;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "ly*|OOI", keywords, &fd, &view, &address, &user_data, &flags)) {
        return NULL;
    }
    if (fd < 0 || fd > INT_MAX) {
        PyBuffer_Release(&view);
        PyErr_SetString(PyExc_ValueError, "fd must fit in a non-negative int");
        return NULL;
    }

    completion = UringApiCompletion_new_pending_sendmsg(URING_API_PENDING_SENDMSG, user_data, &view);
    if (!completion) {
        PyBuffer_Release(&view);
        return NULL;
    }
    pending = (UringApiCompletion *)completion;
    if (address != Py_None) {
        if (parse_numeric_sockaddr((int)fd, address, &pending->addr, &pending->addrlen) < 0) {
            Py_DECREF(completion);
            return NULL;
        }
        pending->msg.msg_name = &pending->addr;
        pending->msg.msg_namelen = pending->addrlen;
    }

    Py_BEGIN_CRITICAL_SECTION(self);
    if (ring_check_open(self) < 0) {
        failed = 1;
    } else {
        sqe = get_sqe(self);
        if (!sqe) {
            failed = 1;
        } else {
            io_uring_prep_sendmsg(sqe, (int)fd, &pending->msg, flags);
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

static PyObject *UringApiRing_submit_sendmsg_zc(UringApiRing *self, PyObject *args, PyObject *kwargs) {
    static char *keywords[] = {"fd", "data", "address", "user_data", "flags", NULL};
    struct io_uring_sqe *sqe;
    Py_buffer view;
    long fd;
    unsigned int flags = 0;
    PyObject *address = Py_None;
    PyObject *user_data = Py_None;
    PyObject *completion = NULL;
    UringApiCompletion *pending;
    int failed = 0;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "ly*|OOI", keywords, &fd, &view, &address, &user_data, &flags)) {
        return NULL;
    }
    if (fd < 0 || fd > INT_MAX) {
        PyBuffer_Release(&view);
        PyErr_SetString(PyExc_ValueError, "fd must fit in a non-negative int");
        return NULL;
    }

    completion = UringApiCompletion_new_pending_sendmsg(URING_API_PENDING_SENDMSG_ZC, user_data, &view);
    if (!completion) {
        PyBuffer_Release(&view);
        return NULL;
    }
    pending = (UringApiCompletion *)completion;
    if (address != Py_None) {
        if (parse_numeric_sockaddr((int)fd, address, &pending->addr, &pending->addrlen) < 0) {
            Py_DECREF(completion);
            return NULL;
        }
        pending->msg.msg_name = &pending->addr;
        pending->msg.msg_namelen = pending->addrlen;
    }

    Py_BEGIN_CRITICAL_SECTION(self);
    if (ring_check_open(self) < 0) {
        failed = 1;
    } else {
        sqe = get_sqe(self);
        if (!sqe) {
            failed = 1;
        } else {
            io_uring_prep_sendmsg_zc(sqe, (int)fd, &pending->msg, flags);
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

static PyObject *UringApiRing_submit_accept(UringApiRing *self, PyObject *args, PyObject *kwargs) {
    static char *keywords[] = {"fd", "user_data", "flags", NULL};
    struct io_uring_sqe *sqe;
    long fd;
    unsigned int flags = 0;
    PyObject *user_data = Py_None;
    PyObject *completion = NULL;
    UringApiCompletion *pending;
    int failed = 0;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "l|OI", keywords, &fd, &user_data, &flags)) {
        return NULL;
    }
    if (fd < 0 || fd > INT_MAX) {
        PyErr_SetString(PyExc_ValueError, "fd must fit in a non-negative int");
        return NULL;
    }

    completion = UringApiCompletion_new_pending_accept(user_data);
    if (!completion) {
        return NULL;
    }
    pending = (UringApiCompletion *)completion;

    Py_BEGIN_CRITICAL_SECTION(self);
    if (ring_check_open(self) < 0) {
        failed = 1;
    } else {
        sqe = get_sqe(self);
        if (!sqe) {
            failed = 1;
        } else {
            io_uring_prep_accept(sqe, (int)fd, (struct sockaddr *)&pending->addr, &pending->addrlen, flags);
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

static PyObject *UringApiRing_submit_accept_multishot(UringApiRing *self, PyObject *args, PyObject *kwargs) {
    static char *keywords[] = {"fd", "user_data", "flags", NULL};
    struct io_uring_sqe *sqe;
    long fd;
    unsigned int flags = 0;
    PyObject *user_data = Py_None;
    PyObject *completion = NULL;
    UringApiCompletion *pending;
    int failed = 0;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "l|OI", keywords, &fd, &user_data, &flags)) {
        return NULL;
    }
    if (fd < 0 || fd > INT_MAX) {
        PyErr_SetString(PyExc_ValueError, "fd must fit in a non-negative int");
        return NULL;
    }

    completion = UringApiCompletion_new_pending_accept(user_data);
    if (!completion) {
        return NULL;
    }
    pending = (UringApiCompletion *)completion;

    Py_BEGIN_CRITICAL_SECTION(self);
    if (ring_check_open(self) < 0) {
        failed = 1;
    } else {
        sqe = get_sqe(self);
        if (!sqe) {
            failed = 1;
        } else {
            io_uring_prep_multishot_accept(sqe, (int)fd, (struct sockaddr *)&pending->addr, &pending->addrlen, flags);
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

static PyObject *UringApiRing_submit_connect(UringApiRing *self, PyObject *args, PyObject *kwargs) {
    static char *keywords[] = {"fd", "address", "user_data", NULL};
    struct io_uring_sqe *sqe;
    long fd;
    PyObject *address;
    PyObject *user_data = Py_None;
    PyObject *completion = NULL;
    UringApiCompletion *pending;
    int failed = 0;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "lO|O", keywords, &fd, &address, &user_data)) {
        return NULL;
    }
    if (fd < 0 || fd > INT_MAX) {
        PyErr_SetString(PyExc_ValueError, "fd must fit in a non-negative int");
        return NULL;
    }

    completion = UringApiCompletion_new_pending(URING_API_PENDING_CONNECT, user_data, NULL);
    if (!completion) {
        return NULL;
    }
    pending = (UringApiCompletion *)completion;
    if (parse_numeric_sockaddr((int)fd, address, &pending->addr, &pending->addrlen) < 0) {
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
            io_uring_prep_connect(sqe, (int)fd, (struct sockaddr *)&pending->addr, pending->addrlen);
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

static PyObject *UringApiRing_submit_cancel(UringApiRing *self, PyObject *args, PyObject *kwargs) {
    static char *keywords[] = {"completion", NULL};
    struct io_uring_sqe *sqe;
    PyObject *target_completion;
    PyObject *completion = NULL;
    int failed = 0;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O!", keywords, &UringApiCompletion_Type, &target_completion)) {
        return NULL;
    }

    completion = UringApiCompletion_new_pending(URING_API_PENDING_CANCEL, target_completion, NULL);
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

static PyObject *UringApiRing_submit_shutdown(UringApiRing *self, PyObject *args, PyObject *kwargs) {
    static char *keywords[] = {"fd", "how", "user_data", NULL};
    struct io_uring_sqe *sqe;
    long fd;
    long how;
    PyObject *user_data = Py_None;
    PyObject *completion = NULL;
    int failed = 0;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "ll|O", keywords, &fd, &how, &user_data)) {
        return NULL;
    }
    if (fd < 0 || fd > INT_MAX) {
        PyErr_SetString(PyExc_ValueError, "fd must fit in a non-negative int");
        return NULL;
    }
    if (how < 0 || how > INT_MAX) {
        PyErr_SetString(PyExc_ValueError, "how must fit in a non-negative int");
        return NULL;
    }

    completion = UringApiCompletion_new_pending(URING_API_PENDING_SHUTDOWN, user_data, NULL);
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
            io_uring_prep_shutdown(sqe, (int)fd, (int)how);
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

static PyObject *UringApiRing_submit_close(UringApiRing *self, PyObject *args, PyObject *kwargs) {
    static char *keywords[] = {"fd", "user_data", NULL};
    struct io_uring_sqe *sqe;
    long fd;
    PyObject *user_data = Py_None;
    PyObject *completion = NULL;
    int failed = 0;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "l|O", keywords, &fd, &user_data)) {
        return NULL;
    }
    if (fd < 0 || fd > INT_MAX) {
        PyErr_SetString(PyExc_ValueError, "fd must fit in a non-negative int");
        return NULL;
    }

    completion = UringApiCompletion_new_pending(URING_API_PENDING_CLOSE, user_data, NULL);
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
            io_uring_prep_close(sqe, (int)fd);
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

static PyObject *UringApiRing_submit_socket(UringApiRing *self, PyObject *args, PyObject *kwargs) {
    static char *keywords[] = {"domain", "type", "protocol", "flags", "user_data", NULL};
    struct io_uring_sqe *sqe;
    long domain;
    long type;
    long protocol = 0;
    unsigned int flags = 0;
    PyObject *user_data = Py_None;
    PyObject *completion = NULL;
    int failed = 0;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "ll|lIO", keywords, &domain, &type, &protocol, &flags, &user_data)) {
        return NULL;
    }
    if (domain < 0 || domain > INT_MAX) {
        PyErr_SetString(PyExc_ValueError, "domain must fit in a non-negative int");
        return NULL;
    }
    if (type < 0 || type > INT_MAX) {
        PyErr_SetString(PyExc_ValueError, "type must fit in a non-negative int");
        return NULL;
    }
    if (protocol < 0 || protocol > INT_MAX) {
        PyErr_SetString(PyExc_ValueError, "protocol must fit in a non-negative int");
        return NULL;
    }

    completion = UringApiCompletion_new_pending(URING_API_PENDING_SOCKET, user_data, NULL);
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
            io_uring_prep_socket(sqe, (int)domain, (int)type, (int)protocol, flags);
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
