/*
 * Runtime capability probes for the _uring_api extension.
 */

#include "uring_api_probe.h"
#include "uring_api_capi_impl.h"
#include "uring_api_core.h"

static PyObject *build_probe_result(bool available) {
    PyObject *result;

    if (!available) {
        return PyDict_New();
    }

    result = build_capability_dict();
    if (!result) {
        return NULL;
    }
    if (PyDict_SetItemString(result, "available", Py_True) < 0) {
        Py_DECREF(result);
        return NULL;
    }
    return result;
}

static PyObject *uring_api_probe_impl(unsigned int entries, unsigned int flags) {
    struct io_uring ring;
    struct io_uring_params params;
    int ret;

    if (entries == 0) {
        PyErr_SetString(PyExc_ValueError, "entries must be between 1 and UINT_MAX");
        return NULL;
    }

    memset(&ring, 0, sizeof(ring));
    memset(&params, 0, sizeof(params));
    params.flags = flags;

    errno = 0;
    Py_BEGIN_ALLOW_THREADS;
    ret = io_uring_queue_init_params(entries, &ring, &params);
    Py_END_ALLOW_THREADS;

    if (ret < 0) {
        return build_probe_result(false);
    }

    io_uring_queue_exit(&ring);
    return build_probe_result(true);
}

PyObject *uring_api_probe(PyObject *self, PyObject *args, PyObject *kwargs) {
    unsigned int entries;
    unsigned int flags;

    if (parse_entries_flags(args, kwargs, 2, &entries, &flags) < 0) {
        return NULL;
    }
    return uring_api_probe_impl(entries, flags);
}

static void close_if_open(int *fd) {
    if (*fd >= 0) {
        close(*fd);
        *fd = -1;
    }
}

static PyObject *uring_api_probe_accept_multishot_impl(void) {
#ifndef IORING_ACCEPT_MULTISHOT
    return build_feature_probe_result(false, ENOSYS, "liburing headers do not define IORING_ACCEPT_MULTISHOT");
#else
    struct io_uring ring;
    struct io_uring_sqe *sqe;
    struct io_uring_cqe *cqe = NULL;
    struct __kernel_timespec timeout;
    struct sockaddr_in listen_addr;
    struct sockaddr_storage accepted_addr;
    socklen_t listen_addrlen = sizeof(listen_addr);
    socklen_t accepted_addrlen = sizeof(accepted_addr);
    int server_fd = -1;
    int client_fd = -1;
    int accepted_fd = -1;
    int optval = 1;
    int ret;
    PyObject *result;

    memset(&ring, 0, sizeof(ring));
    ret = io_uring_queue_init(8, &ring, 0);
    if (ret < 0) {
        int errnum = normalize_ret_errno(ret);
        return build_feature_probe_result(false, errnum, strerror(errnum));
    }

    server_fd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (server_fd < 0) {
        result = build_feature_probe_result(false, errno, strerror(errno));
        goto cleanup;
    }
    (void)setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval));

    memset(&listen_addr, 0, sizeof(listen_addr));
    listen_addr.sin_family = AF_INET;
    listen_addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    listen_addr.sin_port = 0;
    if (bind(server_fd, (struct sockaddr *)&listen_addr, sizeof(listen_addr)) < 0) {
        result = build_feature_probe_result(false, errno, strerror(errno));
        goto cleanup;
    }
    if (listen(server_fd, 1) < 0) {
        result = build_feature_probe_result(false, errno, strerror(errno));
        goto cleanup;
    }
    if (getsockname(server_fd, (struct sockaddr *)&listen_addr, &listen_addrlen) < 0) {
        result = build_feature_probe_result(false, errno, strerror(errno));
        goto cleanup;
    }

    sqe = io_uring_get_sqe(&ring);
    if (!sqe) {
        result = build_feature_probe_result(false, EBUSY, "no submission queue entry available for probe");
        goto cleanup;
    }
    memset(&accepted_addr, 0, sizeof(accepted_addr));
    io_uring_prep_multishot_accept(sqe, server_fd, (struct sockaddr *)&accepted_addr, &accepted_addrlen, SOCK_CLOEXEC);
    io_uring_sqe_set_data64(sqe, 1);
    ret = io_uring_submit(&ring);
    if (ret < 0) {
        int errnum = normalize_ret_errno(ret);
        result = build_feature_probe_result(false, errnum, strerror(errnum));
        goto cleanup;
    }

    client_fd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (client_fd < 0) {
        result = build_feature_probe_result(false, errno, strerror(errno));
        goto cleanup;
    }
    if (connect(client_fd, (struct sockaddr *)&listen_addr, listen_addrlen) < 0) {
        result = build_feature_probe_result(false, errno, strerror(errno));
        goto cleanup;
    }

    timeout.tv_sec = 1;
    timeout.tv_nsec = 0;
    ret = io_uring_wait_cqe_timeout(&ring, &cqe, &timeout);
    if (ret < 0) {
        int errnum = normalize_ret_errno(ret);
        result = build_feature_probe_result(false, errnum, strerror(errnum));
        goto cleanup;
    }
    if (!cqe) {
        result = build_feature_probe_result(false, ETIMEDOUT, "multishot accept probe timed out");
        goto cleanup;
    }
    if (cqe->res < 0) {
        int errnum = -cqe->res;
        result = build_feature_probe_result(false, errnum, strerror(errnum));
        io_uring_cqe_seen(&ring, cqe);
        goto cleanup;
    }

    accepted_fd = cqe->res;
    if (cqe->flags & IORING_CQE_F_MORE) {
        result = build_feature_probe_result(true, 0, NULL);
    } else {
        result = build_feature_probe_result(false, EOPNOTSUPP, "accept completed without IORING_CQE_F_MORE");
    }
    io_uring_cqe_seen(&ring, cqe);

cleanup:
    close_if_open(&accepted_fd);
    close_if_open(&client_fd);
    close_if_open(&server_fd);
    io_uring_queue_exit(&ring);
    return result;
#endif
}

static PyObject *uring_api_probe_recv_multishot_impl(void) {
    struct io_uring ring;
    struct io_uring_sqe *sqe;
    struct io_uring_cqe *cqe = NULL;
    struct io_uring_buf_ring *ring_buffer = NULL;
    struct __kernel_timespec timeout;
    unsigned char storage[8];
    int sockets[2] = {-1, -1};
    int ret = 0;
    int mask;
    PyObject *result;

    memset(&ring, 0, sizeof(ring));
    ret = io_uring_queue_init(8, &ring, 0);
    if (ret < 0) {
        int errnum = normalize_ret_errno(ret);
        return build_feature_probe_result(false, errnum, strerror(errnum));
    }

    ring_buffer = io_uring_setup_buf_ring(&ring, 1, 1, 0, &ret);
    if (!ring_buffer) {
        int errnum = normalize_ret_errno(ret);
        result = build_feature_probe_result(false, errnum, strerror(errnum));
        goto cleanup;
    }
    mask = io_uring_buf_ring_mask(1);
    io_uring_buf_ring_add(ring_buffer, storage, sizeof(storage), 0, mask, 0);
    io_uring_buf_ring_advance(ring_buffer, 1);

    if (socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, sockets) < 0) {
        result = build_feature_probe_result(false, errno, strerror(errno));
        goto cleanup;
    }

    sqe = io_uring_get_sqe(&ring);
    if (!sqe) {
        result = build_feature_probe_result(false, EBUSY, "no submission queue entry available for probe");
        goto cleanup;
    }
    io_uring_prep_recv_multishot(sqe, sockets[0], NULL, 0, 0);
    sqe->flags |= IOSQE_BUFFER_SELECT;
    sqe->buf_group = 1;
    io_uring_sqe_set_data64(sqe, 1);
    ret = io_uring_submit(&ring);
    if (ret < 0) {
        int errnum = normalize_ret_errno(ret);
        result = build_feature_probe_result(false, errnum, strerror(errnum));
        goto cleanup;
    }

    if (send(sockets[1], "x", 1, 0) < 0) {
        result = build_feature_probe_result(false, errno, strerror(errno));
        goto cleanup;
    }

    timeout.tv_sec = 1;
    timeout.tv_nsec = 0;
    ret = io_uring_wait_cqe_timeout(&ring, &cqe, &timeout);
    if (ret < 0) {
        int errnum = normalize_ret_errno(ret);
        result = build_feature_probe_result(false, errnum, strerror(errnum));
        goto cleanup;
    }
    if (!cqe) {
        result = build_feature_probe_result(false, ETIMEDOUT, "recv multishot probe timed out");
        goto cleanup;
    }
    if (cqe->res < 0) {
        int errnum = -cqe->res;
        result = build_feature_probe_result(false, errnum, strerror(errnum));
        io_uring_cqe_seen(&ring, cqe);
        cqe = NULL;
        goto cleanup;
    }
    if (cqe->res != 1) {
        result = build_feature_probe_result(false, EIO, "recv multishot probe returned an unexpected length");
        io_uring_cqe_seen(&ring, cqe);
        cqe = NULL;
        goto cleanup;
    }
    if (!(cqe->flags & IORING_CQE_F_BUFFER)) {
        result = build_feature_probe_result(false, EPROTO, "recv multishot probe did not select a buffer");
        io_uring_cqe_seen(&ring, cqe);
        cqe = NULL;
        goto cleanup;
    }
    if (cqe->flags & IORING_CQE_F_MORE) {
        result = build_feature_probe_result(true, 0, NULL);
    } else {
        result = build_feature_probe_result(false, EOPNOTSUPP, "recv completed without IORING_CQE_F_MORE");
    }
    io_uring_cqe_seen(&ring, cqe);
    cqe = NULL;

cleanup:
    close_if_open(&sockets[1]);
    if (cqe) {
        io_uring_cqe_seen(&ring, cqe);
    }
    timeout.tv_sec = 0;
    timeout.tv_nsec = 1000000;
    if (io_uring_wait_cqe_timeout(&ring, &cqe, &timeout) == 0 && cqe) {
        io_uring_cqe_seen(&ring, cqe);
    }
    close_if_open(&sockets[0]);
    if (ring_buffer) {
        (void)io_uring_free_buf_ring(&ring, ring_buffer, 1, 1);
    }
    io_uring_queue_exit(&ring);
    return result;
}

static PyObject *uring_api_probe_socket_impl(void) {
    struct io_uring ring;
    struct io_uring_sqe *sqe;
    struct io_uring_cqe *cqe = NULL;
    struct __kernel_timespec timeout;
    int socket_fd = -1;
    int ret;
    PyObject *result;

    memset(&ring, 0, sizeof(ring));
    ret = io_uring_queue_init(2, &ring, 0);
    if (ret < 0) {
        int errnum = normalize_ret_errno(ret);
        return build_feature_probe_result(false, errnum, strerror(errnum));
    }

    sqe = io_uring_get_sqe(&ring);
    if (!sqe) {
        result = build_feature_probe_result(false, EBUSY, "no submission queue entry available for probe");
        goto cleanup;
    }
    io_uring_prep_socket(sqe, AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0, 0);
    io_uring_sqe_set_data64(sqe, 1);
    ret = io_uring_submit(&ring);
    if (ret < 0) {
        int errnum = normalize_ret_errno(ret);
        result = build_feature_probe_result(false, errnum, strerror(errnum));
        goto cleanup;
    }

    timeout.tv_sec = 1;
    timeout.tv_nsec = 0;
    ret = io_uring_wait_cqe_timeout(&ring, &cqe, &timeout);
    if (ret < 0) {
        int errnum = normalize_ret_errno(ret);
        result = build_feature_probe_result(false, errnum, strerror(errnum));
        goto cleanup;
    }
    if (!cqe) {
        result = build_feature_probe_result(false, ETIMEDOUT, "socket probe timed out");
        goto cleanup;
    }
    if (cqe->res < 0) {
        int errnum = -cqe->res;
        result = build_feature_probe_result(false, errnum, strerror(errnum));
        io_uring_cqe_seen(&ring, cqe);
        goto cleanup;
    }

    socket_fd = cqe->res;
    result = build_feature_probe_result(true, 0, NULL);
    io_uring_cqe_seen(&ring, cqe);

cleanup:
    close_if_open(&socket_fd);
    io_uring_queue_exit(&ring);
    return result;
}

static PyObject *uring_api_probe_sendmsg_zc_impl(void) {
    struct io_uring ring;
    struct io_uring_sqe *sqe;
    struct io_uring_cqe *cqe = NULL;
    struct __kernel_timespec timeout;
    struct sockaddr_in receiver_addr;
    socklen_t receiver_addrlen = sizeof(receiver_addr);
    struct iovec iov;
    struct msghdr msg;
    int sender_fd = -1;
    int receiver_fd = -1;
    int ret;
    char payload = 'x';
    PyObject *result;

    memset(&ring, 0, sizeof(ring));
    ret = io_uring_queue_init(2, &ring, 0);
    if (ret < 0) {
        int errnum = normalize_ret_errno(ret);
        return build_feature_probe_result(false, errnum, strerror(errnum));
    }

    receiver_fd = socket(AF_INET, SOCK_DGRAM | SOCK_CLOEXEC, 0);
    sender_fd = socket(AF_INET, SOCK_DGRAM | SOCK_CLOEXEC, 0);
    if (receiver_fd < 0 || sender_fd < 0) {
        result = build_feature_probe_result(false, errno, strerror(errno));
        goto cleanup;
    }

    memset(&receiver_addr, 0, sizeof(receiver_addr));
    receiver_addr.sin_family = AF_INET;
    receiver_addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    receiver_addr.sin_port = 0;
    if (bind(receiver_fd, (struct sockaddr *)&receiver_addr, sizeof(receiver_addr)) < 0 ||
        getsockname(receiver_fd, (struct sockaddr *)&receiver_addr, &receiver_addrlen) < 0) {
        result = build_feature_probe_result(false, errno, strerror(errno));
        goto cleanup;
    }

    memset(&iov, 0, sizeof(iov));
    memset(&msg, 0, sizeof(msg));
    iov.iov_base = &payload;
    iov.iov_len = sizeof(payload);
    msg.msg_name = &receiver_addr;
    msg.msg_namelen = receiver_addrlen;
    msg.msg_iov = &iov;
    msg.msg_iovlen = 1;

    sqe = io_uring_get_sqe(&ring);
    if (!sqe) {
        result = build_feature_probe_result(false, EBUSY, "no submission queue entry available for probe");
        goto cleanup;
    }
    io_uring_prep_sendmsg_zc(sqe, sender_fd, &msg, 0);
    io_uring_sqe_set_data64(sqe, 1);
    ret = io_uring_submit(&ring);
    if (ret < 0) {
        int errnum = normalize_ret_errno(ret);
        result = build_feature_probe_result(false, errnum, strerror(errnum));
        goto cleanup;
    }

    timeout.tv_sec = 1;
    timeout.tv_nsec = 0;
    ret = io_uring_wait_cqe_timeout(&ring, &cqe, &timeout);
    if (ret < 0) {
        int errnum = normalize_ret_errno(ret);
        result = build_feature_probe_result(false, errnum, strerror(errnum));
        goto cleanup;
    }
    if (!cqe) {
        result = build_feature_probe_result(false, ETIMEDOUT, "sendmsg_zc probe timed out");
        goto cleanup;
    }
    if (cqe->res < 0) {
        int errnum = -cqe->res;
        result = build_feature_probe_result(false, errnum, strerror(errnum));
    } else if (cqe->res == (int)sizeof(payload)) {
        result = build_feature_probe_result(true, 0, NULL);
    } else {
        result = build_feature_probe_result(false, EIO, "sendmsg_zc probe returned an unexpected length");
    }
    io_uring_cqe_seen(&ring, cqe);
    cqe = NULL;

cleanup:
    if (cqe) {
        io_uring_cqe_seen(&ring, cqe);
    }
    timeout.tv_sec = 0;
    timeout.tv_nsec = 1000000;
    if (io_uring_wait_cqe_timeout(&ring, &cqe, &timeout) == 0 && cqe) {
        io_uring_cqe_seen(&ring, cqe);
    }
    close_if_open(&sender_fd);
    close_if_open(&receiver_fd);
    io_uring_queue_exit(&ring);
    return result;
}

static int add_bool_from_feature_probe(PyObject *capabilities, const char *name, PyObject *probe_result) {
    PyObject *available;
    int truth;

    available = PyDict_GetItemString(probe_result, "available");
    if (!available) {
        PyErr_SetString(PyExc_RuntimeError, "feature probe result is missing 'available'");
        return -1;
    }
    truth = PyObject_IsTrue(available);
    if (truth < 0) {
        return -1;
    }
    return PyDict_SetItemString(capabilities, name, truth ? Py_True : Py_False);
}

PyObject *build_capability_dict(void) {
    PyObject *capabilities;
    PyObject *probe_result;

    capabilities = PyDict_New();
    if (!capabilities) {
        return NULL;
    }

    probe_result = uring_api_probe_accept_multishot_impl();
    if (!probe_result) {
        Py_DECREF(capabilities);
        return NULL;
    }
    if (add_bool_from_feature_probe(capabilities, "IORING_ACCEPT_MULTISHOT", probe_result) < 0) {
        Py_DECREF(probe_result);
        Py_DECREF(capabilities);
        return NULL;
    }
    Py_DECREF(probe_result);

    probe_result = uring_api_probe_recv_multishot_impl();
    if (!probe_result) {
        Py_DECREF(capabilities);
        return NULL;
    }
    if (add_bool_from_feature_probe(capabilities, "IORING_RECV_MULTISHOT", probe_result) < 0) {
        Py_DECREF(probe_result);
        Py_DECREF(capabilities);
        return NULL;
    }
    Py_DECREF(probe_result);

    probe_result = uring_api_probe_socket_impl();
    if (!probe_result) {
        Py_DECREF(capabilities);
        return NULL;
    }
    if (add_bool_from_feature_probe(capabilities, "IORING_OP_SOCKET", probe_result) < 0) {
        Py_DECREF(probe_result);
        Py_DECREF(capabilities);
        return NULL;
    }
    Py_DECREF(probe_result);
    probe_result = uring_api_probe_sendmsg_zc_impl();
    if (!probe_result) {
        Py_DECREF(capabilities);
        return NULL;
    }
    if (add_bool_from_feature_probe(capabilities, "IORING_OP_SEND_ZC", probe_result) < 0) {
        Py_DECREF(probe_result);
        Py_DECREF(capabilities);
        return NULL;
    }
    if (add_bool_from_feature_probe(capabilities, "IORING_OP_SENDMSG_ZC", probe_result) < 0) {
        Py_DECREF(probe_result);
        Py_DECREF(capabilities);
        return NULL;
    }
    Py_DECREF(probe_result);
    return capabilities;
}

static PyObject *UringApiCapi_Probe(unsigned int entries, unsigned int flags) {
    return uring_api_probe_impl(entries, flags);
}

static const UringApi_CAPI uring_api_capi_table = {
    URING_API_CAPI_ABI_VERSION,
    sizeof(UringApi_CAPI),
    URING_API_CAPI_FEATURES,
    IO_URING_VERSION_MAJOR,
    IO_URING_VERSION_MINOR,
    UringApiCapi_Probe,
    UringApiCapi_RingNew,
    UringApiCapi_RingCheck,
    UringApiCapi_RingClose,
    UringApiCapi_RingFd,
    UringApiCapi_RingFeatures,
    UringApiCapi_RingSqEntries,
    UringApiCapi_RingCqEntries,
    UringApiCapi_RingClosed,
    UringApiCapi_RingRunning,
    UringApiCapi_RingSubmitRecv,
    UringApiCapi_RingSubmitRecvMultishot,
    UringApiCapi_RingSubmitSend,
    UringApiCapi_RingSubmitSendZc,
    UringApiCapi_RingSubmitRecvmsg,
    UringApiCapi_RingSubmitSendto,
    UringApiCapi_RingSubmitSendmsg,
    UringApiCapi_RingSubmitSendmsgZc,
    UringApiCapi_RingSubmitAccept,
    UringApiCapi_RingSubmitAcceptMultishot,
    UringApiCapi_RingSubmitConnect,
    UringApiCapi_RingSubmitShutdown,
    UringApiCapi_RingSubmitClose,
    UringApiCapi_RingSubmitSocket,
    UringApiCapi_RingBreakWait,
    UringApiCapi_RingWait,
    UringApiCapi_RingSetCallback,
    UringApiCapi_RingSetCCallback,
    UringApiCapi_RingServeCompletions,
    UringApiCapi_RingStopServing,
    UringApiCapi_RingResetServing,
    UringApiCapi_CompletionCheck,
    UringApiCapi_CompletionUserData,
    UringApiCapi_CompletionRes,
    UringApiCapi_CompletionFlags,
    UringApiCapi_CompletionSequence,
    UringApiCapi_CompletionResult,
    {NULL},
};

int uring_api_export_capi(PyObject *module) {
    PyObject *capsule;

    capsule = PyCapsule_New((void *)&uring_api_capi_table, URING_API_CAPI_CAPSULE_NAME, NULL);
    if (!capsule) {
        return -1;
    }
    if (PyModule_AddObject(module, "_C_API", capsule) < 0) {
        Py_DECREF(capsule);
        return -1;
    }
    if (PyModule_AddIntConstant(module, "C_API_ABI_VERSION", (long)URING_API_CAPI_ABI_VERSION) < 0) {
        return -1;
    }
    if (module_add_uint64_constant(module, "C_API_FEATURE_CORE", URING_API_CAPI_FEATURE_CORE) < 0) {
        return -1;
    }
    if (module_add_uint64_constant(module, "C_API_FEATURES", URING_API_CAPI_FEATURES) < 0) {
        return -1;
    }
    return 0;
}
