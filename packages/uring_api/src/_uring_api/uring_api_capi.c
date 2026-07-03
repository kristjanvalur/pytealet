/*
 * Public C API export for the _uring_api extension.
 */

#include "uring_api_bufgroup.h"
#include "uring_api_capi_impl.h"
#include "uring_api_completion.h"
#include "uring_api_core.h"
#include "uring_api_dispatch.h"
#include "uring_api_ring.h"
#include "uring_api_submit.h"

static int discard_completion_result(PyObject *result) {
    if (!result) {
        return -1;
    }
    Py_DECREF(result);
    return 0;
}

static PyObject *ring_submit_buffer_view(UringApiRing *ring, int fd, PyObject *buf, PyObject *user_data, int writable,
                                         PyObject *(*submit_impl)(UringApiRing *, int, Py_buffer *, PyObject *)) {
    Py_buffer view;
    int flags = writable ? PyBUF_WRITABLE : PyBUF_SIMPLE;

    if (PyObject_GetBuffer(buf, &view, flags) < 0) {
        return NULL;
    }
    return submit_impl(ring, fd, &view, user_data);
}

static int ring_submit_buffer_view_status(UringApiRing *ring, int fd, PyObject *buf, PyObject *user_data, int writable,
                                          PyObject *(*submit_impl)(UringApiRing *, int, Py_buffer *, PyObject *)) {
    return discard_completion_result(ring_submit_buffer_view(ring, fd, buf, user_data, writable, submit_impl));
}

static PyObject *ring_submit_file_buffer(UringApiRing *ring, int fd, PyObject *buf, unsigned long long offset,
                                         PyObject *user_data, int writable,
                                         PyObject *(*submit_impl)(UringApiRing *, int, Py_buffer *, unsigned long long,
                                                                  PyObject *)) {
    Py_buffer view;
    int flags = writable ? PyBUF_WRITABLE : PyBUF_SIMPLE;

    if (PyObject_GetBuffer(buf, &view, flags) < 0) {
        return NULL;
    }
    return submit_impl(ring, fd, &view, offset, user_data);
}

static int ring_submit_file_buffer_status(UringApiRing *ring, int fd, PyObject *buf, unsigned long long offset,
                                          PyObject *user_data, int writable,
                                          PyObject *(*submit_impl)(UringApiRing *, int, Py_buffer *, unsigned long long,
                                                                   PyObject *)) {
    return discard_completion_result(ring_submit_file_buffer(ring, fd, buf, offset, user_data, writable, submit_impl));
}

static int ring_submit_send_buffer(UringApiRing *ring, int fd, PyObject *data, unsigned int flags, PyObject *user_data,
                                   PyObject *(*submit_impl)(UringApiRing *, int, Py_buffer *, unsigned int,
                                                            PyObject *)) {
    Py_buffer view;

    if (PyObject_GetBuffer(data, &view, PyBUF_SIMPLE) < 0) {
        return -1;
    }
    return discard_completion_result(submit_impl(ring, fd, &view, flags, user_data));
}

static int ring_submit_send_zc_buffer(UringApiRing *ring, int fd, PyObject *data, unsigned int flags,
                                      unsigned int zc_flags, PyObject *user_data,
                                      PyObject *(*submit_impl)(UringApiRing *, int, Py_buffer *, unsigned int,
                                                               unsigned int, PyObject *)) {
    Py_buffer view;

    if (PyObject_GetBuffer(data, &view, PyBUF_SIMPLE) < 0) {
        return -1;
    }
    return discard_completion_result(submit_impl(ring, fd, &view, flags, zc_flags, user_data));
}

static int ring_submit_sendto_buffer(UringApiRing *ring, int fd, PyObject *data, PyObject *address, unsigned int flags,
                                     PyObject *user_data,
                                     PyObject *(*submit_impl)(UringApiRing *, int, Py_buffer *, PyObject *,
                                                              unsigned int, PyObject *)) {
    Py_buffer view;

    if (PyObject_GetBuffer(data, &view, PyBUF_SIMPLE) < 0) {
        return -1;
    }
    return discard_completion_result(submit_impl(ring, fd, &view, address, flags, user_data));
}

static int ring_submit_sendmsg_buffer(UringApiRing *ring, int fd, PyObject *data, PyObject *address, unsigned int flags,
                                      PyObject *user_data,
                                      PyObject *(*submit_impl)(UringApiRing *, int, Py_buffer *, PyObject *,
                                                               unsigned int, PyObject *)) {
    Py_buffer view;

    if (PyObject_GetBuffer(data, &view, PyBUF_SIMPLE) < 0) {
        return -1;
    }
    return discard_completion_result(submit_impl(ring, fd, &view, address ? address : Py_None, flags, user_data));
}

PyObject *UringApiCapi_RingNew(unsigned int entries, unsigned int flags) {
    PyObject *args = Py_BuildValue("(II)", entries, flags);
    PyObject *ring;

    if (!args) {
        return NULL;
    }
    ring = PyObject_CallObject((PyObject *)&UringApiRing_Type, args);
    Py_DECREF(args);
    return ring;
}

int UringApiCapi_RingCheck(PyObject *ring) { return ring_type_check(ring); }

int UringApiCapi_RingClose(PyObject *ring) {
    PyObject *result;
    if (!ring_type_check(ring)) {
        return -1;
    }
    result = UringApiRing_close((UringApiRing *)ring, NULL);
    if (!result) {
        return -1;
    }
    Py_DECREF(result);
    return 0;
}

int UringApiCapi_RingFd(PyObject *ring) {
    if (!ring_type_check(ring)) {
        return -1;
    }
    if (!((UringApiRing *)ring)->initialized) {
        return -1;
    }
    return ((UringApiRing *)ring)->ring.ring_fd;
}

unsigned int UringApiCapi_RingFeatures(PyObject *ring) {
    if (!ring_type_check(ring) || !((UringApiRing *)ring)->initialized) {
        return 0;
    }
    return ((UringApiRing *)ring)->ring.features;
}

unsigned int UringApiCapi_RingSqEntries(PyObject *ring) {
    if (!ring_type_check(ring) || !((UringApiRing *)ring)->initialized) {
        return 0;
    }
    return ring_sq_entries((UringApiRing *)ring);
}

unsigned int UringApiCapi_RingCqEntries(PyObject *ring) {
    if (!ring_type_check(ring) || !((UringApiRing *)ring)->initialized) {
        return 0;
    }
    return ring_cq_entries((UringApiRing *)ring);
}

int UringApiCapi_RingClosed(PyObject *ring) {
    if (!ring_type_check(ring)) {
        return -1;
    }
    return !((UringApiRing *)ring)->initialized;
}

int UringApiCapi_RingRunning(PyObject *ring) {
    if (!ring_type_check(ring)) {
        return -1;
    }
    return ((UringApiRing *)ring)->receive_state == URING_API_RECEIVE_DELIVERING;
}

int UringApiCapi_RingSubmitRecv(PyObject *ring, int fd, PyObject *buf, PyObject *user_data) {
    if (!ring_type_check(ring)) {
        return -1;
    }
    return ring_submit_buffer_view_status((UringApiRing *)ring, fd, buf, user_data, 1, UringApiRing_submit_recv_impl);
}

int UringApiCapi_RingSubmitRecvBuf(PyObject *ring, int fd, PyObject *buf_group, unsigned int flags,
                                   PyObject *user_data) {
    if (!ring_type_check(ring)) {
        return -1;
    }
    return discard_completion_result(
        UringApiRing_submit_recv_buf_impl((UringApiRing *)ring, fd, buf_group, flags, user_data));
}

int UringApiCapi_RingSubmitRecvMultishot(PyObject *ring, int fd, PyObject *buf_group, unsigned int flags,
                                         PyObject *user_data) {
    if (!ring_type_check(ring)) {
        return -1;
    }
    if (!buf_group || !PyObject_TypeCheck(buf_group, &UringApiBufGroup_Type)) {
        PyErr_SetString(PyExc_TypeError, "buf_group must be a BufGroup");
        return -1;
    }
    return discard_completion_result(
        UringApiRing_submit_recv_multishot_impl((UringApiRing *)ring, fd, buf_group, flags, user_data));
}

int UringApiCapi_RingSubmitSend(PyObject *ring, int fd, PyObject *data, unsigned int flags, PyObject *user_data) {
    if (!ring_type_check(ring)) {
        return -1;
    }
    return ring_submit_send_buffer((UringApiRing *)ring, fd, data, flags, user_data, UringApiRing_submit_send_impl);
}

int UringApiCapi_RingSubmitSendZc(PyObject *ring, int fd, PyObject *data, unsigned int flags, unsigned int zc_flags,
                                  PyObject *user_data) {
    if (!ring_type_check(ring)) {
        return -1;
    }
    return ring_submit_send_zc_buffer((UringApiRing *)ring, fd, data, flags, zc_flags, user_data,
                                      UringApiRing_submit_send_zc_impl);
}

int UringApiCapi_RingSubmitRecvmsg(PyObject *ring, int fd, PyObject *buf, PyObject *user_data) {
    if (!ring_type_check(ring)) {
        return -1;
    }
    return ring_submit_buffer_view_status((UringApiRing *)ring, fd, buf, user_data, 1,
                                          UringApiRing_submit_recvmsg_impl);
}

int UringApiCapi_RingSubmitSendto(PyObject *ring, int fd, PyObject *data, PyObject *address, unsigned int flags,
                                  PyObject *user_data) {
    if (!ring_type_check(ring)) {
        return -1;
    }
    return ring_submit_sendto_buffer((UringApiRing *)ring, fd, data, address, flags, user_data,
                                     UringApiRing_submit_sendto_impl);
}

int UringApiCapi_RingSubmitSendmsg(PyObject *ring, int fd, PyObject *data, PyObject *address, unsigned int flags,
                                   PyObject *user_data) {
    if (!ring_type_check(ring)) {
        return -1;
    }
    return ring_submit_sendmsg_buffer((UringApiRing *)ring, fd, data, address, flags, user_data,
                                      UringApiRing_submit_sendmsg_impl);
}

int UringApiCapi_RingSubmitSendmsgZc(PyObject *ring, int fd, PyObject *data, PyObject *address, unsigned int flags,
                                     PyObject *user_data) {
    if (!ring_type_check(ring)) {
        return -1;
    }
    return ring_submit_sendmsg_buffer((UringApiRing *)ring, fd, data, address, flags, user_data,
                                      UringApiRing_submit_sendmsg_zc_impl);
}

int UringApiCapi_RingSubmitAccept(PyObject *ring, int fd, unsigned int flags, PyObject *user_data) {
    if (!ring_type_check(ring)) {
        return -1;
    }
    return discard_completion_result(UringApiRing_submit_accept_impl((UringApiRing *)ring, fd, flags, user_data));
}

int UringApiCapi_RingSubmitAcceptMultishot(PyObject *ring, int fd, unsigned int flags, PyObject *user_data) {
    if (!ring_type_check(ring)) {
        return -1;
    }
    return discard_completion_result(
        UringApiRing_submit_accept_multishot_impl((UringApiRing *)ring, fd, flags, user_data));
}

int UringApiCapi_RingSubmitConnect(PyObject *ring, int fd, PyObject *address, PyObject *user_data) {
    if (!ring_type_check(ring)) {
        return -1;
    }
    return discard_completion_result(UringApiRing_submit_connect_impl((UringApiRing *)ring, fd, address, user_data));
}

int UringApiCapi_RingSubmitPoll(PyObject *ring, int fd, unsigned int mask, PyObject *user_data) {
    if (!ring_type_check(ring)) {
        return -1;
    }
    return discard_completion_result(UringApiRing_submit_poll_impl((UringApiRing *)ring, fd, mask, user_data));
}

int UringApiCapi_RingSubmitPollMultishot(PyObject *ring, int fd, unsigned int mask, PyObject *user_data) {
    if (!ring_type_check(ring)) {
        return -1;
    }
    return discard_completion_result(
        UringApiRing_submit_poll_multishot_impl((UringApiRing *)ring, fd, mask, user_data));
}

int UringApiCapi_RingSubmitPollRemove(PyObject *ring, PyObject *target_completion) {
    if (!ring_type_check(ring)) {
        return -1;
    }
    return discard_completion_result(UringApiRing_submit_poll_remove_impl((UringApiRing *)ring, target_completion));
}

int UringApiCapi_RingSubmitCancel(PyObject *ring, PyObject *target_completion) {
    if (!ring_type_check(ring)) {
        return -1;
    }
    if (!completion_type_check(target_completion)) {
        return -1;
    }
    return discard_completion_result(UringApiRing_submit_cancel_impl((UringApiRing *)ring, target_completion));
}

int UringApiCapi_RingSubmitShutdown(PyObject *ring, int fd, int how, PyObject *user_data) {
    if (!ring_type_check(ring)) {
        return -1;
    }
    return discard_completion_result(UringApiRing_submit_shutdown_impl((UringApiRing *)ring, fd, how, user_data));
}

int UringApiCapi_RingSubmitClose(PyObject *ring, int fd, PyObject *user_data) {
    if (!ring_type_check(ring)) {
        return -1;
    }
    return discard_completion_result(UringApiRing_submit_close_impl((UringApiRing *)ring, fd, user_data));
}

int UringApiCapi_RingSubmitRead(PyObject *ring, int fd, PyObject *buf, unsigned long long offset, PyObject *user_data) {
    if (!ring_type_check(ring)) {
        return -1;
    }
    return ring_submit_file_buffer_status((UringApiRing *)ring, fd, buf, offset, user_data, 1,
                                          UringApiRing_submit_read_impl);
}

int UringApiCapi_RingSubmitWrite(PyObject *ring, int fd, PyObject *data, unsigned long long offset,
                                 PyObject *user_data) {
    if (!ring_type_check(ring)) {
        return -1;
    }
    return ring_submit_file_buffer_status((UringApiRing *)ring, fd, data, offset, user_data, 0,
                                          UringApiRing_submit_write_impl);
}

int UringApiCapi_RingSubmitOpenat(PyObject *ring, int dfd, PyObject *path, int flags, unsigned int mode,
                                  PyObject *user_data) {
    if (!ring_type_check(ring)) {
        return -1;
    }
    return discard_completion_result(
        UringApiRing_submit_openat_impl((UringApiRing *)ring, dfd, path, flags, mode, user_data));
}

static int ring_submit_statx_buffer_status(UringApiRing *ring, int dfd, PyObject *path, int flags, unsigned int mask,
                                           PyObject *buf, PyObject *user_data) {
    Py_buffer view;

    if (PyObject_GetBuffer(buf, &view, PyBUF_WRITABLE) < 0) {
        return -1;
    }
    return discard_completion_result(UringApiRing_submit_statx_impl(ring, dfd, path, flags, mask, &view, user_data));
}

int UringApiCapi_RingSubmitStatx(PyObject *ring, int dfd, PyObject *path, int flags, unsigned int mask, PyObject *buf,
                                 PyObject *user_data) {
    if (!ring_type_check(ring)) {
        return -1;
    }
    return ring_submit_statx_buffer_status((UringApiRing *)ring, dfd, path, flags, mask, buf, user_data);
}

int UringApiCapi_RingSubmitStatxFdsize(PyObject *ring, int fd, PyObject *user_data) {
    if (!ring_type_check(ring)) {
        return -1;
    }
    return discard_completion_result(UringApiRing_submit_statx_fdsize_impl((UringApiRing *)ring, fd, user_data));
}

int UringApiCapi_RingSubmitSocket(PyObject *ring, int domain, int type, int protocol, unsigned int flags,
                                  PyObject *user_data) {
    if (!ring_type_check(ring)) {
        return -1;
    }
    return discard_completion_result(
        UringApiRing_submit_socket_impl((UringApiRing *)ring, domain, type, protocol, flags, user_data));
}

int UringApiCapi_RingBreakWait(PyObject *ring) {
    PyObject *result;
    if (!ring_type_check(ring)) {
        return -1;
    }
    result = UringApiRing_break_wait((UringApiRing *)ring, NULL);
    if (!result) {
        return -1;
    }
    Py_DECREF(result);
    return 0;
}

PyObject *UringApiCapi_RingWait(PyObject *ring, double timeout) {
    struct __kernel_timespec timeout_value;
    int timeout_kind;
    if (!ring_type_check(ring)) {
        return NULL;
    }
    if (timeout < 0.0) {
        return UringApiRing_wait_impl((UringApiRing *)ring, 0, NULL, false);
    }
    timeout_value.tv_sec = (long long)timeout;
    timeout_value.tv_nsec = (long long)((timeout - (double)timeout_value.tv_sec) * 1000000000.0);
    if (timeout_value.tv_nsec < 0) {
        timeout_value.tv_nsec = 0;
    }
    if (timeout_value.tv_nsec > 999999999) {
        timeout_value.tv_nsec = 999999999;
    }
    timeout_kind = timeout == 0.0 ? 2 : 1;
    return UringApiRing_wait_impl((UringApiRing *)ring, timeout_kind, &timeout_value, false);
}

int UringApiCapi_RingSetCallback(PyObject *ring, PyObject *callback) {
    if (!ring_type_check(ring)) {
        return -1;
    }
    return UringApiRing_set_callback((UringApiRing *)ring, callback ? callback : Py_None, NULL);
}

int UringApiCapi_RingSetCCallback(PyObject *ring, UringApi_CCompletionCallback callback, void *user_data) {
    if (!ring_type_check(ring)) {
        return -1;
    }
    return UringApiRing_set_c_callback_impl((UringApiRing *)ring, callback, user_data);
}

int UringApiCapi_RingServeCompletions(PyObject *ring) {
    PyObject *result;
    if (!ring_type_check(ring)) {
        return -1;
    }
    result = UringApiRing_serve_completions((UringApiRing *)ring, NULL);
    if (!result) {
        return -1;
    }
    Py_DECREF(result);
    return 0;
}

int UringApiCapi_RingStopServing(PyObject *ring) {
    PyObject *result;
    if (!ring_type_check(ring)) {
        return -1;
    }
    result = UringApiRing_stop_serving((UringApiRing *)ring, NULL);
    if (!result) {
        return -1;
    }
    Py_DECREF(result);
    return 0;
}

int UringApiCapi_RingResetServing(PyObject *ring) {
    PyObject *result;
    if (!ring_type_check(ring)) {
        return -1;
    }
    result = UringApiRing_reset_serving((UringApiRing *)ring, NULL);
    if (!result) {
        return -1;
    }
    Py_DECREF(result);
    return 0;
}

int UringApiCapi_CompletionCheck(PyObject *completion) { return completion_type_check(completion); }

PyObject *UringApiCapi_CompletionUserData(PyObject *completion) {
    if (!completion_type_check(completion)) {
        return NULL;
    }
    return Py_NewRef(((UringApiCompletion *)completion)->user_data);
}

int UringApiCapi_CompletionRes(PyObject *completion, int *value) {
    if (!completion_type_check(completion)) {
        return -1;
    }
    if (!value) {
        PyErr_SetString(PyExc_ValueError, "value must not be NULL");
        return -1;
    }
    *value = ((UringApiCompletion *)completion)->res;
    return 0;
}

int UringApiCapi_CompletionFlags(PyObject *completion, unsigned int *value) {
    if (!completion_type_check(completion)) {
        return -1;
    }
    if (!value) {
        PyErr_SetString(PyExc_ValueError, "value must not be NULL");
        return -1;
    }
    *value = ((UringApiCompletion *)completion)->flags;
    return 0;
}

int UringApiCapi_CompletionSequence(PyObject *completion, unsigned long long *value) {
    UringApiCompletion *uring_completion;
    if (!completion_type_check(completion)) {
        return -1;
    }
    if (!value) {
        PyErr_SetString(PyExc_ValueError, "value must not be NULL");
        return -1;
    }
    uring_completion = (UringApiCompletion *)completion;
    *value = uring_completion->sequence;
    return 0;
}

PyObject *UringApiCapi_CompletionResult(PyObject *completion) {
    PyObject *result;
    if (!completion_type_check(completion)) {
        return NULL;
    }
    result = ((UringApiCompletion *)completion)->result;
    if (!result) {
        Py_RETURN_NONE;
    }
    return Py_NewRef(result);
}

int UringApiCapi_CompletionKind(PyObject *completion, int *value) {
    if (!completion_type_check(completion)) {
        return -1;
    }
    if (!value) {
        PyErr_SetString(PyExc_ValueError, "value must not be NULL");
        return -1;
    }
    *value = (int)((UringApiCompletion *)completion)->kind;
    return 0;
}
