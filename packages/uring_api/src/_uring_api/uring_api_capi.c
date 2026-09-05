/*
 * Public C API export for the _uring_api extension.
 */

#include "uring_api_bufgroup.h"
#include "uring_api_capi_impl.h"
#include "uring_api_completion.h"
#include "uring_api_core.h"
#include "uring_api_dispatch.h"
#include "uring_api_idle.h"
#include "uring_api_prepare.h"
#include "uring_api_ring.h"

static PyObject *ring_construct_buffer_view(UringApiRing *ring, int fd, PyObject *buf, unsigned int recv_flags,
                                            PyObject *user_data, int writable,
                                            PyObject *(*construct_impl)(UringApiRing *, int, Py_buffer *, unsigned int,
                                                                        PyObject *)) {
    Py_buffer view;
    int flags = writable ? PyBUF_WRITABLE : PyBUF_SIMPLE;

    if (PyObject_GetBuffer(buf, &view, flags) < 0) {
        return NULL;
    }
    return construct_impl(ring, fd, &view, recv_flags, user_data);
}

static PyObject *ring_construct_file_buffer(UringApiRing *ring, int fd, PyObject *buf, unsigned long long offset,
                                            PyObject *user_data, int writable,
                                            PyObject *(*construct_impl)(UringApiRing *, int, Py_buffer *,
                                                                        unsigned long long, PyObject *)) {
    Py_buffer view;
    int flags = writable ? PyBUF_WRITABLE : PyBUF_SIMPLE;

    if (PyObject_GetBuffer(buf, &view, flags) < 0) {
        return NULL;
    }
    return construct_impl(ring, fd, &view, offset, user_data);
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

int UringApiCapi_RingPendingCount(PyObject *ring, unsigned int *value) {
    if (!ring_type_check(ring)) {
        return -1;
    }
    if (!value) {
        PyErr_SetString(PyExc_ValueError, "value must not be NULL");
        return -1;
    }
    *value = ring_pending_count((UringApiRing *)ring);
    return 0;
}

int UringApiCapi_CompletionSetSequence(PyObject *completion, unsigned long long value) {
    if (!completion_type_check(completion)) {
        return -1;
    }
    ((UringApiCompletion *)completion)->sequence = value;
    return 0;
}

PyObject *UringApiCapi_CompletionTakeUserData(PyObject *completion) {
    if (!completion_type_check(completion)) {
        return NULL;
    }
    return UringApiCompletion_take_user_data((UringApiCompletion *)completion);
}

int UringApiCapi_RingWaitIdle(PyObject *ring, double timeout, int *signaled) {
    UringApiRing *self;
    const double *timeout_ptr;

    if (!ring_type_check(ring)) {
        return -1;
    }
    if (!signaled) {
        PyErr_SetString(PyExc_ValueError, "signaled must not be NULL");
        return -1;
    }
    self = (UringApiRing *)ring;
    if (!self->initialized) {
        PyErr_SetString(PyExc_RuntimeError, "ring is closed");
        return -1;
    }
    if (timeout < 0.0) {
        timeout_ptr = NULL;
    } else {
        timeout_ptr = &timeout;
    }
    *signaled = UringApiIdlePark_wait(&self->idle, timeout_ptr);
    return 0;
}

int UringApiCapi_RingRunning(PyObject *ring) {
    if (!ring_type_check(ring)) {
        return -1;
    }
    return ((UringApiRing *)ring)->receive_state == URING_API_RECEIVE_DELIVERING;
}

int UringApiCapi_RingBreakWait(PyObject *ring) {
    if (!ring_type_check(ring)) {
        return -1;
    }
    return UringApiRing_break_wait_impl((UringApiRing *)ring, 0);
}

PyObject *UringApiCapi_RingWait(PyObject *ring, double timeout) {
    struct __kernel_timespec timeout_value;
    PyObject *ready;

    if (!ring_type_check(ring)) {
        return NULL;
    }
    if (timeout < 0.0) {
        ready = UringApiRing_wait_impl((UringApiRing *)ring, URING_API_WAIT_BLOCKING, NULL, false, NULL);
    } else if (timeout == 0.0) {
        ready = UringApiRing_wait_impl((UringApiRing *)ring, URING_API_WAIT_PEEK, NULL, false, NULL);
    } else {
        timeout_value.tv_sec = (long long)timeout;
        timeout_value.tv_nsec = (long long)((timeout - (double)timeout_value.tv_sec) * 1000000000.0);
        if (timeout_value.tv_nsec < 0) {
            timeout_value.tv_nsec = 0;
        }
        if (timeout_value.tv_nsec > 999999999) {
            timeout_value.tv_nsec = 999999999;
        }
        ready = UringApiRing_wait_impl((UringApiRing *)ring, URING_API_WAIT_TIMEOUT, &timeout_value, false, NULL);
    }
    return UringApiRing_wait_finish_with_optional_delivery((UringApiRing *)ring, ready);
}

int UringApiCapi_RingSetCallback(PyObject *ring, PyObject *callback) {
    if (!ring_type_check(ring)) {
        return -1;
    }
    return UringApiRing_set_callback((UringApiRing *)ring, callback ? callback : Py_None, NULL);
}

int UringApiCapi_RingSetExceptionHandler(PyObject *ring, PyObject *handler) {
    if (!ring_type_check(ring)) {
        return -1;
    }
    return UringApiRing_set_exception_handler((UringApiRing *)ring, handler ? handler : Py_None, NULL);
}

int UringApiCapi_RingSetNowaitErrorHandler(PyObject *ring, PyObject *handler) {
    if (!ring_type_check(ring)) {
        return -1;
    }
    return UringApiRing_set_nowait_error_handler((UringApiRing *)ring, handler ? handler : Py_None, NULL);
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

int UringApiCapi_RingSubmit(PyObject *ring, int *submitted) {
    UringApiRing *self;
    int count = 0;
    int failed = 0;

    if (!ring_type_check(ring)) {
        return -1;
    }
    self = (UringApiRing *)ring;
    Py_BEGIN_CRITICAL_SECTION(self);
    if (ring_check_open(self) < 0) {
        failed = 1;
    } else if (ring_check_submit_thread(self, 1) < 0) {
        failed = 1;
    } else if (drain_parked(self, 1, &count) < 0) {
        failed = 1;
    } else if (ring_flush_pending(self, &count) < 0) {
        failed = 1;
    }
    Py_END_CRITICAL_SECTION();
    if (failed) {
        return -1;
    }
    if (submitted) {
        *submitted = count;
    }
    return 0;
}

int UringApiCapi_RingAutoSubmit(PyObject *ring, int *value) {
    if (!ring_type_check(ring)) {
        return -1;
    }
    if (!value) {
        PyErr_SetString(PyExc_ValueError, "value output pointer is required");
        return -1;
    }
    *value = ((UringApiRing *)ring)->auto_submit ? 1 : 0;
    return 0;
}

int UringApiCapi_RingSetAutoSubmit(PyObject *ring, int value) {
    if (!ring_type_check(ring)) {
        return -1;
    }
    ((UringApiRing *)ring)->auto_submit = value != 0;
    return 0;
}

int UringApiCapi_CompletionCheck(PyObject *completion) { return completion_type_check(completion); }

PyObject *UringApiCapi_CompletionUserData(PyObject *completion) {
    if (!completion_type_check(completion)) {
        return NULL;
    }
    return Py_NewRef(((UringApiCompletion *)completion)->user_data);
}

int UringApiCapi_CompletionSetUserData(PyObject *completion, PyObject *value) {
    if (!completion_type_check(completion)) {
        return -1;
    }
    return UringApiCompletion_assign_user_data((UringApiCompletion *)completion, value);
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

PyObject *UringApiCapi_RingConstructSend(PyObject *ring, int fd, PyObject *data, unsigned int flags,
                                         PyObject *user_data) {
    Py_buffer view;

    if (!ring_type_check(ring)) {
        return NULL;
    }
    if (PyObject_GetBuffer(data, &view, PyBUF_SIMPLE) < 0) {
        return NULL;
    }
    return UringApiRing_construct_send_impl((UringApiRing *)ring, fd, &view, flags, user_data);
}

PyObject *UringApiCapi_RingConstructSendAll(PyObject *ring, int fd, PyObject *data, unsigned int flags,
                                            PyObject *user_data) {
    Py_buffer view;

    if (!ring_type_check(ring)) {
        return NULL;
    }
    if (PyObject_GetBuffer(data, &view, PyBUF_SIMPLE) < 0) {
        return NULL;
    }
    return UringApiRing_construct_send_all_impl((UringApiRing *)ring, fd, &view, flags, user_data);
}

int UringApiCapi_RingPrepare(PyObject *ring, PyObject *completions, int *prepared) {
    int count = 0;

    if (!ring_type_check(ring)) {
        return -1;
    }
    if (UringApiRing_prepare_impl((UringApiRing *)ring, completions, &count) < 0) {
        return -1;
    }
    if (prepared) {
        *prepared = count;
    }
    return 0;
}

int UringApiCapi_CompletionPrepared(PyObject *completion, int *value) {
    if (!completion_type_check(completion)) {
        return -1;
    }
    if (!value) {
        PyErr_SetString(PyExc_ValueError, "value must not be NULL");
        return -1;
    }
    *value = completion_has_bit((UringApiCompletion *)completion, URING_API_C_PREPARED) ? 1 : 0;
    return 0;
}

PyObject *UringApiCapi_RingConstructSendZc(PyObject *ring, int fd, PyObject *data, unsigned int flags,
                                           unsigned int zc_flags, PyObject *user_data) {
    Py_buffer view;

    if (!ring_type_check(ring)) {
        return NULL;
    }
    if (PyObject_GetBuffer(data, &view, PyBUF_SIMPLE) < 0) {
        return NULL;
    }
    return UringApiRing_construct_send_zc_impl((UringApiRing *)ring, fd, &view, flags, zc_flags, user_data);
}

PyObject *UringApiCapi_RingConstructRecv(PyObject *ring, int fd, PyObject *buf, unsigned int flags,
                                         PyObject *user_data) {
    if (!ring_type_check(ring)) {
        return NULL;
    }
    return ring_construct_buffer_view((UringApiRing *)ring, fd, buf, flags, user_data, 1,
                                      UringApiRing_construct_recv_impl);
}

PyObject *UringApiCapi_RingConstructRead(PyObject *ring, int fd, PyObject *buf, unsigned long long offset,
                                         PyObject *user_data) {
    if (!ring_type_check(ring)) {
        return NULL;
    }
    return ring_construct_file_buffer((UringApiRing *)ring, fd, buf, offset, user_data, 1,
                                      UringApiRing_construct_read_impl);
}

PyObject *UringApiCapi_RingConstructWrite(PyObject *ring, int fd, PyObject *data, unsigned long long offset,
                                          PyObject *user_data) {
    if (!ring_type_check(ring)) {
        return NULL;
    }
    return ring_construct_file_buffer((UringApiRing *)ring, fd, data, offset, user_data, 0,
                                      UringApiRing_construct_write_impl);
}

PyObject *UringApiCapi_RingConstructSendto(PyObject *ring, int fd, PyObject *data, PyObject *address,
                                           unsigned int flags, PyObject *user_data) {
    Py_buffer view;

    if (!ring_type_check(ring)) {
        return NULL;
    }
    if (PyObject_GetBuffer(data, &view, PyBUF_SIMPLE) < 0) {
        return NULL;
    }
    return UringApiRing_construct_sendto_impl((UringApiRing *)ring, fd, &view, address, flags, user_data);
}

PyObject *UringApiCapi_RingConstructRecvmsg(PyObject *ring, int fd, PyObject *buf, unsigned int flags,
                                            PyObject *user_data) {
    if (!ring_type_check(ring)) {
        return NULL;
    }
    return ring_construct_buffer_view((UringApiRing *)ring, fd, buf, flags, user_data, 1,
                                      UringApiRing_construct_recvmsg_impl);
}

PyObject *UringApiCapi_RingConstructSendmsg(PyObject *ring, int fd, PyObject *data, PyObject *address,
                                            unsigned int flags, PyObject *user_data) {
    Py_buffer view;

    if (!ring_type_check(ring)) {
        return NULL;
    }
    if (PyObject_GetBuffer(data, &view, PyBUF_SIMPLE) < 0) {
        return NULL;
    }
    return UringApiRing_construct_sendmsg_impl((UringApiRing *)ring, fd, &view, address ? address : Py_None, flags,
                                               user_data);
}

PyObject *UringApiCapi_RingConstructSendmsgZc(PyObject *ring, int fd, PyObject *data, PyObject *address,
                                              unsigned int flags, PyObject *user_data) {
    Py_buffer view;

    if (!ring_type_check(ring)) {
        return NULL;
    }
    if (PyObject_GetBuffer(data, &view, PyBUF_SIMPLE) < 0) {
        return NULL;
    }
    return UringApiRing_construct_sendmsg_zc_impl((UringApiRing *)ring, fd, &view, address ? address : Py_None, flags,
                                                  user_data);
}

PyObject *UringApiCapi_RingConstructConnect(PyObject *ring, int fd, PyObject *address, PyObject *user_data) {
    if (!ring_type_check(ring)) {
        return NULL;
    }
    return UringApiRing_construct_connect_impl((UringApiRing *)ring, fd, address, user_data);
}

PyObject *UringApiCapi_RingConstructRecvBuf(PyObject *ring, int fd, PyObject *buf_group, unsigned int flags,
                                            PyObject *user_data) {
    if (!ring_type_check(ring)) {
        return NULL;
    }
    return UringApiRing_construct_recv_buf_impl((UringApiRing *)ring, fd, buf_group, flags, user_data);
}

PyObject *UringApiCapi_RingConstructRecvMultishot(PyObject *ring, int fd, PyObject *buf_group, unsigned int flags,
                                                  PyObject *user_data) {
    if (!ring_type_check(ring)) {
        return NULL;
    }
    return UringApiRing_construct_recv_multishot_impl((UringApiRing *)ring, fd, buf_group, flags, user_data);
}

PyObject *UringApiCapi_RingConstructOpenat(PyObject *ring, int dfd, PyObject *path, int flags, unsigned int mode,
                                           PyObject *user_data) {
    if (!ring_type_check(ring)) {
        return NULL;
    }
    return UringApiRing_construct_openat_impl((UringApiRing *)ring, dfd, path, flags, mode, user_data);
}

PyObject *UringApiCapi_RingConstructStatx(PyObject *ring, int dfd, PyObject *path, int flags, unsigned int mask,
                                          PyObject *buf, PyObject *user_data) {
    Py_buffer view;

    if (!ring_type_check(ring)) {
        return NULL;
    }
    if (PyObject_GetBuffer(buf, &view, PyBUF_WRITABLE) < 0) {
        return NULL;
    }
    return UringApiRing_construct_statx_impl((UringApiRing *)ring, dfd, path, flags, mask, &view, user_data);
}

PyObject *UringApiCapi_RingConstructStatxFdsize(PyObject *ring, int fd, PyObject *user_data) {
    if (!ring_type_check(ring)) {
        return NULL;
    }
    return UringApiRing_construct_statx_fdsize_impl((UringApiRing *)ring, fd, user_data);
}

PyObject *UringApiCapi_RingConstructAccept(PyObject *ring, int fd, unsigned int flags, PyObject *user_data) {
    if (!ring_type_check(ring)) {
        return NULL;
    }
    return UringApiRing_construct_accept_impl((UringApiRing *)ring, fd, flags, user_data);
}

PyObject *UringApiCapi_RingConstructAcceptMultishot(PyObject *ring, int fd, unsigned int flags, PyObject *user_data) {
    if (!ring_type_check(ring)) {
        return NULL;
    }
    return UringApiRing_construct_accept_multishot_impl((UringApiRing *)ring, fd, flags, user_data);
}

PyObject *UringApiCapi_RingConstructPoll(PyObject *ring, int fd, unsigned int mask, PyObject *user_data) {
    if (!ring_type_check(ring)) {
        return NULL;
    }
    return UringApiRing_construct_poll_impl((UringApiRing *)ring, fd, mask, user_data);
}

PyObject *UringApiCapi_RingConstructPollMultishot(PyObject *ring, int fd, unsigned int mask, PyObject *user_data) {
    if (!ring_type_check(ring)) {
        return NULL;
    }
    return UringApiRing_construct_poll_multishot_impl((UringApiRing *)ring, fd, mask, user_data);
}

PyObject *UringApiCapi_RingConstructShutdown(PyObject *ring, int fd, int how, PyObject *user_data) {
    if (!ring_type_check(ring)) {
        return NULL;
    }
    return UringApiRing_construct_shutdown_impl((UringApiRing *)ring, fd, how, user_data);
}

PyObject *UringApiCapi_RingConstructClose(PyObject *ring, int fd, PyObject *user_data) {
    if (!ring_type_check(ring)) {
        return NULL;
    }
    return UringApiRing_construct_close_impl((UringApiRing *)ring, fd, user_data);
}

PyObject *UringApiCapi_RingConstructSocket(PyObject *ring, int domain, int type, int protocol, unsigned int flags,
                                           PyObject *user_data) {
    if (!ring_type_check(ring)) {
        return NULL;
    }
    return UringApiRing_construct_socket_impl((UringApiRing *)ring, domain, type, protocol, flags, user_data);
}

PyObject *UringApiCapi_RingConstructCancel(PyObject *ring, PyObject *target_completion, PyObject *user_data) {
    if (!ring_type_check(ring)) {
        return NULL;
    }
    return UringApiRing_construct_cancel_impl((UringApiRing *)ring, target_completion, user_data);
}

PyObject *UringApiCapi_RingConstructPollRemove(PyObject *ring, PyObject *target_completion, PyObject *user_data) {
    if (!ring_type_check(ring)) {
        return NULL;
    }
    return UringApiRing_construct_poll_remove_impl((UringApiRing *)ring, target_completion, user_data);
}

int UringApiCapi_CompletionNowait(PyObject *completion, int *value) {
    if (!completion_type_check(completion)) {
        return -1;
    }
    if (!value) {
        PyErr_SetString(PyExc_ValueError, "value must not be NULL");
        return -1;
    }
    *value = completion_has_bit((UringApiCompletion *)completion, URING_API_C_NOWAIT) ? 1 : 0;
    return 0;
}

int UringApiCapi_CompletionSetNowait(PyObject *completion, int value) {
    if (!completion_type_check(completion)) {
        return -1;
    }
    return UringApiCompletion_set_nowait_flag((UringApiCompletion *)completion, value);
}
