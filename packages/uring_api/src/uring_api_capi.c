/*
 * Public C API export for the _uring_api extension.
 */

#include "uring_api_capi_impl.h"
#include "uring_api_completion.h"
#include "uring_api_core.h"
#include "uring_api_dispatch.h"
#include "uring_api_ring.h"

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
    PyObject *result;
    if (!ring_type_check(ring)) {
        return -1;
    }
    result = PyObject_CallMethod(ring, "submit_recv", "iOO", fd, buf, user_data ? user_data : Py_None);
    if (!result) {
        return -1;
    }
    Py_DECREF(result);
    return 0;
}

int UringApiCapi_RingSubmitRecvMultishot(PyObject *ring, int fd, unsigned int buffer_size, unsigned int buffer_count,
                                         unsigned int flags, PyObject *user_data) {
    PyObject *result;
    if (!ring_type_check(ring)) {
        return -1;
    }
    result = PyObject_CallMethod(ring, "submit_recv_multishot", "ikkOI", fd, (unsigned long)buffer_size,
                                 (unsigned long)buffer_count, user_data ? user_data : Py_None, flags);
    if (!result) {
        return -1;
    }
    Py_DECREF(result);
    return 0;
}

int UringApiCapi_RingSubmitSend(PyObject *ring, int fd, PyObject *data, unsigned int flags, PyObject *user_data) {
    PyObject *result;
    if (!ring_type_check(ring)) {
        return -1;
    }
    result = PyObject_CallMethod(ring, "submit_send", "iOOI", fd, data, user_data ? user_data : Py_None, flags);
    if (!result) {
        return -1;
    }
    Py_DECREF(result);
    return 0;
}

int UringApiCapi_RingSubmitSendZc(PyObject *ring, int fd, PyObject *data, unsigned int flags, unsigned int zc_flags,
                                  PyObject *user_data) {
    PyObject *result;
    if (!ring_type_check(ring)) {
        return -1;
    }
    result = PyObject_CallMethod(ring, "submit_send_zc", "iOOII", fd, data, user_data ? user_data : Py_None, flags,
                                 zc_flags);
    if (!result) {
        return -1;
    }
    Py_DECREF(result);
    return 0;
}

int UringApiCapi_RingSubmitRecvmsg(PyObject *ring, int fd, PyObject *buf, PyObject *user_data) {
    PyObject *result;
    if (!ring_type_check(ring)) {
        return -1;
    }
    result = PyObject_CallMethod(ring, "submit_recvmsg", "iOO", fd, buf, user_data ? user_data : Py_None);
    if (!result) {
        return -1;
    }
    Py_DECREF(result);
    return 0;
}

int UringApiCapi_RingSubmitSendto(PyObject *ring, int fd, PyObject *data, PyObject *address, unsigned int flags,
                                  PyObject *user_data) {
    PyObject *result;
    if (!ring_type_check(ring)) {
        return -1;
    }
    result =
        PyObject_CallMethod(ring, "submit_sendto", "iOOOI", fd, data, address, user_data ? user_data : Py_None, flags);
    if (!result) {
        return -1;
    }
    Py_DECREF(result);
    return 0;
}

int UringApiCapi_RingSubmitSendmsg(PyObject *ring, int fd, PyObject *data, PyObject *address, unsigned int flags,
                                   PyObject *user_data) {
    PyObject *result;
    if (!ring_type_check(ring)) {
        return -1;
    }
    result = PyObject_CallMethod(ring, "submit_sendmsg", "iOOOI", fd, data, address ? address : Py_None,
                                 user_data ? user_data : Py_None, flags);
    if (!result) {
        return -1;
    }
    Py_DECREF(result);
    return 0;
}

int UringApiCapi_RingSubmitSendmsgZc(PyObject *ring, int fd, PyObject *data, PyObject *address, unsigned int flags,
                                     PyObject *user_data) {
    PyObject *result;
    if (!ring_type_check(ring)) {
        return -1;
    }
    result = PyObject_CallMethod(ring, "submit_sendmsg_zc", "iOOOI", fd, data, address ? address : Py_None,
                                 user_data ? user_data : Py_None, flags);
    if (!result) {
        return -1;
    }
    Py_DECREF(result);
    return 0;
}

int UringApiCapi_RingSubmitAccept(PyObject *ring, int fd, unsigned int flags, PyObject *user_data) {
    PyObject *result;
    if (!ring_type_check(ring)) {
        return -1;
    }
    result = PyObject_CallMethod(ring, "submit_accept", "iOI", fd, user_data ? user_data : Py_None, flags);
    if (!result) {
        return -1;
    }
    Py_DECREF(result);
    return 0;
}

int UringApiCapi_RingSubmitAcceptMultishot(PyObject *ring, int fd, unsigned int flags, PyObject *user_data) {
    PyObject *result;
    if (!ring_type_check(ring)) {
        return -1;
    }
    result = PyObject_CallMethod(ring, "submit_accept_multishot", "iOI", fd, user_data ? user_data : Py_None, flags);
    if (!result) {
        return -1;
    }
    Py_DECREF(result);
    return 0;
}

int UringApiCapi_RingSubmitConnect(PyObject *ring, int fd, PyObject *address, PyObject *user_data) {
    PyObject *result;
    if (!ring_type_check(ring)) {
        return -1;
    }
    result = PyObject_CallMethod(ring, "submit_connect", "iOO", fd, address, user_data ? user_data : Py_None);
    if (!result) {
        return -1;
    }
    Py_DECREF(result);
    return 0;
}

int UringApiCapi_RingSubmitShutdown(PyObject *ring, int fd, int how, PyObject *user_data) {
    PyObject *result;
    if (!ring_type_check(ring)) {
        return -1;
    }
    result = PyObject_CallMethod(ring, "submit_shutdown", "iiO", fd, how, user_data ? user_data : Py_None);
    if (!result) {
        return -1;
    }
    Py_DECREF(result);
    return 0;
}

int UringApiCapi_RingSubmitClose(PyObject *ring, int fd, PyObject *user_data) {
    PyObject *result;
    if (!ring_type_check(ring)) {
        return -1;
    }
    result = PyObject_CallMethod(ring, "submit_close", "iO", fd, user_data ? user_data : Py_None);
    if (!result) {
        return -1;
    }
    Py_DECREF(result);
    return 0;
}

int UringApiCapi_RingSubmitSocket(PyObject *ring, int domain, int type, int protocol, unsigned int flags,
                                  PyObject *user_data) {
    PyObject *result;
    if (!ring_type_check(ring)) {
        return -1;
    }
    result = PyObject_CallMethod(ring, "submit_socket", "iiiIO", domain, type, protocol, flags,
                                 user_data ? user_data : Py_None);
    if (!result) {
        return -1;
    }
    Py_DECREF(result);
    return 0;
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
