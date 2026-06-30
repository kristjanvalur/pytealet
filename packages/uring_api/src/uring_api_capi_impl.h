#ifndef URING_API_CAPI_IMPL_H
#define URING_API_CAPI_IMPL_H

/* private implementation header; not part of the public C API. */

#include "uring_api_common.h"

#include "uring_api_capi.h"

PyObject *UringApiCapi_RingNew(unsigned int entries, unsigned int flags);
int UringApiCapi_RingCheck(PyObject *ring);
int UringApiCapi_RingClose(PyObject *ring);
int UringApiCapi_RingFd(PyObject *ring);
unsigned int UringApiCapi_RingFeatures(PyObject *ring);
unsigned int UringApiCapi_RingSqEntries(PyObject *ring);
unsigned int UringApiCapi_RingCqEntries(PyObject *ring);
int UringApiCapi_RingClosed(PyObject *ring);
int UringApiCapi_RingRunning(PyObject *ring);
int UringApiCapi_RingSubmitRecv(PyObject *ring, int fd, PyObject *buf, PyObject *user_data);
int UringApiCapi_RingSubmitRecvMultishot(PyObject *ring, int fd, PyObject *buf_group, unsigned int flags,
                                         PyObject *user_data);
int UringApiCapi_RingSubmitSend(PyObject *ring, int fd, PyObject *data, unsigned int flags, PyObject *user_data);
int UringApiCapi_RingSubmitSendZc(PyObject *ring, int fd, PyObject *data, unsigned int flags, unsigned int zc_flags,
                                  PyObject *user_data);
int UringApiCapi_RingSubmitRecvmsg(PyObject *ring, int fd, PyObject *buf, PyObject *user_data);
int UringApiCapi_RingSubmitSendto(PyObject *ring, int fd, PyObject *data, PyObject *address, unsigned int flags,
                                  PyObject *user_data);
int UringApiCapi_RingSubmitSendmsg(PyObject *ring, int fd, PyObject *data, PyObject *address, unsigned int flags,
                                   PyObject *user_data);
int UringApiCapi_RingSubmitSendmsgZc(PyObject *ring, int fd, PyObject *data, PyObject *address, unsigned int flags,
                                     PyObject *user_data);
int UringApiCapi_RingSubmitAccept(PyObject *ring, int fd, unsigned int flags, PyObject *user_data);
int UringApiCapi_RingSubmitAcceptMultishot(PyObject *ring, int fd, unsigned int flags, PyObject *user_data);
int UringApiCapi_RingSubmitConnect(PyObject *ring, int fd, PyObject *address, PyObject *user_data);
int UringApiCapi_RingSubmitShutdown(PyObject *ring, int fd, int how, PyObject *user_data);
int UringApiCapi_RingSubmitClose(PyObject *ring, int fd, PyObject *user_data);
int UringApiCapi_RingSubmitSocket(PyObject *ring, int domain, int type, int protocol, unsigned int flags,
                                  PyObject *user_data);
int UringApiCapi_RingSubmitPoll(PyObject *ring, int fd, unsigned int mask, PyObject *user_data);
int UringApiCapi_RingSubmitPollMultishot(PyObject *ring, int fd, unsigned int mask, PyObject *user_data);
int UringApiCapi_RingSubmitPollRemove(PyObject *ring, PyObject *target_completion);
int UringApiCapi_RingSubmitRead(PyObject *ring, int fd, PyObject *buf, unsigned long long offset, PyObject *user_data);
int UringApiCapi_RingSubmitWrite(PyObject *ring, int fd, PyObject *data, unsigned long long offset,
                                 PyObject *user_data);
int UringApiCapi_RingBreakWait(PyObject *ring);
PyObject *UringApiCapi_RingWait(PyObject *ring, double timeout);
int UringApiCapi_RingSetCallback(PyObject *ring, PyObject *callback);
int UringApiCapi_RingSetCCallback(PyObject *ring, UringApi_CCompletionCallback callback, void *user_data);
int UringApiCapi_RingServeCompletions(PyObject *ring);
int UringApiCapi_RingStopServing(PyObject *ring);
int UringApiCapi_RingResetServing(PyObject *ring);
int UringApiCapi_CompletionCheck(PyObject *completion);
PyObject *UringApiCapi_CompletionUserData(PyObject *completion);
int UringApiCapi_CompletionRes(PyObject *completion, int *value);
int UringApiCapi_CompletionFlags(PyObject *completion, unsigned int *value);
int UringApiCapi_CompletionSequence(PyObject *completion, unsigned long long *value);
PyObject *UringApiCapi_CompletionResult(PyObject *completion);
int UringApiCapi_CompletionKind(PyObject *completion, int *value);

#endif