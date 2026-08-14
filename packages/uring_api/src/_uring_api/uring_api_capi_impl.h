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
int UringApiCapi_RingBreakWait(PyObject *ring);
PyObject *UringApiCapi_RingWait(PyObject *ring, double timeout);
int UringApiCapi_RingSetCallback(PyObject *ring, PyObject *callback);
int UringApiCapi_RingSetExceptionHandler(PyObject *ring, PyObject *handler);
int UringApiCapi_RingSetNowaitErrorHandler(PyObject *ring, PyObject *handler);
int UringApiCapi_RingSetCCallback(PyObject *ring, UringApi_CCompletionCallback callback, void *user_data);
int UringApiCapi_RingServeCompletions(PyObject *ring);
int UringApiCapi_RingStopServing(PyObject *ring);
int UringApiCapi_RingResetServing(PyObject *ring);
int UringApiCapi_RingSubmit(PyObject *ring, int *submitted);
int UringApiCapi_RingAutoSubmit(PyObject *ring, int *value);
int UringApiCapi_RingSetAutoSubmit(PyObject *ring, int value);
int UringApiCapi_CompletionCheck(PyObject *completion);
PyObject *UringApiCapi_CompletionUserData(PyObject *completion);
int UringApiCapi_CompletionSetUserData(PyObject *completion, PyObject *value);
int UringApiCapi_CompletionRes(PyObject *completion, int *value);
int UringApiCapi_CompletionFlags(PyObject *completion, unsigned int *value);
int UringApiCapi_CompletionSequence(PyObject *completion, unsigned long long *value);
PyObject *UringApiCapi_CompletionResult(PyObject *completion);
int UringApiCapi_CompletionKind(PyObject *completion, int *value);
int UringApiCapi_StatxStSize(PyObject *buf, unsigned long long *value);
PyObject *UringApiCapi_RingConstructSend(PyObject *ring, int fd, PyObject *data, unsigned int flags,
                                         PyObject *user_data);
int UringApiCapi_RingPrepare(PyObject *ring, PyObject *completions, int *prepared);
int UringApiCapi_CompletionPrepared(PyObject *completion, int *value);
PyObject *UringApiCapi_RingConstructSendZc(PyObject *ring, int fd, PyObject *data, unsigned int flags,
                                           unsigned int zc_flags, PyObject *user_data);
PyObject *UringApiCapi_RingConstructRecv(PyObject *ring, int fd, PyObject *buf, PyObject *user_data);
PyObject *UringApiCapi_RingConstructRead(PyObject *ring, int fd, PyObject *buf, unsigned long long offset,
                                         PyObject *user_data);
PyObject *UringApiCapi_RingConstructWrite(PyObject *ring, int fd, PyObject *data, unsigned long long offset,
                                          PyObject *user_data);
PyObject *UringApiCapi_RingConstructSendto(PyObject *ring, int fd, PyObject *data, PyObject *address,
                                           unsigned int flags, PyObject *user_data);
PyObject *UringApiCapi_RingConstructRecvmsg(PyObject *ring, int fd, PyObject *buf, PyObject *user_data);
PyObject *UringApiCapi_RingConstructSendmsg(PyObject *ring, int fd, PyObject *data, PyObject *address,
                                            unsigned int flags, PyObject *user_data);
PyObject *UringApiCapi_RingConstructSendmsgZc(PyObject *ring, int fd, PyObject *data, PyObject *address,
                                              unsigned int flags, PyObject *user_data);
PyObject *UringApiCapi_RingConstructConnect(PyObject *ring, int fd, PyObject *address, PyObject *user_data);
PyObject *UringApiCapi_RingConstructRecvBuf(PyObject *ring, int fd, PyObject *buf_group, unsigned int flags,
                                            PyObject *user_data);
PyObject *UringApiCapi_RingConstructRecvMultishot(PyObject *ring, int fd, PyObject *buf_group, unsigned int flags,
                                                  PyObject *user_data, unsigned long long base_sequence);
PyObject *UringApiCapi_RingConstructOpenat(PyObject *ring, int dfd, PyObject *path, int flags, unsigned int mode,
                                           PyObject *user_data);
PyObject *UringApiCapi_RingConstructStatx(PyObject *ring, int dfd, PyObject *path, int flags, unsigned int mask,
                                          PyObject *buf, PyObject *user_data);
PyObject *UringApiCapi_RingConstructStatxFdsize(PyObject *ring, int fd, PyObject *user_data);
PyObject *UringApiCapi_RingConstructAccept(PyObject *ring, int fd, unsigned int flags, PyObject *user_data);
PyObject *UringApiCapi_RingConstructAcceptMultishot(PyObject *ring, int fd, unsigned int flags, PyObject *user_data,
                                                    unsigned long long base_sequence);
PyObject *UringApiCapi_RingConstructPoll(PyObject *ring, int fd, unsigned int mask, PyObject *user_data);
PyObject *UringApiCapi_RingConstructPollMultishot(PyObject *ring, int fd, unsigned int mask, PyObject *user_data);
PyObject *UringApiCapi_RingConstructShutdown(PyObject *ring, int fd, int how, PyObject *user_data);
PyObject *UringApiCapi_RingConstructClose(PyObject *ring, int fd, PyObject *user_data);
PyObject *UringApiCapi_RingConstructSocket(PyObject *ring, int domain, int type, int protocol, unsigned int flags,
                                           PyObject *user_data);
PyObject *UringApiCapi_RingConstructCancel(PyObject *ring, PyObject *target_completion, PyObject *user_data);
PyObject *UringApiCapi_RingConstructPollRemove(PyObject *ring, PyObject *target_completion, PyObject *user_data);
int UringApiCapi_CompletionNowait(PyObject *completion, int *value);
int UringApiCapi_CompletionSetNowait(PyObject *completion, int value);

#endif
