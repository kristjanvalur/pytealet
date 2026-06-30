#ifndef URING_API_CORE_H
#define URING_API_CORE_H

/* private implementation header; not part of the public C API. */

#include "uring_api_common.h"

int ring_type_check(PyObject *ring);
int completion_type_check(PyObject *completion);
int normalize_ret_errno(int ret);
PyObject *liburing_version_string(void);
PyObject *liburing_version_info(void);
int module_add_uint64_constant(PyObject *module, const char *name, unsigned long long value);
int module_add_setup_flag_constants(PyObject *module);
int module_add_cqe_flag_constants(PyObject *module);
int module_add_recvsend_flag_constants(PyObject *module);
int module_add_completion_kind_constants(PyObject *module);
void sqe_set_completion(UringApiRing *self, struct io_uring_sqe *sqe, PyObject *completion);
UringApiCompletion *cqe_get_completion(UringApiRing *self, struct io_uring_cqe *cqe);
unsigned int ring_sq_entries(UringApiRing *self);
unsigned int ring_cq_entries(UringApiRing *self);
PyObject *build_feature_probe_result(bool available, int errnum, const char *message);
int parse_entries_flags(PyObject *args, PyObject *kwargs, unsigned int default_entries, unsigned int *entries,
                        unsigned int *flags);
int parse_numeric_sockaddr(int fd, PyObject *address, struct sockaddr_storage *storage, socklen_t *addrlen);
int ring_check_open(UringApiRing *self);
UringApiRecvBufferPool *UringApiRecvBufferPool_new(UringApiRing *ring, unsigned int buffer_size,
                                                   unsigned int buffer_count);
PyObject *UringApiCompletion_new_pending(UringApiPendingKind kind, PyObject *user_data, PyObject *buffer);
PyObject *UringApiCompletion_new_pending_view(UringApiPendingKind kind, PyObject *user_data, Py_buffer *view);
PyObject *UringApiCompletion_new_pending_recvmsg(UringApiPendingKind kind, PyObject *user_data, Py_buffer *view);
PyObject *UringApiCompletion_new_pending_sendmsg(UringApiPendingKind kind, PyObject *user_data, Py_buffer *view);
bool is_zero_copy_send_kind(UringApiPendingKind kind);
PyObject *UringApiCompletion_new_pending_accept(PyObject *user_data);
PyObject *UringApiCompletion_new_delivered_copy(UringApiCompletion *source);
void UringApiCompletion_clear_pending_state(UringApiCompletion *self);
int UringApiCompletion_complete(UringApiCompletion *self, int res, unsigned int flags);
void UringApiCompletion_dealloc(UringApiCompletion *self);
int UringApiCompletion_traverse(UringApiCompletion *self, visitproc visit, void *arg);
int UringApiCompletion_clear(UringApiCompletion *self);
PyObject *UringApiCompletion_get_user_data(UringApiCompletion *self, void *closure);
PyObject *UringApiCompletion_get_kind(UringApiCompletion *self, void *closure);
PyObject *UringApiCompletion_get_res(UringApiCompletion *self, void *closure);
PyObject *UringApiCompletion_get_flags(UringApiCompletion *self, void *closure);
PyObject *UringApiCompletion_get_result(UringApiCompletion *self, void *closure);
PyObject *UringApiCompletion_get_sequence(UringApiCompletion *self, void *closure);
int submit_one(UringApiRing *self);
int receive_wait_begin(UringApiRing *self, bool from_delivery_thread);
void receive_wait_end(UringApiRing *self, bool from_delivery_thread);
bool delivery_is_running_locked(UringApiRing *self);
int delivery_check_not_running(UringApiRing *self);
void delivery_mark_exited(UringApiRing *self);
struct io_uring_sqe *get_sqe(UringApiRing *self);

#endif