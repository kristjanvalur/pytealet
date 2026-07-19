#ifndef URING_API_CORE_H
#define URING_API_CORE_H

/* private implementation header; not part of the public C API. */

#include "uring_api_common.h"

int ring_type_check(PyObject *ring);
int normalize_ret_errno(int ret);
PyObject *liburing_version_string(void);
PyObject *liburing_version_info(void);
int module_add_uint64_constant(PyObject *module, const char *name, unsigned long long value);
int module_add_setup_flag_constants(PyObject *module);
int module_add_cqe_flag_constants(PyObject *module);
int module_add_recvsend_flag_constants(PyObject *module);
int module_add_completion_kind_constants(PyObject *module);
int module_add_statx_constants(PyObject *module);
void sqe_set_completion(UringApiRing *self, struct io_uring_sqe *sqe, PyObject *completion);
UringApiCompletion *cqe_get_completion(UringApiRing *self, struct io_uring_cqe *cqe);
unsigned int ring_sq_entries(UringApiRing *self);
unsigned int ring_cq_entries(UringApiRing *self);

int parse_entries_flags(PyObject *args, PyObject *kwargs, unsigned int default_entries, unsigned int *entries,
                        unsigned int *flags);
int parse_numeric_sockaddr(int fd, PyObject *address, struct sockaddr_storage *storage, socklen_t *addrlen);
int ring_check_open(UringApiRing *self);
int ring_check_submit_thread(UringApiRing *self);
int ring_check_client_thread(UringApiRing *self);
int submit_one(UringApiRing *self);
/* pre-submit hook then submit_one; retracts with (user_data, None) if arm or submit fails */
int submit_one_completion(UringApiRing *self, PyObject *completion);
int receive_wait_begin(UringApiRing *self, bool from_delivery_thread);
void receive_wait_end(UringApiRing *self, bool from_delivery_thread);
bool delivery_is_running_locked(UringApiRing *self);
int delivery_check_not_running(UringApiRing *self);
void delivery_mark_exited(UringApiRing *self);
struct io_uring_sqe *get_sqe(UringApiRing *self);

#endif