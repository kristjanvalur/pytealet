#ifndef URING_API_SUBMIT_H
#define URING_API_SUBMIT_H

/* private implementation header; not part of the public C API. */

#include "uring_api_common.h"

PyObject *UringApiRing_prepare_recv_impl(UringApiRing *self, int fd, Py_buffer *view, PyObject *user_data);
PyObject *UringApiRing_prepare_recv_buf_impl(UringApiRing *self, int fd, PyObject *buf_group_obj, unsigned int flags,
                                             PyObject *user_data);
PyObject *UringApiRing_prepare_recv_multishot_impl(UringApiRing *self, int fd, PyObject *buf_group, unsigned int flags,
                                                   PyObject *user_data, unsigned long long base_sequence);
PyObject *UringApiRing_prepare_read_impl(UringApiRing *self, int fd, Py_buffer *view, unsigned long long offset,
                                         PyObject *user_data);
PyObject *UringApiRing_prepare_write_impl(UringApiRing *self, int fd, Py_buffer *view, unsigned long long offset,
                                          PyObject *user_data);
PyObject *UringApiRing_prepare_openat_impl(UringApiRing *self, int dfd, PyObject *path, int flags, unsigned int mode,
                                           PyObject *user_data);
PyObject *UringApiRing_prepare_statx_impl(UringApiRing *self, int dfd, PyObject *path, int flags, unsigned int mask,
                                          Py_buffer *view, PyObject *user_data);
PyObject *UringApiRing_construct_send_impl(UringApiRing *self, int fd, Py_buffer *view, unsigned int flags,
                                           PyObject *user_data);
PyObject *UringApiRing_construct_send_zc_impl(UringApiRing *self, int fd, Py_buffer *view, unsigned int flags,
                                              unsigned int zc_flags, PyObject *user_data);
PyObject *UringApiRing_construct_recv_impl(UringApiRing *self, int fd, Py_buffer *view, PyObject *user_data);
PyObject *UringApiRing_construct_recv_buf_impl(UringApiRing *self, int fd, PyObject *buf_group_obj, unsigned int flags,
                                               PyObject *user_data);
PyObject *UringApiRing_construct_recv_multishot_impl(UringApiRing *self, int fd, PyObject *buf_group,
                                                     unsigned int flags, PyObject *user_data,
                                                     unsigned long long base_sequence);
PyObject *UringApiRing_construct_read_impl(UringApiRing *self, int fd, Py_buffer *view, unsigned long long offset,
                                           PyObject *user_data);
PyObject *UringApiRing_construct_write_impl(UringApiRing *self, int fd, Py_buffer *view, unsigned long long offset,
                                            PyObject *user_data);
PyObject *UringApiRing_construct_openat_impl(UringApiRing *self, int dfd, PyObject *path, int flags, unsigned int mode,
                                             PyObject *user_data);
PyObject *UringApiRing_construct_statx_impl(UringApiRing *self, int dfd, PyObject *path, int flags, unsigned int mask,
                                            Py_buffer *view, PyObject *user_data);
PyObject *UringApiRing_construct_statx_fdsize_impl(UringApiRing *self, int fd, PyObject *user_data);
PyObject *UringApiRing_construct_sendto_impl(UringApiRing *self, int fd, Py_buffer *view, PyObject *address,
                                             unsigned int flags, PyObject *user_data);
PyObject *UringApiRing_construct_recvmsg_impl(UringApiRing *self, int fd, Py_buffer *view, PyObject *user_data);
PyObject *UringApiRing_construct_sendmsg_impl(UringApiRing *self, int fd, Py_buffer *view, PyObject *address,
                                              unsigned int flags, PyObject *user_data);
PyObject *UringApiRing_construct_sendmsg_zc_impl(UringApiRing *self, int fd, Py_buffer *view, PyObject *address,
                                                 unsigned int flags, PyObject *user_data);
PyObject *UringApiRing_construct_connect_impl(UringApiRing *self, int fd, PyObject *address, PyObject *user_data);
PyObject *UringApiRing_construct_accept_impl(UringApiRing *self, int fd, unsigned int flags, PyObject *user_data);
PyObject *UringApiRing_construct_accept_multishot_impl(UringApiRing *self, int fd, unsigned int flags,
                                                       PyObject *user_data, unsigned long long base_sequence);
PyObject *UringApiRing_construct_poll_impl(UringApiRing *self, int fd, unsigned int poll_mask, PyObject *user_data);
PyObject *UringApiRing_construct_poll_multishot_impl(UringApiRing *self, int fd, unsigned int poll_mask,
                                                     PyObject *user_data);
PyObject *UringApiRing_construct_shutdown_impl(UringApiRing *self, int fd, int how, PyObject *user_data);
PyObject *UringApiRing_construct_close_impl(UringApiRing *self, int fd, PyObject *user_data);
PyObject *UringApiRing_construct_socket_impl(UringApiRing *self, int domain, int type, int protocol, unsigned int flags,
                                             PyObject *user_data);
PyObject *UringApiRing_construct_poll_remove_impl(UringApiRing *self, PyObject *target_completion, PyObject *user_data);
PyObject *UringApiRing_construct_cancel_impl(UringApiRing *self, PyObject *target_completion, PyObject *user_data);
PyObject *UringApiRing_construct_close_nowait_impl(UringApiRing *self, int fd);
PyObject *UringApiRing_construct_shutdown_nowait_impl(UringApiRing *self, int fd, int how);
PyObject *UringApiRing_construct_cancel_nowait_impl(UringApiRing *self, PyObject *target_completion);
PyObject *UringApiRing_construct_poll_remove_nowait_impl(UringApiRing *self, PyObject *target_completion);
PyObject *UringApiRing_prepare_send_impl(UringApiRing *self, int fd, Py_buffer *view, unsigned int flags,
                                         PyObject *user_data);
/* Prepare constructed completions (get_sqe + fill). On error the prefix
 * of *completions* is already prepared (and may have been flushed). */
int UringApiRing_prepare_impl(UringApiRing *self, PyObject *completions, int *prepared_out);
PyObject *UringApiRing_prepare_send_zc_impl(UringApiRing *self, int fd, Py_buffer *view, unsigned int flags,
                                            unsigned int zc_flags, PyObject *user_data);
PyObject *UringApiRing_prepare_sendto_impl(UringApiRing *self, int fd, Py_buffer *view, PyObject *address,
                                           unsigned int flags, PyObject *user_data);
PyObject *UringApiRing_prepare_recvmsg_impl(UringApiRing *self, int fd, Py_buffer *view, PyObject *user_data);
PyObject *UringApiRing_prepare_sendmsg_impl(UringApiRing *self, int fd, Py_buffer *view, PyObject *address,
                                            unsigned int flags, PyObject *user_data);
PyObject *UringApiRing_prepare_sendmsg_zc_impl(UringApiRing *self, int fd, Py_buffer *view, PyObject *address,
                                               unsigned int flags, PyObject *user_data);
PyObject *UringApiRing_prepare_accept_impl(UringApiRing *self, int fd, unsigned int flags, PyObject *user_data);
PyObject *UringApiRing_prepare_accept_multishot_impl(UringApiRing *self, int fd, unsigned int flags,
                                                     PyObject *user_data, unsigned long long base_sequence);
PyObject *UringApiRing_prepare_connect_impl(UringApiRing *self, int fd, PyObject *address, PyObject *user_data);
PyObject *UringApiRing_prepare_poll_impl(UringApiRing *self, int fd, unsigned int poll_mask, PyObject *user_data);
PyObject *UringApiRing_prepare_poll_multishot_impl(UringApiRing *self, int fd, unsigned int poll_mask,
                                                   PyObject *user_data);
PyObject *UringApiRing_prepare_poll_remove_impl(UringApiRing *self, PyObject *target_completion, PyObject *user_data);
PyObject *UringApiRing_prepare_cancel_impl(UringApiRing *self, PyObject *target_completion, PyObject *user_data);
PyObject *UringApiRing_prepare_shutdown_impl(UringApiRing *self, int fd, int how, PyObject *user_data);
PyObject *UringApiRing_prepare_close_impl(UringApiRing *self, int fd, PyObject *user_data);
/* Nowait: no Completion, no delivery. Return None. */
PyObject *UringApiRing_prepare_close_nowait_impl(UringApiRing *self, int fd);
PyObject *UringApiRing_prepare_shutdown_nowait_impl(UringApiRing *self, int fd, int how);
PyObject *UringApiRing_prepare_cancel_nowait_impl(UringApiRing *self, PyObject *target_completion);
PyObject *UringApiRing_prepare_poll_remove_nowait_impl(UringApiRing *self, PyObject *target_completion);
PyObject *UringApiRing_prepare_socket_impl(UringApiRing *self, int domain, int type, int protocol, unsigned int flags,
                                           PyObject *user_data);

PyObject *UringApiRing_prepare_read(UringApiRing *self, PyObject *args, PyObject *kwargs);
PyObject *UringApiRing_prepare_write(UringApiRing *self, PyObject *args, PyObject *kwargs);
PyObject *UringApiRing_prepare_openat(UringApiRing *self, PyObject *args, PyObject *kwargs);
PyObject *UringApiRing_prepare_statx(UringApiRing *self, PyObject *args, PyObject *kwargs);
PyObject *UringApiRing_prepare_statx_fdsize(UringApiRing *self, PyObject *args, PyObject *kwargs);
PyObject *UringApiRing_prepare_statx_fdsize_impl(UringApiRing *self, int fd, PyObject *user_data);
PyObject *UringApiRing_prepare_recv(UringApiRing *self, PyObject *args, PyObject *kwargs);
PyObject *UringApiRing_prepare_recv_buf(UringApiRing *self, PyObject *args, PyObject *kwargs);
PyObject *UringApiRing_prepare_recv_multishot(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs);
PyObject *UringApiRing_construct_send(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs);
PyObject *UringApiRing_construct_send_zc(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs);
PyObject *UringApiRing_construct_recv(UringApiRing *self, PyObject *args, PyObject *kwargs);
PyObject *UringApiRing_construct_recv_buf(UringApiRing *self, PyObject *args, PyObject *kwargs);
PyObject *UringApiRing_construct_recv_multishot(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs);
PyObject *UringApiRing_construct_read(UringApiRing *self, PyObject *args, PyObject *kwargs);
PyObject *UringApiRing_construct_write(UringApiRing *self, PyObject *args, PyObject *kwargs);
PyObject *UringApiRing_construct_openat(UringApiRing *self, PyObject *args, PyObject *kwargs);
PyObject *UringApiRing_construct_statx(UringApiRing *self, PyObject *args, PyObject *kwargs);
PyObject *UringApiRing_construct_statx_fdsize(UringApiRing *self, PyObject *args, PyObject *kwargs);
PyObject *UringApiRing_construct_sendto(UringApiRing *self, PyObject *args, PyObject *kwargs);
PyObject *UringApiRing_construct_recvmsg(UringApiRing *self, PyObject *args, PyObject *kwargs);
PyObject *UringApiRing_construct_sendmsg(UringApiRing *self, PyObject *args, PyObject *kwargs);
PyObject *UringApiRing_construct_sendmsg_zc(UringApiRing *self, PyObject *args, PyObject *kwargs);
PyObject *UringApiRing_construct_connect(UringApiRing *self, PyObject *args, PyObject *kwargs);
PyObject *UringApiRing_construct_accept(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs);
PyObject *UringApiRing_construct_accept_multishot(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs);
PyObject *UringApiRing_construct_poll(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs);
PyObject *UringApiRing_construct_poll_multishot(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs);
PyObject *UringApiRing_construct_shutdown(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs);
PyObject *UringApiRing_construct_close(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs);
PyObject *UringApiRing_construct_socket(UringApiRing *self, PyObject *args, PyObject *kwargs);
PyObject *UringApiRing_construct_poll_remove(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs);
PyObject *UringApiRing_construct_cancel(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs);
PyObject *UringApiRing_construct_close_nowait(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs);
PyObject *UringApiRing_construct_shutdown_nowait(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs);
PyObject *UringApiRing_construct_cancel_nowait(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs);
PyObject *UringApiRing_construct_poll_remove_nowait(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs);
PyObject *UringApiRing_prepare(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs);
PyObject *UringApiRing_prepare_send(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs);
PyObject *UringApiRing_prepare_send_zc(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs);
PyObject *UringApiRing_prepare_sendto(UringApiRing *self, PyObject *args, PyObject *kwargs);
PyObject *UringApiRing_prepare_recvmsg(UringApiRing *self, PyObject *args, PyObject *kwargs);
PyObject *UringApiRing_prepare_sendmsg(UringApiRing *self, PyObject *args, PyObject *kwargs);
PyObject *UringApiRing_prepare_sendmsg_zc(UringApiRing *self, PyObject *args, PyObject *kwargs);
PyObject *UringApiRing_prepare_accept(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs);
PyObject *UringApiRing_prepare_accept_multishot(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs);
PyObject *UringApiRing_prepare_connect(UringApiRing *self, PyObject *args, PyObject *kwargs);
PyObject *UringApiRing_prepare_poll(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs);
PyObject *UringApiRing_prepare_poll_multishot(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs);
PyObject *UringApiRing_prepare_poll_remove(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs);
PyObject *UringApiRing_prepare_poll_remove_nowait(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs);
PyObject *UringApiRing_prepare_cancel(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs);
PyObject *UringApiRing_prepare_cancel_nowait(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs);
PyObject *UringApiRing_prepare_shutdown(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs);
PyObject *UringApiRing_prepare_shutdown_nowait(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs);
PyObject *UringApiRing_prepare_close(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs);
PyObject *UringApiRing_prepare_close_nowait(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs);
PyObject *UringApiRing_prepare_socket(UringApiRing *self, PyObject *args, PyObject *kwargs);

#endif
