#ifndef URING_API_SUBMIT_H
#define URING_API_SUBMIT_H

/* private implementation header; not part of the public C API. */

#include "uring_api_common.h"

PyObject *UringApiRing_submit_recv_impl(UringApiRing *self, int fd, Py_buffer *view, PyObject *user_data);
PyObject *UringApiRing_submit_recv_buf_impl(UringApiRing *self, int fd, PyObject *buf_group_obj, unsigned int flags,
                                            PyObject *user_data);
PyObject *UringApiRing_submit_recv_multishot_impl(UringApiRing *self, int fd, PyObject *buf_group, unsigned int flags,
                                                  PyObject *user_data, unsigned long long base_sequence);
PyObject *UringApiRing_submit_read_impl(UringApiRing *self, int fd, Py_buffer *view, unsigned long long offset,
                                        PyObject *user_data);
PyObject *UringApiRing_submit_write_impl(UringApiRing *self, int fd, Py_buffer *view, unsigned long long offset,
                                         PyObject *user_data);
PyObject *UringApiRing_submit_openat_impl(UringApiRing *self, int dfd, PyObject *path, int flags, unsigned int mode,
                                          PyObject *user_data);
PyObject *UringApiRing_submit_statx_impl(UringApiRing *self, int dfd, PyObject *path, int flags, unsigned int mask,
                                         Py_buffer *view, PyObject *user_data);
PyObject *UringApiRing_submit_send_impl(UringApiRing *self, int fd, Py_buffer *view, unsigned int flags,
                                        PyObject *user_data);
PyObject *UringApiRing_submit_send_zc_impl(UringApiRing *self, int fd, Py_buffer *view, unsigned int flags,
                                           unsigned int zc_flags, PyObject *user_data);
PyObject *UringApiRing_submit_sendto_impl(UringApiRing *self, int fd, Py_buffer *view, PyObject *address,
                                          unsigned int flags, PyObject *user_data);
PyObject *UringApiRing_submit_recvmsg_impl(UringApiRing *self, int fd, Py_buffer *view, PyObject *user_data);
PyObject *UringApiRing_submit_sendmsg_impl(UringApiRing *self, int fd, Py_buffer *view, PyObject *address,
                                           unsigned int flags, PyObject *user_data);
PyObject *UringApiRing_submit_sendmsg_zc_impl(UringApiRing *self, int fd, Py_buffer *view, PyObject *address,
                                              unsigned int flags, PyObject *user_data);
PyObject *UringApiRing_submit_accept_impl(UringApiRing *self, int fd, unsigned int flags, PyObject *user_data);
PyObject *UringApiRing_submit_accept_multishot_impl(UringApiRing *self, int fd, unsigned int flags, PyObject *user_data,
                                                    unsigned long long base_sequence);
PyObject *UringApiRing_submit_connect_impl(UringApiRing *self, int fd, PyObject *address, PyObject *user_data);
PyObject *UringApiRing_submit_poll_impl(UringApiRing *self, int fd, unsigned int poll_mask, PyObject *user_data);
PyObject *UringApiRing_submit_poll_multishot_impl(UringApiRing *self, int fd, unsigned int poll_mask,
                                                  PyObject *user_data);
PyObject *UringApiRing_submit_poll_remove_impl(UringApiRing *self, PyObject *target_completion, PyObject *user_data);
PyObject *UringApiRing_submit_cancel_impl(UringApiRing *self, PyObject *target_completion, PyObject *user_data);
PyObject *UringApiRing_submit_shutdown_impl(UringApiRing *self, int fd, int how, PyObject *user_data);
PyObject *UringApiRing_submit_close_impl(UringApiRing *self, int fd, PyObject *user_data);
PyObject *UringApiRing_submit_socket_impl(UringApiRing *self, int domain, int type, int protocol, unsigned int flags,
                                          PyObject *user_data);

PyObject *UringApiRing_submit_read(UringApiRing *self, PyObject *args, PyObject *kwargs);
PyObject *UringApiRing_submit_write(UringApiRing *self, PyObject *args, PyObject *kwargs);
PyObject *UringApiRing_submit_openat(UringApiRing *self, PyObject *args, PyObject *kwargs);
PyObject *UringApiRing_submit_statx(UringApiRing *self, PyObject *args, PyObject *kwargs);
PyObject *UringApiRing_submit_statx_fdsize(UringApiRing *self, PyObject *args, PyObject *kwargs);
PyObject *UringApiRing_submit_statx_fdsize_impl(UringApiRing *self, int fd, PyObject *user_data);
PyObject *UringApiRing_submit_recv(UringApiRing *self, PyObject *args, PyObject *kwargs);
PyObject *UringApiRing_submit_recv_buf(UringApiRing *self, PyObject *args, PyObject *kwargs);
PyObject *UringApiRing_submit_recv_multishot(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs);
PyObject *UringApiRing_submit_send(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs);
PyObject *UringApiRing_submit_send_zc(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs);
PyObject *UringApiRing_submit_sendto(UringApiRing *self, PyObject *args, PyObject *kwargs);
PyObject *UringApiRing_submit_recvmsg(UringApiRing *self, PyObject *args, PyObject *kwargs);
PyObject *UringApiRing_submit_sendmsg(UringApiRing *self, PyObject *args, PyObject *kwargs);
PyObject *UringApiRing_submit_sendmsg_zc(UringApiRing *self, PyObject *args, PyObject *kwargs);
PyObject *UringApiRing_submit_accept(UringApiRing *self, PyObject *args, PyObject *kwargs);
PyObject *UringApiRing_submit_accept_multishot(UringApiRing *self, PyObject *const *args, Py_ssize_t nargs);
PyObject *UringApiRing_submit_connect(UringApiRing *self, PyObject *args, PyObject *kwargs);
PyObject *UringApiRing_submit_poll(UringApiRing *self, PyObject *args, PyObject *kwargs);
PyObject *UringApiRing_submit_poll_multishot(UringApiRing *self, PyObject *args, PyObject *kwargs);
PyObject *UringApiRing_submit_poll_remove(UringApiRing *self, PyObject *args, PyObject *kwargs);
PyObject *UringApiRing_submit_cancel(UringApiRing *self, PyObject *args, PyObject *kwargs);
PyObject *UringApiRing_submit_shutdown(UringApiRing *self, PyObject *args, PyObject *kwargs);
PyObject *UringApiRing_submit_close(UringApiRing *self, PyObject *args, PyObject *kwargs);
PyObject *UringApiRing_submit_socket(UringApiRing *self, PyObject *args, PyObject *kwargs);

#endif
