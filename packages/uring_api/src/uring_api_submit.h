#ifndef URING_API_SUBMIT_H
#define URING_API_SUBMIT_H

/* private implementation header; not part of the public C API. */

#include "uring_api_common.h"

PyObject *UringApiRing_submit_recv(UringApiRing *self, PyObject *args, PyObject *kwargs);
PyObject *UringApiRing_submit_recv_multishot(UringApiRing *self, PyObject *args, PyObject *kwargs);
PyObject *UringApiRing_submit_send(UringApiRing *self, PyObject *args, PyObject *kwargs);
PyObject *UringApiRing_submit_send_zc(UringApiRing *self, PyObject *args, PyObject *kwargs);
PyObject *UringApiRing_submit_sendto(UringApiRing *self, PyObject *args, PyObject *kwargs);
PyObject *UringApiRing_submit_recvmsg(UringApiRing *self, PyObject *args, PyObject *kwargs);
PyObject *UringApiRing_submit_sendmsg(UringApiRing *self, PyObject *args, PyObject *kwargs);
PyObject *UringApiRing_submit_sendmsg_zc(UringApiRing *self, PyObject *args, PyObject *kwargs);
PyObject *UringApiRing_submit_accept(UringApiRing *self, PyObject *args, PyObject *kwargs);
PyObject *UringApiRing_submit_accept_multishot(UringApiRing *self, PyObject *args, PyObject *kwargs);
PyObject *UringApiRing_submit_connect(UringApiRing *self, PyObject *args, PyObject *kwargs);
PyObject *UringApiRing_submit_cancel(UringApiRing *self, PyObject *args, PyObject *kwargs);
PyObject *UringApiRing_submit_shutdown(UringApiRing *self, PyObject *args, PyObject *kwargs);
PyObject *UringApiRing_submit_close(UringApiRing *self, PyObject *args, PyObject *kwargs);
PyObject *UringApiRing_submit_socket(UringApiRing *self, PyObject *args, PyObject *kwargs);

#endif