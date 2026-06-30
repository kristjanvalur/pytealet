#ifndef URING_API_PROPERTIES_H
#define URING_API_PROPERTIES_H

/* private implementation header; not part of the public C API. */

#include "uring_api_common.h"

PyObject *UringApiRing_get_fd(UringApiRing *self, void *closure);
PyObject *UringApiRing_get_features(UringApiRing *self, void *closure);
PyObject *UringApiRing_get_sq_entries(UringApiRing *self, void *closure);
PyObject *UringApiRing_get_cq_entries(UringApiRing *self, void *closure);
PyObject *UringApiRing_get_closed(UringApiRing *self, void *closure);
PyObject *UringApiRing_get_running(UringApiRing *self, void *closure);
PyObject *UringApiRing_get_callback(UringApiRing *self, void *closure);
int UringApiRing_set_callback(UringApiRing *self, PyObject *value, void *closure);

#endif