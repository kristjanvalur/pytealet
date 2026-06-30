/*
 * Ring property accessors for the _uring_api extension.
 */

#include "uring_api_properties.h"
#include "uring_api_core.h"

PyObject *UringApiRing_get_fd(UringApiRing *self, void *closure) {
    if (!self->initialized) {
        return PyLong_FromLong(-1);
    }
    return PyLong_FromLong(self->ring.ring_fd);
}

PyObject *UringApiRing_get_features(UringApiRing *self, void *closure) {
    if (!self->initialized) {
        return PyLong_FromUnsignedLong(0);
    }
    return PyLong_FromUnsignedLong(self->ring.features);
}

PyObject *UringApiRing_get_sq_entries(UringApiRing *self, void *closure) {
    if (!self->initialized) {
        return PyLong_FromUnsignedLong(0);
    }
    return PyLong_FromUnsignedLong(ring_sq_entries(self));
}

PyObject *UringApiRing_get_cq_entries(UringApiRing *self, void *closure) {
    if (!self->initialized) {
        return PyLong_FromUnsignedLong(0);
    }
    return PyLong_FromUnsignedLong(ring_cq_entries(self));
}

PyObject *UringApiRing_get_closed(UringApiRing *self, void *closure) {
    if (self->initialized) {
        Py_RETURN_FALSE;
    }
    Py_RETURN_TRUE;
}

PyObject *UringApiRing_get_running(UringApiRing *self, void *closure) {
    if (self->receive_state == URING_API_RECEIVE_DELIVERING) {
        Py_RETURN_TRUE;
    }
    Py_RETURN_FALSE;
}

PyObject *UringApiRing_get_callback(UringApiRing *self, void *closure) {
    PyObject *callback;

    Py_BEGIN_CRITICAL_SECTION_MUTEX(&self->receive_mutex);
    callback = Py_XNewRef(self->delivery_callback);
    Py_END_CRITICAL_SECTION_MUTEX();
    if (!callback) {
        Py_RETURN_NONE;
    }
    return callback;
}

int UringApiRing_set_callback(UringApiRing *self, PyObject *value, void *closure) {
    PyObject *callback;
    PyObject *old_callback = NULL;
    int ret = 0;

    if (!value) {
        PyErr_SetString(PyExc_TypeError, "cannot delete callback");
        return -1;
    }
    if (value != Py_None && !PyCallable_Check(value)) {
        PyErr_SetString(PyExc_TypeError, "callback must be callable or None");
        return -1;
    }

    callback = value == Py_None ? NULL : Py_NewRef(value);
    Py_BEGIN_CRITICAL_SECTION_MUTEX(&self->receive_mutex);
    if (delivery_is_running_locked(self)) {
        PyErr_SetString(PyExc_RuntimeError, "cannot change callback while completion service is active");
        ret = -1;
    } else {
        old_callback = self->delivery_callback;
        self->delivery_callback = callback;
        callback = NULL;
    }
    Py_END_CRITICAL_SECTION_MUTEX();
    Py_XDECREF(callback);
    Py_XDECREF(old_callback);
    return ret;
}
