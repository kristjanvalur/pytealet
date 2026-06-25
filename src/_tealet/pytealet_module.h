/* pytealet_module.h - module-lifecycle state layout for _tealet.
 *
 * Defines the per-module state structure shared by the module lifecycle and
 * runtime sources.
 */

#ifndef PYTEALET_MODULE_H
#define PYTEALET_MODULE_H

#include "pytealet.h"

struct PyTealetModuleState {
    Py_tss_t tls_key;
    PyThread_type_lock thread_data_lock;
    struct PyTealetMainData *thread_data_ring;
    int frame_introspection_enabled;
    PyTypeObject *tealet_type;
    PyObject *tealet_factory;
    unsigned long tealet_factory_version;
    int tealet_factory_call_depth;
    PyObject *tealet_error;
    PyObject *invalid_error;
    PyObject *thread_mismatch_error;
    PyObject *state_error;
    PyObject *defunct_error;
    PyObject *panic_error;
    PyObject *tealet_exit_error;
};

/* Internal domain-lock helpers shared with runtime code. */
PyObject *pytealet_domain_lock_obj_new(void);
void pytealet_domain_lock_obj_lock(PyObject *domain_lock_obj);
void pytealet_domain_lock_obj_unlock(PyObject *domain_lock_obj);

#endif
