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
    int frame_introspection_enabled;
    PyTypeObject *tealet_type;
    PyObject *tealet_error;
    PyObject *invalid_error;
    PyObject *state_error;
    PyObject *defunct_error;
};

#endif