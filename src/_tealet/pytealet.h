/* pytealet.h - module-level API shared across pytealet translation units.
 *
 * Declares symbols exported by pytealet runtime sources that are referenced
 * from other internal C files.
 */

#ifndef PYTEALET_H
#define PYTEALET_H

#include "pytealet_common.h"
#include "tealet.h"

typedef struct PyTealetObject PyTealetObject;
typedef struct PyTealetModuleState PyTealetModuleState;

extern PyType_Spec pytealet_type_spec;
extern struct PyModuleDef _tealet_module;

PyTealetObject *GetMain(PyTealetModuleState *mstate, int create);
PyTealetObject *GetCurrent(PyTealetModuleState *mstate, PyTealetObject *pytealet, int create_main);
#if !defined(Py312P)
Py_ssize_t PyTealet_WeaklistOffset(void);
#endif

/* push an object into the tealet dustbin, to be decrefed later. */
void PyTealet_dustbin_push(tealet_t *tealet, PyObject *obj);

#endif