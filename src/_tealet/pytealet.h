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
typedef struct PyTealetMainData PyTealetMainData;

extern PyType_Spec pytealet_type_spec;
extern struct PyModuleDef _tealet_module;

PyTealetObject *GetMain(PyTealetModuleState *mstate, int create, PyTealetMainData **mdata_out);
PyTealetObject *GetCurrent(PyTealetModuleState *mstate, PyTealetObject *pytealet, int create_main,
						   PyTealetMainData **mdata_out);
PyObject *PyTealet_ThreadCleanup(PyTealetModuleState *mstate, Py_ssize_t cleanup_passes);
PyObject *PyTealet_ActiveTealets(PyTealetModuleState *mstate);
PyObject *PyTealet_ThreadKill(PyTealetModuleState *mstate, Py_ssize_t cleanup_passes);
int PyTealet_ThreadCleanupMdataForTeardown(PyTealetModuleState *mstate, PyTealetMainData *mdata);
#if !defined(Py312P)
Py_ssize_t PyTealet_WeaklistOffset(void);
#endif

/* push an object into the tealet dustbin, to be decrefed later. */
void PyTealet_dustbin_push(tealet_t *tealet, PyObject *obj);

#endif