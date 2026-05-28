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

PyTealetObject *PyTealet_GetOrCreateMain(PyTealetModuleState *mstate, PyTealetMainData **mdata_out);
PyTealetObject *PyTealet_GetOrCreateCurrent(PyTealetModuleState *mstate, PyTealetMainData **mdata_out);
PyObject *PyTealet_ThreadCleanup(PyTealetModuleState *mstate, Py_ssize_t cleanup_passes, PyObject *kill_exc_spec);
PyObject *PyTealet_ActiveTealets(PyTealetModuleState *mstate);
PyObject *PyTealet_ThreadKill(PyTealetModuleState *mstate, Py_ssize_t cleanup_passes, PyObject *kill_exc_spec);
int PyTealet_ThreadCleanupMdataForTeardown(PyTealetModuleState *mstate, PyTealetMainData *mdata);
int PyTealet_ErrorWasRemote(PyTealetModuleState *mstate);
#if !defined(Py312P)
Py_ssize_t PyTealet_WeaklistOffset(void);
#endif

/* push an object into the tealet dustbin, to be decrefed later. */
void PyTealet_dustbin_push(tealet_t *tealet, PyObject *obj);

/* a macro, similar to Py_CLEAR, but optionally pushes it to the dustbin */
#define PyTealet_CLEAR(tealet, obj) \
    do { \
        if ((obj) && (tealet)) { \
            PyObject *_tmp = (PyObject *)(obj); \
            (obj) = NULL; \
            PyTealet_dustbin_push(tealet, _tmp); \
        } else { \
            Py_CLEAR(obj); \
        } \
    } while (0)

 /* and a similar XDECREF */
#define PyTealet_XDECREF(tealet, obj) \
    do { \
        if ((obj) && (tealet)) { \
            PyTealet_dustbin_push(tealet, (PyObject *)(obj)); \
        } else { \
            Py_XDECREF(obj); \
        } \
    } while (0)

#endif