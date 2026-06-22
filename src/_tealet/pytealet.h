/* pytealet.h - module-level API shared across pytealet translation units.
 *
 * Declares symbols exported by pytealet runtime sources that are referenced
 * from other internal C files.
 */

#ifndef PYTEALET_H
#define PYTEALET_H

#include "pytealet_capi.h"
#include "pytealet_common.h"
#include "tealet.h"

typedef struct PyTealetObject PyTealetObject;
typedef struct PyTealetModuleState PyTealetModuleState;
typedef struct PyTealetMainData PyTealetMainData;

extern PyType_Spec pytealet_type_spec;
extern struct PyModuleDef _tealet_module;

PyTealetObject *PyTealet_GetOrCreateMain(PyTealetModuleState *mstate, PyTealetMainData **mdata_out);
PyTealetObject *PyTealet_GetOrCreateCurrent(PyTealetModuleState *mstate, PyTealetMainData **mdata_out);
PyObject *PyTealet_ThreadReap(PyTealetModuleState *mstate, Py_ssize_t cleanup_passes, PyObject *kill_exc_spec);
PyObject *PyTealet_ThreadSweep(PyTealetModuleState *mstate);
PyObject *PyTealet_ThreadActive(PyTealetModuleState *mstate);
PyObject *PyTealet_ThreadKill(PyTealetModuleState *mstate, Py_ssize_t cleanup_passes, PyObject *kill_exc_spec);
int PyTealet_ThreadReapMdataForTeardown(PyTealetMainData *mdata);
int PyTealet_ErrorWasRemote(PyTealetModuleState *mstate);
PyObject *PyTealetApi_Create(PyTealetModuleState *mstate);
PyObject *PyTealetApi_Duplicate(PyTealetModuleState *mstate, PyObject *source_obj);
int PyTealetApi_Stub(PyTealetModuleState *mstate, PyObject *target_obj);
int PyTealetApi_SetStub(PyTealetModuleState *mstate, PyObject *target_obj, PyObject *source_obj, int duplicate);
int PyTealetApi_Prepare(PyTealetModuleState *mstate, PyObject *target_obj, PyObject *func,
                        PyTealetApi_RunCFunc cfunc);
PyObject *PyTealetApi_Run(PyTealetModuleState *mstate, PyObject *target_obj, PyObject *func,
                          PyTealetApi_RunCFunc cfunc, PyObject *arg);
PyObject *PyTealetApi_Switch(PyTealetModuleState *mstate, PyObject *target_obj, PyObject *arg, uint32_t flags);
PyObject *PyTealetApi_Throw(PyTealetModuleState *mstate, PyObject *target_obj, PyObject *exc,
                            PyObject *return_target, uint32_t flags);
int PyTealetApi_SetException(PyTealetModuleState *mstate, PyObject *target_obj, PyObject *exc, PyObject *fallback);
PyObject *PyTealetApi_ThreadReap(PyTealetModuleState *mstate, Py_ssize_t cleanup_passes, PyObject *kill_exc_spec);
PyObject *PyTealetApi_ThreadActive(PyTealetModuleState *mstate);
PyObject *PyTealetApi_ThreadKill(PyTealetModuleState *mstate, Py_ssize_t cleanup_passes, PyObject *kill_exc_spec);
int PyTealetApi_ErrorWasRemote(PyTealetModuleState *mstate);
PyObject *PyTealetApi_Previous(PyTealetModuleState *mstate);
int PyTealetApi_FrameIntrospectionGet(PyTealetModuleState *mstate);
int PyTealetApi_FrameIntrospectionSet(PyTealetModuleState *mstate, int enabled);
int PyTealetApi_IsForeign(PyTealetModuleState *mstate, PyObject *target_obj);
int PyTealetApi_StateGet(PyTealetModuleState *mstate, PyObject *target_obj, PyTealet_State *state_out);
int PyTealetApi_ThreadIdGet(PyTealetModuleState *mstate, PyObject *target_obj, unsigned long *thread_id_out);
#if !defined(Py312P)
Py_ssize_t PyTealet_WeaklistOffset(void);
#endif


/* push an object into the tealet dustbin, to be decrefed later. */
void PyTealet_dustbin_push(tealet_t *tealet, PyObject *obj);

/* a macro, similar to Py_CLEAR, but optionally pushes it to the dustbin */
#define PyTealet_CLEAR(tealet, obj)                                                                                    \
    do {                                                                                                               \
        if ((obj) && (tealet)) {                                                                                       \
            PyObject *_tmp = (PyObject *)(obj);                                                                        \
            (obj) = NULL;                                                                                              \
            PyTealet_dustbin_push(tealet, _tmp);                                                                       \
        } else {                                                                                                       \
            Py_CLEAR(obj);                                                                                             \
        }                                                                                                              \
    } while (0)

/* and a similar XDECREF */
#define PyTealet_XDECREF(tealet, obj)                                                                                  \
    do {                                                                                                               \
        if ((obj) && (tealet)) {                                                                                       \
            PyTealet_dustbin_push(tealet, (PyObject *)(obj));                                                          \
        } else {                                                                                                       \
            Py_XDECREF(obj);                                                                                           \
        }                                                                                                              \
    } while (0)

#endif
