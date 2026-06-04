/* pytealet_throw.h - internal throw/exception transport helpers.
 *
 * These helpers isolate throw-registry bookkeeping and deferred exception
 * delivery from the main runtime object implementation.
 */

#ifndef PYTEALET_THROW_H
#define PYTEALET_THROW_H

#include "pytealet_runtime.h"

uint64_t PyTealetThrow_NextToken(PyTealetMainData *mdata);
int PyTealetThrow_RegistrySet(PyTealetMainData *mdata, uint64_t token, PyObject *exc, PyObject *fallback);
int PyTealetThrow_RegistryPop(PyTealetMainData *mdata, uint64_t token, PyObject **exc_out, PyObject **fallback_out);

void PyTealetThrow_ClearPendingException(PyTealetMainData *mdata);
PyObject *PyTealetThrow_TakePendingException(PyTealetMainData *mdata);

int PyTealetThrow_ExceptionChainContains(PyObject *raised, PyObject *needle);
PyObject *PyTealetThrow_GetRaisedException(void);
void PyTealetThrow_SetRaisedException(PyObject *exc);

int PyTealetThrow_RedirectUncaught(PyTealetModuleState *mstate, PyTealetMainData *mdata, PyTealetObject *tealet,
                                   PyObject *exception, PyTealetObject **return_to_io);
PyObject *PyTealetThrow_MaybeRaisePending(PyTealetMainData *mdata, PyTealetObject *current, PyObject *result);

#endif
