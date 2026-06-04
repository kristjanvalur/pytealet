/* pytealet_lineage.h - internal lineage/ring lifecycle helpers.
 *
 * This module isolates thread-lineage registry maintenance and stale-lineage
 * sweeping/teardown from the core tealet runtime paths.
 */

#ifndef PYTEALET_LINEAGE_H
#define PYTEALET_LINEAGE_H

#include "pytealet_runtime.h"

int PyTealet_LineageLinkThreadData(PyTealetModuleState *mstate, PyTealetMainData *mdata);
int PyTealet_LineageReapInner(PyTealetModuleState *mstate, PyTealetMainData *mdata, PyObject *nerfed,
                              int clear_current_tss, int best_effort);
int PyTealet_LineageThreadIdentIsAlive(unsigned long thread_id, int *alive_out);

#endif
