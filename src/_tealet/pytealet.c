
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include "Python.h"
#include "structmember.h"
#include "frameobject.h"
#include "pythread.h"

#include "tealet.h"
#include "tools.h"

/* Python minor-version helpers for readable version-specific conditionals. */
#if PY_VERSION_HEX >= 0x030A0000 && PY_VERSION_HEX < 0x030B0000
#define PY310 1
#endif

#if PY_VERSION_HEX >= 0x030B0000 && PY_VERSION_HEX < 0x030C0000
#define PY311 1
#endif

#if PY_VERSION_HEX >= 0x030C0000 && PY_VERSION_HEX < 0x030D0000
#define PY312 1
#endif

#if PY_VERSION_HEX >= 0x030D0000 && PY_VERSION_HEX < 0x030E0000
#define PY313 1
#endif

#if PY_VERSION_HEX >= 0x030E0000 && PY_VERSION_HEX < 0x030F0000
#define PY314 1
#endif

#if PY_VERSION_HEX >= 0x030F0000 && PY_VERSION_HEX < 0x03100000
#define PY315 1
#endif

#if defined(PY310) || defined(PY311) || defined(PY312)
#define PY_HAS_CFRAME
#endif

#define STATE_NEW 0
#define STATE_STUB 1
#define STATE_RUN 2
#define STATE_EXIT 3

/* Forward declaration */
typedef struct PyTealetObject PyTealetObject;
static struct PyModuleDef _tealet_module;

typedef struct PyTealetModuleState {
	Py_tss_t tls_key;
	PyTypeObject *tealet_type;
	PyObject *tealet_error;
	PyObject *invalid_error;
	PyObject *state_error;
	PyObject *defunct_error;
} PyTealetModuleState;

typedef struct PyTealetNewArg {
	PyTealetObject *dest;
	PyObject *func;
	PyObject *arg;
} PyTealetNewArg;

/* the structure we associate with the main tealet */
typedef struct PyTealetMainData
{
	long tid;
	PyTealetNewArg new_arg;
	PyObject *dustbin;
} PyTealetMainData;

/* initial number of slots in dustbin, to avoid realloc on push */
#define DUSTBIN_PREALLOC 10


/* Extra data stored with each tealet for the Python binding.
 * This structure is stored in tealet->extra and provides type-safe
 * access to the associated PyTealetObject.
 */
typedef struct PyTealetExtra {
	PyTealetObject *pytealet;
} PyTealetExtra;

/* Helper macros for type-safe access to the tealet extra data */
#define TEALET_PYOBJECT(t) (TEALET_EXTRA((t), PyTealetExtra)->pytealet)
#define TEALET_SET_PYOBJECT(t, obj) (TEALET_EXTRA((t), PyTealetExtra)->pytealet = (obj))


/* a structure that captures the tstate of a tealet.  The fields stored
 * and their semantics may change from python version to version.
 * Before switching away from a tealet, we capture the current tstate into
 * this structure, with our own private references.  When we return back,
 * we restore the tstate and release our references.
 * 
 * There exists an optmizable path switch-switch path between tealet a and b:
 * 1) switch from a to b, creating new tstate refs in a, but leaving tstate intact.
 * 2) in B, switch back, clearing the tstate and moving b's tstate copy to the python tstate.
 * 
 * In this case, when we know that this symmetry exist, we can simply _move_ the tstate
 * from python to local, and flom local to python, without adjusting references.
 * 
 */
struct PyTealetTstate {
	PyFrameObject *frame;
	PyObject *exc_type;
	PyObject *exc_val;
	PyObject *exc_tb;
	_PyErr_StackItem *exc_info;
	_PyErr_StackItem exc_state;
	int recursion_depth;
	int trash_delete_nesting;
	PyObject *context; /* Python 3.7+ contextvars */
	int has_state; /* Debug helper: 1 when this struct currently stores a saved tstate */
	/* Python 3.10-3.12: cframe tracks C-level call frames (removed in 3.13)
	 * Stack-slicing preserves the CFrame struct itself; we just save the pointer */
#if defined(PY_HAS_CFRAME)
	CFrame* cframe;
#endif
};

typedef struct PyTealetTstate PyTealetTstate;


/* The python tealet object */
struct PyTealetObject {
    PyObject_HEAD
    int state;
	tealet_t *tealet;
	PyObject *weakreflist; /* List of weak references */

	/* thread state information */
	PyTealetTstate tstate;
};

/* helpers for getting main and current and checking relationship */
static PyTealetObject *GetMainWithState(int create, PyTealetModuleState *mstate);
static PyTealetObject *GetMain(int create);
static PyTealetObject *GetCurrent(PyTealetObject *main);
static int CheckTarget(PyTealetObject *target, PyTealetObject *main);

static tealet_t * pytealet_main(tealet_t *t_current, void *arg);

static PyTealetModuleState *
PyTealet_GetModuleState(void)
{
	PyObject *module = PyDict_GetItemString(PyImport_GetModuleDict(), "_tealet");
	if (!module)
		return NULL;
	return (PyTealetModuleState*)PyModule_GetState(module);
}

static PyTealetModuleState *
PyTealet_GetModuleStateOrError(void)
{
	PyTealetModuleState *mstate = PyTealet_GetModuleState();
	if (!mstate) {
		PyErr_SetString(PyExc_RuntimeError, "_tealet module state unavailable");
		return NULL;
	}
	return mstate;
}

static PyTealetModuleState *
PyTealet_GetModuleStateFromDefiningClass(PyTypeObject *defining_class)
{
	PyTealetModuleState *mstate;
	if (!defining_class) {
		PyErr_SetString(PyExc_RuntimeError, "defining class unavailable");
		return NULL;
	}
	mstate = (PyTealetModuleState*)PyType_GetModuleState(defining_class);
	if (!mstate) {
		PyErr_SetString(PyExc_RuntimeError, "_tealet module state unavailable from type");
		return NULL;
	}
	return mstate;
}

static int
PyTealet_Check(PyObject *op)
{
	PyTealetModuleState *mstate = PyTealet_GetModuleState();
	return mstate && mstate->tealet_type && PyObject_TypeCheck(op, mstate->tealet_type);
}

static int
PyTealet_CheckExact(PyObject *op)
{
	PyTealetModuleState *mstate = PyTealet_GetModuleState();
	return mstate && mstate->tealet_type && (Py_TYPE(op) == mstate->tealet_type);
}


static void PyTealetTstate_Init(PyTealetTstate *saved)
{
	saved->has_state = 0;
}

/* Raw copy the tstate files from PyThreadState to our local structure */
static void PyTealetTstate_Get(PyTealetTstate *dst, const PyThreadState *src)
{
	dst->frame = src->frame;
	dst->recursion_depth = src->recursion_depth;
	
	dst->exc_type = src->curexc_type;
	dst->exc_val = src->curexc_value;
	dst->exc_tb = src->curexc_traceback;
	
	dst->exc_state = src->exc_state;
	/* Keep dst->exc_info self-contained when it points at exc_state. */
	if (src->exc_info == &src->exc_state)
		dst->exc_info = &dst->exc_state;
	else
		dst->exc_info = src->exc_info;
	
	dst->context = src->context;
	
#if defined(PY_HAS_CFRAME)
	dst->cframe = src->cframe;
#endif
	dst->trash_delete_nesting = src->trash_delete_nesting;
}

/* Raw copy previously saved tealet tstate into PyThreadState. */
static void PyTealetTstate_Put(const PyTealetTstate *src, PyThreadState *dst)
{
	dst->frame = src->frame;
	dst->recursion_depth = src->recursion_depth;

	dst->curexc_type = src->exc_type;
	dst->curexc_value = src->exc_val;
	dst->curexc_traceback = src->exc_tb;

	dst->exc_state = src->exc_state;
	if (src->exc_info == &src->exc_state)
		dst->exc_info = &dst->exc_state;
	else
		dst->exc_info = src->exc_info;

	dst->context = src->context;
	dst->context_ver++;  /* Invalidate contextvars cache */

#if defined(PY_HAS_CFRAME)
	dst->cframe = src->cframe;
#endif
	dst->trash_delete_nesting = src->trash_delete_nesting;
}

/* Increment and decrement the reference count of the tstate's references.
 * we need to Increment the references when we create new tealets from an existing
 * one (or main), and decrement when a tealet terminates.
 */
static void PyTealetTstate_IncRef(PyTealetTstate *saved)
{
	assert(saved->has_state == 1);
	Py_XINCREF(saved->frame);
	Py_XINCREF(saved->exc_type);
	Py_XINCREF(saved->exc_val);
	Py_XINCREF(saved->exc_tb);
	Py_XINCREF(saved->exc_state.exc_value);
	/* exc_info is a pointer to exc_state or a stack item, so we don't own a reference to it */
	Py_XINCREF(saved->context);
}

static void
dustbin_push(tealet_t *tealet, PyObject *obj)
{
	PyTealetMainData *mdata;
	if (!obj)
		return;
	if (!tealet) {
		Py_DECREF(obj);
		return;
	}
	mdata = (PyTealetMainData*)*tealet_main_userpointer(tealet);
	if (!mdata || !mdata->dustbin || !PyList_Check(mdata->dustbin)) {
		Py_DECREF(obj);
		return;
	}
	if (PyList_Append(mdata->dustbin, obj) < 0) {
		Py_DECREF(obj);
		PyErr_WriteUnraisable(Py_None);
		PyErr_Clear();
		return;
	}
	Py_DECREF(obj);
}

static void PyTealetTstate_DecRef(PyTealetTstate *saved, tealet_t *dustbin_tealet)
{
	assert(saved->has_state == 1);
	if (dustbin_tealet) {
		dustbin_push(dustbin_tealet, (PyObject*)saved->frame);
		dustbin_push(dustbin_tealet, saved->exc_type);
		dustbin_push(dustbin_tealet, saved->exc_val);
		dustbin_push(dustbin_tealet, saved->exc_tb);
		dustbin_push(dustbin_tealet, saved->exc_state.exc_value);
		dustbin_push(dustbin_tealet, saved->context);
	} else {
		Py_XDECREF(saved->frame);
		Py_XDECREF(saved->exc_type);
		Py_XDECREF(saved->exc_val);
		Py_XDECREF(saved->exc_tb);
		Py_XDECREF(saved->exc_state.exc_value);
		Py_XDECREF(saved->context);
	}
}

/* helper to clear the python threadstate for hygiene */
static void PyTealetTstate_ClearPy(PyThreadState *py_tstate)
{
	py_tstate->frame = NULL;
	py_tstate->curexc_type = NULL;
	py_tstate->curexc_value = NULL;
	py_tstate->curexc_traceback = NULL;
	py_tstate->exc_info = NULL;  /* use this as a sentinel, should never be null in a valid situation */
	py_tstate->exc_state.exc_value = NULL;
	py_tstate->recursion_depth = 0;
	py_tstate->trash_delete_nesting = 0;
	py_tstate->context = NULL;
#if defined(PY_HAS_CFRAME)
	py_tstate->cframe = NULL;
#endif
}

/* helper to null or clear the python threadstate for hygiene */
static void PyTealetTstate_AssertClearPy(PyThreadState *py_tstate)
{
	/* should never be null in a valid situation, null indicates that we previously cleared it.*/
	assert(py_tstate->exc_info == NULL);
}

/* copy the threadstate, e.g. when we create a stub */
static void PyTealetTstate_Copy(PyTealetTstate *dst, const PyThreadState *src)
{
	assert(dst->has_state == 0);
	PyTealetTstate_Get(dst, src);
	dst->has_state = 1;
	PyTealetTstate_IncRef(dst);
}

/* drop our own threadstate refs, e.g. after failure, or at tealet end */
static void PyTealetTstate_Drop(PyTealetTstate *dst, tealet_t *dustbin_tealet)
{
	if (!dst->has_state)
		return;
	PyTealetTstate_DecRef(dst, dustbin_tealet);
	dst->has_state = 0;
}

/* Move out the threadstate to a saved struct before switch. someone will restore after. */
static void PyTealetTstate_Save(PyTealetTstate *dst, PyThreadState *src)
{
	assert(dst->has_state == 0);
	PyTealetTstate_Get(dst, src);
	PyTealetTstate_ClearPy(src);
	dst->has_state = 1;
}

/* restore the threadstate, after someon has saved it.*/
static void PyTealetTstate_Restore(PyTealetTstate *src, PyThreadState *dst)
{
	assert(src->has_state == 1);
	PyTealetTstate_AssertClearPy(dst);
	PyTealetTstate_Put(src, dst);
	src->has_state = 0;
}


/* get the far pointer that we need at least ot store any stack based data currently
 * in the python tstate.  this varies by python version
 */

static void * PyTealet_GetStackFar(const PyThreadState *py_tstate)
{
#if defined(PY_HAS_CFRAME)
	/* python 3.10 has cframe on stack.  make sure we save our stacks to include
	 * this whole structure
	 */
	if (py_tstate->cframe)
		return tealet_stack_further(&py_tstate->cframe[0], &py_tstate->cframe[1]);
#else
	(void)py_tstate;
#endif
	return NULL;
}

/* Helper functions to fill/empty the dustbin.  We must be careful not to
 * clear references at a delicate moment before switching, rather
 * references must be cleared after, so that any side-effects of
 * clearing references won't affect the state of the program.
 */
static void
dustbin_clear(tealet_t *tealet)
{
	PyTealetMainData *mdata = (PyTealetMainData*)*tealet_main_userpointer(tealet);
	Py_ssize_t n;
	n = PyList_GET_SIZE(mdata->dustbin);
	if (n == 0)
		return;
	if (PyList_SetSlice(mdata->dustbin, 0, n, NULL) < 0) {
		PyErr_WriteUnraisable(Py_None);
		PyErr_Clear();
	}
}

static PyObject *
pytealet_new(PyTypeObject *subtype, PyObject *args, PyObject *kwds)
{
	PyTealetObject *src = NULL;
	PyTealetObject *result;
	if (args && PyTuple_GET_SIZE(args)>0) {
		src = (PyTealetObject*)PyTuple_GET_ITEM(args, 0);
		if (!PyTealet_Check((PyObject*)src)) {
			PyErr_SetNone(PyExc_TypeError);
			return NULL;
		}
		if (src->state != STATE_NEW && src->state != STATE_STUB) {
			PyTealetModuleState *mstate = PyTealet_GetModuleStateOrError();
			if (!mstate)
				return NULL;
			PyErr_SetString(mstate->state_error, "state must be new or stub");
			return NULL;
		}
	}
	result = (PyTealetObject*)subtype->tp_alloc(subtype, 0);
	if (!result)
		return NULL;
	result->state = STATE_NEW;
	result->tealet = NULL;
	PyTealetTstate_Init(&result->tstate);
	result->weakreflist = NULL;


	if (src) {
		if (src->state == STATE_STUB) {
			/* duplicate the stub tealet and the tstate */
			result->tealet = tealet_duplicate(src->tealet);
			if (!result->tealet) {
				Py_DECREF(result);
				return PyErr_NoMemory();
			}
			TEALET_SET_PYOBJECT(result->tealet, result);
			result->tstate = src->tstate;
			PyTealetTstate_IncRef(&result->tstate);
		}
		result->state = src->state;
	}
	return (PyObject*) result;
}

static void
pytealet_dealloc(PyObject *obj)
{
	PyTealetObject *tealet = (PyTealetObject *)obj;
	if (tealet->state == STATE_RUN) {
		int err = PyErr_WarnEx(PyExc_RuntimeWarning, "freeing an active tealet leaks memory", 1);
		if (err) {
			PyErr_WriteUnraisable(Py_None);
		}
	}
	/* Release any owned saved thread-state references */
	PyTealetTstate_Drop(&tealet->tstate, NULL);
	if (tealet->weakreflist != NULL)
        PyObject_ClearWeakRefs(obj);
	if (tealet->tealet)
		tealet_delete(tealet->tealet);
	Py_TYPE(obj)->tp_free(obj);
}

/* make stub here */
static PyObject *
pytealet_stub_impl(PyObject *self, PyTealetModuleState *mstate)
{
	PyTealetObject *main, *pytealet = (PyTealetObject*)self;
	tealet_t *tresult;
	PyThreadState *tstate = PyThreadState_GET();
	void *stack_far;
	if (pytealet->state != STATE_NEW) {
		PyErr_SetString(mstate->state_error, "must be new");
		return NULL;
	}
	assert(pytealet->tealet == NULL);
	main = GetMain(1);
	if (!main)
		return NULL;
	stack_far = PyTealet_GetStackFar(PyThreadState_GET());
	tresult = tealet_stub_new(main->tealet, stack_far);
	if (!tresult)
		return PyErr_NoMemory();
	// capture the tstate of the current thread, which we will restore when the stub runs
	PyTealetTstate_Copy(&pytealet->tstate, tstate);
	pytealet->tealet = tresult;
	pytealet->state = STATE_STUB;
	TEALET_SET_PYOBJECT(tresult, pytealet);
	Py_INCREF(self);
	return self;
}

static PyObject *
pytealet_stub(PyObject *self, PyTypeObject *defining_class, PyObject *const *args,
			 Py_ssize_t nargs, PyObject *kwnames)
{
	PyTealetModuleState *mstate;
	if (nargs != 0 || (kwnames && PyTuple_GET_SIZE(kwnames) > 0)) {
		PyErr_SetString(PyExc_TypeError, "stub() takes no arguments");
		return NULL;
	}
	mstate = PyTealet_GetModuleStateFromDefiningClass(defining_class);
	if (!mstate)
		return NULL;
	return pytealet_stub_impl(self, mstate);
}

/* run a tealet and optinonally run */
static PyObject *
pytealet_run_impl(PyObject *self, PyObject *args, PyObject *kwds, PyTealetModuleState *mstate)
{
	PyTealetObject *target = (PyTealetObject *)self;
	PyTealetObject *current;
	PyObject *func; 
	PyObject *farg = Py_None;
	int fail;
	tealet_t *tealet;
	char *keywords[] = {"function", "arg", NULL};
	PyThreadState *tstate = PyThreadState_GET();
	PyObject *result = NULL;
	int created_from_new;
	PyTealetMainData *mdata;
	PyTealetNewArg *ptarg;
	void *switch_arg;

	/* target->tealet is null or a stub tealet.  GetCurrent works either way. */
	current = GetCurrent(target);
	if (!current)
		return NULL;
	if (CheckTarget(target, current))
		return NULL;

	if (target->state != STATE_NEW && target->state != STATE_STUB) {
		PyErr_SetString(mstate->state_error, "must be new or stub");
		return NULL;
	}
	if (!PyArg_ParseTupleAndKeywords(args, kwds, "O|O:run", keywords,
		&func, &farg))
		return NULL;
	
	created_from_new = (target->state == STATE_NEW);
	mdata = (PyTealetMainData*)*tealet_main_userpointer(current->tealet);
	ptarg = &mdata->new_arg;
	switch_arg = (void*)ptarg;
	
	ptarg->dest = target;
	ptarg->func = func;
	ptarg->arg = farg;
	
	if (!created_from_new) {
		/* running the stub is like switching to it.  It owns its own
		 * thread state already and will apply it
		 */
		PyTealetTstate_Save(&current->tstate, tstate);
		fail = tealet_stub_run(target->tealet, pytealet_main, &switch_arg);
		PyTealetTstate_Restore(&current->tstate, tstate);
		if (fail) {
			PyErr_NoMemory();
			goto err;
		}
	} else {
		void *stack_limit = PyTealet_GetStackFar(tstate);
		// get our own copy of the tstate, the new tealet inherits the current.
		PyTealetTstate_Copy(&current->tstate, tstate);
		tealet = tealet_new(current->tealet, pytealet_main, &switch_arg, stack_limit);
		if (!tealet) {
			PyTealetTstate_Drop(&current->tstate, NULL);
			PyErr_NoMemory();
			goto err;
		}
		// success, the target has just done a PyTealetTstate_Save(), now we complete the move
		PyTealetTstate_Restore(&current->tstate, tstate);
	}
	/* success */
	result = (PyObject *)switch_arg;
err:
	/* clear garbage */
	dustbin_clear(current->tealet);
	return result;
}

static PyObject *
pytealet_run(PyObject *self, PyTypeObject *defining_class, PyObject *const *args,
			Py_ssize_t nargs, PyObject *kwnames)
{
	PyTealetModuleState *mstate;
	PyObject *tuple_args;
	PyObject *kwds = NULL;
	PyObject *result;
	Py_ssize_t i;
	Py_ssize_t nkw = kwnames ? PyTuple_GET_SIZE(kwnames) : 0;

	mstate = PyTealet_GetModuleStateFromDefiningClass(defining_class);
	if (!mstate)
		return NULL;

	tuple_args = PyTuple_New(nargs);
	if (!tuple_args)
		return NULL;
	for (i = 0; i < nargs; i++) {
		PyObject *item = args[i];
		Py_INCREF(item);
		PyTuple_SET_ITEM(tuple_args, i, item);
	}

	if (nkw > 0) {
		kwds = PyDict_New();
		if (!kwds) {
			Py_DECREF(tuple_args);
			return NULL;
		}
		for (i = 0; i < nkw; i++) {
			PyObject *key = PyTuple_GET_ITEM(kwnames, i);
			if (PyDict_SetItem(kwds, key, args[nargs + i]) < 0) {
				Py_DECREF(kwds);
				Py_DECREF(tuple_args);
				return NULL;
			}
		}
	}

	result = pytealet_run_impl(self, tuple_args, kwds, mstate);
	Py_XDECREF(kwds);
	Py_DECREF(tuple_args);
	return result;
}

/* switch to a different tealet */
static PyObject *
pytealet_switch_impl(PyObject *_self, PyObject *args, PyTealetModuleState *mstate)
{
	PyTealetObject *self = (PyTealetObject *)_self;
	PyTealetObject *current;
	int fail;
	PyThreadState *tstate = PyThreadState_GET();
	PyObject *pyarg = Py_None;
	void *switch_arg;
	
	if (!PyArg_ParseTuple(args, "|O:switch", &pyarg))
		return NULL;

	if (self->state != STATE_RUN) {
		PyErr_SetString(mstate->state_error, "must be active");
		return NULL;
	}
	assert(self->tealet);
	current = GetCurrent(NULL);
	if (!current)
		return NULL;
	if (CheckTarget(self, current))
		return NULL;
	
	Py_INCREF(pyarg);
	switch_arg = (void*)pyarg;
	/* switch */
	PyTealetTstate_Save(&current->tstate, tstate);
	fail = tealet_switch(self->tealet, &switch_arg);
	PyTealetTstate_Restore(&current->tstate, tstate);

	/* clear out garbage */
	dustbin_clear(current->tealet);
	
	if (fail == TEALET_ERR_DEFUNCT) {
		Py_DECREF(pyarg);
		PyErr_SetString(mstate->defunct_error, "target is defunct");
		return NULL;
	} else if (fail == TEALET_ERR_MEM) {
		Py_DECREF(pyarg);
		return PyErr_NoMemory();
	}
	/* return the arg passed to us */
	pyarg = (PyObject *)switch_arg;
	return pyarg;
}

static PyObject *
pytealet_switch(PyObject *self, PyTypeObject *defining_class, PyObject *const *args,
			   Py_ssize_t nargs, PyObject *kwnames)
{
	PyTealetModuleState *mstate;
	PyObject *tuple_args;
	PyObject *result;
	Py_ssize_t i;

	if (kwnames && PyTuple_GET_SIZE(kwnames) > 0) {
		PyErr_SetString(PyExc_TypeError, "switch() takes no keyword arguments");
		return NULL;
	}
	mstate = PyTealet_GetModuleStateFromDefiningClass(defining_class);
	if (!mstate)
		return NULL;
	tuple_args = PyTuple_New(nargs);
	if (!tuple_args)
		return NULL;
	for (i = 0; i < nargs; i++) {
		PyObject *item = args[i];
		Py_INCREF(item);
		PyTuple_SET_ITEM(tuple_args, i, item);
	}
	result = pytealet_switch_impl(self, tuple_args, mstate);
	Py_DECREF(tuple_args);
	return result;
}
	
static struct PyMethodDef pytealet_methods[] = {
	{"stub", (PyCFunction)(void(*)(void))pytealet_stub, METH_METHOD|METH_FASTCALL|METH_KEYWORDS, ""},
	{"run", (PyCFunction)(void(*)(void))pytealet_run, METH_METHOD|METH_FASTCALL|METH_KEYWORDS, ""},
    {"switch", (PyCFunction)(void(*)(void))pytealet_switch, METH_METHOD|METH_FASTCALL|METH_KEYWORDS, ""},
	{NULL,       NULL}          /* sentinel */
};

/************
 * Properties
 */
static PyObject *
pytealet_get_main(PyObject *_self, void *_closure)
{
	PyTealetObject *self = (PyTealetObject *)_self;
	PyTealetObject *main;
	
	if (!self->tealet) {
		/* New tealet not yet initialized (STATE_NEW) or
		 * tealet has exited and been auto-deleted (STATE_EXIT).
		 * Return the thread's main tealet.
		 * TODO: Review if STATE_NEW should exist without tealet (lazy creation)
		 */
		main = GetMain(1);
		if (!main)
			return NULL;
	} else {
		main = TEALET_PYOBJECT(self->tealet->main);
	}
	Py_INCREF(main);
	return (PyObject*)main;
}

static PyObject *
pytealet_get_state(PyObject *_self, void *_closure)
{
	PyTealetObject *self = (PyTealetObject *)_self;
	return PyLong_FromLong(self->state);
}

static PyObject *
pytealet_get_frame(PyObject *_self, void *_closure)
{
	PyTealetObject *self = (PyTealetObject *)_self;
	PyObject *frame = (PyObject*)self->tstate.frame;
	if (!frame) {
		/* is it the current tealet of the current thread? */
		if (self == GetCurrent(NULL)) {
			PyThreadState *tstate = PyThreadState_GET();
			frame = (PyObject*)tstate->frame;
		}
	}
	if (!frame)
		frame = Py_None;
	Py_INCREF(frame);
	return frame;
}

static PyObject *
pytealet_get_tid(PyObject *_self, void *_closure)
{
	PyTealetObject *self = (PyTealetObject *)_self;
	long tid = 0;
	if (self->tealet) {
		PyTealetMainData *mdata = (PyTealetMainData*)*tealet_main_userpointer(self->tealet);
		tid = mdata->tid;
	}
	return PyLong_FromLong(tid);
}


static struct PyGetSetDef pytealet_getset[] = {
	{"main", pytealet_get_main, NULL, "", NULL},
	{"state", pytealet_get_state, NULL, "", NULL},
	{"frame", pytealet_get_frame, NULL, "", NULL},
	{"thread_id", pytealet_get_tid, NULL, "", NULL},
	{0}
};


/* CPython type slot table stores C function pointers in void* fields by API design. */
#if defined(__GNUC__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
#endif
static PyType_Slot pytealet_type_slots[] = {
	{Py_tp_dealloc, pytealet_dealloc},
	{Py_tp_methods, pytealet_methods},
	{Py_tp_getset, pytealet_getset},
	{Py_tp_new, pytealet_new},
	{0, NULL}
};
#if defined(__GNUC__)
#pragma GCC diagnostic pop
#endif

static PyType_Spec pytealet_type_spec = {
	"_tealet.tealet",
	sizeof(PyTealetObject),
	0,
	Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
	pytealet_type_slots
};


/* The main function.  Invoked either from tealet.new or tealet.run */
static tealet_t *
pytealet_main(tealet_t *t_current, void *arg)
{
	PyTealetNewArg *targ = (PyTealetNewArg*)arg;
	PyTealetObject *tealet = targ->dest;
	PyObject *func = targ->func;
	PyObject *farg = targ->arg;
	PyObject *result, *return_arg;
	PyTealetObject *return_to;
	tealet_t *t_return;
	PyThreadState *tstate = PyThreadState_GET();
	
	if (tealet->state == STATE_STUB) {
		assert(t_current == tealet->tealet);
		assert(TEALET_PYOBJECT(t_current) == tealet);

		/* set the tstate from our own copy */
		PyTealetTstate_Restore(&tealet->tstate, tstate);
	} else {
		assert(tealet->state == STATE_NEW);
		/* set up the pointer in the tealet */
		tealet->tealet = t_current;
		TEALET_SET_PYOBJECT(t_current, tealet);
	}

	/* We only have borrowed references from the calling tealet.
	 * the argument to the function will get their own reference, but
	 * anything we need after the function we keep oru own references
	 * for, because when the function returns, the calling tealet
	 * may have exited and dropped the references we borrowed.
	 */
	Py_INCREF(func);
	Py_INCREF(tealet);
	
	/* clear frame and run the tealet function */
	tealet->state = STATE_RUN;
	result = PyObject_CallFunctionObjArgs(func, tealet, farg, NULL);
	
	/* return_to can be a tuple of tealet, arg */
	return_to = NULL;
	return_arg = NULL;
	if (result && PyTuple_Check(result)) {
		/* arg and return_to are borrowed refs */
		if (PyTuple_GET_SIZE(result)>0)
			return_to = (PyTealetObject*)PyTuple_GET_ITEM(result, 0);
		if (PyTuple_GET_SIZE(result)>1)
			return_arg = PyTuple_GET_ITEM(result, 1);
	} else
		return_to = (PyTealetObject*)result;
		
	/* perform sanity checks on the result */
	if (return_to) {
		/* it is ok to rock the GC boat here, because we will switch to
		 * main in case of error, and main is always around
		 */
	if (!PyTealet_Check((PyObject*)return_to)) {
		return_to = NULL;
		PyErr_SetString(PyExc_TypeError, "tealet object expected");
	} else if (return_to->state != STATE_RUN) {
		PyTealetModuleState *mstate = PyTealet_GetModuleStateOrError();
		if (!mstate) {
			Py_CLEAR(result);
			return NULL;
		}
		return_to = NULL;
		PyErr_SetString(mstate->state_error, "must be 'run'");
	} else if (CheckTarget(return_to, tealet))
		return_to = NULL;
	}
	if (!return_to) {
		Py_CLEAR(result);
		return_arg = NULL;
	}
	if (!return_arg)
		return_arg = Py_None;
	
	/* handle errors */
	if (!return_to) {
		PyErr_WriteUnraisable(func);
		/* must switch to main */
		return_to = GetMain(0);
		assert(return_to);
		result = (PyObject*)return_to;
		Py_INCREF(result);
	}
	/* now, the reference to return_to and return_arg are borrowed, kept alive
	 * by 'result', which may be the same as return_to.
	 */
	
	/* clear the old tealet */
	tealet->state = STATE_EXIT;
	tealet->tealet = NULL; /* will be auto-deleted on return */
	TEALET_SET_PYOBJECT(t_current, NULL);
	t_return = return_to->tealet;
	
	/* decref the objects after the switch */
	dustbin_push(t_return, func);
	dustbin_push(t_return, (PyObject*)tealet);
	dustbin_push(t_return, result);
	
	Py_INCREF(return_arg);

	/* Tealet is exiting permanently: clear active PyThreadState for the switch,
	 * then drop saved refs immediately so frame locals (including 'current')
	 * do not keep the Python tealet object alive until GC.
	 */
	PyTealetTstate_Save(&tealet->tstate, tstate);
	PyTealetTstate_Drop(&tealet->tstate, t_return);

	if (tealet_exit(t_return, (void*)return_arg, TEALET_EXIT_DELETE))
		tealet_exit(t_return->main, (void *)return_arg, TEALET_EXIT_DELETE);
	/* never reach here */
	return 0;
}

/* Wrapper functions for system malloc/free to match libtealet's allocator API. */
static void* tealet_malloc_wrapper(size_t size, void *context)
{
	(void)context;  /* unused */
	return malloc(size);
}

static void tealet_free_wrapper(void *ptr, void *context)
{
	(void)context;  /* unused */
	free(ptr);
}

/* return a borrowed reference to this thread's main tealet */
static PyTealetObject *GetMain(int create)
{
	return GetMainWithState(create, NULL);
}

/* return a borrowed reference to this thread's main tealet */
static PyTealetObject *GetMainWithState(int create, PyTealetModuleState *mstate)
{
	/* Get the thread's main tealet */
	PyTealetObject *t_main;
	if (!mstate) {
		mstate = PyTealet_GetModuleStateOrError();
		if (!mstate)
			return NULL;
	}
	t_main = (PyTealetObject*)PyThread_tss_get(&mstate->tls_key);
	if (!t_main && !create) {
		return NULL;
	}

	/* main tealet doesn't exist yet.  create it. */
	if (!t_main) {
		tealet_alloc_t talloc;
		tealet_t *tmain;
		PyTealetMainData *mdata;
		/* Use system malloc/free so valgrind can detect heap corruption */
		talloc.malloc_p = tealet_malloc_wrapper;
		talloc.free_p = tealet_free_wrapper;
		talloc.context = NULL;
		tmain = tealet_initialize(&talloc, sizeof(PyTealetExtra));
		if (!tmain) {
			PyErr_NoMemory();
			return NULL;
		}
		{
			const char *check_stack_env = getenv("PYTEALET_CHECK_STACK");
			if (check_stack_env && *check_stack_env && *check_stack_env != '0') {
				if (tealet_configure_check_stack(tmain, 0) < 0) {
					tealet_finalize(tmain);
					PyErr_SetString(PyExc_RuntimeError, "tealet_configure_check_stack failed");
					return NULL;
				}
			}
		}
		mdata = (PyTealetMainData*)PyMem_Malloc(sizeof(*mdata));
		if (!mdata) {
			tealet_finalize(tmain);
			PyErr_NoMemory();
			return NULL;
		}
		memset(mdata, 0, sizeof(*mdata));
		mdata->tid = PyThread_get_thread_ident();
		mdata->dustbin = PyList_New(DUSTBIN_PREALLOC);
		if (!mdata->dustbin) {
			tealet_finalize(tmain);
			PyMem_Free(mdata);
			PyErr_NoMemory();
			return NULL;
		}
		if (PyList_SetSlice(mdata->dustbin, 0, DUSTBIN_PREALLOC, NULL) < 0) {
			Py_DECREF(mdata->dustbin);
			tealet_finalize(tmain);
			PyMem_Free(mdata);
			return NULL;
		}
		*tealet_main_userpointer(tmain) = (void*)mdata;

		/* create the main tealet */
		t_main = (PyTealetObject*)pytealet_new(mstate->tealet_type, NULL, NULL);
		if (!t_main) {
			tealet_finalize(tmain);
			PyMem_Free(mdata);
			return NULL;
		}
		t_main->tealet = tmain;
		t_main->state = STATE_RUN;
		TEALET_SET_PYOBJECT(tmain, t_main); /* back link */
		if (PyThread_tss_set(&mstate->tls_key, (void*)t_main) != 0) {
			TEALET_SET_PYOBJECT(tmain, NULL);
			t_main->tealet = NULL;
			Py_DECREF(t_main);
			tealet_finalize(tmain);
			PyMem_Free(mdata);
			PyErr_SetString(PyExc_RuntimeError, "failed to set thread-local main tealet");
			return NULL;
		}
	}
	assert(t_main->tealet);
	assert(TEALET_IS_MAIN(t_main->tealet));
	assert(t_main->state == STATE_RUN);		
	return t_main;
}

/* return a borrowed ref to this threads current tealet */
static PyTealetObject *
GetCurrent(PyTealetObject *pytealet)
{
	/* if we are being passed no tealet, or it is a new tealet, 
	 * we must get the current main from the thread-local storage */
	if (!pytealet || !pytealet->tealet)
		pytealet = GetMain(1);
	if (!pytealet)
		return NULL;
	return TEALET_PYOBJECT(tealet_current(pytealet->tealet));
}

/* check if a target tealet is valid */
static int
CheckTarget(PyTealetObject *target, PyTealetObject *ref)
{
	if (!ref)
		ref = GetMain(1);
	if (!ref)
		return -1;
	if (!target->tealet)
		return 0; /* no tealet yet */
	if (ref->tealet->main != target->tealet->main) {
		PyTealetModuleState *mstate = PyTealet_GetModuleStateOrError();
		if (!mstate)
			return -1;
		PyErr_SetString(mstate->invalid_error, "foreign tealet");
		return -1;
	}
	return 0;
}

/******************************************
 * Module methods
 */


static PyObject *
module_current()
{
	PyTealetObject* current = GetCurrent(NULL);
	Py_XINCREF(current);
	return (PyObject*)current;
}

static PyObject *
module_main()
{
	PyTealetObject* main = GetMain(1);
	Py_XINCREF(main);
	return (PyObject*)main;
}

static PyObject *
hide_frame(PyObject *self, PyObject *_args)
{
	/* this function calls a method, clearing the frame.  This hides
	 * higher frames in the callstack
	 */
	PyObject *func, *args=NULL, *kwds=NULL;
	PyThreadState *tstate = PyThreadState_GET();
	PyFrameObject *f = tstate->frame;
	PyObject *result;
	if (!PyArg_ParseTuple(_args, "O|OO:hide_frame", &func, &args, &kwds))
		return NULL;
	if (!args) {
		PyObject *empty = PyTuple_New(0);
		if (!empty)
			return NULL;
		tstate->frame = NULL;
		result = PyObject_Call(func, empty, kwds);
		Py_DECREF(empty);
	} else {
		tstate->frame = NULL;
		result = PyObject_Call(func, args, kwds);
	}
	tstate->frame = f;
	return result;
}

static PyMethodDef module_methods[] = {
	{"current", (PyCFunction)module_current, METH_NOARGS, ""},
	{"main", (PyCFunction)module_main, METH_NOARGS, ""},
	{"hide_frame", (PyCFunction)hide_frame, METH_VARARGS, ""},
	{NULL, NULL, 0, NULL}  /* Sentinel */
};

static int
pytealet_module_exec(PyObject *m)
{
	PyTealetModuleState *mstate = (PyTealetModuleState*)PyModule_GetState(m);
	PyTealetObject *main;
	PyObject *type_obj;

	if (!mstate) {
		PyErr_SetString(PyExc_RuntimeError, "failed to get _tealet module state");
		return -1;
	}

	memset(&mstate->tls_key, 0, sizeof(mstate->tls_key));
	mstate->tealet_type = NULL;
	mstate->tealet_error = NULL;
	mstate->invalid_error = NULL;
	mstate->state_error = NULL;
	mstate->defunct_error = NULL;

	if (!PyThread_tss_is_created(&mstate->tls_key)) {
		if (PyThread_tss_create(&mstate->tls_key) != 0) {
			PyErr_SetString(PyExc_RuntimeError, "failed to create thread-local key");
			return -1;
		}
	}

	type_obj = PyType_FromModuleAndSpec(m, &pytealet_type_spec, NULL);
	if (!type_obj)
		return -1;
	mstate->tealet_type = (PyTypeObject*)type_obj;
	if (PyModule_AddObjectRef(m, "tealet", type_obj) < 0) {
		Py_DECREF(type_obj);
		return -1;
	}
	Py_DECREF(type_obj);

	main = GetMainWithState(1, mstate);
	if (!main)
		return -1;

	mstate->tealet_error = PyErr_NewException("_tealet.TealetError", NULL, NULL);
	if (!mstate->tealet_error)
		return -1;
	Py_INCREF(mstate->tealet_error);
	if (PyModule_AddObject(m, "TealetError", mstate->tealet_error) < 0)
		return -1;

	mstate->defunct_error = PyErr_NewException("_tealet.DefunctError", mstate->tealet_error, NULL);
	if (!mstate->defunct_error)
		return -1;
	Py_INCREF(mstate->defunct_error);
	if (PyModule_AddObject(m, "DefunctError", mstate->defunct_error) < 0)
		return -1;

	mstate->invalid_error = PyErr_NewException("_tealet.InvalidError", mstate->tealet_error, NULL);
	if (!mstate->invalid_error)
		return -1;
	Py_INCREF(mstate->invalid_error);
	if (PyModule_AddObject(m, "InvalidError", mstate->invalid_error) < 0)
		return -1;

	mstate->state_error = PyErr_NewException("_tealet.StateError", mstate->tealet_error, NULL);
	if (!mstate->state_error)
		return -1;
	Py_INCREF(mstate->state_error);
	if (PyModule_AddObject(m, "StateError", mstate->state_error) < 0)
		return -1;

	PyModule_AddIntMacro(m, STATE_NEW);
	PyModule_AddIntMacro(m, STATE_STUB);
	PyModule_AddIntMacro(m, STATE_RUN);
	PyModule_AddIntMacro(m, STATE_EXIT);

	return 0;
}

static int
pytealet_module_traverse(PyObject *m, visitproc visit, void *arg)
{
	PyTealetModuleState *mstate = (PyTealetModuleState*)PyModule_GetState(m);
	if (!mstate)
		return 0;
	Py_VISIT(mstate->tealet_error);
	Py_VISIT(mstate->invalid_error);
	Py_VISIT(mstate->state_error);
	Py_VISIT(mstate->defunct_error);
	return 0;
}

static int
pytealet_module_clear(PyObject *m)
{
	PyTealetModuleState *mstate = (PyTealetModuleState*)PyModule_GetState(m);
	if (!mstate)
		return 0;
	Py_CLEAR(mstate->tealet_error);
	Py_CLEAR(mstate->invalid_error);
	Py_CLEAR(mstate->state_error);
	Py_CLEAR(mstate->defunct_error);
	mstate->tealet_type = NULL;
	return 0;
}

static void
pytealet_module_free(void *m)
{
	PyTealetModuleState *mstate = (PyTealetModuleState*)PyModule_GetState((PyObject*)m);
	if (!mstate)
		return;
	if (PyThread_tss_is_created(&mstate->tls_key))
		PyThread_tss_delete(&mstate->tls_key);
}

/* CPython API uses void* in module slots; this conversion is intentional. */
#if defined(__GNUC__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
#endif
static PyModuleDef_Slot _tealet_module_slots[] = {
	{Py_mod_exec, pytealet_module_exec},
	{0, NULL}
};
#if defined(__GNUC__)
#pragma GCC diagnostic pop
#endif

static struct PyModuleDef _tealet_module = {
	PyModuleDef_HEAD_INIT,
	"_tealet",   /* name of module */
	NULL,        /* module documentation, may be NULL */
	sizeof(PyTealetModuleState),
	module_methods,
	_tealet_module_slots,
	pytealet_module_traverse,
	pytealet_module_clear,
	pytealet_module_free
};

PyMODINIT_FUNC
PyInit__tealet(void)
{
	return PyModuleDef_Init(&_tealet_module);
}

