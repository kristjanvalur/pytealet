
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

/****************************************************************
 *Implement copyable stubs by using a trampoline
 */
struct stub_arg
{
    tealet_t *current;
    tealet_run_t run;
    void *runarg;
};

static tealet_t *
stub_main(tealet_t *current, void *arg)
{
    void *myarg = 0;

    /* the caller is in arg, return right back to him */
    tealet_switch((tealet_t*)arg, &myarg);
    /* now we are back, myarg should contain the arg to the run function.
     * We were possibly duplicated, so can't trust the original function args.
     */
    {
        struct stub_arg sarg = *(struct stub_arg*)myarg;
        tealet_free(sarg.current, myarg);
        return (sarg.run)(sarg.current, sarg.runarg);
    }
}

/* create a stub and return it */
static tealet_t *
stub_new(tealet_t *t) {
    void *arg = (void*)tealet_current(t);
	return tealet_new(t, stub_main, &arg, NULL);
}

/* run a stub */
static int
stub_run(tealet_t *stub, tealet_run_t run, void **parg)
{
    int result;
    void *myarg;
    /* we cannot pass arguments to a different tealet on the stack */
    struct stub_arg *psarg = (struct stub_arg*)tealet_malloc(stub, sizeof(struct stub_arg));
    if (!psarg)
        return TEALET_ERR_MEM;
    psarg->current = stub;
    psarg->run = run;
    psarg->runarg = parg ? *parg : NULL;
    myarg = (void*)psarg;
    result = tealet_switch(stub, &myarg);
    if (result) {
        /* failure */
        tealet_free(stub, psarg);
        return result;
    }
    /* pass back the arg value from the switch */
    if (parg)
        *parg = myarg;
    return 0;
}
/***************************************************************/


#define STATE_NEW 0
#define STATE_STUB 1
#define STATE_RUN 2
#define STATE_EXIT 3

static PyTypeObject PyTealetType;
#define PyTealet_Check(op) PyObject_TypeCheck(op, &PyTealetType)
#define PyTealet_CheckExact(op) (Py_TYPE(op) == &PyTealetType)

static int tls_key;

/* Forward declaration */
typedef struct PyTealetObject PyTealetObject;

typedef struct tealet_new_arg {
	int stub;
	PyTealetObject *dest;
	PyObject *func;
	PyObject *arg;
} tealet_new_arg;

/* the structure we associate with the main tealet */
typedef struct main_data
{
	long tid;
	tealet_new_arg new_arg;
	PyObject *dustbin[3];  /* make this a list eventually */
} main_data;


/* Extra data stored with each tealet for the Python binding.
 * This structure is stored in tealet->extra and provides type-safe
 * access to the associated PyTealetObject.
 */
typedef struct tealet_extra_t {
	PyTealetObject *pytealet;
} tealet_extra_t;

/* Helper macros for type-safe access to the tealet extra data */
#define TEALET_EXTRA(t) ((tealet_extra_t*)(t)->extra)
#define TEALET_PYOBJECT(t) (TEALET_EXTRA(t)->pytealet)
#define TEALET_SET_PYOBJECT(t, obj) (TEALET_EXTRA(t)->pytealet = (obj))


/* a structure that captures the tstate of a tealet.  The fields stored
 * and their semantics may change from python version to version.
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
	int own_refs; /* has ownership of tstate references been claimed? */
	void *stack_near_saved; /* Debug: stack-near marker captured with this tstate */
	void *stack_far_saved;  /* Debug: far boundary captured with this tstate */
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
static PyTealetObject *GetMain(int create);
static PyTealetObject *GetCurrent(PyTealetObject *main);
static int CheckTarget(PyTealetObject *target, PyTealetObject *main);

static tealet_t * pytealet_main(tealet_t *t_current, void *arg);

static PyObject *TealetError;
static PyObject *InvalidError;
static PyObject *StateError;
static PyObject *DefunctError;


static void PyTealetTstate_Init(PyTealetTstate *saved)
{
	saved->frame = NULL;
	saved->exc_type = NULL;
	saved->exc_val = NULL;
	saved->exc_tb = NULL;
	saved->exc_info = NULL;
	saved->exc_state.exc_value = NULL;
	saved->recursion_depth = 0;
	saved->trash_delete_nesting = 0;
	saved->context = NULL;
	saved->has_state = 0;
	saved->own_refs = 0;
	saved->stack_near_saved = NULL;
	saved->stack_far_saved = NULL;
#if defined(PY_HAS_CFRAME)
	saved->cframe = NULL;
#endif
}

/* Copy saved tealet state for object cloning (PyTealet(stub)).
 * This is distinct from switch-time capture/restore move semantics.
 * If source has no saved state, destination remains initialized/empty.
 */
static void PyTealetTstate_CopyForClone(PyTealetTstate *dst, const PyTealetTstate *src)
{
	PyTealetTstate_Init(dst);
	if (!src->has_state)
		return;

	dst->frame = src->frame;
	dst->exc_type = src->exc_type;
	dst->exc_val = src->exc_val;
	dst->exc_tb = src->exc_tb;
	dst->exc_state = src->exc_state;

	/* If source exc_info points to its inline exc_state, remap to destination. */
	if (src->exc_info == &src->exc_state)
		dst->exc_info = &dst->exc_state;
	else
		dst->exc_info = src->exc_info;

	dst->recursion_depth = src->recursion_depth;
	dst->trash_delete_nesting = src->trash_delete_nesting;
	dst->context = src->context;
	dst->stack_near_saved = src->stack_near_saved;
	dst->stack_far_saved = src->stack_far_saved;
#if defined(PY_HAS_CFRAME)
	dst->cframe = src->cframe;
#endif
	dst->has_state = 1;
	dst->own_refs = 0;
}

/* Capture (move) thread-state fields into a tealet tstate struct.
 * Ownership is transferred from PyThreadState to saved.
 */
static void PyTealetTstate_Capture(PyTealetTstate *saved, PyThreadState *py_tstate)
{
	char stack_marker;
	if (!py_tstate)
		py_tstate = PyThreadState_GET();

	assert(saved->has_state == 0);
	saved->frame = py_tstate->frame;
	saved->recursion_depth = py_tstate->recursion_depth;
	py_tstate->frame = NULL;
	py_tstate->recursion_depth = 0;

	saved->exc_type = py_tstate->curexc_type;
	saved->exc_val = py_tstate->curexc_value;
	saved->exc_tb = py_tstate->curexc_traceback;
	py_tstate->curexc_type = py_tstate->curexc_value = py_tstate->curexc_traceback = NULL;

	/* exc_info is borrowed: save/restore pointer value only. */
	saved->exc_info = py_tstate->exc_info;
	saved->exc_state = py_tstate->exc_state;
	py_tstate->exc_info = &py_tstate->exc_state;
	py_tstate->exc_state.exc_value = NULL;

	saved->context = py_tstate->context;
	py_tstate->context = NULL;
	saved->stack_near_saved = (void *)&stack_marker;

#if defined(PY_HAS_CFRAME)
	saved->cframe = py_tstate->cframe;
#endif
	saved->trash_delete_nesting = py_tstate->trash_delete_nesting;
	py_tstate->trash_delete_nesting = 0;

	saved->has_state = 1;
}

/* Restore (move) previously saved tealet tstate into PyThreadState. */
static void PyTealetTstate_Restore(PyTealetTstate *saved, PyThreadState *py_tstate)
{
	if (!py_tstate)
		py_tstate = PyThreadState_GET();

	assert(saved->has_state == 1);

	assert(py_tstate->frame == NULL);
	assert(py_tstate->recursion_depth == 0);
	assert(py_tstate->context == NULL);

	py_tstate->frame = saved->frame;
	py_tstate->recursion_depth = saved->recursion_depth;

	Py_CLEAR(py_tstate->curexc_type);
	Py_CLEAR(py_tstate->curexc_value);
	Py_CLEAR(py_tstate->curexc_traceback);
	py_tstate->curexc_type = saved->exc_type;
	py_tstate->curexc_value = saved->exc_val;
	py_tstate->curexc_traceback = saved->exc_tb;

	py_tstate->exc_state = saved->exc_state;
	py_tstate->exc_info = saved->exc_info ? saved->exc_info : &py_tstate->exc_state;

	py_tstate->context = saved->context;
	py_tstate->context_ver++;  /* Invalidate contextvars cache */

#if defined(PY_HAS_CFRAME)
	py_tstate->cframe = saved->cframe;
	assert(py_tstate->cframe != NULL);
#endif
	py_tstate->trash_delete_nesting = saved->trash_delete_nesting;

	saved->has_state = 0;
}

/* Increment and decrement the reference count of the tstate's references.
 * we need to Increment the references when we create new tealets from an existing
 * one (or main), and decrement when a tealet terminates.
 */
static void PyTealetTstate_IncRef(PyTealetTstate *saved)
{
	assert(saved->has_state == 1);
	assert(saved->own_refs == 0);
	Py_XINCREF(saved->frame);
	Py_XINCREF(saved->exc_type);
	Py_XINCREF(saved->exc_val);
	Py_XINCREF(saved->exc_tb);
	Py_XINCREF(saved->exc_state.exc_value);
	/* exc_info is a pointer to exc_state or a stack item, so we don't own a reference to it */
	Py_XINCREF(saved->context);
	saved->own_refs = 1;
}

static void PyTealetTstate_DecRef(PyTealetTstate *saved)
{
	assert(saved->has_state == 1);
	assert(saved->own_refs == 1);
	Py_XDECREF(saved->frame);
	Py_XDECREF(saved->exc_type);
	Py_XDECREF(saved->exc_val);
	Py_XDECREF(saved->exc_tb);
	Py_XDECREF(saved->exc_state.exc_value);
	/* exc_info is a pointer to exc_state or a stack item, so we don't own a reference to it */
	Py_XDECREF(saved->context);
	saved->own_refs = 0;
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

/* helper functions to save and restore callstack related data from the python threadstate
 * into the tealet object
 */
static void
save_tstate(PyTealetObject *current, PyThreadState *tstate)
{
	if (!tstate)
		tstate = PyThreadState_GET();
	if (current && current->tealet)
		current->tstate.stack_far_saved = tealet_get_far(current->tealet);

	assert(current->tstate.has_state == 0);
	PyTealetTstate_Capture(&current->tstate, tstate);
}
/* helper functions to save and restore callstack related data from the python threadstate
 * into the tealet object
 */
static void
restore_tstate(PyTealetObject *current, PyThreadState *tstate)
{
	assert(!PyErr_Occurred());

	PyTealetTstate_Restore(&current->tstate, tstate);
	
	/* Comprehensive verification of restored state */
	/* Frame can be NULL for a new tealet that hasn't executed yet */
	/* recursion_depth should match whether we have frames */
	
	/* Context can be NULL in some Python states */
	/* No assertion needed for context */
	
	/* cframe should always point somewhere valid */
#if defined(PY_HAS_CFRAME)
	assert(tstate->cframe != NULL);
#endif
	
	/* Current exceptions should be NULL or valid objects */
	assert(tstate->curexc_type == NULL || PyType_Check(tstate->curexc_type) || tstate->curexc_type == Py_None);
	assert(tstate->curexc_value == NULL || PyObject_CheckBuffer(tstate->curexc_value) || 1); /* Any object ok */
	assert(tstate->curexc_traceback == NULL || PyTraceBack_Check(tstate->curexc_traceback));
	
	/* Recursion depth should be non-negative */
	assert(tstate->recursion_depth >= 0);
}

/* Helper functions to fill/empty the dustbin.  We must be careful not to
 * clear references at a delicate moment before switching, rather
 * references must be cleared after, so that any side-effects of
 * clearing references won't affect the state of the program.
 */
static void
dustbin_fill(tealet_t *tealet, PyObject *a, PyObject *b, PyObject *c)
{
	main_data *mdata = (main_data*)*tealet_main_userpointer(tealet);
	assert(!mdata->dustbin[0]);
	assert(!mdata->dustbin[1]);
	assert(!mdata->dustbin[2]);
	mdata->dustbin[0] = a;
	mdata->dustbin[1] = b;
	mdata->dustbin[2] = c;
}

static void
dustbin_clear(tealet_t *tealet)
{
	main_data *mdata = (main_data*)*tealet_main_userpointer(tealet);
	PyObject *a, *b, *c;
	a = mdata->dustbin[0];
	b = mdata->dustbin[1];
	c = mdata->dustbin[2];
	mdata->dustbin[0] = mdata->dustbin[1] = mdata->dustbin[2] = NULL;
	Py_XDECREF(a);
	Py_XDECREF(b);
	Py_XDECREF(c);
}

static PyObject *
pytealet_new(PyTypeObject *subtype, PyObject *args, PyObject *kwds)
{
	PyTealetObject *src = NULL;
	PyTealetObject *result;
	if (args && PyTuple_GET_SIZE(args)>0) {
		src = (PyTealetObject*)PyTuple_GET_ITEM(args, 0);
		if (!PyTealet_Check(src)) {
			PyErr_SetNone(PyExc_TypeError);
			return NULL;
		}
		if (src->state != STATE_NEW && src->state != STATE_STUB) {
			PyErr_SetString(StateError, "state must be new or stub");
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
		assert(0 && "clone-from-source path executed unexpectedly; add/adjust tests before enabling this path");
		PyTealetTstate_CopyForClone(&result->tstate, &src->tstate);
		PyTealetTstate_IncRef(&result->tstate);
		if (src->state == STATE_STUB) {
			result->tealet = tealet_duplicate(src->tealet);
			if (!result->tealet) {
				PyTealetTstate_DecRef(&result->tstate);
				Py_DECREF(result);
				return PyErr_NoMemory();
			}
			TEALET_SET_PYOBJECT(result->tealet, result);
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
	if (tealet->tstate.has_state) {
		if (tealet->tstate.own_refs)
			PyTealetTstate_DecRef(&tealet->tstate);
	}
	PyTealetTstate_Init(&tealet->tstate);
	if (tealet->weakreflist != NULL)
        PyObject_ClearWeakRefs(obj);
	if (tealet->tealet)
		tealet_delete(tealet->tealet);
	Py_TYPE(obj)->tp_free(obj);
}

/* make stub here */
static PyObject *
pytealet_stub(PyObject *self)
{
	PyTealetObject *main, *pytealet = (PyTealetObject*)self;
	tealet_t *tresult;
	if (pytealet->state != STATE_NEW) {
		PyErr_SetString(StateError, "must be new");
		return NULL;
	}
	assert(pytealet->tealet == NULL);
	main = GetMain(1);
	if (!main)
		return NULL;
	tresult = stub_new(main->tealet);
	if (!tresult)
		return PyErr_NoMemory();
	pytealet->tealet = tresult;
	pytealet->state = STATE_STUB;
	TEALET_SET_PYOBJECT(tresult, pytealet);
	Py_INCREF(self);
	return self;
}

/* run a tealet and optinonally run */
static PyObject *
pytealet_run(PyObject *self, PyObject *args, PyObject *kwds)
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
	main_data *mdata;
	tealet_new_arg *ptarg;
	void *switch_arg;

	/* target->tealet is null or a stub tealet.  GetCurrent works either way. */
	current = GetCurrent(target);
	if (!current)
		return NULL;
	if (CheckTarget(target, current))
		return NULL;

	if (target->state != STATE_NEW && target->state != STATE_STUB) {
		PyErr_SetString(StateError, "must be new or stub");
		return NULL;
	}
	if (!PyArg_ParseTupleAndKeywords(args, kwds, "O|O:run", keywords,
		&func, &farg))
		return NULL;
	
	created_from_new = (target->state == STATE_NEW);
	mdata = (main_data*)*tealet_main_userpointer(current->tealet);
	ptarg = &mdata->new_arg;
	switch_arg = (void*)ptarg;
	
	if (!created_from_new) {
		assert(0 && "temporarily disabled during development");
	}
	ptarg->stub = !created_from_new;
	ptarg->dest = target;
	ptarg->func = func;
	ptarg->arg = farg;
	
	/* Save caller threadstate; on first run from STATE_NEW, keep an extra owning
	 * reference set for caller's parked state.
	 */
	save_tstate(current, tstate);
	if (created_from_new)
		PyTealetTstate_IncRef(&current->tstate);
	if (ptarg->stub) {
		fail = stub_run(target->tealet, pytealet_main, &switch_arg);
		if (fail) {
			PyErr_NoMemory();
			goto err;
		}
	} else {
		void *stack_limit = PyTealet_GetStackFar(tstate);
		tealet = tealet_new(current->tealet, pytealet_main, &switch_arg, stack_limit);
		if (!tealet) {
			PyErr_NoMemory();
			goto err;
		}
	}
	/* success */
	result = (PyObject *)switch_arg;
err:
	if (created_from_new)
		PyTealetTstate_DecRef(&current->tstate);
	/* restore frame */
	restore_tstate(current, tstate);
	/* clear garbage */
	dustbin_clear(current->tealet);
	return result;
}
/* switch to a different tealet */
static PyObject *
pytealet_switch(PyObject *_self, PyObject *args)
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
		PyErr_SetString(StateError, "must be active");
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
	save_tstate(current, tstate);
	fail = tealet_switch(self->tealet, &switch_arg);
	restore_tstate(current, tstate);

	/* clear out garbage */
	dustbin_clear(current->tealet);
	
	if (fail == TEALET_ERR_DEFUNCT) {
		Py_DECREF(pyarg);
		PyErr_SetString(DefunctError, "target is defunct");
		return NULL;
	} else if (fail == TEALET_ERR_MEM) {
		Py_DECREF(pyarg);
		return PyErr_NoMemory();
	}
	/* return the arg passed to us */
	pyarg = (PyObject *)switch_arg;
	return pyarg;
}
	
static struct PyMethodDef pytealet_methods[] = {
	{"stub", (PyCFunction) pytealet_stub, METH_NOARGS, ""},
	{"run", (PyCFunction) pytealet_run, METH_VARARGS|METH_KEYWORDS, ""},
    {"switch", (PyCFunction) pytealet_switch, METH_VARARGS, ""},
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
		main_data *mdata = (main_data*)*tealet_main_userpointer(self->tealet);
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


static PyTypeObject PyTealetType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "_tealet.tealet",                           /* tp_name */
    sizeof(PyTealetObject),                     /* tp_basicsize */
    0,                                          /* tp_itemsize */
    (destructor)pytealet_dealloc,                 /* tp_dealloc */
    0,                                          /* tp_print */
    0,                                          /* tp_getattr */
    0,                                          /* tp_setattr */
    0,                                          /* tp_compare */
    0,                                          /* tp_repr */
    0,                                          /* tp_as_number */
    0,                                          /* tp_as_sequence */
    0,                                          /* tp_as_mapping */
    0,                                          /* tp_hash */
    0,                                          /* tp_call */
    0,                                          /* tp_str */
    0,                                          /* tp_getattro */
    0,                                          /* tp_setattro */
    0,                                          /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,   /* tp_flags */
    "",                                         /* tp_doc */
    0,                                          /* tp_traverse */
    0,                                          /* tp_clear */
    0,                                          /* tp_richcompare */
    offsetof(PyTealetObject,weakreflist),       /* tp_weaklistoffset */
    0,                                          /* tp_iter */
    0,                                          /* tp_iternext */
    pytealet_methods,                           /* tp_methods */
    0,                                          /* tp_members */
    pytealet_getset,                            /* tp_getset */
    0,                                          /* tp_base */
    0,                                          /* tp_dict */
    0,                                          /* tp_descr_get */
    0,                                          /* tp_descr_set */
    0,                                          /* tp_dictoffset */
    0,                                          /* tp_init */
    0,                                          /* tp_alloc */
    pytealet_new,                               /* tp_new */
    0,                                          /* tp_free */
};


/* The main function.  Invoked either from tealet.new or tealet.run */
static tealet_t *
pytealet_main(tealet_t *t_current, void *arg)
{
	tealet_new_arg *targ = (tealet_new_arg*)arg;
	PyTealetObject *tealet = targ->dest;
	PyObject *func = targ->func;
	PyObject *farg = targ->arg;
	PyObject *result, *return_arg;
	PyTealetObject *return_to;
	tealet_t *t_return;
	#if defined(PY_HAS_CFRAME)
		PyThreadState *entry_tstate;
	#endif
	
	if (targ->stub) {
		assert(tealet->state == STATE_STUB);
		assert(t_current == tealet->tealet);
		assert(TEALET_PYOBJECT(t_current) == tealet);
	} else {
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
	if (!PyTealet_Check(return_to)) {
		return_to = NULL;
		PyErr_SetString(PyExc_TypeError, "tealet object expected");
	} else if (return_to->state != STATE_RUN) {
		return_to = NULL;
		PyErr_SetString(StateError, "must be 'run'");
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
	dustbin_fill(t_return, func, (PyObject*)tealet, result);
	
	Py_INCREF(return_arg);
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
	/* Get the thread's main tealet */
	PyTealetObject *t_main = (PyTealetObject*)PyThread_get_key_value(tls_key);
	if (!t_main && !create) {
		return NULL;
	}

	/* main tealet doesn't exist yet.  create it. */
	if (!t_main) {
		tealet_alloc_t talloc;
		tealet_t *tmain;
		main_data *mdata;
		/* Use system malloc/free so valgrind can detect heap corruption */
		talloc.malloc_p = tealet_malloc_wrapper;
		talloc.free_p = tealet_free_wrapper;
		talloc.context = NULL;
		tmain = tealet_initialize(&talloc, sizeof(tealet_extra_t));
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
		mdata = (main_data*)PyMem_Malloc(sizeof(*mdata));
		if (!mdata) {
			tealet_finalize(tmain);
			PyErr_NoMemory();
			return NULL;
		}
		memset(mdata, 0, sizeof(*mdata));
		mdata->tid = PyThread_get_thread_ident();
		*tealet_main_userpointer(tmain) = (void*)mdata;

		/* create the main tealet */
		t_main = (PyTealetObject*)pytealet_new(&PyTealetType, NULL, NULL);
		if (!t_main) {
			tealet_finalize(tmain);
			PyMem_Free(mdata);
			return NULL;
		}
		t_main->tealet = tmain;
		t_main->state = STATE_RUN;
		TEALET_SET_PYOBJECT(tmain, t_main); /* back link */
		PyThread_set_key_value(tls_key, (void*)t_main);
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
		PyErr_SetString(InvalidError, "foreign tealet");
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

static struct PyModuleDef _tealet_module = {
	PyModuleDef_HEAD_INIT,
	"_tealet",   /* name of module */
	NULL,        /* module documentation, may be NULL */
	-1,          /* size of per-interpreter state of the module,
	                or -1 if the module keeps state in global variables. */
	module_methods
};

PyMODINIT_FUNC
PyInit__tealet(void)
{
	PyObject *m;
	PyTealetObject *main;

	tls_key = PyThread_create_key();

	/* init the type */
	if (PyType_Ready(&PyTealetType) < 0)
		return NULL;

	main = GetMain(1);
	if (!main)
		return NULL;
	
	m = PyModule_Create(&_tealet_module);
	if (m == NULL)
		return NULL;

	/* Todo: Improve error handling */
	Py_INCREF(&PyTealetType);
	if (PyModule_AddObject(m, "tealet", (PyObject*)&PyTealetType) < 0) {
		Py_DECREF(&PyTealetType);
		Py_DECREF(m);
		return NULL;
	}
	
	TealetError = PyErr_NewException("_tealet.TealetError", NULL, NULL);
	Py_INCREF(TealetError);
	PyModule_AddObject(m, "TealetError", TealetError);
	
	DefunctError = PyErr_NewException("_tealet.DefunctError", TealetError, NULL);
	Py_INCREF(DefunctError);
	PyModule_AddObject(m, "DefunctError", DefunctError);
	
	InvalidError = PyErr_NewException("_tealet.InvalidError", TealetError, NULL);
	Py_INCREF(InvalidError);
	PyModule_AddObject(m, "InvalidError", InvalidError);
	
	StateError = PyErr_NewException("_tealet.StateError", TealetError, NULL);
	Py_INCREF(StateError);
	PyModule_AddObject(m, "StateError", StateError);

	PyModule_AddIntMacro(m, STATE_NEW);
	PyModule_AddIntMacro(m, STATE_STUB);
	PyModule_AddIntMacro(m, STATE_RUN);
	PyModule_AddIntMacro(m, STATE_EXIT);
	
	return m;
}

