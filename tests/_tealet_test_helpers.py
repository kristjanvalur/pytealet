import random

import _tealet


random.seed(0)


def join_thread_or_fail(th, timeout=1.0):
    th.join(timeout=timeout)
    assert not th.is_alive(), "worker thread did not terminate in time"


# Utility stuff for creating tealets

def tealet_new_descend(descend, func=None, arg=None, klass=_tealet.tealet, retarg=False):
    while descend > 0:
        return tealet_new_descend(descend - 1, func, arg, klass=klass, retarg=retarg)
    t = klass()
    if func:
        r = t.run(func, arg)
    else:
        t.stub()
        r = None
    return (t, r) if retarg else t


def tealet_new_rnd(func=None, arg=None, klass=_tealet.tealet, retarg=False):
    return tealet_new_descend(random.randint(0, 20), func, arg, klass, retarg)


def stub_new(func=None, arg=None, klass=_tealet.tealet, retarg=False):
    stub = tealet_new_descend(random.randint(0, 20), klass=klass, retarg=False)
    if func:
        r = stub.run(func, arg)
    else:
        r = None
    return (stub, r) if retarg else stub


def stub_new2(func=None, arg=None, klass=_tealet.tealet, retarg=False):
    stub = tealet_new_descend(random.randint(0, 20), klass=klass, retarg=False)
    dup = stub.duplicate()
    if func:
        r = dup.run(func, arg)
    else:
        r = None
    return (dup, r) if retarg else dup


the_stub = [None]


def stub_new3(func=None, arg=None, klass=_tealet.tealet, retarg=False):
    if random.randint(0, 10) == 0:
        the_stub[0] = None
    if not the_stub[0]:
        the_stub[0] = tealet_new_descend(random.randint(0, 20), klass=klass)
    dup = the_stub[0].duplicate()
    if func:
        r = dup.run(func, arg)
    else:
        r = None
    return (dup, r) if retarg else dup


newmode = 0
newarray = [tealet_new_rnd, stub_new, stub_new2, stub_new3]


def get_new():
    if newmode >= 0:
        return newarray[newmode]
    return newarray(random.randint(0, len(newarray) - 1))
