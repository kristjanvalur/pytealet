import pytest
import math

import _tealet
import random
random.seed(0)

# Utility stuff for creating tealets
def tealet_new_descend(descend, func=None, arg=None, klass=_tealet.tealet, retarg=False):
    while descend > 0:
        return tealet_new_descend(descend-1, func, arg, klass=klass, retarg=retarg)
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
    dup = klass(stub)
    if func:
        r = dup.run(func, arg)
    else:
        r = None
    return (dup, r) if retarg else dup

the_stub=[None]
def stub_new3(func=None, arg=None, klass=_tealet.tealet, retarg=False):
    if (random.randint(0, 10) == 0):
        the_stub[0] = None
    if not the_stub[0]:
        the_stub[0] = tealet_new_descend(random.randint(0, 20), klass=klass)
    dup = klass(the_stub[0])
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
    return newarray(random.randint(0, len(newarray)-1))


class TestModule:
    def test_main(self):
        assert _tealet.main() == _tealet.current()

    def test_main2(self):
        assert _tealet.main() == _tealet.current().main()

    def test_main3(self):
        assert _tealet.main().state == _tealet.STATE_RUN


class TestTealetTraversalMethods:
    def test_methods_fail_on_new_tealet(self):
        t = _tealet.tealet()
        with pytest.raises(_tealet.StateError):
            t.current()
        with pytest.raises(_tealet.StateError):
            t.main()
        with pytest.raises(_tealet.StateError):
            t.previous()

    def test_current_main_previous_inside_running_tealet(self):
        seen = {}

        def run(current, arg):
            seen["self_is_current"] = (current.current() == current)
            seen["main"] = current.main()
            seen["previous"] = current.previous()
            return _tealet.main()

        _tealet.tealet().run(run, None)

        assert seen["self_is_current"] is True
        assert seen["main"] == _tealet.main()
        assert seen["previous"] == _tealet.main()

    @pytest.mark.skip(reason="Deferred-delete post-exit behavior is experimental; re-enable when PYTEALET_DEFER_DELETE is being exercised")
    def test_main_on_exited_tealet_depends_on_defer_delete_flag(self):
        def run_and_exit(current, arg):
            return _tealet.main()

        t = _tealet.tealet()
        t.run(run_and_exit, None)
        assert t.state == _tealet.STATE_EXIT

        if getattr(_tealet, "PYTEALET_DEFER_DELETE", 0):
            assert t.main() == _tealet.main()
        else:
            with pytest.raises(_tealet.StateError):
                t.main()

class TestSimple:
    def test_simple(self):
        status = [0]
        def run(current, arg):
            status[0] = 1
            return arg
        get_new()(run, _tealet.current())
        assert status[0] == 1

class TestStatus:
    def test_status_run(self):
        t = _tealet.current()
        assert t.main() == _tealet.main()
        assert t.state == _tealet.STATE_RUN

    @pytest.mark.stub
    def test_status_stub(self):
        stub = get_new()()
        status = [None]
        assert stub.state == _tealet.STATE_STUB
        def run(current, arg):
            status[0] = current.state
            return arg
        stub.run(run, _tealet.current())
        assert status[0] == _tealet.STATE_RUN

class TestSubclass:
    class sc(_tealet.tealet):
        dude = [0]
        def __repr__(self):
            return "<myrepr %r>"%super(TestSubclass.sc, self).__repr__()
        def __del__(self):
            self.dude[0] = 1
    def test_subclass(self):
        def foo(current, arg):
            arg.switch(current)
            return arg
        t = get_new()(foo, _tealet.current(), klass=self.sc)
        assert repr(t)[:7] == "<myrepr"
        assert self.sc.dude[0] == 0
        t.switch()
        assert self.sc.dude[0] == 0
        del t
        assert self.sc.dude[0] == 1

class TestSwitch:
    def test_switch(self):
        status = [0]
        t = [None, None]
        def t2(current, arg):
            assert current != _tealet.main()
            assert current != t[0]
            t[1] = current
            assert status[0] == 1
            status[0] = 2
            assert _tealet.current() == current
            t[0].switch()
            assert status[0] == 3
            status[0] = 4
            assert _tealet.current() == current
            t[0].switch()
            assert status[0] == 5
            status[0] = 6
            assert current == t[1]
            assert _tealet.current() == current
            t[1].switch() #noop
            assert status[0] == 6
            status[0] = 7
            assert _tealet.current() == current
            return _tealet.main()

        def t1(current, arg):
            assert current != _tealet.main()
            t[0] = current
            assert status[0] == 0
            status[0] = 1
            assert current == _tealet.current()
            get_new()(t2)
            assert status[0] == 2
            status[0] = 3
            assert current == _tealet.current()
            t[1].switch()
            assert status[0] == 4
            status[0] = 5
            assert current == _tealet.current()
            return t[1]

        get_new()(t1)
        assert status[0] == 7


    @pytest.mark.stub
    def test_switch_new(self):
        # 1 is high on the stack.  We then create 2 lower on the stack
        # the execution is : m 1 m 2 1 m 2 m */
        def new1(current, arg):
            # switch back to the creator
            arg.switch()
            # now we want to trample the stack
            stub = tealet_new_descend(50)
            del stub
            # back to main
            return _tealet.main()

        def new2(current, arg):
            # switch to tealet 1 to trample the stack
            arg.switch();
            # back to main
            return _tealet.main()

        tealet1 = get_new()(new1, _tealet.current())
        # the tealet is now running
        tealet2 = tealet_new_descend(4, new2, tealet1)

        assert tealet2.state == _tealet.STATE_RUN;
        tealet2.switch()

    @pytest.mark.stub
    def test_switch_arg(self):
        # 1 is high on the stack.  We then create 2 lower on the stack
        # the execution is : m 1 m 2 1 m 2 m */
        def new1(current, arg):
            # switch back to the creator
            r = arg.switch(2)
            assert r == 4
            # now we want to trample the stack
            stub = tealet_new_descend(50)
            del stub
            # back to main
            return _tealet.main(), 5

        def new2(current, arg):
            # switch to tealet 1 to trample the stack
            r = arg.switch(4);
            assert r == 6
            # back to main
            return _tealet.main(), 7

        tealet1, r = get_new()(new1, _tealet.current(), retarg=True)
        assert r == 2
        # the tealet is now running
        tealet2, r = tealet_new_descend(4, new2, tealet1, retarg=True)
        assert r == 5

        assert tealet2.state == _tealet.STATE_RUN;
        r = tealet2.switch(6)
        assert r == 7


class TestRandom1:
    max_status = 10000

    def randomRun(self, index):
        cur = _tealet.current()
        while True:
            i = random.randint(0, len(self.tealets))
            self.status += 1;
            if i == len(self.tealets):
                break
            prevstatus = self.status
            self.got_index = i
            if not self.tealets[i]:
                if self.status >= self.max_status:
                    break
                #print "new", i
                get_new()(self.randomTealet, i)
            else:
                #print 'switch', i
                d = self.tealets[i].switch()
                #assert d == math.sqrt(2344.2)
            assert self.status >= prevstatus
            assert _tealet.current() == cur
            assert self.tealets[index] == cur
            assert self.got_index == index
            if self.status >= self.max_status:
                break

    def randomTealet(self, current, index):
        i = self.got_index;
        assert _tealet.current() == current
        assert i == index
        assert i > 0 and i < len(self.tealets)
        assert self.tealets[i] == None
        self.tealets[i] = current
        self.randomRun(i)
        self.tealets[i] = None

        i = random.randint(0, len(self.tealets)-1)
        if not self.tealets[i]:
            assert self.tealets[0];
            i = 0
        self.got_index = i
        #print "ret", i
        return self.tealets[i]

    def test_random(self):
        self.tealets = [None]*127
        self.status = 0
        self.tealets[0] = _tealet.current()
        while self.status < self.max_status:
            self.randomRun(0)

        assert _tealet.current() == self.tealets[0]
        for i in range(1, len(self.tealets)):
            while self.tealets[i]:
                self.randomRun(0)


class TestRandom2:
    MAX_STATUS = 10000
    N_RUNS = 10
    MAX_DESCEND = 20
    ARRAYSIZE = 127

    def randomTealet(self, cur, index):
        assert _tealet.current() == cur
        assert index > 0 and index < len(self.tealets)
        assert self.tealets[index] == None
        self.tealets[index] = cur
        self.randomRun(index)
        self.tealets[index] = None
        return self.tealets[0] # switch to main

    def randomRun(self, index):
        assert self.tealets[index] == None or self.tealets[index] == _tealet.current()
        self.tealets[index] = _tealet.current()
        for i in range(self.N_RUNS):
            if self.randomDescend(index, random.randint(0, self.MAX_DESCEND+1)) == 0:
                break;
        self.tealets[index] = None

    def randomDescend(self, index, level):
        if level > 0:
            return self.randomDescend(index, level-1)
        # find target
        target = random.randint(0, len(self.tealets)-1)
        if self.status < self.MAX_STATUS:
            self.status += 1;
            if not self.tealets[target]:
                get_new()(self.randomTealet, target)
            else:
                self.tealets[target].switch()
            return 1
        else:
            # find a telet other than us to flush
            for j in range(len(self.tealets)):
                k = (j + target) % len(self.tealets)
                if k != index and self.tealets[k]:
                    self.status += 1;
                    self.tealets[k].switch()
                    return 1
            return 0

    def test_random(self):
        self.tealets = [None] * self.ARRAYSIZE
        self.status = 0
        self.tealets[0] = _tealet.current()

        while self.status < self.MAX_STATUS:
            self.randomRun(0);

        # drain the system
        self.tealets[0] = _tealet.current()
        while True:
            found = False
            for i, t in enumerate(self.tealets[1:]):
                if t:
                    self.status += 1
                    t.switch()
                    found = True
                    break
            if not found:
                break
        self.tealets[0] = None



# Tests can be run with: pytest tests/test_tealet.py
