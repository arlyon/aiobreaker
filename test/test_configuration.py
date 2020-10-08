from datetime import timedelta

from pytest import raises

from aiobreaker import CircuitBreaker
from aiobreaker.listener import CircuitBreakerListener
from aiobreaker.state import CircuitBreakerState
from aiobreaker.storage.memory import CircuitMemoryStorage
from test.util import func_succeed, DummyException


def test_default_state():
    """It should get initial state from state_storage."""
    for state in CircuitBreakerState:
        storage = CircuitMemoryStorage(state)
        breaker = CircuitBreaker(state_storage=storage)
        assert isinstance(breaker.state, state.value)
        assert breaker.state.state == state


def test_default_params():
    """It should define smart defaults."""
    breaker = CircuitBreaker()

    assert 0 == breaker.fail_counter
    assert timedelta(seconds=60) == breaker.timeout_duration
    assert 5 == breaker.fail_max
    assert CircuitBreakerState.CLOSED == breaker.current_state
    assert () == breaker.excluded_exceptions
    assert () == breaker.listeners
    assert 'memory' == breaker._state_storage.name


def test_new_with_custom_reset_timeout():
    """It should support a custom reset timeout value."""
    breaker = CircuitBreaker(timeout_duration=timedelta(seconds=30))

    assert 0 == breaker.fail_counter
    assert timedelta(seconds=30) == breaker.timeout_duration
    assert 5 == breaker.fail_max
    assert () == breaker.excluded_exceptions
    assert () == breaker.listeners
    assert 'memory' == breaker._state_storage.name


def test_new_with_custom_fail_max():
    """It should support a custom maximum number of failures."""
    breaker = CircuitBreaker(fail_max=10)
    assert 0 == breaker.fail_counter
    assert timedelta(seconds=60) == breaker.timeout_duration
    assert 10 == breaker.fail_max
    assert () == breaker.excluded_exceptions
    assert () == breaker.listeners
    assert 'memory' == breaker._state_storage.name


def test_new_with_custom_excluded_exceptions():
    """CircuitBreaker: it should support a custom list of excluded
    exceptions.
    """
    breaker = CircuitBreaker(exclude=[Exception])
    assert 0 == breaker.fail_counter
    assert timedelta(seconds=60) == breaker.timeout_duration
    assert 5 == breaker.fail_max
    assert (Exception,) == breaker.excluded_exceptions
    assert () == breaker.listeners
    assert 'memory' == breaker._state_storage.name


def test_fail_max_setter():
    """CircuitBreaker: it should allow the user to set a new value for
    'fail_max'.
    """
    breaker = CircuitBreaker()

    assert 5 == breaker.fail_max
    breaker.fail_max = 10
    assert 10 == breaker.fail_max


def test_reset_timeout_setter():
    """CircuitBreaker: it should allow the user to set a new value for
    'reset_timeout'.
    """
    breaker = CircuitBreaker()

    assert timedelta(seconds=60) == breaker.timeout_duration
    breaker.timeout_duration = timedelta(seconds=30)
    assert timedelta(seconds=30) == breaker.timeout_duration


def test_call_with_no_args():
    """    It should be able to invoke functions with no-args."""
    breaker = CircuitBreaker()
    assert breaker.call(func_succeed)


def test_call_with_args():
    """    It should be able to invoke functions with args."""

    def func(arg1, arg2):
        return arg1, arg2

    breaker = CircuitBreaker()

    assert (42, 'abc') == breaker.call(func, 42, 'abc')


def test_call_with_kwargs():
    """    It should be able to invoke functions with kwargs."""

    def func(**kwargs):
        return kwargs

    breaker = CircuitBreaker()

    kwargs = {'a': 1, 'b': 2}

    assert kwargs == breaker.call(func, **kwargs)


def test_add_listener():
    """    It should allow the user to add a listener at a later time."""
    breaker = CircuitBreaker()

    assert () == breaker.listeners

    first = CircuitBreakerListener()
    breaker.add_listener(first)
    assert (first,) == breaker.listeners

    second = CircuitBreakerListener()
    breaker.add_listener(second)
    assert (first, second) == breaker.listeners


def test_add_listeners():
    """    It should allow the user to add listeners at a later time."""
    breaker = CircuitBreaker()

    first, second = CircuitBreakerListener(), CircuitBreakerListener()
    breaker.add_listeners(first, second)
    assert (first, second) == breaker.listeners


def test_remove_listener():
    """    it should allow the user to remove a listener."""
    breaker = CircuitBreaker()

    first = CircuitBreakerListener()
    breaker.add_listener(first)
    assert (first,) == breaker.listeners

    breaker.remove_listener(first)
    assert () == breaker.listeners


def test_excluded_exceptions():
    """CircuitBreaker: it should ignore specific exceptions.
    """
    breaker = CircuitBreaker(
        exclude=[LookupError, lambda e: type(e) == DummyException and e.val == 3])

    def err_1(): raise LookupError()

    def err_2(): raise DummyException()

    def err_3(): raise KeyError()

    def err_4(): raise DummyException(val=3)

    # LookupError is not considered a system error
    with raises(LookupError):
        breaker.call(err_1)
    assert 0 == breaker.fail_counter

    with raises(DummyException):
        breaker.call(err_2)
    assert 1 == breaker.fail_counter

    # Should consider subclasses as well (KeyError is a subclass of
    # LookupError)
    with raises(KeyError):
        breaker.call(err_3)
    assert 0 == breaker.fail_counter

    # should filter based on functions as well
    with raises(DummyException):
        breaker.call(err_4)
    assert 0 == breaker.fail_counter


def test_add_excluded_exception():
    """    it should allow the user to exclude an exception at a later time."""
    breaker = CircuitBreaker()

    assert () == breaker.excluded_exceptions

    breaker.add_excluded_exception(NotImplementedError)
    assert (NotImplementedError,) == breaker.excluded_exceptions

    breaker.add_excluded_exception(Exception)
    assert (NotImplementedError, Exception) == breaker.excluded_exceptions


def test_add_excluded_exceptions():
    """    it should allow the user to exclude exceptions at a later time."""
    breaker = CircuitBreaker()

    breaker.add_excluded_exceptions(NotImplementedError, Exception)
    assert (NotImplementedError, Exception) == breaker.excluded_exceptions


def test_remove_excluded_exception():
    """It should allow the user to remove an excluded exception."""
    breaker = CircuitBreaker()

    breaker.add_excluded_exception(NotImplementedError)
    assert (NotImplementedError,) == breaker.excluded_exceptions

    breaker.remove_excluded_exception(NotImplementedError)
    assert () == breaker.excluded_exceptions


def test_decorator():
    """It should be a decorator."""

    breaker = CircuitBreaker()

    @breaker
    def suc():
        """Docstring"""
        pass

    @breaker
    def err():
        """Docstring"""
        raise DummyException()

    assert 'Docstring' == suc.__doc__
    assert 'Docstring' == err.__doc__
    assert 'suc' == suc.__name__
    assert 'err' == err.__name__

    assert 0 == breaker.fail_counter

    with raises(DummyException):
        err()

    assert 1 == breaker.fail_counter

    suc()
    assert 0 == breaker.fail_counter


def test_decorator_arguments():
    """It should accept arguments to the decorator."""

    breaker = CircuitBreaker()

    @breaker(ignore_on_call=True)
    def suc():
        """Docstring"""
        pass

    @breaker()
    def err():
        """Docstring"""
        raise DummyException()

    assert 0 == breaker.fail_counter

    with raises(DummyException):
        err()

    assert 1 == breaker.fail_counter

    suc()
    assert 0 == breaker.fail_counter


def test_decorator_positional_arguments():
    """It should throw an error when positional arguments are supplied to the decorator."""
    breaker = CircuitBreaker()

    with raises(TypeError):
        @breaker(True)
        def suc():
            """Docstring"""
            pass


def test_double_count():
    """It should not trigger twice if you call CircuitBreaker#call on a decorated function."""

    breaker = CircuitBreaker()

    @breaker
    def err():
        """Docstring"""
        raise DummyException()

    assert 0 == breaker.fail_counter

    with raises(DummyException):
        breaker.call(err)

    assert 1 == breaker.fail_counter


def test_name():
    """It should allow an optional name to be set and retrieved."""
    name = "test_breaker"
    breaker = CircuitBreaker(name=name)
    assert breaker.name == name

    name = "breaker_test"
    breaker.name = name
    assert breaker.name == name
