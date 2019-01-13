from datetime import datetime
from time import sleep

from pytest import raises

from aiobreaker import CircuitBreaker, CircuitBreakerError
from aiobreaker.state import CircuitBreakerState
from aiobreaker.storage.memory import CircuitMemoryStorage
from test.util import func_exception, func_succeed, DummyException, func_succeed_counted


def test_successful_call(storage):
    """It should keep the circuit closed after a successful call."""

    breaker = CircuitBreaker(state_storage=storage)
    assert breaker.call(func_succeed)
    assert 0 == breaker.fail_counter
    assert CircuitBreakerState.CLOSED == breaker.current_state


def test_one_failed_call(storage):
    """It should keep the circuit closed after a few failures."""

    breaker = CircuitBreaker(state_storage=storage)

    with raises(DummyException):
        breaker.call(func_exception)

    assert 1 == breaker.fail_counter
    assert CircuitBreakerState.CLOSED == breaker.current_state


def test_one_successful_call_after_failed_call(storage):
    """It should keep the circuit closed after few mixed outcomes."""

    breaker = CircuitBreaker(state_storage=storage)

    with raises(DummyException):
        breaker.call(func_exception)

    assert 1 == breaker.fail_counter

    assert breaker.call(func_succeed)
    assert 0 == breaker.fail_counter
    assert CircuitBreakerState.CLOSED == breaker.current_state


def test_several_failed_calls(storage):
    """It should open the circuit after multiple failures."""

    breaker = CircuitBreaker(state_storage=storage, fail_max=3)

    with raises(DummyException):
        breaker.call(func_exception)

    with raises(DummyException):
        breaker.call(func_exception)

    # Circuit should open
    with raises(CircuitBreakerError):
        breaker.call(func_exception)

    assert breaker.fail_counter == 3
    assert breaker.current_state == CircuitBreakerState.OPEN


def test_failed_call_after_timeout(storage, delta):
    """It should half-open the circuit after timeout and close immediately on fail."""

    breaker = CircuitBreaker(fail_max=3, timeout_duration=delta, state_storage=storage)

    with raises(DummyException):
        breaker.call(func_exception)

    with raises(DummyException):
        breaker.call(func_exception)

    assert CircuitBreakerState.CLOSED == breaker.current_state

    # Circuit should open
    with raises(CircuitBreakerError):
        breaker.call(func_exception)

    assert 3 == breaker.fail_counter

    sleep(delta.total_seconds() * 2)

    # Circuit should open again
    with raises(CircuitBreakerError):
        breaker.call(func_exception)

    assert 4 == breaker.fail_counter
    assert CircuitBreakerState.OPEN == breaker.current_state


def test_successful_after_timeout(storage, delta):
    """It should close the circuit when a call succeeds after timeout."""

    breaker = CircuitBreaker(fail_max=3, timeout_duration=delta, state_storage=storage)
    func_succeed = func_succeed_counted()

    with raises(DummyException):
        breaker.call(func_exception)

    with raises(DummyException):
        breaker.call(func_exception)

    assert CircuitBreakerState.CLOSED == breaker.current_state

    # Circuit should open
    with raises(CircuitBreakerError):
        breaker.call(func_exception)

    assert CircuitBreakerState.OPEN == breaker.current_state

    with raises(CircuitBreakerError):
        breaker.call(func_succeed)

    assert 3 == breaker.fail_counter

    # Wait for timeout, at least a second since redis rounds to a second
    sleep(delta.total_seconds() * 2)

    # Circuit should close again
    assert breaker.call(func_succeed)
    assert 0 == breaker.fail_counter
    assert CircuitBreakerState.CLOSED == breaker.current_state
    assert 1 == func_succeed.call_count


def test_failed_call_when_half_open(storage):
    """It should open the circuit when a call fails in half-open state."""

    breaker = CircuitBreaker(state_storage=storage)

    breaker.half_open()
    assert 0 == breaker.fail_counter
    assert CircuitBreakerState.HALF_OPEN == breaker.current_state

    with raises(CircuitBreakerError):
        breaker.call(func_exception)

    assert 1 == breaker.fail_counter
    assert CircuitBreakerState.OPEN == breaker.current_state


def test_successful_call_when_half_open(storage):
    """It should close the circuit when a call succeeds in half-open state."""

    breaker = CircuitBreaker(state_storage=storage)

    breaker.half_open()
    assert 0 == breaker.fail_counter
    assert CircuitBreakerState.HALF_OPEN == breaker.current_state

    # Circuit should open
    assert breaker.call(func_succeed)
    assert 0 == breaker.fail_counter
    assert CircuitBreakerState.CLOSED == breaker.current_state


def test_state_opened_at_not_reset_during_creation():
    for state in CircuitBreakerState:
        storage = CircuitMemoryStorage(state)
        now = datetime.now()
        storage.opened_at = now

        breaker = CircuitBreaker(state_storage=storage)
        assert breaker.state.state == state
        assert storage.opened_at == now


def test_close(storage):
    """It should allow the circuit to be closed manually."""

    breaker = CircuitBreaker(fail_max=3, state_storage=storage)

    breaker.open()
    assert 0 == breaker.fail_counter
    assert CircuitBreakerState.OPEN == breaker.current_state

    breaker.close()
    assert 0 == breaker.fail_counter
    assert CircuitBreakerState.CLOSED == breaker.current_state


def test_generator(storage):
    """It should support generator values."""

    breaker = CircuitBreaker(state_storage=storage)

    @breaker
    def func_yield_succeed():
        """Docstring"""
        yield True

    @breaker
    def func_yield_exception():
        """Docstring"""
        x = yield True
        raise DummyException(x)

    s = func_yield_succeed()
    e = func_yield_exception()
    next(e)

    with raises(DummyException):
        e.send(True)

    assert 1 == breaker.fail_counter
    assert next(s)

    with raises(StopIteration):
        next(s)

    assert 0 == breaker.fail_counter
