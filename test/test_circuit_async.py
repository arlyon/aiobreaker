import asyncio
from asyncio import sleep

from pytest import mark, raises

from aiobreaker import CircuitBreaker, CircuitBreakerError
from aiobreaker.state import CircuitBreakerState
from test.util import func_succeed_async, DummyException, func_exception_async, func_succeed_counted_async

pytestmark = mark.asyncio


async def test_successful_call_async(storage):
    """
    It should keep the circuit closed after a successful call.
    """
    breaker = CircuitBreaker(state_storage=storage)
    assert await breaker.call_async(func_succeed_async)
    assert 0 == breaker.fail_counter
    assert CircuitBreakerState.CLOSED == breaker.current_state


async def test_one_failed_call(storage):
    """It should keep the circuit closed after a few failures."""

    breaker = CircuitBreaker(state_storage=storage)

    with raises(DummyException):
        await breaker.call_async(func_exception_async)

    assert 1 == breaker.fail_counter
    assert CircuitBreakerState.CLOSED == breaker.current_state


async def test_one_successful_call_after_failed_call(storage):
    """It should keep the circuit closed after few mixed outcomes."""

    breaker = CircuitBreaker(state_storage=storage)

    with raises(DummyException):
        await breaker.call_async(func_exception_async)

    assert 1 == breaker.fail_counter

    assert await breaker.call_async(func_succeed_async)
    assert 0 == breaker.fail_counter
    assert CircuitBreakerState.CLOSED == breaker.current_state


async def test_several_failed_calls(storage):
    """It should open the circuit after multiple failures."""

    breaker = CircuitBreaker(state_storage=storage, fail_max=3)

    with raises(DummyException):
        await breaker.call_async(func_exception_async)

    with raises(DummyException):
        await breaker.call_async(func_exception_async)

    # Circuit should open
    with raises(CircuitBreakerError):
        await breaker.call_async(func_exception_async)

    assert breaker.fail_counter == 3
    assert breaker.current_state == CircuitBreakerState.OPEN


async def test_failed_call_after_timeout(storage, delta):
    """It should half-open the circuit after timeout and close immediately on fail."""

    breaker = CircuitBreaker(fail_max=3, timeout_duration=delta, state_storage=storage)

    with raises(DummyException):
        await breaker.call_async(func_exception_async)

    with raises(DummyException):
        await breaker.call_async(func_exception_async)

    assert CircuitBreakerState.CLOSED == breaker.current_state

    # Circuit should open
    with raises(CircuitBreakerError):
        await breaker.call_async(func_exception_async)

    assert 3 == breaker.fail_counter

    await sleep(delta.total_seconds() * 2)

    # Circuit should open again
    with raises(CircuitBreakerError):
        await breaker.call_async(func_exception_async)

    assert 4 == breaker.fail_counter
    assert CircuitBreakerState.OPEN == breaker.current_state


async def test_successful_after_timeout(storage, delta):
    """It should close the circuit when a call succeeds after timeout."""

    breaker = CircuitBreaker(fail_max=3, timeout_duration=delta, state_storage=storage)
    func_succeed_async = func_succeed_counted_async()

    with raises(DummyException):
        await breaker.call_async(func_exception_async)

    with raises(DummyException):
        await breaker.call_async(func_exception_async)

    assert CircuitBreakerState.CLOSED == breaker.current_state

    # Circuit should open
    with raises(CircuitBreakerError):
        await breaker.call_async(func_exception_async)

    assert CircuitBreakerState.OPEN == breaker.current_state

    with raises(CircuitBreakerError):
        await breaker.call_async(func_succeed_async)

    assert 3 == breaker.fail_counter

    # Wait for timeout, at least a second since redis rounds to a second
    await sleep(delta.total_seconds() * 2)

    # Circuit should close again
    assert await breaker.call_async(func_succeed_async)
    assert 0 == breaker.fail_counter
    assert CircuitBreakerState.CLOSED == breaker.current_state
    assert 1 == func_succeed_async.call_count


async def test_successful_after_wait(storage, delta):
    """It should accurately report the time needed to wait."""

    breaker = CircuitBreaker(fail_max=1, timeout_duration=delta, state_storage=storage)
    func_succeed_async = func_succeed_counted_async()

    try:
        await breaker.call_async(func_exception_async)
    except CircuitBreakerError as e:
        await asyncio.sleep(delta.total_seconds())

    await breaker.call_async(func_succeed_async)
    assert func_succeed_async.call_count == 1


async def test_failed_call_when_half_open(storage):
    """It should open the circuit when a call fails in half-open state."""

    breaker = CircuitBreaker(state_storage=storage)

    breaker.half_open()
    assert 0 == breaker.fail_counter
    assert CircuitBreakerState.HALF_OPEN == breaker.current_state

    with raises(CircuitBreakerError):
        await breaker.call_async(func_exception_async)

    assert 1 == breaker.fail_counter
    assert CircuitBreakerState.OPEN == breaker.current_state


async def test_successful_call_when_half_open(storage):
    """It should close the circuit when a call succeeds in half-open state."""

    breaker = CircuitBreaker(state_storage=storage)

    breaker.half_open()
    assert 0 == breaker.fail_counter
    assert CircuitBreakerState.HALF_OPEN == breaker.current_state

    # Circuit should open
    assert await breaker.call_async(func_succeed_async)
    assert 0 == breaker.fail_counter
    assert CircuitBreakerState.CLOSED == breaker.current_state


async def test_close(storage):
    """It should allow the circuit to be closed manually."""

    breaker = CircuitBreaker(fail_max=3, state_storage=storage)

    breaker.open()
    assert 0 == breaker.fail_counter
    assert CircuitBreakerState.OPEN == breaker.current_state

    breaker.close()
    assert 0 == breaker.fail_counter
    assert CircuitBreakerState.CLOSED == breaker.current_state


async def test_generator(storage):
    """It should support generator values."""

    breaker = CircuitBreaker(state_storage=storage)

    @breaker
    def func_succeed():
        """Docstring"""
        yield True

    @breaker
    def func_exception():
        """Docstring"""
        x = yield True
        raise DummyException(x)

    s = func_succeed()
    e = func_exception()
    next(e)

    with raises(DummyException):
        e.send(True)

    assert 1 == breaker.fail_counter
    assert next(s)

    with raises(StopIteration):
        next(s)

    assert 0 == breaker.fail_counter
