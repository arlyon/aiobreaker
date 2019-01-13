from _pytest.python_api import raises

from aiobreaker import CircuitBreaker
from aiobreaker.listener import CircuitBreakerListener
from aiobreaker.state import CircuitBreakerState
from test.util import DummyException, func_succeed, func_exception


def test_transition_events(storage):
    """It should call the appropriate functions on every state transition."""

    class Listener(CircuitBreakerListener):
        def __init__(self):
            self.out = []

        def state_change(self, breaker, old, new):
            assert breaker
            self.out.append((old.state, new.state))

    listener = Listener()
    breaker = CircuitBreaker(listeners=(listener,), state_storage=storage)
    assert CircuitBreakerState.CLOSED == breaker.current_state

    breaker.open()
    assert CircuitBreakerState.OPEN == breaker.current_state

    breaker.half_open()
    assert CircuitBreakerState.HALF_OPEN == breaker.current_state

    breaker.close()
    assert CircuitBreakerState.CLOSED == breaker.current_state

    assert [
               (CircuitBreakerState.CLOSED, CircuitBreakerState.OPEN),
               (CircuitBreakerState.OPEN, CircuitBreakerState.HALF_OPEN),
               (CircuitBreakerState.HALF_OPEN, CircuitBreakerState.CLOSED)
           ] == listener.out


def test_call_events(storage):
    """It should call the appropriate functions on every successful/failed call.
    """

    class Listener(CircuitBreakerListener):
        def __init__(self):
            self.out = []

        def before_call(self, breaker, func, *args, **kwargs):
            assert breaker
            self.out.append("CALL")

        def success(self, breaker):
            assert breaker
            self.out.append("SUCCESS")

        def failure(self, breaker, exception):
            assert breaker
            assert isinstance(exception, DummyException)
            self.out.append("FAILURE")

    listener = Listener()
    breaker = CircuitBreaker(listeners=(listener,), state_storage=storage)

    assert breaker.call(func_succeed)

    with raises(DummyException):
        breaker.call(func_exception)

    assert ["CALL", "SUCCESS", "CALL", "FAILURE"] == listener.out
