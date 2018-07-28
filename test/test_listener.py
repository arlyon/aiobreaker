from _pytest.python_api import raises

from pybreaker import CircuitBreakerListener, CircuitBreaker, STATE_CLOSED, STATE_OPEN, STATE_HALF_OPEN
from test.util import DummyException, func_succeed, func_exception

# these are the test fixtures for pytest
from test.fixtures import *


def test_transition_events(storage):
    """It should call the appropriate functions on every state transition."""

    class Listener(CircuitBreakerListener):
        def __init__(self):
            self.out = []

        def state_change(self, breaker, old, new):
            assert breaker
            self.out.append((old.name, new.name))

    listener = Listener()
    breaker = CircuitBreaker(listeners=(listener,), state_storage=storage)
    assert STATE_CLOSED == breaker.current_state

    breaker.open()
    assert STATE_OPEN == breaker.current_state

    breaker.half_open()
    assert STATE_HALF_OPEN == breaker.current_state

    breaker.close()
    assert STATE_CLOSED == breaker.current_state

    assert [(STATE_CLOSED, STATE_OPEN), (STATE_OPEN, STATE_HALF_OPEN), (STATE_HALF_OPEN, STATE_CLOSED)] == listener.out


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