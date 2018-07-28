from time import sleep

from pybreaker import CircuitBreaker, CircuitBreakerListener, CircuitBreakerError
from test.util import DummyException, func_exception, start_threads, func_succeed

# these are the test fixtures for pytest
from test.fixtures import *


def test_fail_thread_safety(delta):
    """It should compute a failed call atomically to avoid race conditions."""

    breaker = CircuitBreaker(fail_max=3000, timeout_duration=delta)
    count = 0

    def trigger_error():
        for n in range(500):
            try:
                breaker.call(func_exception)
            except DummyException:
                pass

    def counter():
        nonlocal count
        count += 1

    # override the counter function to keep track of calls
    breaker._inc_counter = counter
    start_threads(trigger_error, 3)
    assert 1500 == count


def test_success_thread_safety(delta):
    """It should compute a successful call atomically to avoid race conditions."""

    def trigger_success():
        for n in range(500):
            breaker.call(func_succeed)

    class SuccessCountListener(CircuitBreakerListener):
        """Count the number of times the success function is called."""

        def __init__(self):
            self.count = 0

        def success(self, breaker):
            self.count += 1

    breaker = CircuitBreaker(fail_max=3000, timeout_duration=delta)
    listener = SuccessCountListener()

    breaker.add_listener(listener)
    start_threads(trigger_success, 3)
    assert 1500 == listener.count


def test_half_open_thread_safety(delta):
    """It should allow only one trial call then reject others when the circuit is half-open."""

    def trigger_failure():
        try:
            breaker.call(func_exception)
        except DummyException:
            pass
        except CircuitBreakerError:
            pass

    class StateChangeCountListener(CircuitBreakerListener):
        """Counts the number of times the state changes."""

        def __init__(self):
            self.count = 0

        def state_change(self, breaker, old, new):
            if new.name != old.name:
                self.count += 1

    breaker = CircuitBreaker(fail_max=1, timeout_duration=delta)
    breaker.half_open()

    state_listener = StateChangeCountListener()
    breaker.add_listener(state_listener)

    start_threads(trigger_failure, 5)
    assert 1 == state_listener.count


def test_fail_max_thread_safety():
    """It should not allow more failed calls than 'fail_max' setting."""
    breaker = CircuitBreaker()

    def trigger_error():
        for i in range(2000):
            try:
                breaker.call(func_exception)
            except DummyException:
                pass
            except CircuitBreakerError:
                pass

    class SleepListener(CircuitBreakerListener):
        """Sleeps the calls to pad them out a bit."""

        def before_call(self, breaker, func, *args, **kwargs):
            sleep(0.00005)

    breaker.add_listener(SleepListener())
    start_threads(trigger_error, 3)
    assert breaker.fail_max == breaker.fail_counter
