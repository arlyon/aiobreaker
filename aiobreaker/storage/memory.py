from aiobreaker.state import CircuitBreakerState
from .base import CircuitBreakerStorage


class CircuitMemoryStorage(CircuitBreakerStorage):
    """
    Implements a `CircuitBreakerStorage` in local memory.
    """

    def __init__(self, state: CircuitBreakerState):
        """
        Creates a new instance with the given `state`.
        """
        super().__init__('memory')
        self._fail_counter = 0
        self._opened_at = None
        self._state = state

    @property
    def state(self) -> CircuitBreakerState:
        """
        Returns the current circuit breaker state.
        """
        return self._state

    @state.setter
    def state(self, state: CircuitBreakerState):
        """
        Set the current circuit breaker state to `state`.
        """
        self._state = state

    def increment_counter(self):
        """
        Increases the failure counter by one.
        """
        self._fail_counter += 1

    def reset_counter(self):
        """
        Sets the failure counter to zero.
        """
        self._fail_counter = 0

    @property
    def counter(self):
        """
        Returns the current value of the failure counter.
        """
        return self._fail_counter

    @property
    def opened_at(self):
        """
        Returns the most recent value of when the circuit was opened.
        """
        return self._opened_at

    @opened_at.setter
    def opened_at(self, date_time):
        """
        Sets the most recent value of when the circuit was opened to
        `datetime`.
        """
        self._opened_at = date_time
