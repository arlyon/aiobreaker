import asyncio
import types
from abc import ABC
from datetime import datetime, timedelta
from enum import Enum
from typing import Callable, Union, Optional, TypeVar, Awaitable, Generator


class CircuitBreakerError(Exception):
    """
    Raised when the function fails due to the breaker being open.
    """

    def __init__(self, message: str, reopen_time: datetime):
        """
        :param message: The reasoning.
        :param reopen_time: When the breaker re-opens.
        """
        self.message = message
        self.reopen_time = reopen_time

    @property
    def time_remaining(self) -> timedelta:
        return self.reopen_time - datetime.now()

    async def sleep_until_open(self):
        await asyncio.sleep(self.time_remaining.total_seconds())


T = TypeVar('T')


class CircuitBreakerBaseState(ABC):
    """
    Implements the behavior needed by all circuit breaker states.
    """

    def __init__(self, breaker: 'CircuitBreaker', state: 'CircuitBreakerState'):
        """
        Creates a new instance associated with the circuit breaker `cb` and
        identified by `name`.
        """
        self._breaker = breaker
        self._state = state

    @property
    def state(self) -> 'CircuitBreakerState':
        """
        Returns a human friendly name that identifies this state.
        """
        return self._state

    def _handle_error(self, func: Callable, exception: Exception):
        """
        Handles a failed call to the guarded operation.

        :raises: The given exception, after calling all the handlers.
        """
        if self._breaker.is_system_error(exception):
            self._breaker._inc_counter()
            for listener in self._breaker.listeners:
                listener.failure(self._breaker, exception)
            self.on_failure(exception)
        else:
            self._handle_success()
        raise exception

    def _handle_success(self):
        """
        Handles a successful call to the guarded operation.
        """
        self._breaker._state_storage.reset_counter()
        self.on_success()
        for listener in self._breaker.listeners:
            listener.success(self._breaker)

    def call(self, func: Callable[..., T], *args, **kwargs) -> T:
        """
        Calls `func` with the given `args` and `kwargs`, and updates the
        circuit breaker state according to the result.
        """
        ret = None

        self.before_call(func, *args, **kwargs)
        for listener in self._breaker.listeners:
            listener.before_call(self._breaker, func, *args, **kwargs)

        try:
            ret = func(*args, **kwargs)
            if isinstance(ret, types.GeneratorType):
                return self.generator_call(ret)
        except Exception as e:
            self._handle_error(func, e)
        else:
            self._handle_success()
        return ret

    async def call_async(self, func: Callable[..., Awaitable[T]], *args, **kwargs) -> Awaitable[T]:

        ret = None
        self.before_call(func, *args, **kwargs)
        for listener in self._breaker.listeners:
            listener.before_call(self._breaker, func, *args, **kwargs)

        try:
            ret = await func(*args, **kwargs)
        except Exception as e:
            self._handle_error(func, e)
        else:
            self._handle_success()
        return ret

    def generator_call(self, wrapped_generator: Generator):
        try:
            value = yield next(wrapped_generator)
            while True:
                value = yield wrapped_generator.send(value)
        except StopIteration:
            self._handle_success()
            return
        except Exception as e:
            self._handle_error(None, e)

    def before_call(self, func: Union[Callable[..., any], Callable[..., Awaitable]], *args, **kwargs):
        """
        Override this method to be notified before a call to the guarded
        operation is attempted.
        """
        pass

    def on_success(self):
        """
        Override this method to be notified when a call to the guarded
        operation succeeds.
        """
        pass

    def on_failure(self, exception: Exception):
        """
        Override this method to be notified when a call to the guarded
        operation fails.
        """
        pass


class CircuitClosedState(CircuitBreakerBaseState):
    """
    In the normal "closed" state, the circuit breaker executes operations as
    usual. If the call succeeds, nothing happens. If it fails, however, the
    circuit breaker makes a note of the failure.

    Once the number of failures exceeds a threshold, the circuit breaker trips
    and "opens" the circuit.
    """

    def __init__(self, breaker, prev_state: Optional[CircuitBreakerBaseState] = None, notify=False):
        """
        Moves the given circuit breaker to the "closed" state.
        """
        super().__init__(breaker, CircuitBreakerState.CLOSED)
        if notify:
            # We only reset the counter if notify is True, otherwise the CircuitBreaker
            # will lose it's failure count due to a second CircuitBreaker being created
            # using the same _state_storage object, or if the _state_storage objects
            # share a central source of truth (as would be the case with the redis
            # storage).
            self._breaker._state_storage.reset_counter()
            for listener in self._breaker.listeners:
                listener.state_change(self._breaker, prev_state, self)

    def on_failure(self, exception: Exception):
        """
        Moves the circuit breaker to the "open" state once the failures
        threshold is reached.
        """
        if self._breaker._state_storage.counter >= self._breaker.fail_max:
            self._breaker.open()
            raise CircuitBreakerError('Failures threshold reached, circuit breaker opened.', self._breaker.opens_at) from exception


class CircuitOpenState(CircuitBreakerBaseState):
    """
    When the circuit is "open", calls to the circuit breaker fail immediately,
    without any attempt to execute the real operation. This is indicated by the
    ``CircuitBreakerError`` exception.

    After a suitable amount of time, the circuit breaker decides that the
    operation has a chance of succeeding, so it goes into the "half-open" state.
    """

    def __init__(self, breaker, prev_state=None, notify=False):
        """
        Moves the given circuit breaker to the "open" state.
        """
        super().__init__(breaker, CircuitBreakerState.OPEN)
        if notify:
            for listener in self._breaker.listeners:
                listener.state_change(self._breaker, prev_state, self)

    def before_call(self, func, *args, **kwargs):
        """
        After the timeout elapses, move the circuit breaker to the "half-open" state.
        :raises CircuitBreakerError: if the timeout has still to be elapsed.
        """
        timeout = self._breaker.timeout_duration
        opened_at = self._breaker._state_storage.opened_at
        if opened_at and datetime.utcnow() < opened_at + timeout:
            raise CircuitBreakerError('Timeout not elapsed yet, circuit breaker still open', self._breaker.opens_at)

    def call(self, func, *args, **kwargs):
        """
        Call before_call to check if the breaker should close and open it if it passes.
        """
        self.before_call(func, *args, **kwargs)
        self._breaker.half_open()
        return self._breaker.call(func, *args, **kwargs)

    async def call_async(self, func, *args, **kwargs):
        """
        Call before_call to check if the breaker should close and open it if it passes.
        """
        self.before_call(func, *args, **kwargs)
        self._breaker.half_open()
        return await self._breaker.call_async(func, *args, **kwargs)


class CircuitHalfOpenState(CircuitBreakerBaseState):
    """
    In the "half-open" state, the next call to the circuit breaker is allowed
    to execute the dangerous operation. Should the call succeed, the circuit
    breaker resets and returns to the "closed" state. If this trial call fails,
    however, the circuit breaker returns to the "open" state until another
    timeout elapses.
    """

    def __init__(self, breaker, prev_state=None, notify=False):
        """
        Moves the given circuit breaker to the "half-open" state.
        """
        super().__init__(breaker, CircuitBreakerState.HALF_OPEN)
        if notify:
            for listener in self._breaker._listeners:
                listener.state_change(self._breaker, prev_state, self)

    def on_failure(self, exception):
        """
        Opens the circuit breaker.
        """
        self._breaker.open()
        raise CircuitBreakerError('Trial call failed, circuit breaker opened.',
                                  self._breaker.opens_at) from exception

    def on_success(self):
        """
        Closes the circuit breaker.
        """
        self._breaker.close()


class CircuitBreakerState(Enum):

    OPEN = CircuitOpenState
    CLOSED = CircuitClosedState
    HALF_OPEN = CircuitHalfOpenState
