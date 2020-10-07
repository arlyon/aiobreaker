import asyncio
import inspect
from datetime import timedelta, datetime
from functools import wraps
from typing import Optional, Iterable, Callable, Coroutine, Type, List, Union, Tuple

from .listener import CircuitBreakerListener
from .state import CircuitBreakerState, CircuitBreakerBaseState
from .storage.base import CircuitBreakerStorage
from .storage.memory import CircuitMemoryStorage


class CircuitBreaker:
    """
    A circuit breaker is a route through which functions are executed.
    When a function is executed via a circuit breaker, the breaker is notified.
    Multiple failed attempts will open the breaker and block additional calls.
    """

    def __init__(self, fail_max=5,
                 timeout_duration: Optional[timedelta] = None,
                 exclude: Optional[Iterable[Union[Callable, Type[Exception]]]] = None,
                 listeners: Optional[Iterable[CircuitBreakerListener]] = None,
                 state_storage: Optional[CircuitBreakerStorage] = None,
                 name: Optional[str] = None):
        """
        Creates a new circuit breaker with the given parameters.

        :param fail_max: The maximum number of failures for the breaker.
        :param timeout_duration: The timeout to elapse for a breaker to close again.
        :param exclude: A list of excluded :class:`Exception` types to ignore.
        :param listeners: A list of :class:`CircuitBreakerListener`
        :param state_storage: A type of storage. Defaults to :class:`~aiobreaker.storage.memory.CircuitMemoryStorage`
        """
        self._state_storage = state_storage or CircuitMemoryStorage(CircuitBreakerState.CLOSED)
        self._state = self._create_new_state(self.current_state)

        self._fail_max = fail_max
        self._timeout_duration = timeout_duration if timeout_duration else timedelta(seconds=60)

        self._excluded_exceptions: List[Union[Callable, Type[Exception]]] = list(exclude or [])
        self._listeners = list(listeners or [])
        self._name = name

    @property
    def fail_counter(self):
        """
        Returns the current number of consecutive failures.
        """
        return self._state_storage.counter

    @property
    def fail_max(self):
        """
        Returns the maximum number of failures tolerated before the circuit is opened.
        """
        return self._fail_max

    @fail_max.setter
    def fail_max(self, number):
        """
        Sets the maximum `number` of failures tolerated before the circuit is opened.
        """
        self._fail_max = number

    @property
    def timeout_duration(self):
        """
        Once this circuit breaker is opened, it should remain opened until the timeout period elapses.
        """
        return self._timeout_duration

    @timeout_duration.setter
    def timeout_duration(self, timeout: datetime):
        """
        Sets the timeout period this circuit breaker should be kept open.
        """
        self._timeout_duration = timeout

    @property
    def opens_at(self) -> Optional[datetime]:
        """Gets the remaining timeout for a breaker."""
        open_at = self._state_storage.opened_at + self.timeout_duration
        if open_at < datetime.now():
            return None
        else:
            return open_at

    @property
    def time_until_open(self) -> Optional[timedelta]:
        """
        Returns a timedelta representing the difference between the current
        time and when the breaker closes again, or None if it has elapsed.
        """
        opens_in = self.opens_at - datetime.now()
        if opens_in < timedelta(0):
            return None
        return opens_in

    async def sleep_until_open(self):
        await asyncio.sleep(self.time_until_open.total_seconds())

    def _create_new_state(self, new_state: CircuitBreakerState, prev_state=None,
                          notify=False) -> 'CircuitBreakerBaseState':
        """
        Return state object from state string, i.e.,
        'closed' -> <CircuitClosedState>
        """

        try:
            return new_state.value(self, prev_state=prev_state, notify=notify)
        except KeyError:
            msg = "Unknown state {!r}, valid states: {}"
            raise ValueError(msg.format(new_state, ', '.join(state.value for state in CircuitBreakerState)))

    @property
    def state(self):
        """
        Update (if needed) and returns the cached state object.
        """
        # Ensure cached state is up-to-date
        if self.current_state != self._state.state:
            # If cached state is out-of-date, that means that it was likely
            # changed elsewhere (e.g. another process instance). We still send
            # out a notification, informing others that this particular circuit
            # breaker instance noticed the changed circuit.
            self.state = self.current_state
        return self._state

    @state.setter
    def state(self, state_str):
        """
        Set cached state and notify listeners of newly cached state.
        """
        self._state = self._create_new_state(
            state_str, prev_state=self._state, notify=True)

    @property
    def current_state(self) -> CircuitBreakerState:
        """
        Returns a CircuitBreakerState that identifies the state of the circuit breaker.
        """
        return self._state_storage.state

    @property
    def excluded_exceptions(self) -> tuple:
        """
        Returns a tuple of the excluded exceptions, e.g., exceptions that should
        not be considered system errors by this circuit breaker.
        """
        return tuple(self._excluded_exceptions)

    def add_excluded_exception(self, exception: Type[Exception]):
        """
        Adds an exception to the list of excluded exceptions.
        """
        self._excluded_exceptions.append(exception)

    def add_excluded_exceptions(self, *exceptions):
        """
        Adds exceptions to the list of excluded exceptions.

        :param exceptions: Any Exception types you wish to ignore.
        """
        for exc in exceptions:
            self.add_excluded_exception(exc)

    def remove_excluded_exception(self, exception: Type[Exception]):
        """
        Removes an exception from the list of excluded exceptions.
        """
        self._excluded_exceptions.remove(exception)

    def _inc_counter(self):
        """
        Increments the counter of failed calls.
        """
        self._state_storage.increment_counter()

    def is_system_error(self, exception: Exception):
        """
        Returns whether the exception `exception` is considered a signal of
        system malfunction. Business exceptions should not cause this circuit
        breaker to open.
        """
        exception_type = type(exception)
        for exclusion in self._excluded_exceptions:
            if type(exclusion) is type:
                if issubclass(exception_type, exclusion):
                    return False
            elif callable(exclusion):
                if exclusion(exception):
                    return False

        return True

    def call(self, func: Callable, *args, **kwargs):
        """
        Calls `func` with the given `args` and `kwargs` according to the rules
        implemented by the current state of this circuit breaker.
        """
        if getattr(func, "_ignore_on_call", False):
            # if the function has set `_ignore_on_call` to True,
            # it is a decorator that needs to avoid triggering
            # the circuit breaker twice
            return func(*args, **kwargs)

        return self.state.call(func, *args, **kwargs)

    async def call_async(self, func: Callable[..., Coroutine], *args, **kwargs):
        """
        Calls `func` with the given `args` and `kwargs` according to the rules
        implemented by the current state of this circuit breaker.
        """
        if getattr(func, "_ignore_on_call", False):
            # if the function has set `_ignore_on_call` to True,
            # it is a decorator that needs to avoid triggering
            # the circuit breaker twice
            return await func(*args, **kwargs)

        return await self.state.call_async(func, *args, **kwargs)

    def open(self):
        """
        Opens the circuit, e.g., the following calls will immediately fail
        until timeout elapses.
        """
        self._state_storage.opened_at = datetime.utcnow()
        self.state = self._state_storage.state = CircuitBreakerState.OPEN

    def half_open(self):
        """
        Half-opens the circuit, e.g. lets the following call pass through and
        opens the circuit if the call fails (or closes the circuit if the call
        succeeds).
        """
        self.state = self._state_storage.state = CircuitBreakerState.HALF_OPEN

    def close(self):
        """
        Closes the circuit, e.g. lets the following calls execute as usual.
        """
        self.state = self._state_storage.state = CircuitBreakerState.CLOSED

    def __call__(self, *call_args, ignore_on_call=True):
        """
        Decorates the function such that calls are handled according to the rules
        implemented by the current state of this circuit breaker.

        :param ignore_on_call: Whether the decorated function should be ignored when using
            :func:`~CircuitBreaker.call`, preventing the breaker being triggered twice.
        """

        def _outer_wrapper(func):
            @wraps(func)
            def _inner_wrapper(*args, **kwargs):
                return self.call(func, *args, **kwargs)

            @wraps(func)
            async def _inner_wrapper_async(*args, **kwargs):
                return await self.call_async(func, *args, **kwargs)

            return_func = _inner_wrapper_async if inspect.iscoroutinefunction(func) else _inner_wrapper
            return_func._ignore_on_call = ignore_on_call
            return return_func

        if len(call_args) == 1 and inspect.isfunction(call_args[0]):
            # if decorator called without arguments, pass the function on
            return _outer_wrapper(*call_args)
        elif len(call_args) == 0:
            # if decorator called with arguments, _outer_wrapper will receive the function
            return _outer_wrapper
        else:
            raise TypeError("Decorator does not accept positional arguments.")

    @property
    def listeners(self):
        """
        Returns the registered listeners as a tuple.
        """
        return tuple(self._listeners)

    def add_listener(self, listener):
        """
        Registers a listener for this circuit breaker.
        """
        self._listeners.append(listener)

    def add_listeners(self, *listeners):
        """
        Registers listeners for this circuit breaker.
        """
        for listener in listeners:
            self.add_listener(listener)

    def remove_listener(self, listener):
        """
        Unregisters a listener of this circuit breaker.
        """
        self._listeners.remove(listener)

    @property
    def name(self) -> str:
        """
        Returns the name of this circuit breaker. Useful for logging.
        """
        return self._name

    @name.setter
    def name(self, name: str):
        """
        Set the name of this circuit breaker.
        """
        self._name = name
