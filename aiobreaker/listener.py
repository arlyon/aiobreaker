from typing import Callable


class CircuitBreakerListener:
    """
    Listener class used to plug code to a CircuitBreaker instance when certain events happen.

    todo async listener handlers
    """

    def before_call(self, breaker: 'CircuitBreaker', func: Callable, *args, **kwargs) -> None:
        """
        Called before a function is executed over a breaker.

        :param breaker: The breaker that is used.
        :param func: The function that is called.
        :param args: The args to the function.
        :param kwargs: The kwargs to the function.
        """

    def failure(self, breaker: 'CircuitBreaker', exception: Exception) -> None:
        """
        Called when a function executed over the circuit breaker 'breaker' fails.
        """

    def success(self, breaker: 'CircuitBreaker') -> None:
        """
        Called when a function executed over the circuit breaker 'breaker' succeeds.
        """

    def state_change(self, breaker: 'CircuitBreaker', old: 'CircuitBreakerState', new: 'CircuitBreakerState') -> None:
        """
        Called when the state of the circuit breaker 'breaker' changes.
        """
