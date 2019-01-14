Usage
-----

.. include:: ../../readme.rst
    :start-after:
        Usage
        -----

Calling Functions With The Breaker
==================================

There are two primary ways of calling functions using the
breaker. The first is to wrap the functions you want in a
decorator, as seen above.

Using a :class:`~aiobreaker.circuitbreaker.CircuitBreaker`
to decorate the functions you wish to use it with is a
"set it and forget it" approach and works quite well
however it requires you to `always` use the circuit breaker.
Another option is the :func:`~aiobreaker.circuitbreaker.CircuitBreaker.call`
or :func:`~aiobreaker.circuitbreaker.CircuitBreaker.call_async`
functions which allow you to route functions through a breaker at will.

.. code:: python

    from aiobreaker import CircuitBreaker

    breaker = CircuitBreaker()
    bar = breaker.call(foo)
    async_bar = await breaker.call_async(async_foo)

By default there is a check to make sure if you were to pass
a decorated function into a breaker's `call` method that the
validation logic isn't performed twice. To disable this, set
the ``ignore_on_call`` parameter to ``False`` on the decorator
:func:`~aiobreaker.circuitbreaker.CircuitBreaker.__call__`.

Manually Setting Or Resetting The Circuit
=========================================

The circuit can be manually opened or closed if needed.
:func:`~aiobreaker.circuitbreaker.CircuitBreaker.open` will open
the circuit causing subsequent calls to fail until the
:attr:`~aiobreaker.circuitbreaker.CircuitBreaker.timeout_duration`
elapses. Similarly, :func:`~aiobreaker.circuitbreaker.CircuitBreaker.close`
will override the checks and immediately close the breaker, causing
further calls to succeed again.

Listening For Events On The Breaker
===================================

To listen for events, you must implement a listener. A listener
is anything that subclasses and overrides the functions on the
:class:`~aiobreaker.listener.CircuitBreakerListener` class.

.. code:: python

    from aiobreaker import CircuitBreaker, CircuitBreakerListener

    class LogListener(CircuitBreakerListener):

        def state_change(self, breaker, old, new):
            logger.info(f"{old.state} -> {new.state}")


    breaker = CircuitBreaker(listeners=[LogListener()])

Listeners can be added and removed on the fly with the
:func:`~aiobreaker.circuitbreaker.CircuitBreaker.add_listener` and
:func:`~aiobreaker.circuitbreaker.CircuitBreaker.remove_listener`
functions.


Ignoring Specific Exceptions
============================

If there are specific exceptions that shouldn't be considered
by the breaker, they can be specified, added, and removed in
a similar way to the way Listeners are handled.

.. code:: python

    from aiobreaker import CircuitBreaker
    import sqlite3

    breaker = CircuitBreaker(exclude=[sqlite3.Error])

Exceptions can be ignored on the fly with the
:func:`~aiobreaker.circuitbreaker.CircuitBreaker.add_excluded_exception` and
:func:`~aiobreaker.circuitbreaker.CircuitBreaker.remove_excluded_exception`
functions.
