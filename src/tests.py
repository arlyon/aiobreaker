# -*- coding:utf-8 -*-
from functools import partial
from time import sleep

import fakeredis
import logging
import threading
from abc import ABC, abstractmethod
from datetime import timedelta
from redis.exceptions import RedisError
from types import MethodType
from typing import Dict

from unittest import TestCase, main
from unittest.mock import MagicMock, patch
from aiounittest import AsyncTestCase

from pybreaker import CircuitBreaker, CircuitBreakerError, CircuitBreakerListener, STATE_OPEN, STATE_CLOSED, \
    STATE_HALF_OPEN, CircuitMemoryStorage, CircuitRedisStorage


class DummyException(Exception):
    """
    A more specific error to call during the tests.
    """
    pass


class BaseTestCases:
    """
    Stores the base test cases so that they don't appear in the test suite.
    Would be nice if unittest detected ABCs... https://bugs.python.org/issue17519
    """

    class CircuitBreakerStorageBasedTestCase(ABC, TestCase):
        """
        Mix in to test against different storage backings.
        """

        @property
        @abstractmethod
        def breaker_kwargs(self) -> Dict:
            pass

        def test_successful_call(self):
            """CircuitBreaker: it should keep the circuit closed after a successful
            call.
            """

            def func():
                return True

            breaker = CircuitBreaker()
            self.assertTrue(breaker.call(func))
            self.assertEqual(0, breaker.fail_counter)
            self.assertEqual(STATE_CLOSED, breaker.current_state)

        def test_one_failed_call(self):
            """CircuitBreaker: it should keep the circuit closed after a few
            failures.
            """

            def func():
                raise DummyException()

            breaker = CircuitBreaker()

            self.assertRaises(DummyException, breaker.call, func)
            self.assertEqual(1, breaker.fail_counter)
            self.assertEqual(STATE_CLOSED, breaker.current_state)

        def test_one_successful_call_after_failed_call(self):
            """
            it should keep the circuit closed after few mixed outcomes.
            """

            def suc():
                return True

            def err():
                raise DummyException()

            breaker = CircuitBreaker()

            with self.assertRaises(DummyException):
                breaker.call(err)

            self.assertEqual(1, breaker.fail_counter)

            self.assertTrue(breaker.call(suc))
            self.assertEqual(0, breaker.fail_counter)
            self.assertEqual(STATE_CLOSED, breaker.current_state)

        def test_several_failed_calls(self):
            """
            it should open the circuit after multiple failures.
            """
            breaker = CircuitBreaker(fail_max=3, **self.breaker_kwargs)

            def raiser():
                raise DummyException()

            with self.assertRaises(DummyException):
                breaker.call(raiser)

            with self.assertRaises(DummyException):
                breaker.call(raiser)

            # Circuit should open
            with self.assertRaises(CircuitBreakerError):
                breaker.call(raiser)

            self.assertEqual(3, breaker.fail_counter)
            self.assertEqual('open', breaker.current_state)

        def test_failed_call_after_timeout(self):
            """CircuitBreaker: it should half-open the circuit after timeout.
            """
            breaker = CircuitBreaker(fail_max=3, reset_timeout=timedelta(seconds=0.5), **self.breaker_kwargs)

            def func():
                raise DummyException()

            self.assertRaises(DummyException, breaker.call, func)
            self.assertRaises(DummyException, breaker.call, func)
            self.assertEqual('closed', breaker.current_state)

            # Circuit should open
            self.assertRaises(CircuitBreakerError, breaker.call, func)
            self.assertEqual(3, breaker.fail_counter)

            # Wait for timeout
            sleep(0.6)

            # Circuit should open again
            self.assertRaises(CircuitBreakerError, breaker.call, func)
            self.assertEqual(4, breaker.fail_counter)
            self.assertEqual('open', breaker.current_state)

        def test_successful_after_timeout(self):
            """CircuitBreaker: it should close the circuit when a call succeeds
            after timeout. The successful function should only be called once.
            """
            breaker = CircuitBreaker(fail_max=3, reset_timeout=timedelta(seconds=1), **self.breaker_kwargs)

            suc = MagicMock(return_value=True)

            def err(): raise NotImplementedError()

            self.assertRaises(NotImplementedError, breaker.call, err)
            self.assertRaises(NotImplementedError, breaker.call, err)
            self.assertEqual('closed', breaker.current_state)

            # Circuit should open
            self.assertRaises(CircuitBreakerError, breaker.call, err)
            self.assertRaises(CircuitBreakerError, breaker.call, suc)
            self.assertEqual(3, breaker.fail_counter)

            # Wait for timeout, at least a second since redis rounds to a second
            sleep(2)

            # Circuit should close again
            self.assertTrue(breaker.call(suc))
            self.assertEqual(0, breaker.fail_counter)
            self.assertEqual('closed', breaker.current_state)
            self.assertEqual(1, suc.call_count)

        def test_failed_call_when_halfopen(self):
            """CircuitBreaker: it should open the circuit when a call fails in
            half-open state.
            """

            def func():
                raise DummyException()

            breaker = CircuitBreaker()

            breaker.half_open()
            self.assertEqual(0, breaker.fail_counter)
            self.assertEqual('half-open', breaker.current_state)

            # Circuit should open
            self.assertRaises(CircuitBreakerError, breaker.call, func)
            self.assertEqual(1, breaker.fail_counter)
            self.assertEqual('open', breaker.current_state)

        def test_successful_call_when_halfopen(self):
            """CircuitBreaker: it should close the circuit when a call succeeds in
            half-open state.
            """

            def func():
                return True

            breaker = CircuitBreaker()
            breaker.half_open()
            self.assertEqual(0, breaker.fail_counter)
            self.assertEqual('half-open', breaker.current_state)

            # Circuit should open
            self.assertTrue(breaker.call(func))
            self.assertEqual(0, breaker.fail_counter)
            self.assertEqual('closed', breaker.current_state)

        def test_close(self):
            """CircuitBreaker: it should allow the circuit to be closed manually.
            """
            breaker = CircuitBreaker(fail_max=3, **self.breaker_kwargs)

            def func():
                raise DummyException()

            self.assertRaises(DummyException, breaker.call, func)
            self.assertRaises(DummyException, breaker.call, func)

            # Circuit should open
            self.assertRaises(CircuitBreakerError, breaker.call, func)
            self.assertEqual(3, breaker.fail_counter)
            self.assertEqual('open', breaker.current_state)

            # Circuit should close again
            breaker.close()
            self.assertEqual(0, breaker.fail_counter)
            self.assertEqual('closed', breaker.current_state)

        def test_transition_events(self):
            """CircuitBreaker: it should call the appropriate functions on every
            state transition.
            """

            class Listener(CircuitBreakerListener):
                def __init__(self):
                    self.out = ''

                def state_change(self, breaker, old, new):
                    assert breaker
                    if old:
                        self.out += old.name
                    if new:
                        self.out += '->' + new.name
                    self.out += ','

            listener = Listener()
            breaker = CircuitBreaker(listeners=(listener,), **self.breaker_kwargs)
            self.assertEqual(STATE_CLOSED, breaker.current_state)

            breaker.open()
            self.assertEqual(STATE_OPEN, breaker.current_state)

            breaker.half_open()
            self.assertEqual(STATE_HALF_OPEN, breaker.current_state)

            breaker.close()
            self.assertEqual('closed', breaker.current_state)

            self.assertEqual('closed->open,open->half-open,half-open->closed,', listener.out)

        def test_call_events(self):
            """CircuitBreaker: it should call the appropriate functions on every
            successful/failed call.
            """
            self.out = ''

            def suc(): return True

            def err(): raise NotImplementedError()

            class Listener(CircuitBreakerListener):
                def __init__(self):
                    self.out = ''

                def before_call(self, breaker, func, *args, **kwargs):
                    assert breaker
                    self.out += '-'

                def success(self, breaker):
                    assert breaker
                    self.out += 'success'

                def failure(self, breaker, exception):
                    assert breaker
                    assert exception
                    self.out += 'failure'

            listener = Listener()
            breaker = CircuitBreaker(listeners=(listener,), **self.breaker_kwargs)

            self.assertTrue(breaker.call(suc))
            self.assertRaises(NotImplementedError, breaker.call, err)
            self.assertEqual('-success-failure', listener.out)

        def test_generator(self):
            """
            it should inspect generator values.
            """

            breaker = CircuitBreaker()

            @breaker
            def suc(value):
                """Docstring"""
                yield value

            @breaker
            def err(value):
                """Docstring"""
                x = yield value
                raise NotImplementedError(x)

            s = suc(True)
            e = err(True)
            next(e)

            self.assertRaises(NotImplementedError, e.send, True)
            self.assertEqual(1, breaker.fail_counter)
            self.assertTrue(next(s))
            self.assertRaises(StopIteration, lambda: next(s))
            self.assertEqual(0, breaker.fail_counter)

    class CircuitBreakerStorageBasedTestCaseAsync(ABC, AsyncTestCase):

        @property
        @abstractmethod
        def breaker_kwargs(self) -> Dict:
            pass

        async def test_successful_call_async(self):
            """
            It should keep the circuit closed after a successful call.
            """

            async def func():
                return True

            breaker = CircuitBreaker()
            self.assertTrue(await breaker.call_async(func))
            self.assertEqual(0, breaker.fail_counter)
            self.assertEqual(STATE_CLOSED, breaker.current_state)

        async def test_one_failed_call_async(self):
            """CircuitBreaker: it should keep the circuit closed after a few
            failures.
            """

            async def func():
                raise DummyException()

            breaker = CircuitBreaker()

            with self.assertRaises(DummyException):
                await breaker.call_async(func)

            self.assertEqual(1, breaker.fail_counter)
            self.assertEqual(STATE_CLOSED, breaker.current_state)

        async def test_one_successful_call_after_failed_call_async(self):
            """
            it should keep the circuit closed after few mixed outcomes.
            """

            async def suc():
                return True

            async def err():
                raise DummyException()

            breaker = CircuitBreaker()

            with self.assertRaises(DummyException):
                await breaker.call_async(err)

            self.assertEqual(1, breaker.fail_counter)

            self.assertTrue(await breaker.call_async(suc))
            self.assertEqual(0, breaker.fail_counter)
            self.assertEqual(STATE_CLOSED, breaker.current_state)

        async def test_several_failed_calls_async(self):
            """
            it should open the circuit after multiple failures.
            """
            breaker = CircuitBreaker(fail_max=3, **self.breaker_kwargs)

            async def raiser():
                raise DummyException()

            with self.assertRaises(DummyException):
                await breaker.call_async(raiser)

            with self.assertRaises(DummyException):
                await breaker.call_async(raiser)

            # Circuit should open
            with self.assertRaises(CircuitBreakerError):
                await breaker.call_async(raiser)

            self.assertEqual(3, breaker.fail_counter)
            self.assertEqual('open', breaker.current_state)

        async def test_failed_call_after_timeout_async(self):
            """CircuitBreaker: it should half-open the circuit after timeout.
            """
            breaker = CircuitBreaker(fail_max=3, reset_timeout=timedelta(seconds=0.5), **self.breaker_kwargs)

            async def func():
                raise DummyException()

            with self.assertRaises(DummyException):
                await breaker.call_async(func)

            with self.assertRaises(DummyException):
                await breaker.call_async(func)

            self.assertEqual('closed', breaker.current_state)

            # Circuit should open
            with self.assertRaises(CircuitBreakerError):
                await breaker.call_async(func)

            self.assertEqual(3, breaker.fail_counter)

            # Wait for timeout
            sleep(0.6)

            # Circuit should open again
            with self.assertRaises(CircuitBreakerError):
                await breaker.call_async(func)

            self.assertEqual(4, breaker.fail_counter)
            self.assertEqual('open', breaker.current_state)

        async def test_successful_after_timeout_async(self):
            """
            It should close the circuit when a call succeeds after timeout.
            The successful function should only be called once.
            """
            breaker = CircuitBreaker(fail_max=3, reset_timeout=timedelta(seconds=1), **self.breaker_kwargs)

            async def suc():
                suc.call_count += 1
                return True

            suc.call_count = 0

            async def err():
                raise DummyException()

            with self.assertRaises(DummyException):
                await breaker.call_async(err)

            with self.assertRaises(DummyException):
                await breaker.call_async(err)

            self.assertEqual('closed', breaker.current_state)

            # Circuit should open
            with self.assertRaises(CircuitBreakerError):
                await breaker.call_async(err)

            with self.assertRaises(CircuitBreakerError):
                await breaker.call_async(suc)

            self.assertEqual(3, breaker.fail_counter)

            # Wait for timeout, at least a second since redis rounds to a second
            sleep(2)

            # Circuit should close again
            self.assertTrue(await breaker.call_async(suc))
            self.assertEqual(0, breaker.fail_counter)
            self.assertEqual('closed', breaker.current_state)
            self.assertEqual(1, suc.call_count)

        async def test_failed_call_when_halfopen_async(self):
            """
            It should open the circuit when a call fails in half-open state.
            """

            async def func():
                raise DummyException()

            breaker = CircuitBreaker()

            breaker.half_open()
            self.assertEqual(0, breaker.fail_counter)
            self.assertEqual('half-open', breaker.current_state)

            # Circuit should open
            with self.assertRaises(CircuitBreakerError):
                await breaker.call_async(func)

            self.assertEqual(1, breaker.fail_counter)
            self.assertEqual('open', breaker.current_state)

        async def test_successful_call_when_halfopen_async(self):
            """
            It should close the circuit when a call succeeds in half-open state.
            """

            async def func():
                return True

            breaker = CircuitBreaker()
            breaker.half_open()
            self.assertEqual(0, breaker.fail_counter)
            self.assertEqual('half-open', breaker.current_state)

            # Circuit should open
            self.assertTrue(await breaker.call_async(func))
            self.assertEqual(0, breaker.fail_counter)
            self.assertEqual('closed', breaker.current_state)

        async def test_close_async(self):
            """
            It should allow the circuit to be closed manually.
            """
            breaker = CircuitBreaker(fail_max=3, **self.breaker_kwargs)

            async def func():
                raise DummyException()

            with self.assertRaises(DummyException):
                await breaker.call_async(func)

            with self.assertRaises(DummyException):
                await breaker.call_async(func)

            # Circuit should open
            with self.assertRaises(CircuitBreakerError):
                await breaker.call_async(func)

            self.assertEqual(3, breaker.fail_counter)
            self.assertEqual('open', breaker.current_state)

            # Circuit should close again
            breaker.close()
            self.assertEqual(0, breaker.fail_counter)
            self.assertEqual('closed', breaker.current_state)

    class CircuitBreakerConfigurationTestCase(TestCase):
        """
        Tests for the CircuitBreaker class.
        """

        def test_default_state(self):
            """
            it should get initial state from state_storage.
            """
            for state in (STATE_OPEN, STATE_CLOSED, STATE_HALF_OPEN):
                storage = CircuitMemoryStorage(state)
                breaker = CircuitBreaker(state_storage=storage)
                self.assertEqual(breaker.state.name, state)

        def test_default_params(self):
            """
            it should define smart defaults.
            """
            breaker = CircuitBreaker()

            self.assertEqual(0, breaker.fail_counter)
            self.assertEqual(timedelta(seconds=60), breaker.reset_timeout)
            self.assertEqual(5, breaker.fail_max)
            self.assertEqual(STATE_CLOSED, breaker.current_state)
            self.assertEqual((), breaker.excluded_exceptions)
            self.assertEqual((), breaker.listeners)
            self.assertEqual('memory', breaker._state_storage.name)

        def test_new_with_custom_reset_timeout(self):
            """
            it should support a custom reset timeout value.
            """
            breaker = CircuitBreaker(reset_timeout=timedelta(seconds=30))

            self.assertEqual(0, breaker.fail_counter)
            self.assertEqual(timedelta(seconds=30), breaker.reset_timeout)
            self.assertEqual(5, breaker.fail_max)
            self.assertEqual((), breaker.excluded_exceptions)
            self.assertEqual((), breaker.listeners)
            self.assertEqual('memory', breaker._state_storage.name)

        def test_new_with_custom_fail_max(self):
            """CircuitBreaker: it should support a custom maximum number of
            failures.
            """
            breaker = CircuitBreaker(fail_max=10)
            self.assertEqual(0, breaker.fail_counter)
            self.assertEqual(timedelta(seconds=60), breaker.reset_timeout)
            self.assertEqual(10, breaker.fail_max)
            self.assertEqual((), breaker.excluded_exceptions)
            self.assertEqual((), breaker.listeners)
            self.assertEqual('memory', breaker._state_storage.name)

        def test_new_with_custom_excluded_exceptions(self):
            """CircuitBreaker: it should support a custom list of excluded
            exceptions.
            """
            breaker = CircuitBreaker(exclude=[Exception])
            self.assertEqual(0, breaker.fail_counter)
            self.assertEqual(timedelta(seconds=60), breaker.reset_timeout)
            self.assertEqual(5, breaker.fail_max)
            self.assertEqual((Exception,), breaker.excluded_exceptions)
            self.assertEqual((), breaker.listeners)
            self.assertEqual('memory', breaker._state_storage.name)

        def test_fail_max_setter(self):
            """CircuitBreaker: it should allow the user to set a new value for
            'fail_max'.
            """
            breaker = CircuitBreaker()

            self.assertEqual(5, breaker.fail_max)
            breaker.fail_max = 10
            self.assertEqual(10, breaker.fail_max)

        def test_reset_timeout_setter(self):
            """CircuitBreaker: it should allow the user to set a new value for
            'reset_timeout'.
            """
            breaker = CircuitBreaker()

            self.assertEqual(timedelta(seconds=60), breaker.reset_timeout)
            breaker.reset_timeout = timedelta(seconds=30)
            self.assertEqual(timedelta(seconds=30), breaker.reset_timeout)

        def test_call_with_no_args(self):
            """
            It should be able to invoke functions with no-args.
            """

            def func():
                return True

            breaker = CircuitBreaker()
            self.assertTrue(breaker.call(func))

        def test_call_with_args(self):
            """
            It should be able to invoke functions with args.
            """

            def func(arg1, arg2):
                return arg1, arg2

            breaker = CircuitBreaker()

            self.assertEqual((42, 'abc'), breaker.call(func, 42, 'abc'))

        def test_call_with_kwargs(self):
            """
            It should be able to invoke functions with kwargs.
            """

            def func(**kwargs):
                return kwargs

            breaker = CircuitBreaker()

            kwargs = {'a': 1, 'b': 2}

            self.assertEqual(kwargs, breaker.call(func, **kwargs))

        def test_add_listener(self):
            """
            It should allow the user to add a listener at a later time.
            """
            breaker = CircuitBreaker()

            self.assertEqual((), breaker.listeners)

            first = CircuitBreakerListener()
            breaker.add_listener(first)
            self.assertEqual((first,), breaker.listeners)

            second = CircuitBreakerListener()
            breaker.add_listener(second)
            self.assertEqual((first, second), breaker.listeners)

        def test_add_listeners(self):
            """
            It should allow the user to add listeners at a later time.
            """
            breaker = CircuitBreaker()

            first, second = CircuitBreakerListener(), CircuitBreakerListener()
            breaker.add_listeners(first, second)
            self.assertEqual((first, second), breaker.listeners)

        def test_remove_listener(self):
            """
            it should allow the user to remove a listener.
            """
            breaker = CircuitBreaker()

            first = CircuitBreakerListener()
            breaker.add_listener(first)
            self.assertEqual((first,), breaker.listeners)

            breaker.remove_listener(first)
            self.assertEqual((), breaker.listeners)

        def test_excluded_exceptions(self):
            """CircuitBreaker: it should ignore specific exceptions.
            """
            breaker = CircuitBreaker(exclude=[LookupError])

            def err_1(): raise NotImplementedError()

            def err_2(): raise LookupError()

            def err_3(): raise KeyError()

            self.assertRaises(NotImplementedError, breaker.call, err_1)
            self.assertEqual(1, breaker.fail_counter)

            # LookupError is not considered a system error
            self.assertRaises(LookupError, breaker.call, err_2)
            self.assertEqual(0, breaker.fail_counter)

            self.assertRaises(NotImplementedError, breaker.call, err_1)
            self.assertEqual(1, breaker.fail_counter)

            # Should consider subclasses as well (KeyError is a subclass of
            # LookupError)
            self.assertRaises(KeyError, breaker.call, err_3)
            self.assertEqual(0, breaker.fail_counter)

        def test_add_excluded_exception(self):
            """
            it should allow the user to exclude an exception at a later time.
            """
            breaker = CircuitBreaker()

            self.assertEqual((), breaker.excluded_exceptions)

            breaker.add_excluded_exception(NotImplementedError)
            self.assertEqual((NotImplementedError,), breaker.excluded_exceptions)

            breaker.add_excluded_exception(Exception)
            self.assertEqual((NotImplementedError, Exception), breaker.excluded_exceptions)

        def test_add_excluded_exceptions(self):
            """
            it should allow the user to exclude exceptions at a later time.
            """
            breaker = CircuitBreaker()

            breaker.add_excluded_exceptions(NotImplementedError, Exception)
            self.assertEqual((NotImplementedError, Exception), breaker.excluded_exceptions)

        def test_remove_excluded_exception(self):
            """
            it should allow the user to remove an excluded exception.
            """
            breaker = CircuitBreaker()

            breaker.add_excluded_exception(NotImplementedError)
            self.assertEqual((NotImplementedError,), breaker.excluded_exceptions)

            breaker.remove_excluded_exception(NotImplementedError)
            self.assertEqual((), breaker.excluded_exceptions)

        def test_decorator(self):
            """
            It should be a decorator.
            """

            breaker = CircuitBreaker()

            @breaker
            def suc():
                """Docstring"""
                pass

            @breaker
            def err():
                """Docstring"""
                raise DummyException()

            self.assertEqual('Docstring', suc.__doc__)
            self.assertEqual('Docstring', err.__doc__)
            self.assertEqual('suc', suc.__name__)
            self.assertEqual('err', err.__name__)

            self.assertRaises(DummyException, err)
            self.assertEqual(1, breaker.fail_counter)

            suc()
            self.assertEqual(0, breaker.fail_counter)

        def test_name(self):
            """
            It should allow an optional name to be set and retrieved.
            """
            name = "test_breaker"
            breaker = CircuitBreaker(name=name)
            self.assertEqual(breaker.name, name)

            name = "breaker_test"
            breaker.name = name
            self.assertEqual(breaker.name, name)

    class CircuitBreakerConfigurationTestCaseAsync(AsyncTestCase):

        async def test_decorator_async(self):
            """
            It should also be an async decorator.
            """
            breaker = CircuitBreaker()

            @breaker
            async def suc():
                """Docstring"""
                pass

            @breaker
            async def err():
                """Docstring"""
                raise DummyException()

            self.assertEqual('Docstring', suc.__doc__)
            self.assertEqual('Docstring', err.__doc__)
            self.assertEqual('suc', suc.__name__)
            self.assertEqual('err', err.__name__)

            with self.assertRaises(DummyException):
                await err()
            self.assertEqual(1, breaker.fail_counter)

            await suc()
            self.assertEqual(0, breaker.fail_counter)

        async def test_call_with_no_args_async(self):
            """
            It should be able to invoke functions with no-args.
            """

            async def func():
                return True

            breaker = CircuitBreaker()
            self.assertTrue(await breaker.call_async(func))

        async def test_call_with_args_async(self):
            """
            It should be able to invoke functions with args.
            """

            async def func(arg1, arg2):
                return arg1, arg2

            breaker = CircuitBreaker()

            self.assertEqual((42, 'abc'), await breaker.call_async(func, 42, 'abc'))

        async def test_call_with_kwargs_async(self):
            """
            It should be able to invoke functions with kwargs.
            """

            async def func(**kwargs):
                return kwargs

            breaker = CircuitBreaker()

            kwargs = {'a': 1, 'b': 2}

            self.assertEqual(kwargs, await breaker.call_async(func, **kwargs))


class CircuitBreakerTestCase(BaseTestCases.CircuitBreakerStorageBasedTestCase,
                             BaseTestCases.CircuitBreakerStorageBasedTestCaseAsync,
                             BaseTestCases.CircuitBreakerConfigurationTestCase,
                             BaseTestCases.CircuitBreakerConfigurationTestCaseAsync):
    """
    Tests for the CircuitBreaker class.
    """

    def setUp(self):
        super(CircuitBreakerTestCase, self).setUp()
        self._breaker_kwargs = {}

    @property
    def breaker_kwargs(self):
        return self._breaker_kwargs

    def test_create_new_state__bad_state(self):
        breaker = CircuitBreaker()
        with self.assertRaises(ValueError):
            breaker._create_new_state('foo')

    @patch('pybreaker.CircuitOpenState')
    def test_notify_not_called_on_init(self, open_state):
        storage = CircuitMemoryStorage('open')
        breaker = CircuitBreaker(state_storage=storage)
        open_state.assert_called_once_with(breaker, prev_state=None, notify=False)

    @patch('pybreaker.CircuitOpenState')
    def test_notify_called_on_state_change(self, open_state):
        storage = CircuitMemoryStorage('closed')
        breaker = CircuitBreaker(state_storage=storage)
        prev_state = breaker.state
        breaker.state = 'open'
        open_state.assert_called_once_with(breaker, prev_state=prev_state, notify=True)

    def test_failure_count_not_reset_during_creation(self):
        for state in (STATE_OPEN, STATE_CLOSED, STATE_HALF_OPEN):
            storage = CircuitMemoryStorage(state)
            storage.increment_counter()

            breaker = CircuitBreaker(state_storage=storage)
            self.assertEqual(breaker.state.name, state)
            self.assertEqual(breaker.fail_counter, 1)


class CircuitBreakerRedisTestCase(BaseTestCases.CircuitBreakerStorageBasedTestCase,
                                  BaseTestCases.CircuitBreakerStorageBasedTestCaseAsync):
    """
    Tests for the CircuitBreaker class.
    """

    def setUp(self):
        self.redis = fakeredis.FakeStrictRedis()
        self._breaker_kwargs = {'state_storage': CircuitRedisStorage('closed', self.redis)}

    @property
    def breaker_kwargs(self):
        return self._breaker_kwargs

    def tearDown(self):
        self.redis.flushall()

    def test_namespace(self):
        self.redis.flushall()
        self._breaker_kwargs = {'state_storage': CircuitRedisStorage('closed', self.redis, namespace='my_app')}
        breaker = CircuitBreaker(**self.breaker_kwargs)

        def func(): raise NotImplementedError()

        self.assertRaises(NotImplementedError, breaker.call, func)
        keys = self.redis.keys()
        self.assertEqual(2, len(keys))
        self.assertTrue(keys[0].decode('utf-8').startswith('my_app'))
        self.assertTrue(keys[1].decode('utf-8').startswith('my_app'))

    def test_fallback_state(self):
        def func(_):
            raise RedisError()

        logger = logging.getLogger('pybreaker')
        logger.setLevel(logging.FATAL)
        self._breaker_kwargs = {
            'state_storage': CircuitRedisStorage('closed', self.redis, fallback_circuit_state='open')
        }

        breaker = CircuitBreaker(**self.breaker_kwargs)

        with patch.object(self.redis, 'get', new=func):
            state = breaker.state
            self.assertEqual('open', state.name)

    def test_missing_state(self):
        """CircuitBreakerRedis: If state on Redis is missing, it should set the
        fallback circuit state and reset the fail counter to 0.
        """

        def func():
            raise DummyException()

        self._breaker_kwargs = {
            'state_storage': CircuitRedisStorage('closed', self.redis, fallback_circuit_state='open')
        }

        breaker = CircuitBreaker(**self.breaker_kwargs)
        self.assertRaises(DummyException, breaker.call, func)
        self.assertEqual(1, breaker.fail_counter)

        with patch.object(self.redis, 'get', new=lambda k: None):
            state = breaker.state
            self.assertEqual('open', state.name)
            self.assertEqual(0, breaker.fail_counter)


class CircuitBreakerThreadsTestCase(TestCase):
    """
    Tests to reproduce common synchronization errors on CircuitBreaker class.
    """

    @staticmethod
    def _start_threads(target, n):
        """
        Starts `n` threads that calls `target` and waits for them to finish.
        """
        threads = [threading.Thread(target=target) for _ in range(n)]

        for t in threads:
            t.start()

        for t in threads:
            t.join()

    def test_fail_thread_safety(self):
        """
        It should compute a failed call atomically to avoid race conditions.
        """

        # Create a specific exception to avoid masking other errors
        class SpecificException(Exception):
            pass

        breaker = CircuitBreaker(fail_max=3000, reset_timeout=timedelta(seconds=1))

        @breaker
        def err():
            raise SpecificException()

        def trigger_error():
            for n in range(500):
                try:
                    err()
                except SpecificException:
                    pass

        def _inc_counter():
            c = breaker._state_storage._fail_counter
            sleep(0.00005)
            breaker._state_storage._fail_counter = c + 1

        breaker._inc_counter = _inc_counter
        self._start_threads(trigger_error, 3)
        self.assertEqual(1500, breaker.fail_counter)

    def test_success_thread_safety(self):
        """It should compute a successful call atomically to avoid race conditions."""

        breaker = CircuitBreaker(fail_max=3000, reset_timeout=timedelta(seconds=1))

        @breaker
        def suc():
            return True

        def trigger_success():
            for n in range(500):
                suc()

        class SuccessListener(CircuitBreakerListener):
            def success(self, breaker):
                c = 0
                if hasattr(breaker, '_success_counter'):
                    c = breaker._success_counter
                sleep(0.00005)
                breaker._success_counter = c + 1

        breaker.add_listener(SuccessListener())
        self._start_threads(trigger_success, 3)
        self.assertEqual(1500, breaker._success_counter)

    def test_half_open_thread_safety(self):
        """It should allow only one trial call when the circuit is half-open."""

        breaker = CircuitBreaker(fail_max=1, reset_timeout=timedelta(seconds=0.01))

        breaker.open()
        sleep(0.01)

        @breaker
        def err():
            raise DummyException()

        def trigger_failure():
            try:
                err()
            except DummyException:
                pass
            except CircuitBreakerError:
                pass

        class StateListener(CircuitBreakerListener):
            def __init__(self):
                self._count = 0

            def before_call(self, breaker, func, *args, **kwargs):
                sleep(0.00005)

            def state_change(self, breaker, old, new):
                if new.name == STATE_HALF_OPEN:
                    self._count += 1

        state_listener = StateListener()
        breaker.add_listener(state_listener)

        self._start_threads(trigger_failure, 5)
        self.assertEqual(1, state_listener._count)

    def test_fail_max_thread_safety(self):
        """It should not allow more failed calls than 'fail_max' setting."""
        breaker = CircuitBreaker()

        @breaker
        def err():
            raise DummyException()

        def trigger_error():
            for i in range(2000):
                try:
                    err()
                except DummyException:
                    pass
                except CircuitBreakerError:
                    pass

        class SleepListener(CircuitBreakerListener):
            def before_call(self, breaker, func, *args, **kwargs):
                sleep(0.00005)

        breaker.add_listener(SleepListener())
        self._start_threads(trigger_error, 3)
        self.assertEqual(breaker.fail_max, breaker.fail_counter)


class CircuitBreakerRedisConcurrencyTestCase(TestCase):
    """
    Tests to reproduce common concurrency between different machines
    connecting to redis. This is simulated locally using threads.
    """

    def setUp(self):
        self.redis = fakeredis.FakeStrictRedis()
        self.breaker_kwargs = {'fail_max': 3000, 'reset_timeout': 1,
                               'state_storage': CircuitRedisStorage('closed', self.redis)}
        self.breaker = CircuitBreaker(**self.breaker_kwargs)

    def tearDown(self):
        self.redis.flushall()

    @staticmethod
    def _start_threads(target, n):
        """
        Starts `n` threads that calls `target` and waits for them to finish.
        """
        threads = [threading.Thread(target=target) for _ in range(n)]

        for t in threads:
            t.start()

        for t in threads:
            t.join()

    def _mock_function(self, obj, func):
        """
        Replaces a bounded function in `self.breaker` by another.
        """
        setattr(obj, func.__name__, MethodType(func, self.breaker))

    def test_fail_thread_safety(self):
        """CircuitBreaker: it should compute a failed call atomically to
        avoid race conditions.
        """

        # Create a specific exception to avoid masking other errors
        class SpecificException(Exception):
            pass

        @self.breaker
        def err():
            raise SpecificException()

        def trigger_error():
            for n in range(500):
                try:
                    err()
                except SpecificException:
                    pass

        def _inc_counter(breaker):
            sleep(0.00005)
            breaker._state_storage.increment_counter()

        self._mock_function(self.breaker, _inc_counter)
        self._start_threads(trigger_error, 3)
        self.assertEqual(1500, self.breaker.fail_counter)

    def test_success_thread_safety(self):
        """CircuitBreaker: it should compute a successful call atomically
        to avoid race conditions.
        """

        @self.breaker
        def suc():
            return True

        def trigger_success():
            for n in range(500):
                suc()

        class SuccessListener(CircuitBreakerListener):
            def success(self, breaker):
                c = 0
                if hasattr(breaker, '_success_counter'):
                    c = breaker._success_counter
                sleep(0.00005)
                breaker._success_counter = c + 1

        self.breaker.add_listener(SuccessListener())
        self._start_threads(trigger_success, 3)
        self.assertEqual(1500, self.breaker._success_counter)

    def test_half_open_thread_safety(self):
        """
        it should allow only one trial call when the circuit is half-open.
        """
        breaker = CircuitBreaker(fail_max=1, reset_timeout=timedelta(seconds=0.01))

        breaker.open()
        sleep(0.01)

        @breaker
        def err():
            raise DummyException()

        def trigger_failure():
            try:
                err()
            except DummyException:
                pass
            except CircuitBreakerError:
                pass

        class StateListener(CircuitBreakerListener):
            def __init__(self):
                self._count = 0

            def before_call(self, breaker, func, *args, **kwargs):
                sleep(0.00005)

            def state_change(self, breaker, old, new):
                if new.name == STATE_HALF_OPEN:
                    self._count += 1

        state_listener = StateListener()
        breaker.add_listener(state_listener)

        self._start_threads(trigger_failure, 5)
        self.assertEqual(1, state_listener._count)

    def test_fail_max_thread_safety(self):
        """CircuitBreaker: it should not allow more failed calls than 'fail_max'
        setting. Note that with Redis, where we have separate systems
        incrementing the counter, we can get concurrent updates such that the
        counter is greater than the 'fail_max' by the number of systems. To
        prevent this, we'd need to take out a lock amongst all systems before
        trying the call.
        """

        breaker = CircuitBreaker()

        @breaker
        def err():
            raise DummyException()

        def trigger_error():
            for i in range(2000):
                try:
                    err()
                except DummyException:
                    pass
                except CircuitBreakerError:
                    pass

        class SleepListener(CircuitBreakerListener):
            def before_call(self, breaker, func, *args, **kwargs):
                sleep(0.00005)

        breaker.add_listener(SleepListener())
        num_threads = 3
        self._start_threads(trigger_error, num_threads)
        self.assertTrue(breaker.fail_counter < breaker.fail_max + num_threads)


if __name__ == "__main__":
    main()
