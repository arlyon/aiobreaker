# -*- coding:utf-8 -*-
import logging
import threading
from datetime import timedelta
from time import sleep
from unittest import TestCase, main
from unittest.mock import patch

import fakeredis
from pytest import raises
from redis.exceptions import RedisError

from aiobreaker import CircuitBreaker, CircuitBreakerError
from listener import CircuitBreakerListener
from state import STATE_OPEN, STATE_CLOSED, STATE_HALF_OPEN
from storage.redis import CircuitRedisStorage
from storage.memory import CircuitMemoryStorage


class CircuitBreakerRedisTestCase(CircuitBreakerStorageBasedTestCase,
                                  CircuitBreakerStorageBasedTestCaseAsync):
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

        logger = logging.getLogger('aiobreaker')
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

class CircuitBreakerRedisConcurrencyTestCase(TestCase):
    """
    Tests to reproduce common concurrency between different machines
    connecting to redis. This is simulated locally using threads.
    """

    def setUp(self):
        self.redis = fakeredis.FakeStrictRedis()
        self.storage = CircuitRedisStorage('closed', self.redis)

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

    def test_fail_thread_safety(self):
        """It should compute a failed call atomically to avoid race conditions."""

        breaker = CircuitBreaker(fail_max=3000, timeout_duration=timedelta(seconds=1), state_storage=self.storage)

        @breaker
        def err():
            raise DummyException()

        def trigger_error():
            for n in range(500):
                try:
                    err()
                except DummyException:
                    pass

        def _inc_counter():
            sleep(0.00005)
            breaker._state_storage.increment_counter()

        breaker._inc_counter = _inc_counter
        self._start_threads(trigger_error, 3)
        self.assertEqual(1500, breaker.fail_counter)

    def test_success_thread_safety(self):
        """It should compute a successful call atomically to avoid race conditions."""

        breaker = CircuitBreaker(fail_max=3000, timeout_duration=timedelta(seconds=1), state_storage=self.storage)

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
        """
        it should allow only one trial call when the circuit is half-open.
        """
        breaker = CircuitBreaker(fail_max=1, timeout_duration=timedelta(seconds=0.01))

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
