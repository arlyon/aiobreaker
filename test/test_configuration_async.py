from pytest import raises, mark

from aiobreaker import CircuitBreaker
from test.util import DummyException, func_succeed_async

pytestmark = mark.asyncio

async def test_call_with_no_args_async():
    """
    It should be able to invoke functions with no-args.
    """
    breaker = CircuitBreaker()
    assert await breaker.call_async(func_succeed_async)


async def test_call_with_args_async():
    """
    It should be able to invoke functions with args.
    """

    async def func(arg1, arg2):
        return arg1, arg2

    breaker = CircuitBreaker()
    assert (42, 'abc') == await breaker.call_async(func, 42, 'abc')


async def test_call_with_kwargs_async():
    """
    It should be able to invoke functions with kwargs.
    """

    async def func(**kwargs):
        return kwargs

    breaker = CircuitBreaker()
    kwargs = {'a': 1, 'b': 2}
    assert kwargs == await breaker.call_async(func, **kwargs)


async def test_decorator_async():
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

    assert 'Docstring' == suc.__doc__
    assert 'Docstring' == err.__doc__
    assert 'suc' == suc.__name__
    assert 'err' == err.__name__

    assert 0 == breaker.fail_counter

    with raises(DummyException):
        await err()

    assert 1 == breaker.fail_counter

    await suc()
    assert 0 == breaker.fail_counter
