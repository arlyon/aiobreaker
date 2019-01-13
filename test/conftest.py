from datetime import timedelta

from _pytest.fixtures import fixture
from fakeredis import FakeStrictRedis

from aiobreaker.state import CircuitBreakerState
from aiobreaker.storage.memory import CircuitMemoryStorage
from aiobreaker.storage.redis import CircuitRedisStorage

__all__ = ('redis_storage', 'storage', 'memory_storage', 'delta')


@fixture()
def redis_storage():
    redis = FakeStrictRedis()
    yield CircuitRedisStorage(CircuitBreakerState.CLOSED, redis)
    redis.flushall()


@fixture()
def memory_storage():
    return CircuitMemoryStorage(CircuitBreakerState.CLOSED)


@fixture(params=['memory_storage', 'redis_storage'])
def storage(request):
    return request.getfixturevalue(request.param)


@fixture()
def delta():
    return timedelta(seconds=1)
