from datetime import timedelta

from _pytest.fixtures import fixture
from fakeredis import FakeStrictRedis

from pybreaker import STATE_CLOSED
from storage.redis import CircuitRedisStorage
from storage.memory import CircuitMemoryStorage

__all__ = ('redis_storage', 'storage', 'memory_storage', 'delta')


@fixture()
def redis_storage():
    redis = FakeStrictRedis()
    yield CircuitRedisStorage(STATE_CLOSED, redis)
    redis.flushall()


@fixture(params=['memory_storage', 'redis_storage'])
def storage(request):
    return request.getfuncargvalue(request.param)


@fixture()
def memory_storage():
    return CircuitMemoryStorage(STATE_CLOSED)


@fixture()
def delta():
    return timedelta(seconds=1)
