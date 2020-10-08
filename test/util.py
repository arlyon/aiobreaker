from threading import Thread


class DummyException(Exception):
    """
    A more specific error to call during the tests.
    """

    def __init__(self, val=0):
        self.val = val


def func_exception():
    raise DummyException()


def func_succeed():
    return True


async def func_succeed_async():
    return True


async def func_exception_async():
    raise DummyException()


def func_succeed_counted():
    def func_succeed():
        func_succeed.call_count += 1
        return True

    func_succeed.call_count = 0

    return func_succeed


def func_succeed_counted_async():
    async def func_succeed_async():
        func_succeed_async.call_count += 1
        return True

    func_succeed_async.call_count = 0

    return func_succeed_async


def start_threads(target_function, n):
    """
    Starts `n` threads that calls the target function and waits for them to finish.
    """
    threads = [Thread(target=target_function) for _ in range(n)]

    for t in threads:
        t.start()

    for t in threads:
        t.join()
