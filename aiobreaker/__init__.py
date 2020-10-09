# -*- coding:utf-8 -*-

"""
Threadsafe pure-Python implementation of the Circuit Breaker pattern, described
by Michael T. Nygard in his book 'Release It!'.

For more information on this and other patterns and best practices, buy the
book at https://pragprog.com/titles/mnee2/release-it-second-edition/
"""

from .circuitbreaker import CircuitBreaker
from .listener import CircuitBreakerListener
from .state import CircuitBreakerError, CircuitBreakerState
from . import storage
