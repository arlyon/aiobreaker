# -*- coding:utf-8 -*-

"""
Threadsafe pure-Python implementation of the Circuit Breaker pattern, described
by Michael T. Nygard in his book 'Release It!'.

For more information on this and other patterns and best practices, buy the
book at http://pragprog.com/titles/mnee/release-it
"""

from .circuitbreaker import CircuitBreaker
from .listener import CircuitBreakerListener
from .state import CircuitBreakerError

__all__ = ('CircuitBreakerError', 'CircuitBreaker', 'CircuitBreakerListener')
