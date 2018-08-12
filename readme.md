# AioBreaker

PyBreaker is a Python implementation of the Circuit Breaker pattern, described
in Michael T. Nygard's book [Release It!](http://pragprog.com/titles/mnee/release-it).

Circuit breakers exist to allow one subsystem to fail without destroying
the entire system. This is done by wrapping dangerous operations
(typically integration points) with a component that can circumvent
calls when the system is not healthy.

This project is a fork of [pybreaker](github.com/danielfm/pybreaker)
by Daniel Fernandes Martins for a few reasons:

- learning python packaging
- learning python testing
- learning python documentation
- significant refactor when supporting asyncio
- removal of tornado support
- removal of python 2.7 - 3.4 support

## Features

- Configurable list of excluded exceptions (e.g. business exceptions)
- Configurable failure threshold and reset timeout
- Support for several event listeners per circuit breaker
- Can guard generator functions
- Functions and properties for easy monitoring and management
- Thread-safe
- Asyncio support
- Optional redis backing

## Requirements

All you need is `python 3.5` or higher.

## Installation

To install, simply download from pypi:

    $ pip install aiobreaker

## Usage

The first step is to create an instance of `CircuitBreaker` for each
integration point you want to protect against.

    from aiobreaker import CircuitBreaker

    # Used in database integration points
    db_breaker = CircuitBreaker(fail_max=5, reset_timeout=timedelta(seconds=60))

    @db_breaker
    async def outside_integration():
        """Hits the api"""
        ...

At that point, go ahead and get familiar with the documentation.
