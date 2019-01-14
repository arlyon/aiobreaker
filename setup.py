#!/usr/bin/env python
# -*- coding:utf-8 -*-

from setuptools import setup, find_packages
from aiobreaker import version


with open("readme.md", "r") as fh:
    long_description = fh.read()

setup(
    name='aiobreaker',
    version=version.__version__,
    url='https://github.com/arlyon/aiobreaker',
    license='BSD',
    authors='Alexander Lyon',
    author_email='arlyon@me.com',
    description='Python implementation of the Circuit Breaker pattern.',
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=find_packages(),
    py_modules=['aiobreaker'],
    install_requires=[],
    tests_require=['redis', 'fakeredis', 'pytest', 'pytest-asyncio'],
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.6',
        'Topic :: Software Development :: Libraries',
    ],
)
