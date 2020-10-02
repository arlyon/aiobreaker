#!/usr/bin/env python
# -*- coding:utf-8 -*-

from setuptools import setup, find_packages

from aiobreaker import version

with open("readme.rst", "r") as fh:
    long_description = fh.read()

test_dependencies = ['fakeredis', 'pytest>4', 'pytest-asyncio', 'mypy', 'pylint', 'safety', 'bandit', 'codecov', 'pytest-cov']
redis_dependencies = ['redis']
documentation_dependencies = ['sphinx', 'sphinx_rtd_theme', 'sphinx-autobuild', 'sphinx-autodoc-typehints']

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
    python_requires='>=3.5',
    install_requires=[],
    tests_require=test_dependencies,
    extras_require={
        'test': test_dependencies,
        'docs': documentation_dependencies,
        'redis': redis_dependencies,
    },
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.5',
        'Topic :: Software Development :: Libraries',
    ],
)
