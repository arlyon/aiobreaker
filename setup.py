#!/usr/bin/env python
# -*- coding:utf-8 -*-

from setuptools import setup

setup(
    name='pybreaker',
    version='0.4.4',
    description='Python implementation of the Circuit Breaker pattern',
    long_description=open('README.rst', 'r').read(),
    keywords=['design', 'pattern', 'circuit', 'breaker', 'integration'],
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.6',
        'Topic :: Software Development :: Libraries',
    ],
    platforms=[
        'Any',
    ],
    license='BSD',
    author='Daniel Fernandes Martins',
    author_email='daniel.tritone@gmail.com',
    url='http://github.com/danielfm/pybreaker',
    package_dir={'': 'src'},
    py_modules=['pybreaker'],
    include_package_data=True,
    zip_safe=False,
    test_suite='tests',
    tests_require=['redis', 'fakeredis', 'pytest', 'pytest-asyncio']
)
