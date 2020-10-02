Contributing to `aiobreaker`
============================

We love your input! We want to make contributing to this project as easy and transparent as possible, whether it's:

- Reporting a bug
- Discussing the current state of the code
- Submitting a fix
- Proposing new features
- Becoming a maintainer

Getting Started
---------------

This library has no direct dependencies, so all you need is to create a venv of your choice
and install the extra test dependencies via pip:

.. code:: bash

    pyenv virtualenv aiobreaker
    pip install -e '.[test]'
    pytest test
    mypy test

    # if you'd like to build the docs
    pip install -e '.[docs]'
    sphinx-build docs/source docs/build

License
-------

By contributing, you agree that your contributions will be licensed under its MIT License.