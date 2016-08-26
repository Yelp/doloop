doloop - a task loop for keeping things updated
===============================================

What is doloop?
---------------

:py:mod:`doloop` is a simple system for keeping things with unique IDs updated.
It is concurrency-safe and supports prioritization.

:py:mod:`doloop`'s only dependency is
`MySQL <http://dev.mysql.com>`_; it does not require you to
install or administer a separate server.

:py:mod:`doloop` works with any DBI-compliant Python MySQL library, including:

- `MySQL Connector <https://dev.mysql.com/downloads/connector/python/>`_
- `MySQL-Python <http://mysql-python.sourceforge.net/>`_
- `oursql <https://launchpad.net/oursql>`_
- `PyMySQL <https://github.com/PyMySQL/PyMySQL/>`_

Documentation
-------------

.. toctree::
   :maxdepth: 2
   :numbered:

   tutorial.rst
   functions.rst
   wrapper.rst
   scripts.rst
   utilities.rst

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
