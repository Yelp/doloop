.. doloop documentation master file, created by
   sphinx-quickstart on Fri Jul  8 14:14:31 2011.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

doloop - a task loop for keeping things updated
===============================================

What is doloop?
---------------

:py:mod:`doloop` is a simple system for keeping things with unique IDs updated.
It is concurrency-safe and supports prioritization.

:py:mod:`doloop`'s only dependency is
`MySQL <http://dev.mysql.com>`_; it does not require you to
install or administer a separate server.

:py:mod:`doloop` works with `MySQLdb <http://mysql-python.sourceforge.net/>`_
or any other DBI-compliant Python MySQL library (e.g.
`PyMySQL <https://github.com/petehunt/PyMySQL/>`_,
`oursql <https://launchpad.net/oursql>`_).

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

