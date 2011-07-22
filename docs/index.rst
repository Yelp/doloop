.. doloop documentation master file, created by
   sphinx-quickstart on Fri Jul  8 14:14:31 2011.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

doloop - a task loop for keeping things updated
===============================================

What is doloop?
---------------

:py:mod:`doloop` is a simple but well-thought-out system for keeping things 
with unique IDs updated. It supports concurrency and prioritization.

:py:mod:`doloop`'s only dependency is MySQL; it does not require you to install or administer a separate server.

:py:mod:`doloop` is *not* ideal for queuing tasks that you only ever want 
to do once; for that, you might prefer
`Gearman <http://packages.python.org/gearman/>`_ or something similar.

Documentation
-------------

.. toctree::
   :maxdepth: 2
   :numbered:

   tutorial.rst
   functions.rst
   object-wrapper.rst






Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
