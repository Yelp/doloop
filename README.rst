**doloop** is a simple but well-thought-out system for keeping things 
with unique IDs updated. It supports concurrency and prioritization.

**doloop**'s only dependency is 
`MySQL <http://dev.mysql.com>`_; it does not require you to
install or administer a separate server.

**doloop** works with `MySQLdb <http://mysql-python.sourceforge.net/>`_
or any other DBI-compliant Python MySQL library (e.g.
`PyMySQL <https://github.com/petehunt/PyMySQL/>`_,
`oursql <https://launchpad.net/oursql>`_).

**doloop** is *not* ideal for queuing tasks that you only ever want 
to do once; for that, you might prefer
`Gearman <http://packages.python.org/gearman/>`_ or something similar.

See http://packages.python.org/doloop for tutorial and documentation.
