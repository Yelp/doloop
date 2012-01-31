Scripts
=======

create-doloop-table
-------------------

:command:`create-doloop-table` prints out ``CREATE TABLE`` statements for
one or more doloop tables (which track how recently IDs were updated).

Sample usage:

.. code-block:: sh

    create-doloop-table user_loop | mysql -D test  # or a db of your choice

which would pipe into :command:`mysql` something like this:

.. code-block:: sql

    CREATE TABLE `user_loop` (
        `id` INT NOT NULL,
        `last_updated` INT DEFAULT NULL,
        `lock_until` INT DEFAULT NULL,
        PRIMARY KEY (`id`),
        INDEX (`lock_until`, `last_updated`)
    ) ENGINE=InnoDB

You can set the type of the ``id`` column to something other than ``INT``
with the ``-i`` option, and the storage engine to something other than
``InnoDB`` with the ``-e`` option. For example:

.. code-block:: sh

    create-doloop-table -i 'CHAR(64) CHARSET ascii' -e MyISAM user_loop | mysql -D test
