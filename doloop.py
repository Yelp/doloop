"""Task loop for keeping things updated.

Basic usage:

Use :py:func:`~doloop.create` to create task loop tables. You should have one table for each kind of updating you want to do.

Add the IDs of things you want to update with :py:func:`~doloop.add`.

Run one or more workers (e.g in a crontab), with code like this::

    biz_ids = doloop.get(dbconn, 'biz_reindex_loop', 100)

    for biz_id in biz_ids:
        # run your updating function
        reindex_biz(biz_id)

    doloop.did(foo_ids)
"""
from __future__ import with_statement

from contextlib import contextmanager


ONE_HOUR = 3600.0


### Utils ###

def _to_list(x):
    if isinstance(x, (list, tuple)):
        return x
    elif hasattr(x, '__iter__'):
        return list(x)
    else:
        return [x]


@contextmanager
def _trans(dbconn):
    try:
        cursor = dbconn.cursor()
        cursor.execute('SET TRANSACTION ISOLATION LEVEL READ COMMITTED')
        cursor.execute('BEGIN TRANSACTION')

        yield cursor

        dbconn.commit()
        
    except:
        dbconn.rollback()
        raise


### Creating a task loop ###

def create(cursor, table, id_type='INT'):
    """Create a task loop table. It has a schema like this:

    .. code-block sql::

        CREATE TABLE `foo_bazify_loop` (
            `id` INT NOT NULL,
            `last_updated` INT default NULL,
            `lock_until` INT default NULL,
            PRIMARY KEY (`id`),
            INDEX (`lock_until`, `last_updated`),'
            INDEX (`last_updated`)
        ) ENGINE=InnoDB

    * *id* is the ID of the thing you want to update. It can refer to anything that has a unique ID (doesn't need to be another table in this database). It also need not be an ``INT``, see *id_type*, below.
    * *last_updated*: a unix timestamp; when the thing was last updated, or ``NULL`` if it never was
    * *lock_until* is also a unix timestamp. It's used to keep workers from grabbing the same IDs, and prioritization. See :py:func:`~doloop.get` for details.

    :param str table: name of your task loop table. Something ending in ``_loop`` is recommended.
    :param str id_type: alternate type for the ``id`` field (e.g. ``'VARCHAR(64)``')

    There is no ``drop()`` function because programmatically dropping tables is risky. The relevant SQL is just ``DROP TABLE `foo_bazify_loop```.
    """
    sql = create_sql(table, id_type=id_type)
    cursor.execute(sql)


def create_sql(table, id_type='INT'):
    """Get SQL used by :py:func:`create`.

    Useful to power :command:`create-doloop-table` (included with this package), which you can use to pipe ``CREATE`` statements into :command:`mysql`.
    """
    return ('CREATE TABLE `%s` '
            '(`id` %s NOT NULL,'
            ' `last_updated` INT default NULL,'
            ' `lock_until` INT default NULL,'
            ' PRIMARY KEY (`id`),'
            ' INDEX (`lock_until`, `last_updated`),'
            ' INDEX (`last_updated`)'
            ') ENGINE=InnoDB' % (table, id_type))


### Adding and removing IDs ###

def add(dbconn, table, id_or_ids, updated=False):
    """Add IDs to this task loop.

    :param dbconn: a :py:mod:`MySQLdb` connection object
    :param str table: name of your task loop table
    :param id_or_ids: ID or list of IDs to add
    :param updated: Set this to true if these IDs have already been updated; this will ``last_updated`` to the current time rather than ``NULL``.

    :return: number of IDs that are new
    """
    ids = _to_list(id_or_ids)
    if not ids:
        return 0

    if updated:
        row_sql = '(%s, UNIX_TIMESTAMP())'
    else:
        row_sql = '(%s, NULL)'

    sql = ('INSERT IGNORE INTO `%s` (`id`, `last_updated`)'
           ' VALUES %s' % (table, ', '.join(row_sql for _ in ids)))

    with _trans(dbconn) as cursor:
        cursor.execute(sql, ids)
        return cursor.rowcount


def remove(dbconn, table, id_or_ids):
    """Remove IDs from this task loop.

    :param dbconn: a :py:mod:`MySQLdb` connection object
    :param str table: name of your task loop table
    :param id_or_ids: ID or list of IDs to add

    :return: number of IDs removed
    """
    ids = _to_list(id_or_ids)
    if not ids:
        return 0

    sql = 'DELETE FROM `%s` WHERE `id` IN (%s)' % (
        table, ', '.join('%s' for _ in ids))

    with _trans(dbconn) as cursor:
        cursor.execute(sql)
        return cursor.rowcount

### Getting and running tasks ###

def get(dbconn, table, limit, lock_for=ONE_HOUR, min_loop_time=ONE_HOUR):
    """Get some IDs of things to update and lock them.

    Generally, after you've updated IDs, you'll want to pass them
    to :py:func:`~doloop.did`

    The rules for fetching IDs are:
    * First, fetch IDs with ``locked_until`` in the past, starting with IDs with the oldest ``locked_until`` time. This ensures that IDs don't stay locked forever if a worker gets some IDs and then dies.
    * Then, fetch unlocked IDs (with ``locked_until`` set to ``NULL``), with IDs that have never been updated (``last_updated`` set to ``NULL``).
    * Finally, fetch unlocked IDs starting IDs with the oldest ``last_updated`` time.

    (Note that this means that ``locked_until`` can also be used to prioritize IDs; see :py:func:`bump`.)

    :param dbconn: a :py:mod:`MySQLdb` connection object
    :param str table: name of your task loop table
    :param int limit: max number of IDs to fetch
    :param lock_for: a conservative upper bound for how long we expect to take to update this ID, in seconds. Default is one hour.
    :param min_loop_time: If a job is unlocked, make sure it was last updated at least this many seconds ago, so that we don't spin on the same IDs.

    :return: list of IDs
    """
    if not limit:
        return []
        
    min_loop_time = min_loop_time or 0
    
    select1 = ('SELECT `id` FROM `%s`'
               ' WHERE `locked_until` <= UNIX_TIMESTAMP()'
               ' ORDER BY `locked_until` ASC, `last_updated` ASC'
               ' LIMIT %%s'
               ' FOR UPDATE' % (table,))
    
    select2 = ('SELECT `id` FROM `%s`'
               ' WHERE `locked_until` IS NULL'
               ' AND `last_updated` IS NULL'
               ' ORDER BY `last_updated`'
               ' LIMIT %%s'
               ' FOR UPDATE' % (table,))

    select3 = ('SELECT `id` FROM `%s`'
               ' WHERE `locked_until` IS NULL'
               ' AND `last_updated` <= UNIX_TIMESTAMP() - %%s'
               ' ORDER BY `last_updated`'
               ' LIMIT %%s'
               ' FOR UPDATE' % (table,))

    ids = []

    # this is a function because we need to know how many IDs there are
    def update_sql():
        return ('UPDATE `%s` SET `locked_until` = UNIX_TIMESTAMP() + %%s'
                ' WHERE `id` IN (%s)' %
                (table, ', '.join('%s' for _ in ids)))
    
    with _trans(dbconn) as cursor:
        cursor.execute(select1, [limit])
        ids.extend(row[0] for row in cursor.fetchall())

        if len(ids) < limit:
            cursor.execute(select2, [limit - len(ids)])
            ids.extend(row[0] for row in cursor.fetchall())

        if len(ids) < limit:
            cursor.execute(select3, [limit - len(ids)])
            ids.extend(row[0] for row in cursor.fetchall())

        if not ids:
            return []

        cursor.execute(update_sql(), [lock_for] + ids)

        return ids


def did(dbconn, table, id_or_ids):
    """Mark IDs as updated and unlock them.

    Usually, these will be IDs that you grabbed using :py:func:`~doloop.get`,
    but it's perfectly fine to update arbitrary IDs on your own initiative,
    and marke them as done.

    :param dbconn: a :py:mod:`MySQLdb` connection object
    :param str table: name of your task loop table
    :param id_or_ids: ID or list of IDs that we just updated

    :return: number of rows updated (mostly useful as a sanity check)
    """
    ids = _to_list(id_or_ids)
    if not ids:
        return 0

    sql = ('UPDATE `%s` SET `last_updated` = UNIX_TIMESTAMP(),'
           ' `locked_until` = NULL'
           ' WHERE `id` IN (%s)' % (table,
                                    ', '.join('%s' for _ in ids)))

    with _trans(dbconn) as cursor:
        cursor.execute(sql, ids)
        return cursor.rowcount


def unlock(dbconn, table, id_or_ids):
    """Unlock IDs without marking them updated (i.e. put them back on the
    queue)

    :param dbconn: a :py:mod:`MySQLdb` connection object
    :param str table: name of your task loop table
    :param id_or_ids: ID or list of IDs

    :return: number of rows updated (mostly useful as a sanity check)
    """
    ids = _to_list(id_or_ids)
    if not ids:
        return 0

    sql = ('UPDATE `%s` SET `locked_until` = NULL'
           ' WHERE `id` IN (%s)' % (table, ', '.join('%s' for _ in ids)))

    with _trans(dbconn) as cursor:
        cursor.execute(sql, ids)
        return cursor.rowcount


### Prioritization ###

def bump(dbconn, table, id_or_ids, lock_for=0):
    """Bump priority of IDs.

    Normally we set ``locked_until`` to the current time, which gives them
    priority without actually locking them (see :py:func:`~doloop.get` for
    why this works).

    You can make IDs super-high-priority by setting *lock_for* to a
    negative value.

    You can also lock IDs for a little while, then prioritize them, by setting
    *lock_for* to a positive value. This can be useful in situations where
    you expect IDs might be bumped again in the near future, and you only
    want to run your update function once.

    This function will only ever *decrease* ``locked_until``; it's not
    possible to keep something locked forever by continually bumping it.

    :param dbconn: a :py:mod:`MySQLdb` connection object
    :param str table: name of your task loop table
    :param id_or_ids: ID or list of IDs
    :param lock_for: Number of seconds that the IDs should stay locked.
    
    :return: number of IDs bumped (mostly useful as a sanity check)
    """
    ids = _to_list(id_or_ids)
    if not ids:
        return 0

    sql = ('UPDATE `%s` SET `locked_until` = UNIX_TIMESTAMP() + %%s'
           ' WHERE'
           ' (`locked_until` IS NULL OR'
           ' `locked_until` > UNIX_TIMESTAMP() + %%s)'
           ' AND `id` IN (%s)' %
           (table, ', '.join('%s' for _ in ids)))

    with _trans(dbconn) as cursor:
        cursor.execute(sql, [lock_for, lock_for] + ids)
        return cursor.rowcount
    

