"""Task loop for keeping things updated.

Basic usage:

Use :py:func:`~doloop.create` to create task loop tables. You should have one table for each kind of updating you want to do.

Add the IDs of things you want to update with :py:func:`~doloop.add`.

Run one or more workers (e.g in a crontab), with code like this::

    biz_ids = doloop.get(dbconn, 'biz_reindex_loop', 500)

    for biz_id in biz_ids:
        # run your updating function
        reindex_biz(biz_id)

    doloop.did(foo_ids)
"""
from __future__ import with_statement

from contextlib import contextmanager


ONE_HOUR = 3600.0

### Utils ###

def _in(colname, values):
    """Create an IN statement, with the appropriate placeholders."""
    if not values:
        return 'FALSE'
    elif len(values) == 1:
        return colname + ' = %s'
    else:
        return '%s IN (%s)' % (colname, ', '.join('%s' for _ in values))


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
    """Create a task loop table. It has a schema like this::

        CREATE TABLE `foo_bazify_loop` (
            `id` INT NOT NULL,
            `last_updated` INT default NULL,
            `lock_until` INT default NULL,
            PRIMARY KEY (`id`),
            INDEX (`lock_until`, `last_updated`),'
            INDEX (`last_updated`)
        ) ENGINE=InnoDB

    * *id* is the ID of the thing you want to update. It can refer to anything that has a unique ID (doesn't need to be another table in this database). It also need not be an ``INT``, see *id_type*, below.
    * *last_updated* a unix timestamp, when the thing was last updated, or ``NULL`` if it never was
    * *lock_until* is also a unix timestamp. It's used to keep workers from grabbing the same IDs, and prioritization. See :py:func:`~doloop.get` for details.

    :param str table: name of your task loop table. It's recommended you name it ``<noun>_<verb>_loop`` (e.g. ``'biz_reindex_loop'``)
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


### Interfacing with the task queue ###

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
        sql = ('INSERT IGNORE INTO `%s` (`id`, `last_updated`)'
               ' VALUES %s' % (table,
                               ', '.join('(%s, UNIX_TIMESTAMP())'
                                         for _ in ids)))
    else:
        sql = ('INSERT IGNORE INTO `%s` (`id`)'
               ' VALUES %s' % (table, ', '.join('(%s)' for _ in ids)))

    with _trans(dbconn) as cursor:
        cursor.execute(sql, ids)
        return cursor.rowcount


def delete(dbconn, table, id_or_ids):
    """Remove IDs from this task loop.

    :param dbconn: a :py:mod:`MySQLdb` connection object
    :param str table: name of your task loop table
    :param id_or_ids: ID or list of IDs to add

    :return: number of IDs deleted
    """
    ids = _to_list(id_or_ids)
    if not ids:
        return 0

    sql = 'DELETE FROM `%s` WHERE `id` IN (%s)' % (
        table, ', '.join('%s' for _ in ids))

    with _trans(dbconn) as cursor:
        cursor.execute(sql)
        return cursor.rowcount


def get(dbconn, table, limit, lock_for=ONE_HOUR, min_loop_time=ONE_HOUR):
    """Get some IDs of things to update and lock them.

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

    def update_sql():
        return ('UPDATE `%s` SET `locked_until` = UNIX_TIMESTAMP() + %%s'
                ' WHERE `id` IN (%s)' %
                (table, ', '.join('%s' for _ in ids)))
    
    with _trans(dbconn) as cursor:
        cursor.execute(select1, limit)
        ids.extend(row[0] for row in cursor.fetchall())

        if len(ids) < limit:
            cursor.execute(select2, limit - len(ids))
            ids.extend(row[0] for row in cursor.fetchall())

        if len(ids) < limit:
            cursor.execute(select3, limit - len(ids))
            ids.extend(row[0] for row in cursor.fetchall())

        cursor.execute(update_sql(), lock_for, *ids)

        return ids


def did(dbconn, table, id_or_ids):
    """Mark IDs as updated and unlock them.

    :param dbconn: a :py:mod:`MySQLdb` connection object
    :param str table: name of your task loop table
    :param id_or_ids: ID or list of IDs that we just updated

    :return: number of IDs updated
    """
    ids = _to_list(id_or_ids)
    if not ids:
        return 0

    sql = ('UPDATE `%s` SET `last_updated` = UNIX_TIMESTAMP(),'
           ' `locked_until` = NULL'
           ' WHERE `id` IN (%s)' % (table,
                                    ', '.join('%s' for _ in ids)))

    with _trans(dbconn) as cursor:
        cursor.execute(sql, *ids)
        return cursor.rowcount


def unlock(dbconn, table, id_or_ids):
    """Unlock IDs without marking them updated (i.e. put them back on the
    queue)

    :param dbconn: a :py:mod:`MySQLdb` connection object
    :param str table: name of your task loop table
    :param id_or_ids: ID or list of IDs

    :return: number of IDs unlocked
    """
    ids = _to_list(id_or_ids)
    if not ids:
        return 0

    sql = ('UPDATE `%s` SET `locked_until` = NULL'
           ' WHERE `id` IN (%s)' % (table,
                                    ', '.join('%s' for _ in ids)))

    with _trans(dbconn) as cursor:
        cursor.execute(sql, *ids)
        return cursor.rowcount


def bump(dbconn, table, id_or_ids, lock_for=0, relock=False):
    """Bump priority of IDs.

    We actually do this by locking the IDs (see :py:func:`~doloop.get` for
    why this works).

    :param dbconn: a :py:mod:`MySQLdb` connection object
    :param str table: name of your task loop table
    :param id_or_ids: ID or list of IDs
    :param lock_for: How long the IDs should stay locked. Normally we just set ``lock_until`` to the current time. This can be useful if you think it's likely that you're going to want to bump the priority of the same ID(s) again, so that they don't get dequeued and updated several times. This can also be negative to give an ID even higher priority.
    :param relock: if this is ``True``, update ``lock_until`` even for IDs that are already locked. This is potentially dangerous if an ID is continually bumped, as it will stay locked forever.
    
    :return: number of IDs (re)locked
    """
    ids = _to_list(id_or_ids)
    if not ids:
        return 0

    sql = ('UPDATE `%s` SET `locked_until` = UNIX_TIMESTAMP() + %%s'
           ' WHERE `id` IN (%s)' % (table,
                                    ', '.join('%s' for _ in ids)))
    if not relock:
        sql += ' AND `locked_until` IS NULL'

    with _trans(dbconn) as cursor:
        cursor.execute(sql, *ids)
        return cursor.rowcount
    

