# Copyright 2011 Yelp
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Task loop for keeping things updated.

Basic usage:

Use :py:func:`~doloop.create` to create task loop tables. You should have one table for each kind of updating you want to do.

Add the IDs of things you want to update with :py:func:`~doloop.add`.

Run one or more workers (e.g in a crontab), with code like this::

    foo_ids = doloop.get(dbconn, 'foo_loop', 100)

    for foo_id in foo_ids:
        # update foo_id
        ...

    doloop.did(dbconn, 'foo_loop', foo_ids)
"""
from __future__ import with_statement

__author__ = 'David Marin <dave@yelp.com>'
__version__ = '0.1'

from contextlib import contextmanager

#: One hour, in seconds
ONE_HOUR = 60*60

#: One day, in seconds
ONE_DAY = 60*60*24

#: One week, in seconds
ONE_WEEK = 60*60*24*7


### Utils ###

def _to_list(x):
    if isinstance(x, (list, tuple)):
        return x
    elif hasattr(x, '__iter__'):
        return list(x)
    else:
        return [x]


@contextmanager
def _trans(dbconn, level='REPEATABLE READ', read_only=False):
    try:
        cursor = dbconn.cursor()
        cursor.execute('SET TRANSACTION ISOLATION LEVEL ' + level)
        cursor.execute('START TRANSACTION')

        yield cursor

        if read_only:
            dbconn.rollback()
        else:
            dbconn.commit()
        
    except:
        dbconn.rollback()
        raise

def _check_table_is_a_string(table):
    """Check that table is a string, to avoid cryptic SQL errors"""
    if not isinstance(table, basestring):
        raise TypeError('table must be a string, not %r' % (table,))


### Creating a task loop ###

def create(dbconn, table, id_type='INT'):
    """Create a task loop table. It has a schema like this:

    .. code-block:: sql

        CREATE TABLE `...` (
            `id` INT NOT NULL,
            `last_updated` INT DEFAULT NULL,
            `lock_until` INT DEFAULT NULL,
            PRIMARY KEY (`id`),
            INDEX (`lock_until`, `last_updated`)
        ) ENGINE=InnoDB

    * *id* is the ID of the thing you want to update. It can refer to anything that has a unique ID (doesn't need to be another table in this database). It also need not be an ``INT``; see *id_type*, below.
    * *last_updated*: a unix timestamp; when the thing was last updated, or ``NULL`` if it never was
    * *lock_until* is also a unix timestamp. It's used to keep workers from grabbing the same IDs, and prioritization. See :py:func:`~doloop.get` for details.

    :param dbconn: a :py:mod:`MySQLdb` connection object
    :param str table: name of your task loop table. Something ending in ``_loop`` is recommended.
    :param str id_type: alternate type for the ``id`` field (e.g. ``'VARCHAR(64)``')

    There is no ``drop()`` function because programmatically dropping tables
    is risky. The relevant SQL is just ``DROP TABLE `...```.
    """
    sql = sql_for_create(table, id_type=id_type)
    dbconn.cursor().execute(sql)


def sql_for_create(table, id_type='INT'):
    """Get SQL used by :py:func:`create`.

    Useful to power :command:`create-doloop-table` (included with this package), which you can use to pipe ``CREATE`` statements into :command:`mysql`.
    """
    _check_table_is_a_string(table)

    return """CREATE TABLE `%s` (
    `id` %s NOT NULL,
    `last_updated` INT default NULL,
    `lock_until` INT default NULL,
    PRIMARY KEY (`id`),
    INDEX (`lock_until`, `last_updated`)
) ENGINE=InnoDB""" % (table, id_type)


### Adding and removing IDs ###

def add(dbconn, table, id_or_ids, updated=False):
    """Add IDs to this task loop.

    :param dbconn: a :py:mod:`MySQLdb` connection object
    :param str table: name of your task loop table
    :param id_or_ids: ID or list of IDs to add
    :param updated: Set this to true if these IDs have already been updated; this will ``last_updated`` to the current time rather than ``NULL``.

    :return: number of IDs that are new

    Runs this query in ``REPEATABLE READ`` mode:

    .. code-block:: sql

        INSERT IGNORE INTO `...` (`id`, `last_updated`)
            VALUES (...), ...

    (`last_updated` is omitted if *updated* is ``False``).
    """
    _check_table_is_a_string(table)

    ids = _to_list(id_or_ids)
    if not ids:
        return 0

    with _trans(dbconn) as cursor:
        _add(cursor, table, ids, updated=updated)
        return cursor.rowcount


def _add(cursor, table, ids, updated=False):
    """Helper function to ``INSERT IGNORE`` IDs into the the table. By default,
    ``last_updated`` and ``lock_until`` will be ``NULL``.

    :param dbconn: a :py:mod:`MySQLdb` connection object
    :param str table: name of your task loop table
    :param id_or_ids: ID or list of IDs to add
    :param updated: Set ``last_updated`` to the current time rather than ``NULL``.
    """
    assert ids
    assert isinstance(ids, list)

    if updated:
        cols = '(`id`, `last_updated`)'
        row_sql = '(%s, UNIX_TIMESTAMP())'
    else:
        cols = '(`id`)'
        row_sql = '(%s)'

    sql = ('INSERT IGNORE INTO `%s` %s VALUES %s' %
           (table, cols, ', '.join(row_sql for _ in ids)))

    cursor.execute(sql, ids)


def remove(dbconn, table, id_or_ids):
    """Remove IDs from this task loop.

    :param dbconn: a :py:mod:`MySQLdb` connection object
    :param str table: name of your task loop table
    :param id_or_ids: ID or list of IDs to add

    :return: number of IDs removed

    Runs this query in ``REPEATABLE READ`` mode:

    .. code-block:: sql

        DELETE FROM `...` WHERE `id` IN (...)
    """
    _check_table_is_a_string(table)

    ids = _to_list(id_or_ids)
    if not ids:
        return 0

    sql = 'DELETE FROM `%s` WHERE `id` IN (%s)' % (
        table, ', '.join('%s' for _ in ids))

    with _trans(dbconn) as cursor:
        cursor.execute(sql, ids)
        return cursor.rowcount


### Getting and updating IDs ###

def get(dbconn, table, limit, lock_for=ONE_HOUR, min_loop_time=ONE_HOUR):
    """Get some IDs of things to update and lock them.

    Generally, after you've updated IDs, you'll want to pass them
    to :py:func:`~doloop.did`

    The rules for fetching IDs are:
    * First, fetch IDs where ``lock_until`` is now or some time in the past, starting with IDs with the oldest ``lock_until`` time. This ensures that IDs don't stay locked forever if a worker gets some IDs and then dies.
    * Then, fetch unlocked IDs (with ``lock_until`` set to ``NULL``), with IDs that have never been updated (``last_updated`` set to ``NULL``).
    * Finally, fetch unlocked IDs starting IDs with the oldest ``last_updated`` time.

    (Note that this means that ``lock_until`` can also be used to prioritize IDs; see :py:func:`bump`.)

    :param dbconn: a :py:mod:`MySQLdb` connection object
    :param str table: name of your task loop table
    :param int limit: max number of IDs to fetch
    :param lock_for: a conservative upper bound for how long we expect to take to update this ID, in seconds. Default is one hour. Must be positive
    :param min_loop_time: If a job is unlocked, make sure it was last updated at least this many seconds ago, so that we don't spin on the same IDs.

    :return: list of IDs

    Runs these queries in ``REPEATABLE READ`` mode:

    .. code-block:: sql

        SELECT `id` FROM `...`
            WHERE `lock_until` <= UNIX_TIMESTAMP()
            ORDER BY `lock_until`, `last_updated`, `id`
            LIMIT ...
            FOR UPDATE

        SELECT `id` FROM `...`
            WHERE `lock_until` IS NULL
            AND (`last_updated` IS NULL
                 OR `last_updated` <= UNIX_TIMESTAMP() - ...)
            ORDER BY `last_updated`, `id`
            LIMIT ...
            FOR UPDATE

        UPDATE `...` SET `lock_until` = UNIX_TIMESTAMP() + ...
            WHERE `id` IN (...)

    (Note that all unlocked IDs are handled by a single query.)
    """
    _check_table_is_a_string(table)

    if not isinstance(lock_for, (int, long, float)):
        raise TypeError('lock_for must be a number, not %r' % (lock_for,))

    if not lock_for > 0:
        raise ValueError('lock_for must be positive, not %d' % (lock_for,))
    
    if not isinstance(min_loop_time, (int, long, float)):
        raise TypeError('min_loop_time must be a number, not %r' %
                        (min_loop_time,))

    if not isinstance(limit, (int, long)):
        raise TypeError('limit must be an integer, not %r' % (limit,))

    if not limit >= 0:
        raise ValueError('limit must not be negative, was %r' % (limit,))


    if limit == 0:
        return []

    # order by ID as a tie-breaker, to make tests consistent

    select_bumped = ('SELECT `id` FROM `%s`'
                     ' WHERE `lock_until` <= UNIX_TIMESTAMP()'
                     ' ORDER BY `lock_until`, `last_updated`, `id`'
                     ' LIMIT %%s'
                     ' FOR UPDATE' % (table,))

    select_unlocked = ('SELECT `id` FROM `%s`'
                       ' WHERE `lock_until` IS NULL'
                       ' AND (`last_updated` IS NULL OR'
                       ' `last_updated` <= UNIX_TIMESTAMP() - %%s)'
                       ' ORDER BY `last_updated`, `id`'
                       ' LIMIT %%s'
                       ' FOR UPDATE' % (table,))

    ids = []

    # this is a function because we need to know how many IDs there are
    def update_sql():
        return ('UPDATE `%s` SET `lock_until` = UNIX_TIMESTAMP() + %%s'
                ' WHERE `id` IN (%s)' %
                (table, ', '.join('%s' for _ in ids)))

    with _trans(dbconn) as cursor:
        cursor.execute(select_bumped, [limit])
        ids.extend(row[0] for row in cursor.fetchall())

        if len(ids) < limit:
            cursor.execute(select_unlocked, [min_loop_time, limit - len(ids)])
            ids.extend(row[0] for row in cursor.fetchall())

        if not ids:
            return []

        cursor.execute(update_sql(), [lock_for] + ids)

        return ids


def did(dbconn, table, id_or_ids, auto_add=True):
    """Mark IDs as updated and unlock them.

    Usually, these will be IDs that you grabbed using :py:func:`~doloop.get`,
    but it's perfectly fine to update arbitrary IDs on your own initiative,
    and mark them as done.

    :param dbconn: a :py:mod:`MySQLdb` connection object
    :param str table: name of your task loop table
    :param id_or_ids: ID or list of IDs that we just updated
    :param bool auto_add: Add any IDs that are not already in the table.

    :return: number of rows updated (mostly useful as a sanity check)

    Runs this query in ``REPEATABLE READ`` mode:

    .. code-block:: sql

        UPDATE `...`
            SET `last_updated` = UNIX_TIMESTAMP(),
                `lock_until` = NULL
            WHERE `id` IN (...)
    """
    _check_table_is_a_string(table)

    ids = _to_list(id_or_ids)
    if not ids:
        return 0

    sql = ('UPDATE `%s` SET `last_updated` = UNIX_TIMESTAMP(),'
           ' `lock_until` = NULL'
           ' WHERE `id` IN (%s)' % (table,
                                    ', '.join('%s' for _ in ids)))

    with _trans(dbconn) as cursor:
        if auto_add:
            _add(cursor, table, ids)

        cursor.execute(sql, ids)
        return cursor.rowcount


def unlock(dbconn, table, id_or_ids, auto_add=True):
    """Unlock IDs without marking them updated.

    Useful if you :py:func:`~doloop.get` IDs, but are then unable or unwilling
    to update them.

    :param dbconn: a :py:mod:`MySQLdb` connection object
    :param str table: name of your task loop table
    :param id_or_ids: ID or list of IDs
    :param bool auto_add: Add any IDs that are not already in the table.

    :return: number of rows updated (mostly useful as a sanity check)

    Runs this query in ``REPEATABLE READ`` mode:

    .. code-block:: sql

        UPDATE `...`
            SET `lock_until` = NULL
            WHERE `id` IN (...)
    """
    _check_table_is_a_string(table)

    ids = _to_list(id_or_ids)
    if not ids:
        return 0

    sql = ('UPDATE `%s` SET `lock_until` = NULL'
           ' WHERE `id` IN (%s)' % (table, ', '.join('%s' for _ in ids)))

    with _trans(dbconn) as cursor:
        rowcount = 0

        # newly added rows already have lock_until set to NULL, so these
        # rows won't get hit by the UPDATE statement below
        if auto_add:
            _add(cursor, table, ids)
            rowcount += cursor.rowcount

        cursor.execute(sql, ids)
        rowcount += cursor.rowcount

        return rowcount


### Prioritization ###

def bump(dbconn, table, id_or_ids, lock_for=0, auto_add=True):
    """Bump priority of IDs.

    Normally we set ``lock_until`` to the current time, which gives them
    priority without actually locking them (see :py:func:`~doloop.get` for
    why this works).

    You can make IDs super-high-priority by setting *lock_for* to a
    negative value. For example, bumping an ID with ``lock_for=-600`` will
    give it the same priority as an ID that was bumped 600 seconds ago.

    You can also lock IDs for a little while, then prioritize them, by setting
    *lock_for* to a positive value. This can be useful in situations where
    you expect IDs might be bumped again in the near future, and you only
    want to run your update function once.

    This function will only ever *decrease* ``lock_until``; it's not
    possible to keep something locked forever by continually bumping it.

    :param dbconn: a :py:mod:`MySQLdb` connection object
    :param str table: name of your task loop table
    :param id_or_ids: ID or list of IDs
    :param lock_for: Number of seconds that the IDs should stay locked.
    :param bool auto_add: Add any IDs that are not already in the table.

    :return: number of IDs bumped (mostly useful as a sanity check)

    Runs this query in ``REPEATABLE READ`` mode:

    .. code-block:: sql

        UPDATE `...`
            SET `lock_until` = UNIX_TIMESTAMP() + ...
            WHERE (`lock_until` IS NULL OR
                   `lock_until` > UNIX_TIMESTAMP() + ...)
                  AND `id` IN (...)
    """
    _check_table_is_a_string(table)

    if not isinstance(lock_for, (int, long, float)):
        raise TypeError('lock_for must be a number, not %r' %
                        (lock_for,))

    ids = _to_list(id_or_ids)
    if not ids:
        return 0

    sql = ('UPDATE `%s` SET `lock_until` = UNIX_TIMESTAMP() + %%s'
           ' WHERE'
           ' (`lock_until` IS NULL OR'
           ' `lock_until` > UNIX_TIMESTAMP() + %%s)'
           ' AND `id` IN (%s)' %
           (table, ', '.join('%s' for _ in ids)))

    with _trans(dbconn) as cursor:
        if auto_add:
            _add(cursor, table, ids)

        cursor.execute(sql, [lock_for, lock_for] + ids)
        return cursor.rowcount


### Auditing ###

def check(dbconn, table, id_or_ids):
    """Check the status of particular IDs.

    :param dbconn: a :py:mod:`MySQLdb` connection object
    :param str table: name of your task loop table
    :param id_or_ids: ID or list of IDs

    Returns a dictionary mapping ID to a tuple of ``(since_updated,
    locked_for)``, that is, the current time minus ``last_updated``, and
    ``lock_for`` minus the current time (both of these in seconds).

    This function does not require write access to your database.

    Runs this query in ``READ COMMITTED`` mode:

    .. code-block:: sql

        SELECT `id`,
               UNIX_TIMESTAMP() - `last_updated`,
               `lock_until` - UNIX_TIMESTAMP()`
            FROM `...`
            WHERE `id` IN (...)
    """
    _check_table_is_a_string(table)

    ids = _to_list(id_or_ids)
    if not ids:
        return {}

    sql = ('SELECT `id`,'
           ' UNIX_TIMESTAMP() - `last_updated`,'
           ' `lock_until` - UNIX_TIMESTAMP()'
           ' FROM `%s` WHERE `id` IN (%s)' %
           (table, ', '.join('%s' for _ in ids)))

    with _trans(dbconn, level='READ COMMITTED', read_only=True) as cursor:
        cursor.execute(sql, ids)
        return dict((id_, (since_updated, locked_for))
                    for id_, since_updated, locked_for in cursor.fetchall())


def stats(dbconn, table, delay_thresholds=(ONE_DAY, ONE_WEEK,)):
    """Get stats on the performance of the task loop as a whole.

    :param dbconn: a :py:mod:`MySQLdb` connection object
    :param str table: name of your task loop table
    :param delay_thresholds: controls the *delayed* stat; see below

    This breaks down IDs into four categories:

    * **locked**: ``lock_until`` is some time in the future
    * **bumped**: ``lock_until`` is now or some time in the past.
    * **updated**: ``lock_until`` is ``NULL`` and ``last_updated`` is set
    * **new**: both ``lock_until`` and ``last_updated`` are ``NULL``

    It returns a dictionary mapping the name of each of these categories to the
    number of IDs in that category, plus these additional keys:

    * **min_id**/**max_id**: min and max IDs (or ``None`` if table is empty)
    * **min_lock_time**/**max_lock_time**: min/max times that any ID is locked for
    * **min_bump_time**/**max_bump_time**: min/max times that any ID has been prioritized (``lock_until`` now or in the past)
    * **min_update_time**/**max_update_time**: min/max times that an unlocked ID has gone since being updated
    * **delayed**: map from number of seconds to the number of unlocked IDs where the last time they were updated was at least that long ago. Default thresholds are one day and one week; you can control these with *delay_thresholds*

    For convenience and readability, all times will be floating point numbers.
    If there are no IDs in a particular category, the time will be ``0.0``,
    not ``None``.

    This function does not require write access to your database.

    Don't be surprised if you see minor discrepancies; this function runs
    several separate queries in ``READ UNCOMMITTED`` mode:

    .. code-block:: sql

        SELECT MIN(`id`), MAX(`id`) FROM `...`

        SELECT COUNT(*), 
               MIN(`lock_until`) - UNIX_TIMESTAMP(),
               MAX(`lock_until`) - UNIX_TIMESTAMP()
            FROM `...`
            WHERE `lock_until` > UNIX_TIMESTAMP()

        SELECT COUNT(*),
               UNIX_TIMESTAMP() - MAX(`lock_until`),
               UNIX_TIMESTAMP() - MIN(`lock_until`)
            FROM `...`
            WHERE `lock_until` <= UNIX_TIMESTAMP()

        SELECT COUNT(*),
               UNIX_TIMESTAMP() - MAX(`last_updated`),
               UNIX_TIMESTAMP() - MIN(`last_updated`)
            FROM `...`
            WHERE `lock_until` IS NULL
                  AND `last_updated` IS NOT NULL

         SELECT COUNT(*)
             FROM `...`
             WHERE `lock_until` IS NULL
                   AND `last_updated` IS NULL

         SELECT COUNT(*)
             FROM `...`
             WHERE `lock_until` IS NULL
                   AND `last_updated` <= UNIX_TIMESTAMP() - ...
    """
    _check_table_is_a_string(table)

    delay_thresholds = _to_list(delay_thresholds)

    for threshold in delay_thresholds:
        if not isinstance(threshold, (int, long, float)):
            raise TypeError('delay_thresholds must be numbers, not %r' %
                            (threshold,))

    id_sql = ('SELECT MIN(`id`), MAX(`id`) FROM `%s`' % table)

    locked_sql = ('SELECT COUNT(*), '
                  ' MIN(`lock_until`) - UNIX_TIMESTAMP(),'
                  ' MAX(`lock_until`) - UNIX_TIMESTAMP()'
                  ' FROM `%s` WHERE `lock_until` > UNIX_TIMESTAMP()' % table)

    bumped_sql = ('SELECT COUNT(*),'
                  ' UNIX_TIMESTAMP() - MAX(`lock_until`),'
                  ' UNIX_TIMESTAMP() - MIN(`lock_until`)'
                  ' FROM `%s` WHERE `lock_until` <= UNIX_TIMESTAMP()' % table)

    updated_sql = ('SELECT COUNT(*),'
                  ' UNIX_TIMESTAMP() - MAX(`last_updated`),'
                  ' UNIX_TIMESTAMP() - MIN(`last_updated`)'
                  ' FROM `%s` WHERE `lock_until` IS NULL'
                  ' AND `last_updated` IS NOT NULL' % table)

    new_sql = ('SELECT COUNT(*)'
               ' FROM `%s` WHERE `lock_until` IS NULL'
               ' AND `last_updated` IS NULL' % table)

    delayed_sql = ('SELECT COUNT(*)'
                  ' FROM `%s` WHERE `lock_until` IS NULL'
                  ' AND `last_updated` <= UNIX_TIMESTAMP() - %%s' % table)

    with _trans(dbconn, level='READ UNCOMMITTED', read_only=True) as cursor:
        r = {} # results to return

        cursor.execute(id_sql)
        r['min_id'], r['max_id'] = cursor.fetchall()[0]

        cursor.execute(locked_sql)
        r['locked'], r['min_lock_time'], r['max_lock_time'] = (
            cursor.fetchall()[0])

        cursor.execute(bumped_sql)
        r['bumped'], r['min_bump_time'], r['max_bump_time'] = (
            cursor.fetchall()[0])
        
        cursor.execute(updated_sql)
        r['updated'], r['min_update_time'], r['max_update_time'] = (
            cursor.fetchall()[0])
        
        cursor.execute(new_sql)
        r['new'] = cursor.fetchall()[0][0]

        r['delayed'] = {}
        for threshold in delay_thresholds:
            cursor.execute(delayed_sql, [threshold])
            r['delayed'][threshold] = cursor.fetchall()[0][0]

        # make sure times are always floats
        for key in r:
            if key.endswith('_time'):
                r[key] = float(r[key] or 0)

        return r


### Object-Oriented version ###

class DoLoop(object):
    """A very thin wrapper that stores connection and table name, so you
    don't have have to specify *dbconn* and *table* over and over again.

    For example::

        foo_loop = doloop.DoLoop(dbconn, 'foo_loop')

        foo_ids = foo_loop.get(100)

        for foo_id in foo_ids:
            # update foo_id
            ...

        foo_loop.did(foo_ids)
    """

    def __init__(self, dbconn, table):
        """Wrap a task loop table in an object

        :param dbconn: a :py:mod:`MySQLdb` connection object, or a callable that returns one (since it's kind of lame to store raw DB connections)
        :param string table: name of your task loop table

        You can read (but not change) the table name by calling ``self.table``
        """
        if hasattr(dbconn, '__call__'):
            self._make_dbconn = dbconn
        else:
            self._make_dbconn = lambda: dbconn

        _check_table_is_a_string(table)
        self._table = table

    @property
    def table(self):
        return self._table

    def add(self, id_or_ids, updated=False):
        """Add IDs to this task loop.

        See :py:func:`~doloop.add` for details.
        """
        return add(self._make_dbconn(), self._table, id_or_ids, updated)

    def remove(self, id_or_ids, updated=False):
        """Remove IDs from this task loop.

        See :py:func:`~doloop.remove` for details.
        """
        return remove(self._make_dbconn(), self._table, id_or_ids)

    def get(self, limit, lock_for=ONE_HOUR, min_loop_time=ONE_HOUR):
        """Get some IDs of things to update and lock them.

        See :py:func:`~doloop.get` for details.
        """
        return get(
            self._make_dbconn(), self._table, limit, lock_for, min_loop_time)

    def did(self, id_or_ids, auto_add=True):
        """Mark IDs as updated and unlock them.

        See :py:func:`~doloop.did` for details.
        """
        return did(self._make_dbconn(), self._table, id_or_ids, auto_add)

    def unlock(self, id_or_ids, auto_add=True):
        """Unlock IDs without marking them updated.

        See :py:func:`~doloop.unlock` for details.
        """
        return unlock(self._make_dbconn(), self._table, id_or_ids, auto_add)

    def bump(self, id_or_ids, lock_for=0, auto_add=True):
        """Bump priority of IDs.

        See :py:func:`~doloop.bump` for details.
        """
        return bump(
            self._make_dbconn(), self._table, id_or_ids, lock_for, auto_add)

    def check(self, id_or_ids):
        """Check the status of IDs.

        See :py:func:`~doloop.check` for details.
        """
        return check(self._make_dbconn(), self._table, id_or_ids)

    def stats(self, delay_thresholds=(ONE_DAY, ONE_WEEK,)):
        """Check on the performance of the task loop as a whole.

        See :py:func:`~doloop.stats` for details.
        """
        return stats(self._make_dbconn(), self._table, delay_thresholds)

