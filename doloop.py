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

Use :py:func:`~doloop.create` to create task loop tables. You should have one
table for each kind of updating you want to do.

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
__version__ = '0.2.0-dev'

from decimal import Decimal
import MySQLdb
import MySQLdb.constants.ER

#: One hour, in seconds
ONE_HOUR = 60 * 60

#: One day, in seconds
ONE_DAY = 60 * 60 * 24

#: One week, in seconds
ONE_WEEK = 60 * 60 * 24 * 7

_RECOVERABLE_MYSQL_ERROR_CODES = (
    MySQLdb.constants.ER.LOCK_DEADLOCK,
    MySQLdb.constants.ER.LOCK_WAIT_TIMEOUT,
)


### Utils ###

def _to_list(x):
    if isinstance(x, list):
        return x
    elif hasattr(x, '__iter__'):
        return list(x)
    else:
        return [x]


def _run(query, dbconn, level='REPEATABLE READ', read_only=False, retry=True):
    """Do a query in a transaction.

    :param dbconn: a :py:mod:`MySQLdb` connection object
    :param query: a function which takes a db cursor as its only argument
    :param string level: MySQL transaction isolation level
    :param bool read_only: rollback after the transaction
    :param bool retry: retry on deadlock or lock wait timeout

    If there is already a transaction in progress (i.e. you're using
    *dbconn* for non-:py:mod:`doloop` things), it'll be rolled back.
    """
    return _run_with_rollbacks(lambda cursor, _: query(cursor),
                               dbconn, level=level, read_only=read_only,
                               retry=retry)


def _run_with_rollbacks(query, dbconn, level='REPEATABLE READ',
                        read_only=False, retry=True):
    """Like :py:func:`~doloop._run`, except query takes an additional
    argument which is a function that can be called to roll back and restart
    the transaction.
    """
    def rollback_and_restart_transaction():
        dbconn.rollback()

        cursor = dbconn.cursor()
        cursor.execute('SET TRANSACTION ISOLATION LEVEL ' + level)
        cursor.execute('START TRANSACTION')

    while True:
        try:
            try:
                rollback_and_restart_transaction()

                result = query(
                    dbconn.cursor(), rollback_and_restart_transaction)

                if read_only:
                    dbconn.rollback()
                else:
                    dbconn.commit()

                return result
            except:
                dbconn.rollback()
                raise
        except MySQLdb.OperationalError, e:
            if not (retry and e.args[0] in _RECOVERABLE_MYSQL_ERROR_CODES):
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
            KEY `lock_until` (`lock_until`, `last_updated`)
        ) ENGINE=InnoDB

    * *id* is the ID of the thing you want to update. It can refer to anything
      that has a unique ID (doesn't need to be another table in this database).
      It also need not be an ``INT``; see *id_type*, below.
    * *last_updated*: a unix timestamp; when the thing was last updated, or
      ``NULL`` if it never was
    * *lock_until* is also a unix timestamp. It's used to keep workers from
      grabbing the same IDs, and prioritization. See :py:func:`~doloop.get`
      for details.

    :param dbconn: a :py:mod:`MySQLdb` connection object
    :param str table: name of your task loop table. Something ending in
                      ``_loop`` is recommended.
    :param str id_type: alternate type for the ``id`` field (e.g.
                        ``'VARCHAR(64)``')

    There is no ``drop()`` function because programmatically dropping tables
    is risky. The relevant SQL is just ``DROP TABLE `...```.
    """
    sql = sql_for_create(table, id_type=id_type)
    dbconn.cursor().execute(sql)


def sql_for_create(table, id_type='INT'):
    """Get SQL used by :py:func:`create`.

    Useful to power :command:`create-doloop-table` (included with this
    package), which you can use to pipe ``CREATE`` statements into
    :command:`mysql`.
    """
    _check_table_is_a_string(table)

    return """CREATE TABLE `%s` (
    `id` %s NOT NULL,
    `last_updated` INT default NULL,
    `lock_until` INT default NULL,
    PRIMARY KEY (`id`),
    KEY `lock_until` (`lock_until`, `last_updated`)
) ENGINE=InnoDB""" % (table, id_type)


### Adding and removing IDs ###

def add(dbconn, table, id_or_ids, updated=False):
    """Add IDs to this task loop.

    :param dbconn: a :py:mod:`MySQLdb` connection object
    :param str table: name of your task loop table
    :param id_or_ids: ID or list of IDs to add
    :param updated: Set this to true if these IDs have already been updated;
                    this will ``last_updated`` to the current time rather than
                    ``NULL``.

    :return: number of IDs that are new

    Runs this query in ``REPEATABLE READ`` mode, retrying on deadlock or lock
    wait timeout:

    .. code-block:: sql

        INSERT IGNORE INTO `...` (`id`, `last_updated`)
            VALUES (...), ...

    (`last_updated` is omitted if *updated* is ``False``.)
    """
    _check_table_is_a_string(table)

    ids = _to_list(id_or_ids)
    if not ids:
        return 0

    def query(cursor):
        return _add(cursor, table, ids, updated=updated)

    return _run(query, dbconn)


def _add(cursor, table, ids, updated=False):
    """Helper function to ``INSERT IGNORE`` IDs into the the table. By default,
    ``last_updated`` and ``lock_until`` will be ``NULL``.

    :param dbconn: a :py:mod:`MySQLdb` connection object
    :param str table: name of your task loop table
    :param id_or_ids: ID or list of IDs to add
    :param updated: Set ``last_updated`` to the current time rather than
                    ``NULL``.
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
    return cursor.rowcount


def remove(dbconn, table, id_or_ids):
    """Remove IDs from this task loop.

    :param dbconn: a :py:mod:`MySQLdb` connection object
    :param str table: name of your task loop table
    :param id_or_ids: ID or list of IDs to add

    :return: number of IDs removed

    Runs this query in ``REPEATABLE READ`` mode, retrying on deadlock or lock
    wait timeout:

    .. code-block:: sql

        DELETE FROM `...` WHERE `id` IN (...)
    """
    _check_table_is_a_string(table)

    ids = _to_list(id_or_ids)
    if not ids:
        return 0

    sql = 'DELETE FROM `%s` WHERE `id` IN (%s)' % (
        table, ', '.join('%s' for _ in ids))

    def query(cursor):
        cursor.execute(sql, ids)
        return cursor.rowcount

    return _run(query, dbconn)


### Getting and updating IDs ###

def get(dbconn, table, limit, lock_for=ONE_HOUR, min_loop_time=ONE_HOUR):
    """Get some IDs of things to update, and lock them.

    Generally, after you've updated IDs, you'll want to pass them
    to :py:func:`~doloop.did`.

    The rules for fetching IDs are:

    * First, fetch IDs which are locked but whose locks have expired. Start
      with the ones that have been locked the longest.
    * Then, fetch unlocked IDs. Start with those that have *never* been
      updated, then fetch the ones that have gone the longest without being
      updated.

    Ties are broken by ordering by ID.

    Note that because IDs whose locks have expired are selected first, the
    ``lock_until`` column can also be used to prioritize IDs; see
    :py:func:`bump`.

    :param dbconn: a :py:mod:`MySQLdb` connection object
    :param str table: name of your task loop table
    :param int limit: max number of IDs to fetch
    :param lock_for: a conservative upper bound for how long we expect to take
                     to update this ID, in seconds. Default is one hour. Must
                     be positive.
    :param min_loop_time: If a job is unlocked, make sure it was last updated
                          at least this many seconds ago, so that we don't spin
                          on the same IDs.

    :return: list of IDs

    Runs this query in ``REPEATABLE READ`` mode, retrying on deadlock or lock
    wait timeout:

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

    If the first query returns no rows, we roll back before continuing to
    avoid holding on to a gap lock (we only need to lock actual rows).
    Otherwise, all three queries are run in a single transaction.
    """
    # do type-checking up front, to avoid cryptic MySQL errors

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

    # bail out if no rows requested

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

    # this is a function because we need to know how many IDs there are
    def update_sql(ids):
        return ('UPDATE `%s` SET `lock_until` = UNIX_TIMESTAMP() + %%s'
                ' WHERE `id` IN (%s)' %
                (table, ', '.join('%s' for _ in ids)))

    def query(cursor, rollback_and_restart_transaction):
        ids = []

        cursor.execute(select_bumped, [limit])
        ids.extend(row[0] for row in cursor.fetchall())

        # if there are no locked IDs (the common case), don't hold on to
        # the gap lock; we only need to lock actual rows
        if not ids:
            rollback_and_restart_transaction()

        if len(ids) < limit:
            cursor.execute(select_unlocked,
                           [min_loop_time, limit - len(ids)])
            ids.extend(row[0] for row in cursor.fetchall())

        if not ids:
            return []

        cursor.execute(update_sql(ids), [lock_for] + ids)

        return ids

    return _run_with_rollbacks(query, dbconn)


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

    Runs this query in ``REPEATABLE READ`` mode, retrying on deadlock or lock
    wait timeout:

    .. code-block:: sql

        INSERT IGNORE INTO `...` (`id`) VALUES (...), ...

        UPDATE `...`
            SET `last_updated` = UNIX_TIMESTAMP(),
                `lock_until` = NULL
            WHERE `id` IN (...)

    (``INSERT IGNORE`` is only run if *auto_add* is ``True``.)
    """
    _check_table_is_a_string(table)

    ids = _to_list(id_or_ids)
    if not ids:
        return 0

    sql = ('UPDATE `%s` SET `last_updated` = UNIX_TIMESTAMP(),'
           ' `lock_until` = NULL'
           ' WHERE `id` IN (%s)' % (table,
                                    ', '.join('%s' for _ in ids)))

    def query(cursor):
        if auto_add:
            _add(cursor, table, ids)

        cursor.execute(sql, ids)
        return cursor.rowcount

    return _run(query, dbconn)


def unlock(dbconn, table, id_or_ids, auto_add=True):
    """Unlock IDs without marking them updated.

    Useful if you :py:func:`~doloop.get` IDs, but are then unable or unwilling
    to update them.

    :param dbconn: a :py:mod:`MySQLdb` connection object
    :param str table: name of your task loop table
    :param id_or_ids: ID or list of IDs
    :param bool auto_add: Add any IDs that are not already in the table.

    :return: number of rows updated (mostly useful as a sanity check)

    Runs this query in ``REPEATABLE READ`` mode, retrying on deadlock or lock
    wait timeout:

    .. code-block:: sql

        INSERT IGNORE INTO `...` (`id`) VALUES (...), ...

        UPDATE `...` SET `lock_until` = NULL
            WHERE `id` IN (...)

    (``INSERT IGNORE`` is only run if *auto_add* is ``True``)
    """
    _check_table_is_a_string(table)

    ids = _to_list(id_or_ids)
    if not ids:
        return 0

    sql = ('UPDATE `%s` SET `lock_until` = NULL'
           ' WHERE `id` IN (%s)' % (table, ', '.join('%s' for _ in ids)))

    def query(cursor):
        rowcount = 0

        # add to rowcount now, because the UPDATE statement below
        # won't actually change these new rows
        if auto_add:
            _add(cursor, table, ids)
            rowcount += cursor.rowcount

        cursor.execute(sql, ids)
        rowcount += cursor.rowcount

        return rowcount

    return _run(query, dbconn)


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

    Runs this query in ``REPEATABLE READ`` mode, retrying on deadlock or lock
    wait timeout:

    .. code-block:: sql

        INSERT IGNORE INTO `...` (`id`) VALUES (...), ...

        UPDATE `...`
            SET `lock_until` = UNIX_TIMESTAMP() + ...
            WHERE (`lock_until` IS NULL OR
                   `lock_until` > UNIX_TIMESTAMP() + ...)
                  AND `id` IN (...)

    (``INSERT IGNORE`` is only run if *auto_add* is ``True``)
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

    def query(cursor):
        if auto_add:
            _add(cursor, table, ids)
        cursor.execute(sql, [lock_for, lock_for] + ids)
        return cursor.rowcount

    return _run(query, dbconn)


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

    Runs this query in ``REPEATABLE READ`` mode, retrying on deadlock or lock
    wait timeout:

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

    def query(cursor):
        cursor.execute(sql, ids)
        return dict((id_, (since_updated, locked_for))
                    for id_, since_updated, locked_for in cursor.fetchall())

    return _run(query, dbconn, level='READ COMMITTED', read_only=True)


def stats(dbconn, table, delay_thresholds=None):
    """Get stats on the performance of the task loop as a whole.

    :param dbconn: a :py:mod:`MySQLdb` connection object
    :param str table: name of your task loop table
    :param delay_thresholds: enables the *delayed* stat; see below

    It returns a dictionary containing these keys:
    * **bumped**: number of IDs where ``lock_until`` is now or in the past
    * **delayed**: maps thresholds specified in *delay_thresholds*, which
      are numbers of seconds, to the number of IDs that were updated at least
      that long ago. If *delay_thresholds* is not set, this is ``{}``.
    * **locked**: number of IDs where ``lock_until`` is in the future
    * **min_bump_time**/**max_bump_time**: min/max number of seconds that any
      ID has been prioritized (``lock_until`` now or in the past)
    * **min_id**/**max_id**: min and max IDs (or ``None`` if table is empty)
    * **min_lock_time**/**max_lock_time**: min/max number of seconds that any
      ID is locked for
    * **min_update_time**/**max_update_time**: min/max number of seconds that
      an unlocked ID has gone since being updated
    * **new**: number of IDs where ``last_updated`` is ``NULL``
    * **total**: total number of IDs
    * **updated**: number of IDs where ``last_updated`` is not ``NULL``
    
    For convenience and readability, all times will be floating point numbers.

    Only *min_id* and *max_id* can be ``None`` (when the table is empty).
    Everything else defaults to zero.

    This function does not require write access to your database.

    Don't be surprised if you see minor discrepancies; this function runs
    four separate queries in ``READ UNCOMMITTED`` mode:

    .. code-block:: sql

        SELECT MIN(`id`), MAX(`id`) FROM `...`

        SELECT COUNT(*),
               SUM(IF(`last_updated` IS NULL, 1, 0)),
               UNIX_TIMESTAMP() - MIN(`last_updated`),
               UNIX_TIMESTAMP() - MAX(`last_updated`),
               SUM(IF(`last_updated` <= UNIX_TIMESTAMP() - ..., 1, 0)),
               SUM(IF(`last_updated` <= UNIX_TIMESTAMP() - ..., 1, 0)),
               ...
               FROM `...`

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
    """
    _check_table_is_a_string(table)

    delay_thresholds = _to_list(delay_thresholds or ())

    for threshold in delay_thresholds:
        if not isinstance(threshold, (int, long, float)):
            raise TypeError('delay_thresholds must be numbers, not %r' %
                            (threshold,))

    id_sql = 'SELECT MIN(`id`), MAX(`id`) FROM `%s`' % table

    delay_sql = ', SUM(IF(`last_updated` <= UNIX_TIMESTAMP() - %s, 1, 0))'

    # put all the stats that require scanning over the
    # (`lock_until`, `last_updated`) covering index in the same query
    updated_sql = ('SELECT COUNT(*),'
                    # new
                    ' SUM(IF(`last_updated` IS NULL, 1, 0)),'
                    # updated
                    ' UNIX_TIMESTAMP() - MIN(`last_updated`),'
                    ' UNIX_TIMESTAMP() - MAX(`last_updated`)' +
                    # delayed
                    delay_sql * len(delay_thresholds) +
                    ' FROM `%s`' % table)

    # "locked" and "bumped" queries are cheaper to handle separately;
    # usually this is a small minority of rows, and this saves us from having
    # to do another SUM(IF(...)) over the entire table
    locked_sql = ('SELECT COUNT(*), '
                  ' MIN(`lock_until`) - UNIX_TIMESTAMP(),'
                  ' MAX(`lock_until`) - UNIX_TIMESTAMP()'
                  ' FROM `%s` WHERE `lock_until` > UNIX_TIMESTAMP()' % table)

    bumped_sql = ('SELECT COUNT(*),'
                  ' UNIX_TIMESTAMP() - MAX(`lock_until`),'
                  ' UNIX_TIMESTAMP() - MIN(`lock_until`)'
                  ' FROM `%s` WHERE `lock_until` <= UNIX_TIMESTAMP()' % table)
    
    def query(cursor):
        r = {}  # results to return

        cursor.execute(id_sql)
        r['min_id'], r['max_id'] = cursor.fetchall()[0]

        cursor.execute(updated_sql, delay_thresholds)
        row = cursor.fetchall()[0]
        r['total'], r['new'], r['min_update_time'], r['max_update_time'] = (
            row[:4])
        
        # unpack "delayed" stats, and convert to float/int
        r['delayed'] = {}
        for threshold, amount in zip(delay_thresholds, row[4:]):
            # (SUM(IF(...)) can produce NULL, so convert None to 0)
            r['delayed'][float(threshold)] = int(amount or 0)

        cursor.execute(locked_sql)
        r['locked'], r['min_lock_time'], r['max_lock_time'] = (
            cursor.fetchall()[0])

        cursor.execute(bumped_sql)
        r['bumped'], r['min_bump_time'], r['max_bump_time'] = (
            cursor.fetchall()[0])

        # make sure times are always floats and other numbers are ints
        # make sure only "_id" stats can be None
        for key in r:
            if key.endswith('_time'):
                r[key] = float(r[key] or 0.0)
            elif r[key] is None and not key.endswith('_id'):
                r[key] = 0
            # (SUM(IF(...)) can produce Decimals with MySQLdb)
            elif isinstance (r[key], (long, Decimal)):
                r[key] = int(r[key])

        # compute derived stats now that everything is the right type
        r['updated'] = r['total'] - r['new']

        return r

    return _run(query, dbconn, level='READ UNCOMMITTED', read_only=True,
                retry=False) # no locking, so no plausible need to retry


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

        :param dbconn: a :py:mod:`MySQLdb` connection object, or a callable
                       that returns one. If you use a callable, it'll be called
                       *every time* a method is called on this object (put any
                       caching/pooling/etc. inside your callable)
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
        """Check the status of particular IDs.

        See :py:func:`~doloop.check` for details.
        """
        return check(self._make_dbconn(), self._table, id_or_ids)

    def stats(self, delay_thresholds=(ONE_DAY, ONE_WEEK,)):
        """Check on the performance of the task loop as a whole.

        See :py:func:`~doloop.stats` for details.
        """
        return stats(self._make_dbconn(), self._table, delay_thresholds)
