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
import logging
import MySQLdb
import os
import re
import shutil
import signal
from subprocess import Popen, PIPE
import tempfile
import time
import warnings

from testify import TestCase
from testify import assert_equal
from testify import assert_gte
from testify import assert_in
from testify import assert_lte
from testify import assert_not_in
from testify import assert_not_reached
from testify import assert_raises
from testify import class_setup
from testify import class_teardown
from testify import setup
from testify import teardown
from testify import run

import doloop
from doloop import ONE_HOUR, ONE_DAY, ONE_WEEK

log = logging.getLogger('doloop_test')

WHITESPACE_RE = re.compile('\s+')

MAX_MYSQLD_STARTUP_TIME = 15

# This is the exception through on deadlock, which we should recover from
DEADLOCK_EXC = MySQLdb.OperationalError(
    1213, 'Deadlock found when trying to get lock; try restarting transaction')

# Use a fake error to avoid confusion if this gets thrown.
OTHER_EXC = MySQLdb.OperationalError(
    99999, 'Too many nines')


class ExceptionRaisingDbConnWrapper(object):

    def __init__(self, dbconn):
        self._dbconn = dbconn
        # an exception to raise on calls to execute()
        self._exc = None
        # countdown of # of calls to execute() before self._exc() is raised
        self._num_queries_to_exc = 0

    def raise_exception_later(self, exc, num_queries):
        self._num_queries_to_exc = num_queries
        self._exc = exc

    def maybe_raise_exception(self):
        if not self._exc:
            return

        self._num_queries_to_exc -= 1

        if self._num_queries_to_exc <= 0:
            self._num_queries_to_exc = 0
            exc = self._exc
            self._exc = None
            raise exc

    def cursor(self):
        return ExceptionRaisingCursorWrapper(self._dbconn.cursor(), self)

    def __getattr__(self, attr):
        return getattr(self._dbconn, attr)


class ExceptionRaisingCursorWrapper(object):

    def __init__(self, cursor, dbconn_wrapper):
        self._cursor = cursor
        self._dbconn_wrapper = dbconn_wrapper

    def execute(self, *args, **kwargs):
        self._dbconn_wrapper.maybe_raise_exception()
        return self._cursor.execute(*args, **kwargs)

    def __getattr__(self, attr):
        return getattr(self._cursor, attr)


class DoLoopTestCase(TestCase):

    # we put all these tests in the same TestCase so we

    @class_setup
    def start_mysql_daemon(self):
        d = self.mysql_dir = tempfile.mkdtemp()
        self.mysql_socket = os.path.join(self.mysql_dir, 'mysqld.sock')

        args = [
            'mysqld',
            '--datadir=' + d,
            '--innodb_file_per_table',
            '--log-error=' + os.path.join(d, 'mysqld.err.log'),
            '--log_bin=' + os.path.join(d, 'mysql-bin.log'),
            '--pid-file=' + os.path.join(d, 'mysqld.pid'),
            '--skip-external-locking',
            '--socket=' + self.mysql_socket,
            '--skip-grant-tables',
            '--skip-networking',

            # these are all innodb/mysql options that reduce the safety of MySQL
            # in the case of disk problems, but don't affect locking or general
            # correctness.
            '--innodb_flush_log_at_trx_commit=0', # don't issue fsyncs
            '--innodb_buffer_pool_size=128M',     # bumped up from default of 8M
            '--innodb_fast_shutdown=2',           # do less work on shutdown
            '--skip-innodb_checksums',            # don't do extra checksums
            '--sync-binlog=0',                    # don't fsync binlogs
        ]

        log.info('started mysqld in %s' % self.mysql_dir)
        self.mysqld_proc = Popen(args, stderr=PIPE, stdout=PIPE)
        # wait for mysqld to start up
        for _ in range(MAX_MYSQLD_STARTUP_TIME):
            if os.path.exists(self.mysql_socket):
                return
            log.info('%s does not yet exist, sleeping for 1 second' %
                     self.mysql_socket)
            time.sleep(1)

        log.warn("mysqld didn't start after %.1fs, something is wrong" %
                 MAX_MYSQLD_STARTUP_TIME)
        self.stop_mysql_daemon()
        raise Exception("mysqld didn't start after %.1fs" %
                        MAX_MYSQLD_STARTUP_TIME)

    @class_teardown
    def stop_mysql_daemon(self):
        log.info('shutting down mysqld')
        self.mysqld_proc.terminate()
        self.mysqld_proc.communicate()
        if self.mysqld_proc.returncode != 0:
            return 'mysqld exited with return code %d' % (
                self.mysqld_proc.returncode)
        log.info('deleting %s' % self.mysql_dir)
        shutil.rmtree(self.mysql_dir)

    def make_dbconn(self, raw=False):
        return MySQLdb.connect(unix_socket=self.mysql_socket, db='doloop')

    def create_doloop(self, table='loop', id_type='INT'):
        """Create a loop table in the `doloop` database, and return
        an object wrapping it. By default, this table will be named `loop`"""
        dbconn = self.make_dbconn()
        doloop.create(dbconn, table, id_type)
        return doloop.DoLoop(dbconn, table)

    def create_doloop_and_wrapped_dbconn(self, table='loop', id_type='INT'):
        """Create a loop table in the `doloop` database, and return
        an object wrapping it. By default, this table will be named `loop`"""
        dbconn = ExceptionRaisingDbConnWrapper(self.make_dbconn())
        doloop.create(dbconn, table, id_type)
        return doloop.DoLoop(dbconn, table), dbconn

    @setup
    def create_empty_doloop_db(self):
        """Create an empty database named `doloop`"""
        dbconn = MySQLdb.connect(unix_socket=self.mysql_socket)
        with warnings.catch_warnings():
            warnings.filterwarnings('ignore', category=MySQLdb.Warning)
            dbconn.cursor().execute('DROP DATABASE IF EXISTS `doloop`')
        dbconn.cursor().execute('CREATE DATABASE `doloop`')


    ### tests for create() ###

    def test_create_more_than_one_loop(self):
        dbconn = self.make_dbconn()

        # check that loops with different name are in fact distinct

        doloop.create(dbconn, 'foo_loop')
        foo_loop = doloop.DoLoop(dbconn, 'foo_loop')

        doloop.create(dbconn, 'bar_loop')
        bar_loop = doloop.DoLoop(dbconn, 'bar_loop')

        foo_loop.add(66)
        bar_loop.add(99)

        assert_equal(foo_loop.get(2), [66])
        assert_equal(bar_loop.get(2), [99])

    def test_create_ids_can_be_strings(self):
        dbconn = self.make_dbconn()

        guid_loop = self.create_doloop('guid_loop', id_type='VARCHAR(128)')
        guid_loop.add(['foo', 'bar', 'baz'])
        assert_equal(guid_loop.get(3), ['bar', 'baz', 'foo'])

    def test_create_case_insensitive_id_collation(self):
        id_type = 'VARCHAR(64) CHARACTER SET utf8 COLLATE utf8_unicode_ci'

        ci_loop = self.create_doloop('ci_loop', id_type='VARCHAR(64)')
        ci_loop.add(['aaa', 'Bbb'])
        assert_equal(ci_loop.add('AAA'), 0) # already added as "aaa"

        assert_equal(ci_loop.get(10), ['aaa', 'Bbb'])
        assert_equal(ci_loop.unlock('BBB'), 1) # "BBB" and "Bbb" are the same
        assert_equal(ci_loop.get(10), ['Bbb'])

        # Python dicts can't handle the case-insensitivity
        id_to_status = ci_loop.check(['aaa', 'bbb'])
        assert_in('aaa', id_to_status)
        assert_not_in('bbb', id_to_status,)
        assert_in('Bbb', id_to_status)
        assert_not_in('BBB', id_to_status)

        # so use a dict comprehension:
        id_lower_to_status = dict((id_.lower(), status)
                                  for id_, status in id_to_status.iteritems())
        assert_in('bbb', id_lower_to_status)
        assert_in('Bbb'.lower(), id_lower_to_status)
        assert_in('BBB'.lower(), id_lower_to_status)

    def test_create_table_must_be_a_string(self):
        assert_raises(TypeError,
                      doloop.create, 'foo_loop', self.make_dbconn())

    def test_create_doesnt_retry_on_deadlock(self):
        # creating a table shouldn't raise a deadlock. If it does,
        # something is seriously wrong
        dbconn = ExceptionRaisingDbConnWrapper(self.make_dbconn())

        dbconn.raise_exception_later(DEADLOCK_EXC, num_queries=1)

        assert_raises(MySQLdb.OperationalError,
                      doloop.create, dbconn, 'foo_loop')

    ### tests for add() ###

    def test_add_nothing(self):
        loop = self.create_doloop()
        assert_equal(loop.add([]), 0)

    def test_add(self):
        loop = self.create_doloop()
        assert_equal(loop.add(42), 1)

        assert_equal(loop.get(10), [42])
        assert_equal(loop.add(42), 0) # already added

        assert_equal(loop.add([42, 43]), 1) # 1 already added
        assert_equal(loop.get(10), [43])

        # test sets
        assert_equal(loop.add(set([43, 44, 45])), 2) # 1 already added
        assert_equal(loop.get(10), [44, 45])

        # test tuples
        assert_equal(loop.add((46, 47, 48)), 3)
        assert_equal(loop.get(10), [46, 47, 48])

    def test_add_updated(self):
        loop = self.create_doloop()
        assert_equal(loop.add([42, 43], updated=True), 2)

        assert_equal(loop.get(10), [])

        # if we ignore update time, we can get the IDs, once
        assert_equal(loop.get(1, min_loop_time=0), [42])
        assert_equal(loop.get(1, min_loop_time=0), [43])
        assert_equal(loop.get(1, min_loop_time=0), [])

    def test_add_table_must_be_a_string(self):
        assert_raises(TypeError,
                      doloop.add, self.make_dbconn(), 999, 'foo_loop')

    def test_add_recovers_correctly_from_deadlocks(self):
        loop, dbconn = self.create_doloop_and_wrapped_dbconn()

        dbconn.raise_exception_later(DEADLOCK_EXC, num_queries=3)

        assert_equal(loop.add(42), 1)
        assert_equal(loop.get(10), [42])

    def test_add_lets_other_errors_through(self):
        loop, dbconn = self.create_doloop_and_wrapped_dbconn()

        dbconn.raise_exception_later(OTHER_EXC, num_queries=3)

        assert_raises(MySQLdb.OperationalError, loop.add, 42)


    ### tests for remove() ###

    def test_remove_nothing(self):
        loop = self.create_doloop()
        assert_equal(loop.remove([]), 0)

    def test_remove(self):
        loop = self.create_doloop()
        loop.add(range(10, 15))

        assert_equal(loop.remove(10), 1) # remove one
        assert_equal(loop.remove(10), 0)
        assert_equal(loop.remove([11, 13]), 2) # remove a list
        assert_equal(loop.remove([11, 13]), 0)
        assert_equal(loop.remove(set([11, 12, 13])), 1) # remove a set
        assert_equal(loop.remove(set([11, 12, 13])), 0)

        assert_equal(loop.get(10), [14])

    def test_remove_table_must_be_a_string(self):
        assert_raises(TypeError,
                      doloop.remove, self.make_dbconn(), 999, 'foo_loop')


    def test_remove_recovers_correctly_from_deadlocks(self):
        loop, dbconn = self.create_doloop_and_wrapped_dbconn()

        loop.add(range(10, 15))

        dbconn.raise_exception_later(DEADLOCK_EXC, num_queries=3)

        assert_equal(loop.remove([10, 12]), 2)
        assert_equal(loop.get(10), [11, 13, 14])

    def test_remove_lets_other_errors_through(self):
        loop, dbconn = self.create_doloop_and_wrapped_dbconn()

        loop.add(range(10, 15))

        dbconn.raise_exception_later(OTHER_EXC, num_queries=3)

        assert_raises(MySQLdb.OperationalError, loop.remove, [10, 12])


    ### tests for get() ###

    def test_get_from_empty(self):
        loop = self.create_doloop()

        assert_equal(loop.get(100), [])

    def test_get_locks_ids(self):
        loop = self.create_doloop()
        loop.add(range(10, 25))

        assert_equal(loop.get(10), range(10, 20))
        assert_equal(loop.get(10), range(20, 25))
        assert_equal(loop.get(10), [])

    def test_get_min_loop_time(self):
        loop = self.create_doloop()
        loop.add(range(10, 15))

        ids = loop.get(10)
        assert_equal(ids, range(10, 15))

        num_changed = loop.did(ids)
        assert_equal(num_changed, 5)

        # those IDs won't be ready for another hour
        assert_equal(loop.get(10), [])

        assert_equal(loop.get(10, min_loop_time=0), range(10, 15))

    def test_get_locks_expire_please_wait_3_secs_or_so(self):
        loop = self.create_doloop()
        loop.add(range(10, 15))

        assert_equal(loop.get(10, lock_for=2), range(10, 15))
        # IDs are locked; we can't get them
        assert_equal(loop.get(10), [])

        time.sleep(3)
        assert_equal(loop.get(10), range(10, 15))

    def test_get_prioritization_please_wait_1_sec_or_so(self):
        loop = self.create_doloop()

        loop.add(range(10, 20))
        loop.did(19)
        time.sleep(1.1) # make sure UNIX_TIMESTAMP() changes
        loop.did(13)
        loop.bump([14, 17])
        loop.bump([15, 11], lock_for=ONE_HOUR) # lock for an hour
        loop.bump([16, 12], lock_for=-ONE_HOUR)

        # first get the stuff that was super-bumped, then the stuff
        # that was bumped, then the new stuff, then the stuff that's done
        # already
        assert_equal(loop.get(10, min_loop_time=0),
                     [12, 16, 14, 17, 10, 18, 19, 13])

    def test_get_table_must_be_a_string(self):
        assert_raises(TypeError,
                      doloop.get, self.make_dbconn(), 10, 'foo_loop')

    def test_get_lock_for_must_be_a_positive_number(self):
        loop = self.create_doloop()

        loop.get(10, lock_for=20)
        loop.get(10, lock_for=20.5)

        assert_raises(ValueError, loop.get, 10, lock_for=-600)
        assert_raises(ValueError, loop.get, 10, lock_for=0)

        assert_raises(TypeError, loop.get, 10, lock_for=None)
        assert_raises(TypeError, loop.get, 10, lock_for=[1, 2, 3])

    def test_get_limit_must_be_a_nonnegative_integer(self):
        loop = self.create_doloop()

        loop.get(10)
        loop.get(0)

        assert_raises(ValueError, loop.get, -1)

        assert_raises(TypeError, loop.get, 34.5)
        assert_raises(TypeError, loop.get, 24.0)
        assert_raises(TypeError, loop.get, None)
        assert_raises(TypeError, loop.get, [1, 2, 3])

    def test_get_min_loop_time_must_be_a_number(self):
        loop = self.create_doloop()
        loop.get(10, min_loop_time=20)
        loop.get(10, min_loop_time=20.5)
        loop.get(10, min_loop_time=0)
        loop.get(10, min_loop_time=-11.1) # negative is okay

        assert_raises(TypeError, loop.get, 10, min_loop_time=None)
        assert_raises(TypeError, loop.get, 10, min_loop_time=[1, 2, 3])

    def test_get_recovers_correctly_from_deadlocks(self):
        loop, dbconn = self.create_doloop_and_wrapped_dbconn()

        loop.add(range(10, 25))

        dbconn.raise_exception_later(DEADLOCK_EXC, num_queries=4)
        assert_equal(loop.get(10), range(10, 20))

    def test_get_lets_other_errors_through(self):
        loop, dbconn = self.create_doloop_and_wrapped_dbconn()

        loop.add(range(10, 25))

        dbconn.raise_exception_later(OTHER_EXC, num_queries=4)
        assert_raises(MySQLdb.OperationalError, loop.get, 10)


    ### tests for did() ###

    def test_did_nothing(self):
        loop = self.create_doloop()
        assert_equal(loop.did([]), 0)

    def test_did_please_wait_1_sec_or_so(self):
        loop = self.create_doloop()

        loop.add(range(10, 20))
        assert_equal(loop.did(11), 1)
        time.sleep(1.1) # make sure UNIX_TIMESTAMP() changes
        assert_equal(loop.did([11, 13, 15, 17, 19]), 5) # 11 is updated again

        assert_equal(loop.get(10), [10, 12, 14, 16, 18])

    def test_did_auto_add(self):
        loop = self.create_doloop()

        assert_equal(loop.get(10), [])

        assert_equal(loop.did(111), 1) # 111 auto-added
        loop.add(222)
        assert_equal(loop.did([222, 333], auto_add=False), 1) # no row for 333

        assert_equal(loop.get(10, min_loop_time=0), [111, 222])

    def test_did_table_must_be_a_string(self):
        assert_raises(TypeError,
                      doloop.did, self.make_dbconn(), 999, 'foo_loop')

    def test_did_recovers_correctly_from_deadlocks(self):
        loop, dbconn = self.create_doloop_and_wrapped_dbconn()
        loop.add(range(10, 20))

        dbconn.raise_exception_later(DEADLOCK_EXC, num_queries=4)

        # test auto_add as well
        assert_equal(loop.did([11, 13, 15, 17, 19, 1]), 6)

        assert_equal(loop.get(10, min_loop_time=0),
                     [10, 12, 14, 16, 18, 1, 11, 13, 15, 17])

    def test_did_lets_other_errors_through(self):
        loop, dbconn = self.create_doloop_and_wrapped_dbconn()
        loop.add(range(10, 20))

        dbconn.raise_exception_later(OTHER_EXC, num_queries=4)

        assert_raises(MySQLdb.OperationalError,
                      loop.did, [11, 13, 15, 17, 19, 1])


    ### tests for unlock() ###

    def test_unlock_nothing(self):
        loop = self.create_doloop()
        assert_equal(loop.did([]), 0)

    def test_unlock_table_must_be_a_string(self):
        assert_raises(TypeError,
                      doloop.unlock, self.make_dbconn(), 999, 'foo_loop')

    def test_unlock(self):
        loop = self.create_doloop()

        loop.add(range(10, 20))
        ids = loop.get(5)
        assert_equal(loop.unlock(ids), 5)
        assert_equal(loop.unlock(ids), 0) # already unlocked

        # unlocking doesn't re-prioritize the IDs since it doesn't touch
        # last_updated
        assert_equal(loop.get(10), range(10, 20))
        assert_equal(loop.get(10), [])

        # try unlocking just one ID
        assert_equal(loop.unlock(7), 1)
        assert_equal(loop.get(10), [7])

    def test_unlock_auto_add(self):
        loop = self.create_doloop()

        assert_equal(loop.get(10), [])

        loop.add(111)
        assert_equal(loop.unlock([111, 222]), 1) # 111 already added
        assert_equal(loop.unlock(333, auto_add=False), 0) # no row for 333

        assert_equal(loop.get(10), [111, 222])

    def test_unlock_recovers_correctly_from_deadlocks(self):
        loop, dbconn = self.create_doloop_and_wrapped_dbconn()

        loop.add(111)
        assert_equal(loop.unlock([111, 222]), 1) # 111 already added
        loop.bump(222) # lock 222

        dbconn.raise_exception_later(DEADLOCK_EXC, num_queries=3)

        assert_equal(loop.unlock([111, 222, 333]), 2) # 222 unlocked, 333 new
        assert_equal(loop.get(10), [111, 222, 333])

    def test_unlock_lets_other_errors_through(self):
        loop, dbconn = self.create_doloop_and_wrapped_dbconn()

        loop.add(111)
        assert_equal(loop.unlock([111, 222]), 1) # 111 already added
        loop.bump(222) # lock 222

        dbconn.raise_exception_later(OTHER_EXC, num_queries=3)

        assert_raises(MySQLdb.OperationalError, loop.unlock, [222, 333])


    ### tests for bump() ###

    def test_bump_nothing(self):
        loop = self.create_doloop()
        assert_equal(loop.bump([]), 0)

    def test_bump(self):
        loop = self.create_doloop()
        loop.add(range(10, 20))

        assert_equal(loop.bump(19), 1)
        assert_equal(loop.bump([17, 12], lock_for=-10), 2) # super-bump
        assert_equal(loop.bump([13, 18], lock_for=10), 2) # bump but lock

        assert_equal(loop.get(5), [12, 17, 19, 10, 11])

    def test_bump_same_id_twice_please_wait_4_secs_or_so(self):
        loop = self.create_doloop()
        loop.add(range(10, 20))

        assert_equal(loop.bump(17, lock_for=4), 1)
        assert_equal(loop.get(1), [10]) # 17 is bumped but locked

        time.sleep(2.1)
        assert_equal(loop.bump(17, lock_for=4), 0) # don't re-bump
        assert_equal(loop.get(1), [11]) # 17 is bumped but locked

        time.sleep(2)
        assert_equal(loop.get(1), [17]) # lock on 17 has expired

    def test_bump_auto_add(self):
        loop = self.create_doloop()
        loop.add(range(10, 20))

        assert_equal(loop.bump([7, 17]), 2) # 7 is auto-added
        assert_equal(loop.bump([19, 25], lock_for=-10, auto_add=False),
                     1) # no row for 25
        assert_equal(loop.get(5), [19, 7, 17, 10, 11])

    def test_bump_table_must_be_a_string(self):
        assert_raises(TypeError,
                      doloop.bump, self.make_dbconn(), 999, 'foo_loop')

    def test_bump_min_loop_time_must_be_a_number(self):
        loop = self.create_doloop()
        loop.add(17)

        loop.bump(17, lock_for=20)
        loop.bump(17, lock_for=20.5)
        loop.bump(17, lock_for=0)
        loop.bump(17, lock_for=-11.1) # negative is okay

        assert_raises(TypeError, loop.bump, 17, lock_for=None)
        assert_raises(TypeError, loop.bump, 17, lock_for=[1, 2, 3])

    def test_bump_recovers_correctly_from_deadlock(self):
        loop, dbconn = self.create_doloop_and_wrapped_dbconn()

        loop.add(range(10, 15))

        dbconn.raise_exception_later(DEADLOCK_EXC, num_queries=4)

        assert_equal(loop.bump([12, 16]), 2)
        assert_equal(loop.get(5), [12, 16, 10, 11, 13])

    def test_bump_lets_other_errors_through(self):
        loop, dbconn = self.create_doloop_and_wrapped_dbconn()

        loop.add(range(10, 15))

        dbconn.raise_exception_later(OTHER_EXC, num_queries=4)

        assert_raises(MySQLdb.OperationalError, loop.bump, [12, 16])


    ### tests for check() ###

    def test_check_nothing(self):
        loop = self.create_doloop()
        assert_equal(loop.check([]), {})

    def test_check(self):
        loop = self.create_doloop()
        loop.add(range(10, 20))

        # newly added IDs have no locked or updated time
        assert_equal(loop.check(10), {10: (None, None)})
        assert_equal(loop.check([18, 19]), {18: (None, None),
                                            19: (None, None)})
        assert_equal(loop.check(20), {}) # 20 doesn't exist
        assert_equal(loop.check([18, 19, 20]), {18: (None, None),
                                                19: (None, None)})

        assert_equal(loop.get(2), [10, 11])
        loop.did(11)
        loop.bump(12)

        id_to_status = loop.check([10, 11, 12])
        assert_equal(sorted(id_to_status), [10, 11, 12])

        # allow 2 seconds of wiggle room
        since_updated_10, locked_for_10 = id_to_status[10]
        assert_equal(since_updated_10, None)
        assert_gte(locked_for_10, ONE_HOUR-2)
        assert_lte(locked_for_10, ONE_HOUR)

        since_updated_11, locked_for_11 = id_to_status[11]
        assert_gte(since_updated_11, 0)
        assert_lte(since_updated_11, 2)
        assert_equal(locked_for_11, None)

        since_updated_12, locked_for_12 = id_to_status[12]
        assert_equal(since_updated_12, None)
        assert_gte(locked_for_12, -2)
        assert_lte(locked_for_12, 0)

    def test_check_table_must_be_a_string(self):
        assert_raises(TypeError,
                      doloop.check, self.make_dbconn(), 999, 'foo_loop')

    def test_check_recovers_correctly_from_deadlocks(self):
        loop, dbconn = self.create_doloop_and_wrapped_dbconn()
        loop.add(range(10, 20))

        dbconn.raise_exception_later(DEADLOCK_EXC, num_queries=3)

        assert_equal(loop.check([18, 19, 20]), {18: (None, None),
                                                19: (None, None)})

    def test_check_lets_other_errors_through(self):
        loop, dbconn = self.create_doloop_and_wrapped_dbconn()
        loop.add(range(10, 20))

        dbconn.raise_exception_later(OTHER_EXC, num_queries=3)

        assert_raises(MySQLdb.OperationalError, loop.check, [18, 19, 20])


    ### tests for stats() ###

    def test_stats_empty(self):
        loop = self.create_doloop()

        stats = loop.stats()

        assert_equal(stats, {
            'locked': 0,
            'bumped': 0,
            'updated': 0,
            'new': 0,
            'min_id': None,
            'max_id': None,
            'min_lock_time': 0.0, # times are 0.0, not None, for convenience
            'max_lock_time': 0.0,
            'min_bump_time': 0.0,
            'max_bump_time': 0.0,
            'min_update_time': 0.0,
            'max_update_time': 0.0,
            'delayed': {ONE_DAY: 0, ONE_WEEK: 0},
        })

    def test_stats_please_wait_1_sec_or_so(self):
        loop = self.create_doloop()
        loop.add(range(10, 20))

        assert_equal(loop.get(1), [10])
        loop.did(11)
        time.sleep(1.1) # wait for 11 to be at least 1 sec old
        loop.bump(12)
        loop.bump(13, lock_for=60)
        loop.bump([14, 15], lock_for=-60)

        stats = loop.stats(delay_thresholds=(1, 10))

        assert_equal(stats['locked'], 2) # 10 and 13
        assert_equal(stats['bumped'], 3) # 12, 14, and 15
        assert_equal(stats['updated'], 1) # 11
        assert_equal(stats['new'], 4) # 16-19

        assert_equal(stats['min_id'], 10)
        assert_equal(stats['max_id'], 19)

        # allow five seconds of wiggle room
        assert_gte(stats['min_lock_time'], 55) # 13
        assert_lte(stats['min_lock_time'], 60)
        assert_gte(stats['max_lock_time'], ONE_HOUR-6) # 10
        assert_lte(stats['max_lock_time'], ONE_HOUR-1)

        assert_gte(stats['min_bump_time'], 0) # 12
        assert_lte(stats['min_bump_time'], 5)
        assert_gte(stats['max_bump_time'], 60) # 14 and 15
        assert_lte(stats['max_bump_time'], 65)

        assert_gte(stats['min_update_time'], 1) # 10
        assert_lte(stats['min_update_time'], 6)
        assert_gte(stats['max_update_time'], 1) # 10
        assert_lte(stats['max_update_time'], 6)

        assert_equal(stats['delayed'], {1: 1, 10: 0}) # 11

        # check types
        for key, value in stats.iteritems():
            if key.endswith('_time'):
                assert isinstance(value, float), 'expected stats[%r] to be a float, not %r' % (key, value)
            elif key != 'delayed':
                assert isinstance(value, (int, long)), 'expected stats[%r] to be an integer, not %r' % (key, value)

    def test_stats_table_must_be_a_string(self):
        assert_raises(TypeError,
                      doloop.stats, 'foo_loop', self.make_dbconn())

    def test_stats_delay_thresholds_must_be_numbers(self):
        loop = self.create_doloop()

        loop.stats(delay_thresholds=(1, 10, 100))
        loop.stats(delay_thresholds=set([1000, 10000]))
        loop.stats(delay_thresholds=[1234.5]) # float is okay

        loop.stats(delay_thresholds=()) # empty is okay

        stats = loop.stats(delay_thresholds=100000) # single value is okay
        assert_equal(sorted(stats['delayed']), [100000])

        stats = loop.stats(delay_thresholds={1: 2, 3: 4}) # dict is okay
        assert_equal(sorted(stats['delayed']), [1, 3])

        assert_raises(TypeError, loop.stats, delay_thresholds=[[1, 2, 3]])
        assert_raises(TypeError, loop.stats, delay_thresholds='GNAR')

    def test_stats_doesnt_retry_on_deadlock(self):
        # stats() runs in READ UNCOMMITTED mode, so if we encounter a deadlock,
        # something is very wrong
        loop, dbconn = self.create_doloop_and_wrapped_dbconn()

        dbconn.raise_exception_later(DEADLOCK_EXC, num_queries=5)

        assert_raises(MySQLdb.OperationalError, loop.stats)


    ### tests for the DoLoop wrapper object ###

    def test_wrapper_dbconn_can_be_a_callable(self):
        self.create_doloop('foo_loop')

        foo_loop = doloop.DoLoop(self.make_dbconn, 'foo_loop')

        foo_loop.add(range(10, 20))
        assert_equal(foo_loop.get(5), range(10, 15))

        def bad_conn():
            raise Exception("I'm sorry Dave, I'm afraid I can't do that.")

        # this is okay; bad conn isn't called yet
        foo_loop_bad = doloop.DoLoop(bad_conn, 'foo_loop')
        assert_raises(Exception, foo_loop_bad.add, range(10, 20))

    def test_wrapper_table_attribute(self):
        foo_loop = self.create_doloop('foo_loop')
        assert_equal(foo_loop.table, 'foo_loop')
        try:
            foo_loop.table = 'bar_loop'
            assert_not_reached('Should not be possible to set DoLoop.table!')
        except:
            pass

    def test_wrapper_table_must_be_a_string(self):
        # whoops, table and connection name are reversed
        assert_raises(TypeError, doloop.DoLoop, 'foo_loop', self.make_dbconn())
