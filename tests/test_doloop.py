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
from StringIO import StringIO
import logging
import optparse
import os
import re
import shutil
from subprocess import PIPE
from subprocess import Popen
import sys
import tempfile
import time
import warnings

try:
    import unittest2 as unittest
    unittest  # silence pyflakes warning
except ImportError:
    import unittest


import doloop
from doloop import DEFAULT_ID_TYPE
from doloop import DEFAULT_STORAGE_ENGINE
from doloop import ONE_HOUR
from doloop import _main_for_create_doloop_table

# support arbitrary MySQL drivers
PYTHON_MYSQL_MODULE = os.environ.get('PYTHON_MYSQL_MODULE') or 'MySQLdb'
__import__(PYTHON_MYSQL_MODULE)
mysql_module = sys.modules[PYTHON_MYSQL_MODULE]


log = logging.getLogger('doloop_test')

WHITESPACE_RE = re.compile('\s+')

MAX_MYSQLD_STARTUP_TIME = 15

# A DBI exception we're likely to encounter. Usually error code comes first,
# but we put it second to support mysql.connector; the other drivers don't seem
# to mind
LOCK_WAIT_TIMEOUT_EXC = mysql_module.OperationalError(
    'Lock wait timeout exceeded; try restarting transaction', 1205)


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


class DoLoopTestCase(unittest.TestCase):

    # we put all these tests in the same TestCase so we only have to
    # start up MySQL once

    @classmethod
    def setUpClass(self):
        # turn off warnings while testing
        warnings.simplefilter('ignore')

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

            # don't let tests hang forever if we've screwed up locking
            '--innodb_lock_wait_timeout=5',

            # these are all innodb/mysql options that reduce the safety of
            # MySQL in the case of disk problems, but don't affect locking or
            # general correctness.
            '--innodb_flush_log_at_trx_commit=0',  # don't issue fsyncs
            '--innodb_fast_shutdown=2',            # do less work on shutdown
            '--skip-innodb_checksums',             # don't do extra checksums
            '--sync-binlog=0',                     # don't fsync binlogs
        ]

        log.info('started mysqld in %s' % self.mysql_dir)
        self.mysqld_proc = Popen(args, stderr=PIPE, stdout=PIPE)
        # wait for mysqld to start up
        for _ in xrange(MAX_MYSQLD_STARTUP_TIME):
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

    @classmethod
    def tearDownClass(self):
        self.stop_mysql_daemon()

    @classmethod
    def stop_mysql_daemon(self):
        # this seems to get called multiple times by setup.py and
        # I'm not entirely sure how. These "if" statements fix things.
        if self.mysqld_proc is not None:
            log.info('shutting down mysqld')
            self.mysqld_proc.terminate()
            self.mysqld_proc.communicate()
            if self.mysqld_proc.returncode != 0:
                return 'mysqld exited with return code %d' % (
                    self.mysqld_proc.returncode)
            self.mysqld_proc = None

        if self.mysql_dir is not None:
            log.info('deleting %s' % self.mysql_dir)
            shutil.rmtree(self.mysql_dir)
            self.mysql_dir = None

    def setUp(self):
        """Create an empty database named `doloop`"""
        dbconn = self._connect(unix_socket=self.mysql_socket)
        try:
            dbconn.cursor().execute('DROP DATABASE IF EXISTS `doloop`')
        except:
            pass
        dbconn.cursor().execute('CREATE DATABASE `doloop`')

    def _connect(self, **kwargs):
        """Connect using MySQLdb or whatever driver is specified
        through the PYTHON_MYSQL_MODULE environment variable."""
        # PyMySQL requires user (though it may be empty)
        if not 'user' in kwargs:
            kwargs['user'] = ''
        return mysql_module.connect(**kwargs)

    def make_dbconn(self):
        return self._connect(unix_socket=self.mysql_socket, db='doloop')

    def create_doloop(self, table='loop', id_type=DEFAULT_ID_TYPE,
                      engine=DEFAULT_STORAGE_ENGINE):
        """Create a loop table in the `doloop` database, and return
        an object wrapping it. By default, this table will be named `loop`"""
        dbconn = self.make_dbconn()
        doloop.create(dbconn, table, id_type=id_type, engine=engine)
        return doloop.DoLoop(dbconn, table)

    def create_doloop_and_wrapped_dbconn(self, table='loop',
                                         id_type=DEFAULT_ID_TYPE,
                                         engine=DEFAULT_STORAGE_ENGINE):
        """Create a loop table in the `doloop` database, and return
        an object wrapping it. By default, this table will be named `loop`"""
        dbconn = ExceptionRaisingDbConnWrapper(self.make_dbconn())
        doloop.create(dbconn, table, id_type=id_type, engine=engine)
        return doloop.DoLoop(dbconn, table), dbconn

    ### tests for database wrapper ###

    def test_dbi_paramstyle(self):
        cursor = self.make_dbconn().cursor()
        self.assertNotEqual(doloop._paramstyle(cursor), None)

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

        self.assertEqual(foo_loop.get(2), [66])
        self.assertEqual(bar_loop.get(2), [99])

    def test_create_ids_can_be_strings(self):
        guid_loop = self.create_doloop('guid_loop', id_type='VARCHAR(128)')
        guid_loop.add(['foo', 'bar', 'baz'])
        self.assertEqual(guid_loop.get(3), ['bar', 'baz', 'foo'])

    def test_create_case_insensitive_id_collation(self):
        id_type = 'VARCHAR(64) CHARACTER SET utf8 COLLATE utf8_unicode_ci'

        ci_loop = self.create_doloop('ci_loop', id_type=id_type)
        ci_loop.add(['aaa', 'Bbb'])
        self.assertEqual(ci_loop.add('AAA'), 0)  # already added as "aaa"

        self.assertEqual(ci_loop.get(10), ['aaa', 'Bbb'])
        # "BBB" and "Bbb" are the same
        self.assertEqual(ci_loop.unlock('BBB'), 1)
        self.assertEqual(ci_loop.get(10), ['Bbb'])

        # Python dicts can't handle the case-insensitivity
        id_to_status = ci_loop.check(['aaa', 'bbb'])
        self.assertIn('aaa', id_to_status)
        self.assertNotIn('bbb', id_to_status,)
        self.assertIn('Bbb', id_to_status)
        self.assertNotIn('BBB', id_to_status)

        # so use a dict comprehension:
        id_lower_to_status = dict((id_.lower(), status)
                                  for id_, status in id_to_status.iteritems())
        self.assertIn('bbb', id_lower_to_status)
        self.assertIn('Bbb'.lower(), id_lower_to_status)
        self.assertIn('BBB'.lower(), id_lower_to_status)

    def test_create_myisam_storage_engine(self):
        myisam_loop = self.create_doloop('myisam_loop', engine='MyISAM')

        # verify that engine arg was actually passed through
        cursor = self.make_dbconn().cursor()
        # oursql 0.9.2 appears not to support SHOW CREATE TABLE,
        # so use INFORMATION_SCHEMA instead.
        cursor.execute(
            "SELECT `ENGINE` FROM `INFORMATION_SCHEMA`.`TABLES` WHERE"
            " `TABLE_SCHEMA` = 'doloop' AND `TABLE_NAME` = 'myisam_loop'")
        self.assertEqual(cursor.fetchall()[0][0], 'MyISAM')

        # make sure it works at all
        myisam_loop.add([1, 2, 3])
        self.assertEqual(myisam_loop.get(3), [1, 2, 3])

    def test_create_table_must_be_a_string(self):
        self.assertRaises(TypeError,
                      doloop.create, 'foo_loop', self.make_dbconn())

    def test_create_re_raises_exception(self):
        dbconn = ExceptionRaisingDbConnWrapper(self.make_dbconn())

        dbconn.raise_exception_later(LOCK_WAIT_TIMEOUT_EXC, num_queries=1)
        self.assertRaises(mysql_module.OperationalError,
                      doloop.create, dbconn, 'foo_loop')

    ### tests for add() ###

    def test_add_nothing(self):
        loop = self.create_doloop()
        self.assertEqual(loop.add([]), 0)

    def test_add(self):
        loop = self.create_doloop()
        self.assertEqual(loop.add(42), 1)

        self.assertEqual(loop.get(10), [42])
        self.assertEqual(loop.add(42), 0)  # already added

        self.assertEqual(loop.add([42, 43]), 1)  # 1 already added
        self.assertEqual(loop.get(10), [43])

        # test sets
        self.assertEqual(loop.add(set([43, 44, 45])), 2)  # 1 already added
        self.assertEqual(loop.get(10), [44, 45])

        # test tuples
        self.assertEqual(loop.add((46, 47, 48)), 3)
        self.assertEqual(loop.get(10), [46, 47, 48])

    def test_add_updated(self):
        loop = self.create_doloop()
        self.assertEqual(loop.add([42, 43], updated=True), 2)

        self.assertEqual(loop.get(10), [])

        # if we ignore update time, we can get the IDs, once
        self.assertEqual(loop.get(1, min_loop_time=0), [42])
        self.assertEqual(loop.get(1, min_loop_time=0), [43])
        self.assertEqual(loop.get(1, min_loop_time=0), [])

    def test_add_in_test_mode(self):
        loop = self.create_doloop()
        self.assertEqual(loop.add(42, test=True), 1)

        self.assertEqual(loop.get(10), [])  # wasn't actually added

    def test_add_table_must_be_a_string(self):
        self.assertRaises(TypeError,
                      doloop.add, self.make_dbconn(), 999, 'foo_loop')

    def test_add_unlocks_tables_after_exception(self):
        loop, dbconn = self.create_doloop_and_wrapped_dbconn()

        dbconn.raise_exception_later(LOCK_WAIT_TIMEOUT_EXC, num_queries=3)
        self.assertRaises(mysql_module.OperationalError, loop.add, 42)

        self.assertEqual(loop.add(42), 1)

    ### tests for remove() ###

    def test_remove_nothing(self):
        loop = self.create_doloop()
        self.assertEqual(loop.remove([]), 0)

    def test_remove(self):
        loop = self.create_doloop()
        loop.add([10, 11, 12, 13, 14])

        self.assertEqual(loop.remove(10), 1)  # remove one
        self.assertEqual(loop.remove(10), 0)
        self.assertEqual(loop.remove([11, 13]), 2)  # remove a list
        self.assertEqual(loop.remove([11, 13]), 0)
        self.assertEqual(loop.remove(set([11, 12, 13])), 1)  # remove a set
        self.assertEqual(loop.remove(set([11, 12, 13])), 0)

        self.assertEqual(loop.get(10), [14])

    def test_remove_in_test_mode(self):
        loop = self.create_doloop()
        loop.add([10, 11, 12, 13, 14])

        self.assertEqual(loop.remove(10, test=True), 1)

        self.assertEqual(loop.get(10), [10, 11, 12, 13, 14])

    def test_remove_table_must_be_a_string(self):
        self.assertRaises(TypeError,
                      doloop.remove, self.make_dbconn(), 999, 'foo_loop')

    def test_remove_unlocks_tables_after_exception(self):
        loop, dbconn = self.create_doloop_and_wrapped_dbconn()

        loop.add([10, 11, 12, 13, 14])

        dbconn.raise_exception_later(LOCK_WAIT_TIMEOUT_EXC, num_queries=3)
        self.assertRaises(mysql_module.OperationalError, loop.remove, 10)

        self.assertEqual(loop.remove(11), 1)

        self.assertEqual(loop.get(2), [10, 12])  # 10 wasn't removed

    ### tests for get() ###

    def test_get_from_empty(self):
        loop = self.create_doloop()

        self.assertEqual(loop.get(100), [])

    def test_get_locks_ids(self):
        loop = self.create_doloop()
        loop.add([10, 11, 12, 13, 14, 15, 16])

        self.assertEqual(loop.get(5), [10, 11, 12, 13, 14])
        self.assertEqual(loop.get(5), [15, 16])
        self.assertEqual(loop.get(5), [])

    def test_get_min_loop_time(self):
        loop = self.create_doloop()
        loop.add([10, 11, 12, 13, 14])

        ids = loop.get(10)
        self.assertEqual(ids, [10, 11, 12, 13, 14])

        num_changed = loop.did(ids)
        self.assertEqual(num_changed, 5)

        # those IDs won't be ready for another hour
        self.assertEqual(loop.get(10), [])

        self.assertEqual(loop.get(10, min_loop_time=0), [10, 11, 12, 13, 14])

    def test_get_locks_expire_please_wait_3_secs_or_so(self):
        loop = self.create_doloop()
        loop.add([10, 11, 12, 13, 14])

        self.assertEqual(loop.get(10, lock_for=2), [10, 11, 12, 13, 14])
        # IDs are locked; we can't get them
        self.assertEqual(loop.get(10), [])

        time.sleep(3)
        self.assertEqual(loop.get(10), [10, 11, 12, 13, 14])

    def test_get_prioritization_please_wait_1_sec_or_so(self):
        loop = self.create_doloop()

        loop.add([10, 11, 12, 13, 14, 15, 16, 17, 18, 19])
        loop.did(19)
        time.sleep(1.1)  # make sure UNIX_TIMESTAMP() changes
        loop.did(13)
        loop.bump([14, 17])
        loop.bump([15, 11], lock_for=ONE_HOUR)  # lock for an hour
        loop.bump([16, 12], lock_for=-ONE_HOUR)

        # first get the stuff that was super-bumped, then the stuff
        # that was bumped, then the new stuff, then the stuff that's done
        # already
        self.assertEqual(loop.get(10, min_loop_time=0),
                     [12, 16, 14, 17, 10, 18, 19, 13])

    def test_get_in_test_mode(self):
        loop = self.create_doloop()

        loop.add([10, 11, 12, 13, 14, 15, 16, 17, 18, 19])

        self.assertEqual(loop.get(5, test=True), [10, 11, 12, 13, 14])

        # IDs aren't actually locked since we're in test mode
        self.assertEqual(loop.get(5, test=True), [10, 11, 12, 13, 14])

    def test_get_table_must_be_a_string(self):
        self.assertRaises(TypeError,
                      doloop.get, self.make_dbconn(), 10, 'foo_loop')

    def test_get_lock_for_must_be_a_positive_number(self):
        loop = self.create_doloop()

        loop.get(10, lock_for=20)
        loop.get(10, lock_for=20.5)

        self.assertRaises(ValueError, loop.get, 10, lock_for=-600)
        self.assertRaises(ValueError, loop.get, 10, lock_for=0)

        self.assertRaises(TypeError, loop.get, 10, lock_for=None)
        self.assertRaises(TypeError, loop.get, 10, lock_for=[1, 2, 3])

    def test_get_limit_must_be_a_nonnegative_integer(self):
        loop = self.create_doloop()

        loop.get(10)
        loop.get(0)

        self.assertRaises(ValueError, loop.get, -1)

        self.assertRaises(TypeError, loop.get, 34.5)
        self.assertRaises(TypeError, loop.get, 24.0)
        self.assertRaises(TypeError, loop.get, None)
        self.assertRaises(TypeError, loop.get, [1, 2, 3])

    def test_get_min_loop_time_must_be_a_number(self):
        loop = self.create_doloop()
        loop.get(10, min_loop_time=20)
        loop.get(10, min_loop_time=20.5)
        loop.get(10, min_loop_time=0)
        loop.get(10, min_loop_time=-11.1)  # negative is okay

        self.assertRaises(TypeError, loop.get, 10, min_loop_time=None)
        self.assertRaises(TypeError, loop.get, 10, min_loop_time=[1, 2, 3])

    def test_get_unlocks_tables_after_exception(self):
        loop, dbconn = self.create_doloop_and_wrapped_dbconn()

        loop.add([10, 11, 12, 13, 14])

        dbconn.raise_exception_later(LOCK_WAIT_TIMEOUT_EXC, num_queries=3)
        self.assertRaises(mysql_module.OperationalError, loop.get, 2)

        self.assertEqual(loop.get(2), [10, 11])

    ### tests for did() ###

    def test_did_nothing(self):
        loop = self.create_doloop()
        self.assertEqual(loop.did([]), 0)

    def test_did_please_wait_1_sec_or_so(self):
        loop = self.create_doloop()

        loop.add([10, 11, 12, 13, 14, 15, 16, 17, 18, 19])
        self.assertEqual(loop.did(11), 1)
        time.sleep(1.1)  # make sure UNIX_TIMESTAMP() changes
        self.assertEqual(loop.did([11, 13, 15, 17, 19]), 5)  # 11 updated again

        self.assertEqual(loop.get(10), [10, 12, 14, 16, 18])

    def test_did_auto_add(self):
        loop = self.create_doloop()

        self.assertEqual(loop.get(10), [])

        self.assertEqual(loop.did(111), 1)  # 111 auto-added
        loop.add(222)
        self.assertEqual(loop.did([222, 333], auto_add=False), 1)  # no 333

        self.assertEqual(loop.get(10, min_loop_time=0), [111, 222])

    def test_did_in_test_mode(self):
        loop = self.create_doloop()

        loop.add([10, 11, 12, 13, 14])
        self.assertEqual(loop.did([12, 13], test=True), 2)

        # did() in test mode had no effect
        self.assertEqual(loop.get(10), [10, 11, 12, 13, 14])

    def test_did_table_must_be_a_string(self):
        self.assertRaises(TypeError,
                      doloop.did, self.make_dbconn(), 999, 'foo_loop')

    def test_did_unlocks_tables_after_exception(self):
        loop, dbconn = self.create_doloop_and_wrapped_dbconn()

        loop.add([10, 11, 12, 13, 14])

        dbconn.raise_exception_later(LOCK_WAIT_TIMEOUT_EXC, num_queries=3)
        self.assertRaises(mysql_module.OperationalError, loop.did, 10)

        self.assertEqual(loop.did(11), 1)

        self.assertEqual(loop.get(2), [10, 12])

    ### tests for unlock() ###

    def test_unlock_nothing(self):
        loop = self.create_doloop()
        self.assertEqual(loop.did([]), 0)

    def test_unlock(self):
        loop = self.create_doloop()

        loop.add([10, 11, 12, 13, 14, 15, 16, 17, 18, 19])
        ids = loop.get(5)
        self.assertEqual(loop.unlock(ids), 5)

        # unlocking doesn't re-prioritize the IDs since it doesn't touch
        # last_updated
        self.assertEqual(loop.get(10),
                         [10, 11, 12, 13, 14, 15, 16, 17, 18, 19])
        self.assertEqual(loop.get(10), [])

        # try unlocking just one ID
        self.assertEqual(loop.unlock(13), 1)
        self.assertEqual(loop.get(10), [13])

    def test_unlock_auto_add(self):
        loop = self.create_doloop()

        self.assertEqual(loop.get(10), [])

        loop.add(111)

        # we may or may not count 111 depending on how MySQL is reporting
        # row count. We definitely shouldn't get 0 or 3!
        self.assertIn(loop.unlock([111, 222]), (1, 2))

        self.assertEqual(loop.unlock(333, auto_add=False), 0)  # no row for 333

        self.assertEqual(loop.get(10), [111, 222])

    def test_unlock_in_test_mode(self):
        loop = self.create_doloop()

        loop.add([10, 11, 12, 13, 14, 15, 16, 17, 18, 19])
        ids = loop.get(5)
        self.assertEqual(ids, [10, 11, 12, 13, 14])
        self.assertEqual(loop.unlock(ids, test=True), 5)

        # IDs weren't actually unlocked
        self.assertEqual(loop.get(5), [15, 16, 17, 18, 19])

    def test_unlock_table_must_be_a_string(self):
        self.assertRaises(TypeError,
                      doloop.unlock, self.make_dbconn(), 999, 'foo_loop')

    def test_unlock_unlocks_tables_after_exception(self):
        loop, dbconn = self.create_doloop_and_wrapped_dbconn()

        loop.add([10, 11, 12, 13, 14])
        self.assertEqual(loop.get(3), [10, 11, 12])

        dbconn.raise_exception_later(LOCK_WAIT_TIMEOUT_EXC, num_queries=3)
        self.assertRaises(mysql_module.OperationalError, loop.unlock, 11)

        self.assertEqual(loop.unlock(10), 1)

        self.assertEqual(loop.get(2), [10, 13])

    ### tests for bump() ###

    def test_bump_nothing(self):
        loop = self.create_doloop()
        self.assertEqual(loop.bump([]), 0)

    def test_bump(self):
        loop = self.create_doloop()
        loop.add([10, 11, 12, 13, 14, 15, 16, 17, 18, 19])

        self.assertEqual(loop.bump(19), 1)
        self.assertEqual(loop.bump([17, 12], lock_for=-10), 2)  # super-bump
        self.assertEqual(loop.bump([13, 18], lock_for=10), 2)  # bump but lock

        self.assertEqual(loop.get(5), [12, 17, 19, 10, 11])

    def test_bump_same_id_twice_please_wait_4_secs_or_so(self):
        loop = self.create_doloop()
        loop.add([10, 11, 12, 13, 14, 15, 16, 17, 18, 19])

        self.assertEqual(loop.bump(17, lock_for=4), 1)
        self.assertEqual(loop.get(1), [10])  # 17 is bumped but locked

        time.sleep(2.1)
        self.assertEqual(loop.bump(17, lock_for=4), 0)  # don't re-bump
        self.assertEqual(loop.get(1), [11])  # 17 is bumped but locked

        time.sleep(2)
        self.assertEqual(loop.get(1), [17])  # lock on 17 has expired

    def test_bump_auto_add(self):
        loop = self.create_doloop()
        loop.add([10, 11, 12, 13, 14, 15, 16, 17, 18, 19])

        self.assertEqual(loop.bump([7, 17]), 2)  # 7 is auto-added
        self.assertEqual(loop.bump([19, 25], lock_for=-10, auto_add=False),
                     1)  # no row for 25
        self.assertEqual(loop.get(5), [19, 7, 17, 10, 11])

    def test_bump_in_test_mode(self):
        loop = self.create_doloop()
        loop.add([10, 11, 12, 13, 14])

        self.assertEqual(loop.bump(13, test=True), 1)

        # bump had no effect in test mode
        self.assertEqual(loop.get(2), [10, 11])

    def test_bump_table_must_be_a_string(self):
        self.assertRaises(TypeError,
                      doloop.bump, self.make_dbconn(), 999, 'foo_loop')

    def test_bump_min_loop_time_must_be_a_number(self):
        loop = self.create_doloop()
        loop.add(17)

        loop.bump(17, lock_for=20)
        loop.bump(17, lock_for=20.5)
        loop.bump(17, lock_for=0)
        loop.bump(17, lock_for=-11.1)  # negative is okay

        self.assertRaises(TypeError, loop.bump, 17, lock_for=None)
        self.assertRaises(TypeError, loop.bump, 17, lock_for=[1, 2, 3])

    def test_bump_unlocks_tables_after_exception(self):
        loop, dbconn = self.create_doloop_and_wrapped_dbconn()

        loop.add([10, 11, 12, 13, 14])

        dbconn.raise_exception_later(LOCK_WAIT_TIMEOUT_EXC, num_queries=3)
        self.assertRaises(mysql_module.OperationalError, loop.bump, 14)

        self.assertEqual(loop.bump(13), 1)

        self.assertEqual(loop.get(2), [13, 10])

    ### tests for check() ###

    def test_check_nothing(self):
        loop = self.create_doloop()
        self.assertEqual(loop.check([]), {})

    def test_check(self):
        loop = self.create_doloop()
        loop.add([10, 11, 12, 13, 14, 15, 16, 17, 18, 19])

        # newly added IDs have no locked or updated time
        self.assertEqual(loop.check(10), {10: (None, None)})
        self.assertEqual(loop.check([18, 19]), {18: (None, None),
                                            19: (None, None)})
        self.assertEqual(loop.check(20), {})  # 20 doesn't exist
        self.assertEqual(loop.check([18, 19, 20]), {18: (None, None),
                                                19: (None, None)})

        self.assertEqual(loop.get(2), [10, 11])
        loop.did(11)
        loop.bump(12)

        id_to_status = loop.check([10, 11, 12])
        self.assertEqual(sorted(id_to_status), [10, 11, 12])

        # allow 2 seconds of wiggle room
        since_updated_10, locked_for_10 = id_to_status[10]
        self.assertEqual(since_updated_10, None)
        self.assertGreaterEqual(locked_for_10, ONE_HOUR - 2)
        self.assertLessEqual(locked_for_10, ONE_HOUR)

        since_updated_11, locked_for_11 = id_to_status[11]
        self.assertGreaterEqual(since_updated_11, 0)
        self.assertLessEqual(since_updated_11, 2)
        self.assertEqual(locked_for_11, None)

        since_updated_12, locked_for_12 = id_to_status[12]
        self.assertEqual(since_updated_12, None)
        self.assertGreaterEqual(locked_for_12, -2)
        self.assertLessEqual(locked_for_12, 0)

    def test_check_table_must_be_a_string(self):
        self.assertRaises(TypeError,
                      doloop.check, self.make_dbconn(), 999, 'foo_loop')

    ### tests for stats() ###

    def _sanity_check_stats(self, stats):
        """Type-check the results of stats, and make sure "min_"
        stats are <= their corresponding "max_" stats."""
        # type checking
        for key, value in stats.iteritems():
            if key.endswith('_time'):
                self.assertIsInstance(value, float)
            # IDs can be anything
            elif not key.endswith('_id'):
                self.assertIsInstance(value, int)

            # make sure min_ and max_ are in the right order (Issue #12)
            if key.startswith('min_'):
                max_key = 'max_' + key[4:]
                max_value = stats[max_key]
                self.assertLessEqual(
                    value, max_value,
                    '%s (%r) should be <= %s (%r)' % (
                        key, value, max_key, max_value))

    def test_stats_empty(self):
        loop = self.create_doloop()

        stats = loop.stats()
        self._sanity_check_stats(stats)

        self.assertEqual(stats, {
            'locked': 0,
            'bumped': 0,
            'min_id': None,
            'max_id': None,
            # times are 0.0, not None, for convenience
            'min_lock_time': 0.0,
            'max_lock_time': 0.0,
            'min_bump_time': 0.0,
            'max_bump_time': 0.0,
            'min_update_time': 0.0,
            'max_update_time': 0.0,
        })

    def test_stats_please_wait_1_sec_or_so(self):
        loop = self.create_doloop()
        loop.add([10, 11, 12, 13, 14, 15, 16, 17, 18, 19])

        self.assertEqual(loop.get(1), [10])
        loop.did([11, 12])
        time.sleep(1.1)  # wait for 11, 12 to be at least 1 sec old
        loop.bump(12)
        loop.bump(13, lock_for=60)
        loop.bump([14, 15], lock_for=-60)
        self.assertEqual(loop.get(1), [14])
        loop.did(14)

        stats = loop.stats()
        self._sanity_check_stats(stats)

        self.assertEqual(stats['locked'], 2)  # 10, 13
        self.assertEqual(stats['bumped'], 2)  # 12, 15

        self.assertEqual(stats['min_id'], 10)
        self.assertIsInstance(stats['min_id'], int)
        self.assertEqual(stats['max_id'], 19)
        self.assertIsInstance(stats['max_id'], int)

        # this test should work even if it experienced up to 5 seconds of delay
        self.assertGreaterEqual(stats['min_lock_time'], 55)  # 13
        self.assertLessEqual(stats['min_lock_time'], 60)
        self.assertGreaterEqual(stats['max_lock_time'], ONE_HOUR - 6)  # 10
        self.assertLessEqual(stats['max_lock_time'], ONE_HOUR - 1)

        self.assertGreaterEqual(stats['min_bump_time'], 0)  # 12
        self.assertLessEqual(stats['min_bump_time'], 5)
        self.assertGreaterEqual(stats['max_bump_time'], 60)  # 14 and 15
        self.assertLessEqual(stats['max_bump_time'], 65)

        self.assertGreaterEqual(stats['min_update_time'], 0)  # 14
        self.assertLessEqual(stats['min_update_time'], 5)
        self.assertGreaterEqual(stats['max_update_time'], 1)  # 10
        self.assertLessEqual(stats['max_update_time'], 6)
        self.assertGreater(stats['max_update_time'], stats['min_update_time'])

    def test_stats_table_must_be_a_string(self):
        self.assertRaises(TypeError,
                      doloop.stats, 'foo_loop', self.make_dbconn())

    def test_stats_re_raises_exception(self):
        # stats() runs in READ UNCOMMITTED mode (no locking), so we should
        # never encounter a deadlock or lock wait timeout
        loop, dbconn = self.create_doloop_and_wrapped_dbconn()

        dbconn.raise_exception_later(LOCK_WAIT_TIMEOUT_EXC, num_queries=5)
        self.assertRaises(mysql_module.OperationalError, loop.stats)

    ### tests for the DoLoop wrapper object ###

    def test_wrapper_dbconn_can_be_a_callable(self):
        self.create_doloop('foo_loop')

        foo_loop = doloop.DoLoop(self.make_dbconn, 'foo_loop')

        foo_loop.add([10, 11, 12, 13, 14, 15, 16, 17, 18, 19])
        self.assertEqual(foo_loop.get(5), [10, 11, 12, 13, 14])

        def bad_conn():
            raise Exception("I'm sorry Dave, I'm afraid I can't do that.")

        # this is okay; bad conn isn't called yet
        foo_loop_bad = doloop.DoLoop(bad_conn, 'foo_loop')
        self.assertRaises(Exception, foo_loop_bad.add,
                      [10, 11, 12, 13, 14, 15, 16, 17, 18, 19])

    def test_wrapper_table_attribute(self):
        foo_loop = self.create_doloop('foo_loop')
        self.assertEqual(foo_loop.table, 'foo_loop')
        try:
            foo_loop.table = 'bar_loop'
            self.fail('Should not be possible to set DoLoop.table!')
        except:
            pass

    def test_wrapper_table_must_be_a_string(self):
        # whoops, table and connection name are reversed
        self.assertRaises(TypeError,
                          doloop.DoLoop, 'foo_loop', self.make_dbconn())


class CreateDoloopTableScriptTestCase(unittest.TestCase):

    def setUp(self):
        self._real_stdout = sys.stdout
        sys.stdout = StringIO()

        def error(self, msg):
            raise ValueError(msg)

        self._real_OptionParser_error = optparse.OptionParser.error
        optparse.OptionParser.error = error

    def tearDown(self):
        sys.stdout = self._real_stdout
        optparse.OptionParser.error = self._real_OptionParser_error

    def test_create_script_one_table(self):
        _main_for_create_doloop_table(['foo_loop'])
        output = sys.stdout.getvalue()

        self.assertIn('`foo_loop`', output)
        self.assertIn('INT', output)
        self.assertIn('InnoDB', output)
        self.assertEqual(output,
                         doloop.sql_for_create('foo_loop') + ';\n\n')

    def test_create_script_multiple_tables(self):
        _main_for_create_doloop_table(['foo_loop', 'bar_loop'])
        output = sys.stdout.getvalue()

        self.assertIn('`foo_loop`', output)
        self.assertIn('`bar_loop`', output)
        self.assertIn('INT', output)
        self.assertIn('InnoDB', output)
        self.assertEqual(output,
                         doloop.sql_for_create('foo_loop') + ';\n\n' +
                         doloop.sql_for_create('bar_loop') + ';\n\n')

    def test_create_script_error_if_no_tables(self):
        self.assertRaises(ValueError,
                          _main_for_create_doloop_table, [])

    def test_create_script_id_type(self):
        for opt in ('-i', '--id-type'):
            sys.stdout = StringIO()  # use a fresh buffer
            _main_for_create_doloop_table(['foo_loop', opt, 'BIT(8)'])
            output = sys.stdout.getvalue()

            self.assertIn('`foo_loop`', output)
            self.assertIn('BIT(8)', output)
            self.assertIn('InnoDB', output)
            self.assertEqual(
                output,
                doloop.sql_for_create('foo_loop', id_type='BIT(8)') + ';\n\n')

    def test_create_script_engine(self):
        for opt in ('-e', '--engine'):
            sys.stdout = StringIO()  # use a fresh buffer
            _main_for_create_doloop_table(['foo_loop', opt, 'MyISAM'])
            output = sys.stdout.getvalue()

            self.assertIn('`foo_loop`', output)
            self.assertIn('INT', output)
            self.assertIn('MyISAM', output)
            self.assertEqual(
                output,
                doloop.sql_for_create('foo_loop', engine='MyISAM') + ';\n\n')


if __name__ == '__main__':
    unittest.main()
