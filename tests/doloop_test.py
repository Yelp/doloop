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
from testify import assert_not_reached
from testify import assert_raises
from testify import class_setup
from testify import class_teardown
from testify import setup
from testify import teardown
from testify import run

import doloop

log = logging.getLogger('doloop_test')

WHITESPACE_RE = re.compile('\s+')

MAX_MYSQLD_STARTUP_TIME = 15

class DoLoopTestCase(TestCase):

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

    def make_dbconn(self):
        return MySQLdb.connect(unix_socket=self.mysql_socket, db='doloop')

    def create_doloop(self, table='loop', id_type='INT'):
        """Create a loop table in the `doloop` database, and return
        an object wrapping it. By default, this table will be named `loop`"""
        dbconn = self.make_dbconn()
        doloop.create(dbconn, table, id_type)
        return doloop.DoLoop(dbconn, table)

    @setup
    def create_empty_doloop_db(self):
        """Create an empty database named `doloop`"""
        dbconn = MySQLdb.connect(unix_socket=self.mysql_socket)
        with warnings.catch_warnings():
            warnings.filterwarnings('ignore', category=MySQLdb.Warning)
            dbconn.cursor().execute('DROP DATABASE IF EXISTS `doloop`')
        dbconn.cursor().execute('CREATE DATABASE `doloop`')

    ### tests for create() ###

    def test_can_create_more_than_one_loop(self):
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
        
        assert_equal(loop.add(set([43, 44, 45])), 2) # 1 already added 
        assert_equal(loop.get(10), [44, 45])

    def test_add_updated(self):
        loop = self.create_doloop()
        assert_equal(loop.add([42, 43], updated=True), 2)
        
        assert_equal(loop.get(10), [])

        # if we ignore update time, we can get the IDs, once
        assert_equal(loop.get(1, min_loop_time=0), [42])
        assert_equal(loop.get(1, min_loop_time=0), [43])
        assert_equal(loop.get(1, min_loop_time=0), [])

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

    ### tests for get() ###

    def test_get_empty(self):
        loop = self.create_doloop()

        assert_equal(loop.get(100), [])

    def test_ids_are_locked_after_you_get_them(self):
        loop = self.create_doloop()
        loop.add(range(10, 25))

        assert_equal(loop.get(10), range(10, 20))
        assert_equal(loop.get(10), range(20, 25))
        assert_equal(loop.get(10), [])

    def test_min_loop_time(self):
        loop = self.create_doloop()
        loop.add(range(10, 15))
       
        ids = loop.get(10)
        assert_equal(ids, range(10, 15))

        num_changed = loop.did(ids)
        assert_equal(num_changed, 5)

        # those IDs won't be ready for another hour
        assert_equal(loop.get(10), [])

        assert_equal(loop.get(10, min_loop_time=0), range(10, 15))

    def test_lock_for_must_be_a_positive_number(self):
        loop = self.create_doloop()

        loop.get(10, lock_for=20)
        loop.get(10, lock_for=20.5)
        
        assert_raises(ValueError, loop.get, 10, lock_for=-600)
        assert_raises(ValueError, loop.get, 10, lock_for=0)

        assert_raises(TypeError, loop.get, 10, lock_for=None)
        assert_raises(TypeError, loop.get, 10, lock_for=[1, 2, 3])
       
    def test_limit_must_be_a_nonnegative_integer(self):
        loop = self.create_doloop()

        loop.get(10)
        loop.get(0)

        assert_raises(ValueError, loop.get, -1)
        
        assert_raises(TypeError, loop.get, 34.5)
        assert_raises(TypeError, loop.get, 24.0)
        assert_raises(TypeError, loop.get, None)
        assert_raises(TypeError, loop.get, [1, 2, 3])

    def test_min_loop_time_must_be_a_number(self):
        loop = self.create_doloop()
        loop.get(10, min_loop_time=20)
        loop.get(10, min_loop_time=20.5)
        loop.get(10, min_loop_time=0)
        loop.get(10, min_loop_time=-11.1) # negative is okay

        assert_raises(TypeError, loop.get, 10, min_loop_time=None)
        assert_raises(TypeError, loop.get, 10, min_loop_time=[1, 2, 3])

    def test_locks_eventually_expire_please_wait_3_seconds_or_so(self):
        loop = self.create_doloop()
        loop.add(range(10, 15))

        assert_equal(loop.get(10, lock_for=2), range(10, 15))
        # IDs are locked; we can't get them
        assert_equal(loop.get(10), [])

        time.sleep(3)
        assert_equal(loop.get(10), range(10, 15))

    def test_prioritization_please_wait_1_second_or_so(self):
        loop = self.create_doloop()

        loop.add(range(10, 20))
        loop.did(19)
        time.sleep(1.1) # make sure UNIX_TIMESTAMP() changes
        loop.did(13)
        loop.bump([14, 17])
        loop.bump([15, 11], lock_for=60*60) # lock for an hour
        loop.bump([16, 12], lock_for=-60*60)

        # first get the stuff that was super-bumped, then the stuff
        # that was bumped, then the new stuff, then the stuff that's done
        # already
        assert_equal(loop.get(10, min_loop_time=0),
                     [12, 16, 14, 17, 10, 18, 19, 13])

    ### tests for did() ###

    def test_did_nothing(self):
        loop = self.create_doloop()
        assert_equal(loop.did([]), 0)

    def test_did_please_wait_1_second_or_so(self):
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

    ### tests for unlock() ###

    def test_unlock_nothing(self):
        loop = self.create_doloop()
        assert_equal(loop.did([]), 0)

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

    def test_multi_bump_please_wait_3_seconds_or_so(self):
        loop = self.create_doloop()
        loop.add(range(10, 20))

        assert_equal(loop.bump(17, lock_for=3), 1)
        assert_equal(loop.get(1), [10]) # 17 is bumped but locked

        time.sleep(1.1)
        assert_equal(loop.bump(17, lock_for=3), 0) # don't re-bump
        assert_equal(loop.get(1), [11]) # 17 is bumped but locked
       
        time.sleep(2)
        assert_equal(loop.get(1), [17]) # lock on 17 has expired

    def test_bump_auto_add(self):
        loop = self.create_doloop()
        loop.add(range(10, 20))

        assert_equal(loop.bump(17), 1)
        assert_equal(loop.bump([19, 25], lock_for=-10, auto_add=False),
                     1) # no row for 225
        assert_equal(loop.get(5), [19, 17, 10, 11, 12])

    ### tests for the DoLoop wrapper object ###

    def test_dbconn_can_be_a_callable(self):
        self.create_doloop('foo_loop')

        foo_loop = doloop.DoLoop(self.make_dbconn, 'foo_loop')

        foo_loop.add(range(10, 20))
        assert_equal(foo_loop.get(5), range(10, 15))

        def bad_conn():
            raise Exception("I'm sorry Dave, I'm afraid I can't do that.")

        # this is okay; bad conn isn't called yet
        foo_loop_bad = doloop.DoLoop(bad_conn, 'foo_loop')
        assert_raises(Exception, foo_loop_bad.add, range(10, 20))

    def test_table_attribute(self):
        foo_loop = self.create_doloop('foo_loop')
        assert_equal(foo_loop.table, 'foo_loop')
        try:
            foo_loop.table = 'bar_loop'
            assert_not_reached('Should not be possible to set DoLoop.table!')
        except:
            pass

