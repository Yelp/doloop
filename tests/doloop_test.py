import logging
import MySQLdb
import os
import shutil
import signal
from subprocess import Popen, PIPE
import tempfile
import time
import warnings

from testify import TestCase
from testify import assert_equal
from testify import class_setup
from testify import class_teardown
from testify import setup
from testify import teardown
from testify import run

import doloop

log = logging.getLogger('doloop_test')

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

    def test_get_empty(self):
        loop = self.create_doloop()

        assert_equal(loop.get(100), [])

