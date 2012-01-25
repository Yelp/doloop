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
"""An ad-hoc script for testing concurrent gets"""

import MySQLdb
import doloop
import random
import sys
import time

from multiprocessing import Process

# these settings almost guarantee deadlocks
DB_PARAMS = {
    'charset': 'utf8',
    'db': 'yelp_doloop',
    'host': 'devdb5',
    'passwd': '',
    'port': 3306,
    'use_unicode': True,
    'user': 'yelpdev',
}
TABLE = 'concurrency_test_loop'
NUM_IDS = 10000
NUM_WORKERS = 20
BATCH_SIZE = 500
IDS_PER_WORKER = 50000
SLEEP_TIME = 10.0


def do_work():
    loop = doloop.DoLoop(MySQLdb.connect(**DB_PARAMS), TABLE)

    ids_processed = 0

    while ids_processed < IDS_PER_WORKER:
        ids = loop.get(BATCH_SIZE, min_loop_time=0)

        if SLEEP_TIME:
            time.sleep(random.expovariate(1.0 / SLEEP_TIME))

        loop.did(ids)

        ids_processed += BATCH_SIZE

        sys.stderr.write('processed %d of %d IDs\n' %
                         (ids_processed, IDS_PER_WORKER))

        # do a little prioritization
        loop.bump(random.randint(0, NUM_IDS - 1))


def main():
    dbconn = MySQLdb.connect(**DB_PARAMS)

    #sys.stderr.write('creating table\n')
    #dbconn.cursor().execute('DROP TABLE IF EXISTS `%s`' % TABLE)
    #doloop.create(dbconn, TABLE)

    sys.stderr.write('deleting all rows from %s\n' % TABLE)
    dbconn.cursor().execute('DELETE FROM `%s`' % TABLE)
    dbconn.commit()

    loop = doloop.DoLoop(dbconn, TABLE)

    sys.stderr.write('adding IDs\n')

    for start in xrange(0, NUM_IDS, BATCH_SIZE):
        ids = range(start, min(start+BATCH_SIZE, NUM_IDS))
        loop.add(ids)

    sys.stderr.write('spawning %d workers\n' % NUM_WORKERS)

    workers = []
    for _ in xrange(NUM_WORKERS):
        workers.append(Process(target=do_work))

    start_time = time.time()
    for worker in workers:
        worker.start()

    sys.stderr.write('waiting for workers to finish\n')

    total_time = 0.0
    for workers in workers:
        worker.join()
        total_time += time.time() - start_time

    total_ids = NUM_WORKERS * IDS_PER_WORKER
    
    sys.stderr.write('%d workers took %.1fs (cumulative) to process'
                     ' %d * %d = %d IDs\n' % (
        NUM_WORKERS, total_time, NUM_WORKERS, IDS_PER_WORKER, total_ids))

    sys.stderr.write('%d IDs / %.1fs = %.1f IDs/sec\n' % (
        total_ids, total_time, total_ids / total_time))


if __name__ == '__main__':
    main()

                    
  
