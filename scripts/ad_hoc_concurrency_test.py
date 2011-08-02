"""An ad-hoc script for testing concurrent gets"""

import MySQLdb
import doloop
import random
import sys
import time

from multiprocessing import Process

# these settings almost guarantee deadlocks
DB_PARAMS = {'db': 'test'}
TABLE = 'concurrency_test_loop'
NUM_IDS = 100
NUM_WORKERS = 50
BATCH_SIZE = 10
IDS_PER_WORKER = 500


def do_work():
    loop = doloop.DoLoop(MySQLdb.connect(**DB_PARAMS), TABLE)

    ids_processed = 0

    while ids_processed < IDS_PER_WORKER:
        sys.stdout.flush()
        ids = loop.get(BATCH_SIZE, min_loop_time=0)
        loop.did(ids)

        ids_processed += BATCH_SIZE

        sys.stdout.write('processed %d of %d IDs\r' %
                         (ids_processed, IDS_PER_WORKER))

        # do a little prioritization
        loop.bump(random.randint(0, NUM_IDS - 1))


def main():
    dbconn = MySQLdb.connect(**DB_PARAMS)
    print 'creating table'

    dbconn.cursor().execute('DROP TABLE IF EXISTS `%s`' % TABLE)
    doloop.create(dbconn, TABLE)

    loop = doloop.DoLoop(dbconn, TABLE)

    print 'adding IDs'

    for start in xrange(0, NUM_IDS, BATCH_SIZE):
        ids = range(start, min(start+BATCH_SIZE, NUM_IDS))
        loop.add(ids)

    print 'spawning %d workers' % NUM_WORKERS

    workers = []
    for _ in xrange(NUM_WORKERS):
        workers.append(Process(target=do_work))

    start_time = time.time()
    for worker in workers:
        worker.start()

    print 'waiting for workers to finish'

    total_time = 0.0
    for workers in workers:
        worker.join()
        total_time += time.time() - start_time

    total_ids = NUM_WORKERS * IDS_PER_WORKER
    
    print '%d workers took %.1fs (cumulative) to process %d * %d = %d IDs' % (
        NUM_WORKERS, total_time, NUM_WORKERS, IDS_PER_WORKER, total_ids)

    print '%d IDs / %.1fs = %.1f IDs/sec' % (
        total_ids, total_time, total_ids / total_time)

if __name__ == '__main__':
    main()

                    
  
