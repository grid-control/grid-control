#-#  Copyright 2013-2014 Karlsruhe Institute of Technology
#-#
#-#  Licensed under the Apache License, Version 2.0 (the "License");
#-#  you may not use this file except in compliance with the License.
#-#  You may obtain a copy of the License at
#-#
#-#      http://www.apache.org/licenses/LICENSE-2.0
#-#
#-#  Unless required by applicable law or agreed to in writing, software
#-#  distributed under the License is distributed on an "AS IS" BASIS,
#-#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#-#  See the License for the specific language governing permissions and
#-#  limitations under the License.

from multiprocessing import Process
from multiprocessing.queues import JoinableQueue
from time import sleep
from time import time


class MigrationRequestedState(object):
    def __init__(self, migration_task):
        self.migration_task = migration_task

    def run(self):
        #submit task to DBS 3 migration
        print "MigrationRequestedState:", self.migration_task
        self.migration_task.state = MigrationSubmittedState(self.migration_task)


class MigrationSubmittedState(object):
    def __init__(self, migration_task):
        self.migration_task = migration_task
        self.last_poll_time = time()
        self.max_poll_interval = 10

    def run(self):
        if abs(self.last_poll_time-time()) > self.max_poll_interval:
            #check migration status
            print "MigrationSubmittedState:", self.migration_task
            self.last_poll_time = time()
            self.migration_task.state.__class__ = MigrationDoneState


class MigrationDoneState(object):
    def __init__(self, migration_task):
        self.migration_task = migration_task

    def run(self):
        print "MigrationDoneState:", self.migration_task
        pass


class MigrationFailedState(object):
    def __init__(self, migration_task):
        self.migration_task = migration_task

    def run(self):
        #handle error
        print "MigrationFailedState:", self.migration_task
        pass


class MigrationTask(object):
    def __init__(self, block_name, migration_url):
        self.block_name = block_name
        self.migration_url = migration_url

        self.state = MigrationRequestedState(self)

    def run(self):
        self.state.run()

    def is_done(self):
        return self.state.__class__ == MigrationDoneState

    def is_failed(self):
        return self.state.__class__ == MigrationFailedState

    def __eq__(self, other):
        return (self.block_name == other.block_name) and (self.migration_url == other.migration_url)

    def __ne__(self, other):
        return (self.block_name != other.block_name) or (self.migration_url != other.migration_url)

    def __hash__(self):
        return hash(self.block_name+self.migration_url)

    def __str__(self):
        return repr(self)

    def __repr__(self):
        return '%s(block_name="%s", migration_url="%s")' % (self.__class__.__name__, self.block_name,
                                                            self.migration_url)


class AlreadyQueued(Exception):
    def __init__(self, msg):
        self._msg = msg
        super(AlreadyQueued, self).__init__(self, "AlreadyQueuedException %s" % self._msg)

    def __repr__(self):
        return '%s %r' % (self.__class__.__name__, self._msg)

    def __str__(self):
        return repr(self._msg)


class MigrationFailed(Exception):
    def __init__(self, msg):
        self._msg = msg
        super(MigrationFailed, self).__init__(self, "MigrationFailedException %s" % self._msg)

    def __repr__(self):
        return '%s %r' % (self.__class__.__name__, self._msg)

    def __str__(self):
        return repr(self._msg)


class DBS3MigrationQueue(JoinableQueue):
    _unique_queued_tasks = set()

    def __init__(self, maxsize=0):
        super(DBS3MigrationQueue, self).__init__(maxsize)

    def add_migration_task(self, migration_task):
        if migration_task not in self._unique_queued_tasks:
            self._unique_queued_tasks.add(migration_task)
            self.put(migration_task)
        else:
            raise AlreadyQueued('The migration task %s is already queued!' % migration_task)

    def has_unfinished_tasks(self):
        return not self._unfinished_tasks._semlock._is_zero()

    def release_worker(self):
        while self.has_unfinished_tasks():
            sleep(1)
        self.put(None)
        self.task_done()


def worker(queue):
    while True:
        task = queue.get()
        #quit worker on None, means all tasks are done
        if not task:
            break
        try:
            task.run()
        except:
            raise
        else:
            if not (task.is_done() or task.is_failed()):
                #re-queue task for further processing
                queue.put(task)
            else:
                #last execution of run, to print final migration done/failed message
                task.run()
        finally:
            queue.task_done()


if __name__ == '__main__':
    block_names = ['test1', 'test1', 'test2', 'test3', 'test4']
    migration_queue = DBS3MigrationQueue()

    for block in block_names:
        try:
            migration_queue.add_migration_task(MigrationTask(block_name=block, migration_url='http://a.b.c'))
        except AlreadyQueued as aq:
            print aq

    p = Process(target=worker, args=(migration_queue,))
    p.start()
    sleep(5)
    print "Add new migration request to queue!"
    migration_queue.add_migration_task(MigrationTask(block_name='test5', migration_url='http://a.b.c'))
    migration_queue.release_worker()
    # wait for all tasks to be finished
    p.join()
