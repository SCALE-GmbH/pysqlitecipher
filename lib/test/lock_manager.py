#-*- coding: ISO-8859-1 -*-
# pysqlite2/test/factory.py: tests for the various factories in pysqlite
#
# Copyright (C) 2013 DYNAmore GmbH
#
# This file is part of pysqlite.
#
# This software is provided 'as-is', without any express or implied
# warranty.  In no event will the authors be held liable for any damages
# arising from the use of this software.
#
# Permission is granted to anyone to use this software for any purpose,
# including commercial applications, and to alter it and redistribute it
# freely, subject to the following restrictions:
#
# 1. The origin of this software must not be misrepresented; you must not
#    claim that you wrote the original software. If you use this software
#    in a product, an acknowledgment in the product documentation would be
#    appreciated but is not required.
# 2. Altered source versions must be plainly marked as such, and must not be
#    misrepresented as being the original software.
# 3. This notice may not be removed or altered from any source distribution.

import unittest
import threading
import traceback
import time
import pysqlite2.dbapi2 as sqlite
from pysqlite2.lock_manager import DefaultLockManager, DeadlockError, get_lock_manager, \
        LOCK_NONE, LOCK_SHARED, LOCK_RESERVED, LOCK_PENDING, LOCK_EXCLUSIVE


class LockManagerTests(unittest.TestCase):

    def setUp(self):
        self.manager = DefaultLockManager()
        self.lockfunc = lambda x: None

    def tearDown(self):
        self.manager = None

    def _print(self, message):
        # print fmt
        pass

    def CheckDisabledByDefault(self):
        """The lock manager is disabled by default."""
        self.assertEqual(get_lock_manager(), None)

    def CheckSharedLocks(self):
        """Check that we can acquire many shared locks concurrently."""
        for client in range(10):
            self.manager.lock(self.lockfunc, "filename", LOCK_SHARED, client)
        self._print(self.manager)
        for client in range(10):
            self.manager.unlock("filename", LOCK_NONE, client)
        self._print(self.manager)
        self.assertTrue(self.manager.is_idle())

    def CheckDetectDeadlock(self):
        """Checks that shared can not be raised to a higher level if a reserved lock exists."""
        self.manager.lock(self.lockfunc, "filename", LOCK_RESERVED, "first")

        self.manager.lock(self.lockfunc, "filename", LOCK_SHARED, "second")
        self.assertRaises(DeadlockError, self.manager.lock, self.lockfunc, "filename", LOCK_RESERVED, "second")
        self.assertRaises(DeadlockError, self.manager.lock, self.lockfunc, "filename", LOCK_EXCLUSIVE, "second")

    def CheckUnlockWithoutLock(self):
        """Checks that unlock without first locking is harmless (SQLite seems to do it sometimes)."""
        self.manager.unlock("filename", LOCK_NONE, "client")

    def CheckRaiseLower(self):
        """Checks that the lock level can go up and down all the way."""
        self.manager.lock(self.lockfunc, "filename", LOCK_SHARED, "client")
        self.manager.lock(self.lockfunc, "filename", LOCK_RESERVED, "client")
        self.manager.lock(self.lockfunc, "filename", LOCK_EXCLUSIVE, "client")
        self.manager.unlock("filename", LOCK_RESERVED, "client")
        self.manager.unlock("filename", LOCK_SHARED, "client")
        self.manager.unlock("filename", LOCK_NONE, "client")
        self.assertTrue(self.manager.is_idle())

    def CheckExclusiveBlocksShared(self):
        """Check that a shared lock must wait while an exclusive lock is set."""
        self.manager.lock(self.lockfunc, "filename", LOCK_EXCLUSIVE, "exclusive")

        def shared_locker():
            self.manager.lock(self.lockfunc, "filename", LOCK_SHARED, "shared")
        t = threading.Thread(target=shared_locker)
        t.start()
        t.join(0.1)
        self._print(self.manager)
        self.assertTrue(t.is_alive())

        self.manager.unlock("filename", LOCK_NONE, "exclusive")
        t.join()
        self._print(self.manager)
        self.assertFalse(self.manager.is_idle())

    def CheckPendingBlocksShared(self):
        """Check that a pending lock blocks further shared locks."""
        self.manager.lock(self.lockfunc, "filename", LOCK_SHARED, "shared_1")
        self.manager.lock(self.lockfunc, "filename", LOCK_SHARED, "shared_2")

        def exclusive_locker():
            self.manager.lock(self.lockfunc, "filename", LOCK_EXCLUSIVE, "exclusive")

        def shared_locker():
            self.manager.lock(self.lockfunc, "filename", LOCK_SHARED, "shared_blocked")


        self._print("An exclusive lock will block now (going to PENDING first)")
        t_exclusive = threading.Thread(target=exclusive_locker)
        t_exclusive.start()
        t_exclusive.join(0.1)
        self._print(self.manager)
        self.assertTrue(t_exclusive.is_alive())
        self.assertEqual(self.manager._filelocks["filename"]._lock_holders["exclusive"], LOCK_PENDING)

        self._print("A shared lock will be added to the end of the queue")
        t_shared = threading.Thread(target=shared_locker)
        t_shared.start()
        t_shared.join(0.1)
        self._print(self.manager)
        self.assertTrue(t_shared.is_alive())
        self.assertTrue("shared_blocked" not in self.manager._filelocks["filename"]._lock_holders)

        self._print("Unblocking the exclusive locker")
        self.manager.unlock("filename", LOCK_NONE, "shared_1")
        self.manager.unlock("filename", LOCK_NONE, "shared_2")
        t_exclusive.join()
        self._print(self.manager)

        # Shared lock must still be blocked
        t_shared.join(0.1)
        self.assertTrue(t_shared.is_alive())
        self.assertTrue("shared_blocked" not in self.manager._filelocks["filename"]._lock_holders)

        self._print("Unblocking the shared locker")
        self.manager.unlock("filename", LOCK_NONE, "exclusive")
        t_shared.join()

        self._print(self.manager)
        self.assertFalse(self.manager.is_idle())
        self.manager.unlock("filename", LOCK_NONE, "shared_blocked")
        self.assertTrue(self.manager.is_idle())

    def CheckMutualExclusion(self):
        """Checks that RESERVED or higher locks are mutually exclusive."""

        def locker_thread(clientname):
            try:
                for level in lock_sequence:
                    self.manager.lock(self.lockfunc, "filename", level, clientname)
                    time.sleep(0.05)
                active_threads.add(threading.current_thread())
                # In case the mutual exclusion does not work, this gives other threads
                # the chance to take over.
                time.sleep(0.1)
                concurrent_threads.append(set(active_threads))
                active_threads.discard(threading.current_thread())
                self.manager.unlock("filename", LOCK_NONE, clientname)
            except Exception, e:
                traceback.print_exc()
                exceptions.append(e)

        for lock_sequence in [LOCK_RESERVED, LOCK_EXCLUSIVE], [LOCK_RESERVED], [LOCK_EXCLUSIVE]:
            active_threads = set()
            concurrent_threads = []
            exceptions = []

            threads = [threading.Thread(target=locker_thread, args=(name,), name=name)
                    for name in ("Locker Thread #{0}".format(i) for i in range(5))]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
            self.assertFalse(exceptions, "Exceptions in threads for lock sequence {0!r}: {1!r}.".format(lock_sequence, exceptions))
            for entry in concurrent_threads:
                self.assertTrue(len(entry) == 1, "Concurrent threads detected via lock sequence {0!r}: {1!r}".format(lock_sequence, entry))

    def CheckLockFuncFailure(self):
        """
        Checks that a failure in the underlying lock function (the actual filesystem lock)
        is handled gracefully.
        """
        def bad_lockfunc(level):
            raise SyntheticLockFuncError()

        try:
            self.manager.lock(bad_lockfunc, "filename", LOCK_SHARED, "client")
            self.fail("Should have raised")
        except SyntheticLockFuncError:
            pass
        self._print(self.manager)

        # As the real locking operation failed, the lock manager should not pretend
        # that the client is holding a lock.
        self.assertTrue(self.manager.is_idle())


class SyntheticLockFuncError(RuntimeError):
    """Exception raised in tests to simulate a failure."""
    pass


def suite():
    lock_manager_suite = unittest.makeSuite(LockManagerTests, "Check")
    return unittest.TestSuite((lock_manager_suite))

def test():
    runner = unittest.TextTestRunner()
    runner.run(suite())

if __name__ == "__main__":
    test()
