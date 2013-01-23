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
import time
import pysqlite2.dbapi2 as sqlite
from pysqlite2.lock_manager import DefaultLockManager, DeadlockError, \
        LOCK_NONE, LOCK_SHARED, LOCK_RESERVED, LOCK_PENDING, LOCK_EXCLUSIVE


class LockManagerTests(unittest.TestCase):

    def setUp(self):
        self.manager = DefaultLockManager()
        self.lockfunc = lambda x: None

    def tearDown(self):
        self.manager = None

    def _print(self, fmt, *args):
        # print fmt % args
        pass

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


def suite():
    lock_manager_suite = unittest.makeSuite(LockManagerTests, "Check")
    return unittest.TestSuite((lock_manager_suite))

def test():
    runner = unittest.TextTestRunner()
    runner.run(suite())

if __name__ == "__main__":
    test()
