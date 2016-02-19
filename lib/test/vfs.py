#-*- coding: utf-8 -*-
# pysqlite2/test/vfs.py: virtual file system tests
#
# Copyright (C) 2016 Scale GmbH
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

import sys
import tempfile
import os, unittest
import pysqlite2.dbapi2 as sqlite


class VFSTests(unittest.TestCase):

    def setUp(self):
        self.vfs = sqlite.VFS()
        fd, self.temporary_file = tempfile.mkstemp()
        os.close(fd)

    def tearDown(self):
        self.vfs = None
        os.remove(self.temporary_file)

    def CheckVersionIsInt(self):
        self.assertIsInstance(self.vfs.version, int)

    def CheckNameIsString(self):
        self.assertIsInstance(self.vfs.name, basestring)

    def CheckSharedLock(self):
        """Two handles to the same file can be locked in shared mode at the same time."""
        file_a = self.vfs.open(self.temporary_file, self.vfs.OPEN_READWRITE | self.vfs.OPEN_MAIN_DB)
        file_a.lock(file_a.LOCK_SHARED)

        file_b = self.vfs.open(self.temporary_file, self.vfs.OPEN_READWRITE | self.vfs.OPEN_MAIN_DB)
        file_b.lock(file_b.LOCK_SHARED)

    def CheckSharedBlocksExclusiveLock(self):
        file_a = self.vfs.open(self.temporary_file, self.vfs.OPEN_READWRITE | self.vfs.OPEN_MAIN_DB)
        file_a.lock(file_a.LOCK_SHARED)

        try:
            file_b = self.vfs.open(self.temporary_file, self.vfs.OPEN_READWRITE | self.vfs.OPEN_MAIN_DB)
            file_b.lock(file_b.LOCK_EXCLUSIVE)
            self.fail("lock exclusive should have failed")
        except sqlite.DatabaseError, e:
            if not "database is locked" in str(e):
                raise

    def CheckClosingDropsLock(self):
        """When a file is closed/dropped, the locks should get released."""
        file_a = self.vfs.open(self.temporary_file, self.vfs.OPEN_READWRITE | self.vfs.OPEN_MAIN_DB)
        file_a.lock(file_a.LOCK_EXCLUSIVE)
        file_a = None  # -> closes file, no close() method so far

        file_b = self.vfs.open(self.temporary_file, self.vfs.OPEN_READWRITE | self.vfs.OPEN_MAIN_DB)
        file_b.lock(file_b.LOCK_EXCLUSIVE)


def suite():
    default_suite = unittest.makeSuite(VFSTests, "Check")
    return unittest.TestSuite((default_suite,))

def test():
    runner = unittest.TextTestRunner()
    runner.run(suite())

if __name__ == "__main__":
    test()
