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
import os, unittest
import pysqlite2.dbapi2 as sqlite


class VFSTests(unittest.TestCase):

    def setUp(self):
        self.vfs = sqlite.VFS()

    def tearDown(self):
        self.vfs = None

    def CheckVersionIsInt(self):
        self.assertIsInstance(self.vfs.version, int)

    def CheckNameIsString(self):
        self.assertIsInstance(self.vfs.name, basestring)


def suite():
    default_suite = unittest.makeSuite(VFSTests, "Check")
    return unittest.TestSuite((default_suite,))

def test():
    runner = unittest.TextTestRunner()
    runner.run(suite())

if __name__ == "__main__":
    test()
