# -*- coding: utf-8 -*-

import threading


class DefaultLockManager(object):

    def lock(self, filename, level):
        """
        Called before OS level locking of a database file. This should ensure
        fairness between clients of the same file. level is the desired locking
        level (see http://www.sqlite.org/c3ref/c_lock_exclusive.html for known
        levels, also http://www.sqlite.org/lockingv3.html#locking). Note that
        the pending level is never requested directly, it is managed internally
        by the OS layer.

        :param filename: Absolute name of the file that is addressed.
        :param level (int): Requested locking level
        """
        client = threading.current_thread()
        print "xLock(%r, %r, %r)" % (filename, level, client)

    def unlock(self, filename, level):
        client = threading.current_thread()
        print "xUnlock(%r, %r, %r)" % (filename, level, client)


_lock_manager = DefaultLockManager()
