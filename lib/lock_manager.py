# -*- coding: utf-8 -*-

import pprint
import threading
import traceback
from collections import deque


LOCK_NONE = 0
LOCK_SHARED = 1
LOCK_RESERVED = 2
LOCK_PENDING = 3
LOCK_EXCLUSIVE = 4

LEVEL_NAMES = {
    LOCK_NONE:      "NONE",
    LOCK_SHARED:    "SHARED",
    LOCK_RESERVED:  "RESERVED",
    LOCK_PENDING:   "PENDING",
    LOCK_EXCLUSIVE: "EXCLUSIVE"
}

assert LOCK_NONE < LOCK_SHARED < LOCK_RESERVED < LOCK_PENDING < LOCK_EXCLUSIVE


def _ascend_level(old_level, new_level):
    r = []
    for level in (1, 2, 4):
        if level > new_level:
            break
        if level > old_level:
            r.append(level)
    return r


class DeadlockError(Exception):
    """
    Should only be thrown by a LockManager implementation. This exception
    is caught in the C module and translated to SQLITE_BUSY to tell the caller
    that the lock can not be acquired. It should never get through to the
    client code using the DB-API.
    """
    pass


class LockManager(object):
    """
    Coordinates locking of SQLite database files.

    Plain SQLite does not ensure fairness when accessing database files. Each
    client tries to get a database lock by non-blocking lock attempts. This
    leads to starvation if one client continuously writes to the database,
    as other clients have only a small chance of getting a lock on the
    database file.

    When using PySQLite, a connection can have a lock manager associated which
    coordinates locking of the database. The lock manager can ensure fairness
    or give priority access to specific clients. It still has to use the
    original locking mechanism as a base to ensure protection against database
    corruption, especially when accessing the database from multiple unrelated
    processes.

    """

    def lock(self, lockfunc, filename, level, client):
        """
        Lock the database file given in *filename* to the locking level given
        in *level* for use by *client*. The parameter *lockfunc* represents
        the original locking function of the underlying SQLite VFS. After
        checking for conflicts, *lockfunc* must be called for all locking
        levels from the current level up to the requested locking level.

        Called before OS level locking of a database file. This should ensure
        fairness between clients of the same file. level is the desired locking
        level (see http://www.sqlite.org/c3ref/c_lock_exclusive.html for known
        levels, also http://www.sqlite.org/lockingv3.html#locking). Note that
        the pending level is never requested directly, it is managed internally
        by the OS layer.

        :param filename (unicode): Absolute name of the file that is addressed.
        :param level (int): Requested locking level
        :param client: Some object usable as key to differentiate between
                connections to the same database.
        """

    def lock_result(self, filename, level, client, resultcode):
        """
        Called after OS level locking of a database file. This notifies the
        lock manager of the result of the OS level locking directive. If
        resultcode is non-zero, the attempt to lock the file failed. In that
        case, the effect of the last invocation of lock by the same client
        must be rolled back.

        :param filename (unicode): Absolute name of the file that is addressed.
        :param level (int): Requested locking level
        :param client: Some object usable as key to differentiate between
                connections to the same database.
        :param resultcode (int): SQLite error code, 0 on success
        """
        raise NotImplementedError("{0}.lock_result".format(type(self).__name__))

    def unlock(self, filename, level, client):
        """
        Called after the database file was unlocked on the underlying OS level.
        Should wake up the first client blocked in lock() that waits on the same
        file.

        :param filename (unicode): Absolute name of the file that is addressed.
        :param level (int): New locking level (lower or same to the level of the
                matching lock call by the same client).
        :param client: Connection key.
        """


class DefaultLockManager(LockManager):
    """
    A fair lock manager that can be used to coordinate database threads inside
    a single process.

    Keeps a dictionary of filenames involved in active or pending locks, where
    each filename is associated its own SharedExclusiveLock instance.
    """

    def __init__(self, timeout=5):
        #: Protects the lock manager and its contained SharedExclusiveLocks
        self._mutex = threading.RLock()

        #: Individual lock for each filename, as mapping name -> lock
        self._filelocks = {}

        #: How long to wait to get a lock in seconds (None: block forever)
        self.timeout = timeout

    def lock(self, lockfunc, filename, level, client):
        # print repr(("lock", lockfunc, filename, level, client))
        with self._mutex:
            filelock = self._filelocks.get(filename)
            if not filelock:
                filelock = SharedExclusiveLock(mutex=self._mutex, timeout=self.timeout)
                self._filelocks[filename] = filelock
            try:
                old_level = filelock.lock(level, client)
            except _LockTimeoutError:
                raise DeadlockError()
            finally:
                if filelock.is_idle():
                    self._filelocks.pop(filename, None)

        try:
            levels = _ascend_level(old_level, level)
            for l in levels:
                lockfunc(l)
            self.lock_result(filename, level, client, 0)
        except Exception, e:
            # print "lockfunc raised {0}: {1}.".format(type(e).__name__, repr(e.args))
            self.unlock(filename, old_level, client)
            self.lock_result(filename, level, client, e.message)
            raise

    def lock_result(self, filename, level, client, resultcode):
        # print repr(("lock_result", filename, level, client, resultcode))
        pass

    def unlock(self, filename, level, client):
        # print repr(("unlock", filename, level, client))
        with self._mutex:
            filelock = self._filelocks.get(filename)
            if not filelock:
                filelock = SharedExclusiveLock(mutex=self._mutex, timeout=self.timeout)
                self._filelocks[filename] = filelock
            try:
                return filelock.unlock(level, client)
            finally:
                if filelock.is_idle():
                    self._filelocks.pop(filename, None)

    def is_idle(self):
        with self._mutex:
            return not self._filelocks

    def __repr__(self):
        with self._mutex:
            level_counts = {}
            blocked_count = 0

            for filename, filelock in self._filelocks.iteritems():
                file_level_counts, file_blocked_count = filelock.get_stats()
                for level, count in file_level_counts.items():
                    level_counts[level] = level_counts.get(level, 0) + count
                blocked_count += file_blocked_count

        if level_counts:
            holder_parts = []
            for level, count in sorted(level_counts.items()):
                holder_parts.append("{0}: {1}".format(LEVEL_NAMES.get(level) or level, count))
            holder_summary = ", ".join(holder_parts)
        else:
            holder_summary = "IDLE"

        return '<{0} {1}, {2} blocked>'.format(type(self).__name__, holder_summary, blocked_count)


class SharedExclusiveLock(object):

    def __init__(self, mutex=None, timeout=None):
        #: Mutex used to implement the monitor pattern of the lock.
        self._mutex = threading.RLock() if mutex is None else mutex

        #: Map of clients to the lock level they are holding. Contains only
        #: clients actually holding a lock.
        self._lock_holders = {}

        #: Tracebacks of the locations where each client in _lock_holders got
        #: his lock. Each traceback is stored as a list of tuples as returned
        #: by traceback.extract_tb
        self._lock_holder_traceback = {}

        #: Queue of clients blocked waiting for the lock. Each entry is an
        #: instance of BlockedClientInfo.
        self._blocked_clients = deque()

        #: Timeout for blocked clients. After waiting this many seconds, a
        #: busy error is returned.
        self.timeout = timeout

        self.check_invariant()

    def lock(self, level, client):
        """
        Acquires a lock at the *level* for *client*. If the client already
        holds the lock, its lock level is raised to *level*. This is a no
        op if the new level is the same or lower than the current level.

        .. note::

            This method will never lower the lock level, even if the new level
            is smaller than the previous level.

        :param int level: Requested locking level, see LEVEL_NAMES.keys()
        :param hashable client: Identifier for the client requesting the lock.
                This can be anything that can be used as key for a dictionary.
        :returns: the original locking level of the client
        :rtype: int
        :raises: DeadlockError if the requested lock level leads to a dead
                lock with another thread
        :raises: _LockTimeoutError if the waiting time exceeded the timeout
        """
        with self._mutex:
            old_level = self._lock_holders.get(client, LOCK_NONE)
            if level > old_level:

                if level == LOCK_SHARED:
                    self._acquire_shared(client, old_level)
                elif level == LOCK_RESERVED:
                    self._acquire_reserved(client, old_level)
                elif level == LOCK_EXCLUSIVE:
                    self._acquire_exclusive(client, old_level)
                else:
                    raise ValueError(
                            "Bad lock level {0}, must be LOCK_SHARED ({1}), LOCK_RESERVED ({2}) or LOCK_EXCLUSIVE ({3}))."
                            .format(level, LOCK_SHARED, LOCK_RESERVED, LOCK_EXCLUSIVE))

            self.check_invariant()
            return old_level

    def unlock(self, level, client):
        with self._mutex:
            old_level = self._lock_holders.get(client, LOCK_NONE)
            if level < old_level:
                if level == LOCK_NONE:
                    del self._lock_holders[client]
                    self._lock_holder_traceback.pop(client)
                else:
                    self._lock_holders[client] = level
                self._wakeup_blocked()

    def _acquire_shared(self, client, old_level):
        lock_levels = [v for (k, v) in self._lock_holders.items() if k != client]
        max_level = max(lock_levels) if lock_levels else LOCK_NONE

        if max_level < LOCK_PENDING and not self._blocked_clients:
            # Fast path: No conflicting lock.
            self._lock_holders[client] = LOCK_SHARED
            self._lock_holder_traceback[client] = traceback.extract_stack()
        else:
            self._wait(client, LOCK_SHARED)

    def _acquire_reserved(self, client, old_level):
        lock_levels = [v for (k, v) in self._lock_holders.items() if k != client]
        max_level = max(lock_levels) if lock_levels else LOCK_NONE

        if max_level < LOCK_RESERVED and not self._blocked_clients:
            # Fast path: No conflicting lock.
            self._lock_holders[client] = LOCK_RESERVED
            self._lock_holder_traceback[client] = traceback.extract_stack()
        else:
            if old_level != LOCK_NONE:
                raise DeadlockError()
            self._wait(client, LOCK_RESERVED)

    def _acquire_exclusive(self, client, old_level):
        lock_levels = [v for (k, v) in self._lock_holders.items() if k != client]
        max_level = max(lock_levels) if lock_levels else LOCK_NONE

        if max_level == LOCK_NONE:
            # Fast path: No conflicting lock.
            self._lock_holders[client] = LOCK_EXCLUSIVE
            self._lock_holder_traceback[client] = traceback.extract_stack()
        elif max_level == LOCK_SHARED:
            # Have to wait for shared locks to be released.
            self._lock_holders[client] = LOCK_PENDING
            self._lock_holder_traceback[client] = traceback.extract_stack()
            self._wait(client, LOCK_EXCLUSIVE, enqueue_front=True)
        else:   # max_level >= LOCK_RESERVED
            # We can not have a reserved or higher lock level if any other client has
            # a reserved or higher lock level.
            assert old_level == LOCK_NONE or old_level == LOCK_SHARED

            if old_level == LOCK_NONE:
                self._wait(client, LOCK_EXCLUSIVE)
            else:
                raise DeadlockError()

    def _wait(self, client, level, enqueue_front=False):
        blocked_info = BlockedClientInfo(client, level, self._mutex, self.timeout)
        if enqueue_front:
            self._blocked_clients.appendleft(blocked_info)
        else:
            self._blocked_clients.append(blocked_info)

        blocked_info.wait()
        if blocked_info.got_timeout():
            print "got timeout"
            pprint.pprint(vars(self))
            for pos in range(len(self._blocked_clients)):
                if self._blocked_clients[pos] is blocked_info:
                    del self._blocked_clients[pos]
                    break
            raise _LockTimeoutError()
        assert self._lock_holders[client] == level

    def _wakeup_blocked(self):
        while self._blocked_clients:
            blocked_info = self._blocked_clients.popleft()
            client = blocked_info.client
            level = blocked_info.level
            traceback = blocked_info.traceback
            lock_levels = [v for (k, v) in self._lock_holders.items() if k != client]
            max_level = max(lock_levels) if lock_levels else LOCK_NONE

            if level == LOCK_SHARED:
                if max_level < LOCK_PENDING:
                    self._lock_holders[client] = LOCK_SHARED
                    self._lock_holder_traceback[client] = traceback
                    blocked_info.signal()
                else:
                    self._blocked_clients.appendleft(blocked_info)
                    break

            elif level == LOCK_RESERVED:
                if max_level < LOCK_RESERVED:
                    self._lock_holders[client] = LOCK_RESERVED
                    self._lock_holder_traceback[client] = traceback
                    blocked_info.signal()
                else:
                    self._blocked_clients.appendleft(blocked_info)
                    break

            elif level == LOCK_EXCLUSIVE:
                if max_level == LOCK_NONE:
                    self._lock_holders[client] = LOCK_EXCLUSIVE
                    self._lock_holder_traceback[client] = traceback
                    blocked_info.signal()
                elif max_level == LOCK_SHARED:
                    self._lock_holders[client] = LOCK_PENDING
                    self._lock_holder_traceback[client] = traceback
                    self._blocked_clients.appendleft(blocked_info)
                    break
                else:   # max_level >= LOCK_RESERVED
                    self._blocked_clients.appendleft(blocked_info)
                    break
            else:
                raise Exception("Unexpected lock level in blocked queue: {0}".format(level))

        self.check_invariant()

    def is_idle(self):
        with self._mutex:
            return not self._lock_holders

    def get_stats(self):
        with self._mutex:
            level_counts = {}
            for level in self._lock_holders.values():
                level_counts[level] = level_counts.get(level, 0) + 1
            blocked_count = len(self._blocked_clients)

        return level_counts, blocked_count

    def __repr__(self):
        level_counts, blocked_count = self.get_stats()

        if level_counts:
            holder_parts = []
            for level, count in sorted(level_counts.items()):
                holder_parts.append("{0}: {1}".format(LEVEL_NAMES.get(level) or level, count))
            holder_summary = ", ".join(holder_parts)
        else:
            holder_summary = "IDLE"

        return '<{0} {1}, {2} blocked>'.format(type(self).__name__, holder_summary, blocked_count)

    def check_invariant(self):
        with self._mutex:
            lock_levels = set(self._lock_holders.values())
            max_level = max(lock_levels) if lock_levels else LOCK_NONE

            # lock_levels can only include levels given as key in LEVEL_NAMES
            assert lock_levels.issubset(LEVEL_NAMES)

            # Nobody can be blocked if nobody is holding a lock.
            assert not self._blocked_clients or self._lock_holders

            # A shared lock request can only be blocked by >= PENDING
            assert not (self._blocked_clients and self._blocked_clients[0].level == LOCK_SHARED) or max_level >= LOCK_PENDING

            # At most one client can have lock level > LOCK_SHARED
            assert len([holder for (holder, level) in self._lock_holders.items() if level > LOCK_SHARED]) <= 1

            # If a client holds an exclusive lock then he is the only lock holder
            assert LOCK_EXCLUSIVE not in lock_levels or len(self._lock_holders) == 1

            # A client may only be pending if there are other shared locks to wait for
            assert LOCK_PENDING not in lock_levels or [ \
                    holder for (holder, level) in self._lock_holders.items() if level == LOCK_SHARED]


class BlockedClientInfo(object):

    def __init__(self, client, level, mutex, timeout):
        self.client = client
        self.level = level
        self.timeout = timeout
        self.traceback = traceback.extract_stack()
        self._condition = threading.Condition(mutex)
        self._got_lock = False

    def wait(self):
        self._condition.wait(self.timeout)

    def signal(self):
        self._got_lock = True
        self._condition.notify()

    def got_timeout(self):
        return not self._got_lock

    def __repr__(self):
        return '<BlockedClientInfo {0!r} {1} {2}>'.format(self.client, LEVEL_NAMES[self.level],
                "notified" if self._got_lock else "blocked")


class _LockTimeoutError(Exception):
    """Internal exception class signalling that a lock attempt timed out."""


def get_lock_manager(lock_manager):
    return _lock_manager


def set_lock_manager(lock_manager):
    global _lock_manager
    _lock_manager = lock_manager


# Disable the lock manager by default - it is opt-in for now.
_lock_manager = None
