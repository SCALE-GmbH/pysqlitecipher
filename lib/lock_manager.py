# -*- coding: utf-8 -*-

import threading
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


class DeadlockError(Exception):
    pass


class LockManager(object):

    def lock(self, filename, level, client):
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

    def unlock(self, filename, level, client):
        """
        """


class DefaultLockManager(LockManager):

    def __init__(self):
        #: Mutex used to implement the monitor pattern of the manager.
        self._mutex = threading.RLock()

        #: Map of clients to the lock level they are holding. Contains only
        #: clients actually holding a lock.
        self._lock_holders = {}

        #: Queue of clients blocked waiting for the lock. Each entry is an
        #: instance of BlockedClientInfo.
        self._blocked_clients = deque()

        self.check_invariant()

    def lock(self, filename, level, client):
        if not filename:
            return
        with self._mutex:
            print "xLock(%r, %s, %r)" % (filename, level, client)

            # A call to lock must increase the lock level of the client
            assert level > self._lock_holders.get(client, LOCK_NONE)

            if level > LOCK_SHARED and self._lock_holders.get(client) == LOCK_SHARED:
                max_level = max(self._lock_holders.values())
                if max_level > LOCK_SHARED:
                    raise DeadlockError()

            blocked_info = BlockedClientInfo(client, level, self._mutex)
            self._blocked_clients.append(blocked_info)
            self._update_state()

        try:
            blocked_info.wait()
        except:
            print "Status:", repr(self)
            print "Vars:", vars(self)
            raise

        with self._mutex:
            assert self._lock_holders[client] == level

    def unlock(self, filename, level, client):
        if not filename:
            return
        with self._mutex:
            print "xUnlock(%r, %s, %r)" % (filename, level, client)
            if client not in self._lock_holders and level == LOCK_NONE:
                return

            # Unlock must only be called to decrease the lock level
            assert level < self._lock_holders.get(client, LOCK_NONE)

            if level == LOCK_NONE:
                del self._lock_holders[client]
            else:
                self._lock_holders[client] = level

            self._update_state()

    def _update_state(self):
        with self._mutex:
            done = False
            while self._blocked_clients and not done:
                lock_levels = set(self._lock_holders.values())
                max_level = max(lock_levels) if lock_levels else LOCK_NONE

                blocked_info = self._blocked_clients[0]

                if blocked_info.level == LOCK_SHARED:

                    if max_level <= LOCK_RESERVED:
                        self._blocked_clients.popleft()
                        assert blocked_info not in self._lock_holders
                        self._lock_holders[blocked_info.client] = LOCK_SHARED
                        blocked_info.signal()
                    else:
                        done = True

                elif blocked_info.level == LOCK_RESERVED:

                    if max_level <= LOCK_SHARED:
                        self._blocked_clients.popleft()
                        self._lock_holders[blocked_info.client] = LOCK_RESERVED
                        blocked_info.signal()

                elif blocked_info.level == LOCK_EXCLUSIVE:

                    client_has_lock = blocked_info.client in self._lock_holders

                    if not self._lock_holders or (client_has_lock and len(self._lock_holders) == 1):
                        # Nobody holds a lock or the client is the only holder of a lock
                        self._blocked_clients.popleft()
                        self._lock_holders[blocked_info.client] = LOCK_EXCLUSIVE
                        blocked_info.signal()
                    elif client_has_lock:
                        # Somebody else must be holding a lock as well.
                        if self._lock_holders[blocked_info.client] == LOCK_SHARED and max_level > LOCK_SHARED:
                            # We hold a shared lock and somebody else has a higher lock level. If we block
                            # now, this means we deadlock. Instead reject the request.
                            self._blocked_clients.popleft()
                            blocked_info.signal(deadlock=True)
                        elif self._lock_holders[blocked_info.client] == LOCK_RESERVED or self._lock_holders[blocked_info.client] == LOCK_PENDING:
                            # We hold a reserved lock already. Wait for shared locks to be released.
                            self._lock_holders[blocked_info.client] = LOCK_PENDING
                            done = True
                    else:
                        self._lock_holders[blocked_info.client] = LOCK_PENDING
                        done = True

                else:
                    raise Exception("Invalid requested locking level {0}".format(blocked_info.level))

            print repr(self)
            self.check_invariant()

    def is_idle(self):
        with self._mutex:
            return not self._lock_holders and not self._blocked_clients

    def __repr__(self):
        with self._mutex:
            level_counts = {}
            for level in self._lock_holders.values():
                level_counts[level] = level_counts.get(level, 0) + 1
            blocked_count = len(self._blocked_clients)

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

            # Nobody can be blocked if nobody is holding a lock.
            assert not self._blocked_clients or self._lock_holders

            # A shared lock request can only be blocked by >= PENDING
            assert not (self._blocked_clients and self._blocked_clients[0].level == LOCK_SHARED) or max_level >= LOCK_PENDING

            # At most one client can have lock level > LOCK_SHARED
            assert len([x for x in lock_levels if x > LOCK_SHARED]) <= 1

            # If a client holds an exclusive lock then he is the only lock holder
            assert LOCK_EXCLUSIVE not in lock_levels or len(self._lock_holders) == 1

            # A client may only be pending if there are other shared locks to wait for
            assert LOCK_PENDING not in lock_levels or len(self._lock_holders) > 1


class BlockedClientInfo(object):

    def __init__(self, client, level, mutex):
        self.client = client
        self.level = level
        self._event = threading.Event()

    def wait(self):
        self._event.wait(2)
        if not self._event.is_set():
            print "timeout"
            raise Exception("internal error, timeout")

    def signal(self):
        self._event.set()

    def __repr__(self):
        return '<BlockedClientInfo {0!r} {1} {2}>'.format(self.client, LEVEL_NAMES[self.level],
                "notified" if self._event.is_set() else "blocked")


_lock_manager = DefaultLockManager()
