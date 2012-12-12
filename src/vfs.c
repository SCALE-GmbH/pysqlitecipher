#include "vfs.h"
#include "sqlite3.h"
#include <unistd.h>


/* Wraps sqlite3_io_methods for extension. */

typedef struct _wrapped_io_methods {
    sqlite3_io_methods vmt;
    const sqlite3_io_methods *root_vmt;
} wrapped_io_methods;


/* Forward declarations */

static int wrapped_xOpen(sqlite3_vfs*, const char *zName, sqlite3_file*, int flags, int *pOutFlags);
static int wrapped_xClose(sqlite3_file*);
static int wrapped_xLock(sqlite3_file*, int);
static int wrapped_xUnlock(sqlite3_file*, int);


/* Set up a special VFS for Python usage.

   This VFS is used to support better (customized) locking behaviour when
   SQLite is used from Python. The goal is to coordinate in-process access
   to one database to avoid expensive operating system calls.

   It is automatically registered as default VFS for now so look out if
   you are using SQLite from C inside the same process.
*/
int pysqlite_vfs_setup(void)
{
    sqlite3_vfs *wrapped_vfs = NULL;
    sqlite3_vfs *root_vfs = sqlite3_vfs_find(NULL);

    if (! root_vfs)
        return SQLITE_NOTFOUND;

    wrapped_vfs = sqlite3_malloc(sizeof(*wrapped_vfs));
    if (! wrapped_vfs)
        return SQLITE_NOMEM;

    memcpy(wrapped_vfs, root_vfs, sizeof(*wrapped_vfs));

    wrapped_vfs->zName = "pywrapped_vfs";
    wrapped_vfs->pAppData = root_vfs;
    wrapped_vfs->xOpen = wrapped_xOpen;

    return sqlite3_vfs_register(wrapped_vfs, 1);
}


/* Wrapper for xOpen of the original VFS.

   This is overriden only to allow us to replace the sqlite3_io_methods table
   that is returned by xOpen. Otherwise, we do not currently interfere with
   xOpen behaviour.
*/
static int wrapped_xOpen(
        sqlite3_vfs *vfs,
        const char *zName,
        sqlite3_file *file,
        int flags,
        int *pOutFlags)
{
    int rc;
    sqlite3_vfs *root_vfs = vfs->pAppData;

    fprintf(stderr, "xOpen(%s)\n", zName);
    rc = root_vfs->xOpen(root_vfs, zName, file, flags, pOutFlags);
    if (file->pMethods) {
        wrapped_io_methods *new_methods = sqlite3_malloc(sizeof(*new_methods));
        memcpy(&new_methods->vmt, file->pMethods, sizeof(new_methods->vmt));
        new_methods->root_vmt = file->pMethods;

        new_methods->vmt.xClose = wrapped_xClose;
        new_methods->vmt.xLock = wrapped_xLock;
        new_methods->vmt.xUnlock = wrapped_xUnlock;
        file->pMethods = new_methods;
    }
    fprintf(stderr, "xOpen(%s) -> %i\n", zName, rc);
    return rc;
}


/* Wrapper for xClose of the original VFS.

   We need to wrap this method only to let of of the sqlite3_io_methods table
   that was allocated in wrapped_xOpen.
*/
static int wrapped_xClose(sqlite3_file *file)
{
    int rc;
    wrapped_io_methods *methods = (wrapped_io_methods *) file->pMethods;
    const sqlite3_io_methods *root_vmt = methods->root_vmt;
    rc = root_vmt->xClose(file);
    if (rc == SQLITE_OK) {
        sqlite3_free(methods);
        file->pMethods = 0;
    }
    fprintf(stderr, "xClose() -> %i\n", rc);
    return rc;
}

/* Wrapper for xLock of the original VFS.

   This wrapper is the main reason for creating this VFS. At this point
   we should be able to interfere with SQLite's builtin locking behaviour
   and replace busy waiting with a fair shared-exclusive lock.
*/
static int wrapped_xLock(sqlite3_file *file, int lock_mode)
{
    int rc;
    wrapped_io_methods *methods = (wrapped_io_methods *) file->pMethods;
    const sqlite3_io_methods *root_vmt = methods->root_vmt;
    rc = root_vmt->xLock(file, lock_mode);
    fprintf(stderr, "xLock(%i) -> %i\n", lock_mode, rc);
    return rc;
}


/* Wrapper for xUnlock of the original VFS.

   This wrapper is needed for symmetry with the implementation of xLock.
   We need to know when a lock is released to correctly implement a better
   locking behaviour.
*/
static int wrapped_xUnlock(sqlite3_file *file, int lock_mode)
{
    int rc;
    wrapped_io_methods *methods = (wrapped_io_methods *) file->pMethods;
    const sqlite3_io_methods *root_vmt = methods->root_vmt;
    rc = root_vmt->xUnlock(file, lock_mode);
    fprintf(stderr, "xUnlock(%i) -> %i\n", lock_mode, rc);
    return rc;
}
