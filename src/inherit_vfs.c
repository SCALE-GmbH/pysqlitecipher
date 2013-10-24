#include "inherit_vfs.h"
#include <string.h>


int pysqlite_inherit_vfs(
    sqlite3_vfs *new_vfs,            /* VFS structure to populate */
    const sqlite3_vfs *orig_vfs,     /* original VFS to forward to */
    const char *vfs_name)            /* Name to assign to the new VFS */
{
    if (!(new_vfs && orig_vfs && vfs_name))
        return SQLITE_MISUSE;

    memset(new_vfs, 0, sizeof(*new_vfs));
    new_vfs->zName = vfs_name;

#   define INHERIT(name) new_vfs->name = orig_vfs->name
    INHERIT(iVersion);
    /* We only support version 2 at this time. To keep working with newer
       SQLite releases, we need to fall back in those cases. */
    if (new_vfs->iVersion > 3)
        new_vfs->iVersion = 3;
    INHERIT(szOsFile);
    INHERIT(mxPathname);
    INHERIT(pAppData);
    INHERIT(xOpen);
    INHERIT(xDelete);
    INHERIT(xAccess);
    INHERIT(xFullPathname);
    INHERIT(xDlOpen);
    INHERIT(xDlError);
    INHERIT(xDlSym);
    INHERIT(xDlClose);
    INHERIT(xRandomness);
    INHERIT(xSleep);
    INHERIT(xCurrentTime);
    INHERIT(xGetLastError);

    if (orig_vfs->iVersion >= 2) {
        INHERIT(xCurrentTimeInt64);
    }

    if (orig_vfs->iVersion >= 3) {
        INHERIT(xSetSystemCall);
        INHERIT(xGetSystemCall);
        INHERIT(xNextSystemCall);
    }
#   undef INHERIT

    return SQLITE_OK;
}


int pysqlite_inherit_io_methods(
    sqlite3_io_methods *new_vmt,
    const sqlite3_io_methods *orig_vmt)
{
#   define INHERIT(name)    new_vmt->name = orig_vmt->name
    INHERIT(iVersion);
    /* We support only VMT version <= 2 at this time. */
    if (new_vmt->iVersion > 2)
        new_vmt->iVersion = 2;

    INHERIT(xClose);
    INHERIT(xRead);
    INHERIT(xWrite);
    INHERIT(xTruncate);
    INHERIT(xSync);
    INHERIT(xFileSize);
    INHERIT(xLock);
    INHERIT(xUnlock);
    INHERIT(xCheckReservedLock);
    INHERIT(xFileControl);
    INHERIT(xSectorSize);
    INHERIT(xDeviceCharacteristics);

    if (orig_vmt->iVersion >= 2) {
        INHERIT(xShmMap);
        INHERIT(xShmLock);
        INHERIT(xShmBarrier);
        INHERIT(xShmUnmap);
    }
#   undef INHERIT

    return SQLITE_OK;
}
