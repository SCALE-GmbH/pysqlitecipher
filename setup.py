# coding=utf-8
# setup.py: the distutils script
#
# Copyright (C) 2015 Matthias Büchse <github@mbue.de> (integration of earlier work into one package)
# Copyright (C) 2012-2014 Torsten Landschoff <torsten@landschoff.net> (Python lock manager, SAVEPOINT support)
# Copyright (C) 2013 Kali Kaneko <kali@futeisha.org> (sqlcipher support)
# Copyright (C) 2004-2007 Gerhard Häring <gh@ghaering.de>
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

import glob
from itertools import ifilter
import os
import re
import sys

from distutils.core import setup, Extension, Command

import cross_bdist_wininst

# If you need to change anything, it should be enough to change setup.cfg.
PYSQLITE_EXPERIMENTAL = False

SOURCES = ["src/module.c", "src/connection.c", "src/cursor.c", "src/cache.c",
           "src/microprotocols.c", "src/prepare_protocol.c", "src/statement.c",
           "src/util.c", "src/row.c", "src/vfs.c", "src/inherit_vfs.c"]
if PYSQLITE_EXPERIMENTAL:
    SOURCES.append("src/backup.c")

DEFINE_MACROS = [("SQLITE_ENABLE_FTS3", "1"), ("SQLITE_ENABLE_RTREE", "1"),("THREADSAFE", "1"),
                 ("SQLITE_ENABLE_COLUMN_METADATA", "1")]

LONG_DESCRIPTION = """\
Python interface to SQLite 3

pysqlite is an interface to the SQLite 3.x embedded relational database engine.
It is almost fully compliant with the Python database API version 2.0 also
exposes the unique features of SQLite."""


DATA_FILES = [("pysqlite2-doc", glob.glob("doc/*.html") + glob.glob("doc/*.txt") + glob.glob("doc/*.css")),
              ("pysqlite2-doc/code", glob.glob("doc/code/*.py"))]


class DocBuilder(Command):
    description = "Builds the documentation"
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        import os
        import shutil
        try:
            shutil.rmtree("build/doc")
        except OSError:
            pass
        os.makedirs("build/doc")
        rc = os.system("sphinx-build doc/sphinx build/doc")
        if rc != 0:
            print "Is sphinx installed? If not, try 'sudo easy_install sphinx'."


AMALGAMATION_ROOT = "amalgamation"
QMARK = '"' if sys.platform != "win32" else '\\"'


def get_lite_ext():
    amalgamation_dir = os.path.join(AMALGAMATION_ROOT, "sqlite3")
    extra_macros = [('MODULE_NAME', QMARK + 'pysqlite2.dbapi2' + QMARK)]
    extra_sources = [os.path.join(amalgamation_dir, "sqlite3.c")]
    return Extension(name="pysqlite2._sqlite", sources=SOURCES + extra_sources, include_dirs=[amalgamation_dir],
                     define_macros=DEFINE_MACROS + extra_macros)


def get_cipher_ext():
    amalgamation_dir = os.path.join(AMALGAMATION_ROOT, "sqlcipher")
    extra_macros = [('MODULE_NAME', QMARK + 'pysqlite2.dbapi2cipher' + QMARK),
                    ("SQLITE_ENABLE_LOAD_EXTENSION", "1"), ("SQLITE_HAS_CODEC", "1"), ("SQLITE_TEMP_STORE", "2")]
    extra_sources = [os.path.join(amalgamation_dir, "sqlite3.c")]
    include_dirs = [amalgamation_dir]
    extra_link_args = []
    if sys.platform == "win32":
        # Try to locate openssl
        openssl_conf = os.environ.get('OPENSSL_CONF')
        if not openssl_conf:
            sys.exit('Fatal error: OpenSSL could not be detected!')
        openssl = os.path.dirname(os.path.dirname(openssl_conf))

        # Configure the compiler
        include_dirs.append(os.path.join(openssl, "include"))
        extra_macros.append(("inline", "__inline"))

        # Configure the linker
        extra_link_args.append("libeay32.lib")
        extra_link_args.append("/LIBPATH:" + os.path.join(openssl, "lib"))
    else:
        extra_link_args.append("-lcrypto")
    return Extension(name="pysqlite2._sqlcipher", sources=SOURCES + extra_sources, include_dirs=include_dirs,
                     extra_link_args=extra_link_args, define_macros=DEFINE_MACROS + extra_macros)


def determine_version(module_h_path):
    version_re = re.compile('#define PYSQLITE_VERSION "(.*)"')
    with open(module_h_path) as f:
        match = next(ifilter(bool, (version_re.match(line) for line in f)), None)
    if match is None:
        raise SystemExit("Fatal error: PYSQLITE_VERSION could not be detected!")
    return match.groups()[0]
    # pysqlite_minor_version = ".".join(result.split('.')[:2])


def main():
    setup_args = dict(
        name="pysqlitecipher",
        version=determine_version(os.path.join("src", "module.h")),
        description="DB-API 2.0 interface for both SQLite 3.x and SQLCipher",
        long_description=LONG_DESCRIPTION,
        author="Gerhard Haering",
        author_email="gh@ghaering.de",
        license="zlib/libpng license",
        platforms="ALL",
        url="https://github.com/mbuechse/pysqlite",
        download_url="https://github.com/mbuechse/pysqlite/archive/master.zip",

        # Description of the modules and packages in the distribution
        package_dir={"pysqlite2": "lib"},
        packages=["pysqlite2", "pysqlite2.test"] + (["pysqlite2.test.py25"], [])[sys.version_info < (2, 5)],
        scripts=[],
        data_files=DATA_FILES,
        ext_modules=[get_lite_ext(), get_cipher_ext()],
        classifiers=[
            "Development Status :: 5 - Production/Stable",
            "Intended Audience :: Developers",
            "License :: OSI Approved :: zlib/libpng License",
            "Operating System :: MacOS :: MacOS X",
            "Operating System :: Microsoft :: Windows",
            "Operating System :: POSIX",
            "Programming Language :: C",
            "Programming Language :: Python",
            "Topic :: Database :: Database Engines/Servers",
            "Topic :: Software Development :: Libraries :: Python Modules"],
        cmdclass={"build_docs": DocBuilder, "cross_bdist_wininst": cross_bdist_wininst.bdist_wininst})
    setup(**setup_args)

if __name__ == "__main__":
    main()
