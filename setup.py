from distutils.core import setup
from setuptools import find_packages

setup(name='blitzdb',
version='0.2.1',
author='Andreas Dewes - 7scientists',
author_email = 'andreas@7scientists.com',
license = 'MIT',
entry_points = {
    },
url='https://github.com/adewes/blitzdb',
packages=find_packages(),
zip_safe = False,
description = 'A document-oriented database written purely in Python.',
long_description = """Blitz is a document-oriented database toolkit for Python that is backend-agnostic. 

It comes with a flat-file database for JSON documents and provides MongoDB-like querying capabilities.

Key Features
============

* Document-based, object-oriented interface.
* Powerful and rich querying language.
* Deep document indexes on arbitrary fields.
* Compressed storage of documents.
* Support for multiple backends (e.g. file-based storage, MongoDB).
* Support for database transactions (currently only for the file-based backend).

Documentation
=============

An extensive documentation, including tutorials and installation instructions is available on `ReadTheDocs <http://blitz-db.readthedocs.org/>`_.

Source Code
===========

The source code is available on `GitHub <https://github.com/adewes/blitzdb>`_.

Isssue Tracker
==============

If you should encounter any problems when using BlitzDB, please feel free to `submit an issue <https://github.com/adewes/blitzdb/issues>`_ on Github.

Changelog
=========

* 0.2.1: Better tests.
* 0.2.0: Support for including additional information in DB references. Support for accessing document attributes as dictionary items.
         Added $regex parameter that allows to use regular expressions in queries.
* 0.1.5: MongoDB backend now supports database transactions. Database operations are now read-isolated by default, i.e.
         uncommitted operations will not affect database queries before they are committed.
* 0.1.4: Improved indexing of objects for the file backend, added support for automatic serialization/deserialization
         of object attributes when adding keys to or querying an index.
* 0.1.3: Sorting of query sets is now supported (still experimental)
* 0.1.2: Small bugfixes, BlitzDB version number now contained in DB config dict
* 0.1.1: BlitzDB is now Python3 compatible (thanks to David Koblas)
"""
)
