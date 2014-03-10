:orphan:

.. role:: raw-html(raw)
   :format: html


.. raw:: html

   <a class="github_ribbon" href="https://github.com/adewes/blitz-db"><img style="position: absolute; top: 0; right: 0; border: 0;" src="https://github-camo.global.ssl.fastly.net/652c5b9acfaddf3a9c326fa6bde407b87f7be0f4/68747470733a2f2f73332e616d617a6f6e6177732e636f6d2f6769746875622f726962626f6e732f666f726b6d655f72696768745f6f72616e67655f6666373630302e706e67" alt="Fork me on GitHub" data-canonical-src="https://s3.amazonaws.com/github/ribbons/forkme_right_orange_ff7600.png"></a>

:raw-html:`<i class="fa fa-bolt" style="color:#05a; vertical-align:middle; padding:10px;"></i>` Welcome to Blitz-DB!
*********************************************************************************************************************
**Blitz-DB**, or just **Blitz** :raw-html:`<i class="fa fa-bolt"></i>`, [#f1]_ is a document-based, object-oriented, transactional database written purely in Python. Among other things, it provides a **powerful querying language**, **deep indexing of documents**, **compressed data storage** and **automatic referencing of embedded documents**. It is reasonably fast, can be easily embedded in any Python application and does not have any external dependencies (except when using a third-party backend).

Key Features
------------

* Document-based, object-oriented interface.
* Powerful and rich querying language.
* Deep document indexes on arbitrary fields.
* Compressed storage of documents.
* Support for multiple backends (e.g. file-based storage, MongoDB).
* Support for database transactions (currently only for the file-based backend).

Installation
------------

For detailed installation instructions, have a look at the :doc:`documentation <installation>`. The easiest way to install checkmate is via **pip** or **easy_install**:

.. code-block:: bash

    pip install blitzdb
    #or
    easy_install blitzdb

Alternatively, you can just download the source from Github and install it manually by running (in the project directory):

.. code-block:: bash

    git clone git@github.com:adewes/blitz-db.git
    cd blitz-db
    sudo python setup.py install

Getting Started
---------------

To get started, have a look at the :doc:`tutorial` or check out the API documentation.

Use Cases
---------

Blitz has been designed as an embeddable, easy-to-use and *reasonably* fast database. In general, it performs well even with moderately large (>100.000 entries) collections of documents, provided you make good use of its indexing capabilities. 

It is **NOT** a fully-fledged database system like MySQL or MongoDB: 

* In the current version it does not provide any support for concurrent writes/reads to the database.
* It uses a relatively simple indexing mechanism based on hash tables and stores documents in flat files on the disk, hence querying performance will usually not be comparable to state-of-the art database systems.

However, for more demanding use cases, Blitz can be used as a frontend to a third-party backends, most notably *MongoDB*.

Motivation
----------

Sometimes you just want to store some structured data (think *dictionaries*) in a local database and get it out again when needed. For this, you could use e.g. the *shelve* module, or an embedded database like *sqlite*, or resort to an external database like *MongoDB*. The problem with these is that they either don't support **rich querying** of the data (*shelve*), require you to specify the data format beforehand (*sqlite*) or require additional software running on your machine (*MongoDB*). 

**Blitz** provides a new approach to this problem by giving you an embeddable database with a **flexible data format** and **rich querying** capabilities.


.. rubric:: Footnotes
  
.. [#f1] **Blitz** is the German word for *ligthning*. "**blitzschnell**" means "*really fast*".

.. include:: contents.rst.inc
