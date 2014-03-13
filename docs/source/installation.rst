:orphan:

.. _installation:

.. title:: Installation

.. role:: raw-html(raw)
   :format: html

:raw-html:`<i class="fa fa-2x fa-cloud-download" style="color:#05a; vertical-align:middle; padding-right:10px;"></i>` Installation
======================================================================================================================================

Using pip or easy_install
-------------------------

The easiest way to install Blitz is via `pip` or `easy_install`:

.. code-block:: bash

    pip install blitzdb
    #or...
    easy_install install blitzdb

This will fetch the `latest version <https://pypi.python.org/pypi/blitzdb/>`_ from PyPi and install it on your machine.

Using Github
------------

Since BlitzDB is still in heavy development, installing directly from the Github source will guarantee that your version
will contain the latest features and bugfixes. To clone and install the project, just do the following

.. code-block:: bash

    git clone git@github.com:adewes/blitzdb.git
    cd blitzdb
    sudo python setup.py install

Requirements
------------

The *vanilla* version of Blitz does not require any non-standard Python modules to run. However, you might want to install
the following Python libraries to be able to use all features of Blitz:

* `pymongo <https://pypi.python.org/pypi/pymongo/>`_: Required for the :doc:`MongoDB backend <backends/mongo>`
* `cjson <https://pypi.python.org/pypi/python-cjson/>`_: Required for the CJsonEncoder (improved JSON serialization speed)
* `pytest <https://pypi.python.org/pypi/pytest/>`_: Required for running the test suite
* `fake-factory <https://pypi.python.org/pypi/fake-factory/>`_: Required for generating fake test data

You can install these requirements using pip and the `requirements.txt` file:

.. code-block:: bash

    #in BlitzDB main folder
    pip install -R requirements.txt
