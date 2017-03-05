EventFlow tutorial
==================

Requirements
------------

* python3 (tested with 3.6)
* numpy (tested with 1.11.3)
* pandas (tested with 0.19.2)
* geopandas
* shapely (tested with 1.5.17)
* matplotlib (tested with 2.0.0)
* pymongo (tested with 3.3.0)
* sshtunnel (tested with 0.1.2)

.. note:: Installing these is possible with:

    .. code-block:: bash
        
        $ pip install -r requirements.txt

Recommened (and required for the event explorer):

* basemap (tested with 1.0.7)
* pyqt5 (tested with 5.6.0)
* qt5 (tested with 5.6.2)

.. note:: If installing with pip, follow the `installation guide <http://matplotlib.org/basemap/users/installing.html>`_ for basemap. 
    
    PyQt5 was tested with the anaconda version, without using anaconda check `PyQt5 <http://pyqt.sourceforge.net/Docs/PyQt5/installation.html>`_.

Installation
------------

After installing the requirements, the installation is a simple:

.. code-block:: bash

    $ python setup.py install

Tests
-----

If you want to run the tests, to check if everything works, you can do that with `pytest <http://doc.pytest.org/en/latest/>`_:

.. code-block:: bash

    $ py.test --mpl tests/

.. note:: Currently the tests require a connection to the mongodb.

Usage
-----


