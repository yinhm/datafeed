A Datafeed System for Financial Data.
=====================================
Datafeed is an fast, extensible quotes data storage build on
Python/HDF5. IMIGU (http://imigu.com) has been using Datafeed on production
more than a year.

Datafeed is licensed under the Apache Licence, Version 2.0
(http://www.apache.org/licenses/LICENSE-2.0.html).

## Components

 * Quotes store server.
 * Client to interactive with quote server(get/put).
 * Datafeeds providers client, including Yahoo, Google, etc.


## Python version support

Officially 2.7. Python 3 may works but not tested.


## Dependencies

   * NumPy: 1.5.0 or higher
   * h5py: 2.0 or higher
   * tornado: 2.0 or higher

Install dependent packages with pip:

    pip install -r pip.txt


## Optional packages

 * pandas: dividend/split and more
 * python-dateutil: <2.0, RSS parsing
 * pycurl: url fetch
 * pywin32: only needed if you want to run TongShi client


## INSTALLATION

    git clone git://github.com/yinhm/datafeed.git


## Run

    cd datafeed
    cp config_example.py config.py
    python server.py


## Client

    from datafeed.client import Client
    c = Client()
    c.get_report("SH000001")


## TODO

 * Documentation


## License

Apache Public License (APL) 2.0


## Thanks

 * Big thanks to my company ( http://jeebo.cn ) allow me to open source Datafeed.
