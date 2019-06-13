# WOFpy

[![Build Status](https://travis-ci.org/ODM2/WOFpy.svg?branch=master)](https://travis-ci.org/ODM2/WOFpy) [![Build status](https://ci.appveyor.com/api/projects/status/a8s63uq71w2skyw9?svg=true)](https://ci.appveyor.com/project/odm2bot/wofpy)

WOFpy is a Python package that implements
[CUAHSI's](http://his.cuahsi.org)
WaterOneFlow Web service.
WaterOneFlow is a Web service with methods for querying time series of water data at point locations,
and which returns data in WaterML format,
providing standardized access to water data.

WOFpy reads data from a Data Access Object (DAO) and translates the data into WaterML.
DAOs can represent a variety of data sources, including databases,
text files, and Web sites or services.
You can
[view example DAOs in the examples folder](https://github.com/ODM2/WOFpy/tree/master/wof/examples),
or write your own based on the BaseDao class in `wof/dao.py`.

WOFpy uses Python version 2.7.

Documentation
-------------

WOFpy documentation is currently (2019 January) being overhauled.
Much of the previous documentation available at
[http://odm2.github.io/WOFpy/](http://odm2.github.io/WOFpy/)
is out of date and should be used with care, mainly for conceptual descriptions.

If you happen to run into the older documentation at http://pythonhosted.org/WOFpy, don't use it. It's not being maintained. It'll be removed eventually.

Installation
------------

See
[this interim document](https://github.com/ODM2/WOFpy/blob/master/docs/WOFpyODM2LBR_installation.md)
for up-to-date, detailed installation and configuration instructions.
Configuration examples are based on ODM2 timeseries and measurements DAO's.

ODM2 and ODM1 Instructions
--------------------------

*Note: These materials may be out of date.*

- [ODM2](https://github.com/ODM2/WOFpy/blob/master/docs/Sphinx/ODM2Services.rst)
- [ODM1](https://github.com/ODM2/WOFpy/blob/master/docs/Sphinx/ODM1Services.rst)

Running the Examples
--------------------

Example services are included with WOFpy.
Each example consists of data,
Data Access Objects (DAOs), models,
and the service definition.
The examples are located in the [examples folder](https://github.com/ODM2/WOFpy/tree/master/wof/examples).
See the documentation for more information.

A quick way to use the examples is via the `wofpy_config` CLI.
Type `wofpy_config --help` to display the usage Instructions.

```shell
wofpy_config MyWorkingDirectory
```

The command above will create and populate the `MyWorkingDirectory` with the files needed to run the `odm2 timeseries` example.
Right now there is only one tested example,
in the future extra options will enable the user to choose the base example used.

Publishing Your Data
--------------------

Follow the general steps below to publish your data with WOFpy.

1. Write a new DAO class based on wof.dao.BaseDao;
   the methods should return objects as defined in wof.models.
   This class helps WOFpy read your data.
2. Write a new config file like those found in the examples,
   e.g. `wof/examples/flask/odm2/timeseries/odm2_config_timeseries.cfg`.
   This file contains static descriptors of your Web service such as the name of your water observations network.
3. Write a new runserver script to use the new DAO you implemented.
   See files named runserver_*.py in the examples folder for examples.
4. To test, open a command window,
   navigate where your runserver file is located,
   and use Python to run the script, e.g., python runserver.py.

Credits
-------

WOFpy was
[originally](https://github.com/swtools/WOFpy/)
developed and maintained (2011-2012) by the
[Texas Water Development Board](http://www.twdb.texas.gov).
Stewardship and continued development was taken up in 2015 by a multi-institution team associated with the
[ODM2](http://www.odm2.org)
and
[BiG-CZ](https://bigcz.org/)
projects, with support from National Science Foundation Grant [ACI-1339834](http://www.nsf.gov/awardsearch/showAward?AWD_ID=1339834).

Any opinions, findings,
and conclusions or recommendations expressed in this material are those of the author(s) and do not necessarily reflect the views of the National Science Foundation.
