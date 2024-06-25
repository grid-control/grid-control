| |PyPI Version| |Build Status|

grid-control
============

*grid-control* is a versatile job submission tool for several different batch systems
and grid middleware.
It supports complex parameterized and dataset based jobs with a convenient way to
specify the parameter space to be processed by the jobs.

.. image:: docs/gc_running.gif

Quick HOWTO
-----------

.. code:: sh

    pip install grid-control

For a more instructive introduction, visit the `user's guide`_.

More examples can be found in the `github`_ repository.


Contributing
------------

Base your work on the ``testing`` branch and also use it as the base branch for pull requests.

**Note**: *All branches other than the master branch might be rebased any time.*


.. _github: https://github.com/grid-control/grid-control/tree/testing/docs/examples

.. _user's guide: https://grid-control.github.io

.. |PyPI Version| image:: https://badge.fury.io/py/grid-control.svg
   :target: https://badge.fury.io/py/grid-control
   :alt: Latest PyPI version

.. |Build Status| image:: https://travis-ci.org/grid-control/grid-control.svg?branch=testing
   :target: https://travis-ci.org/grid-control/grid-control
   :alt: Build Status
