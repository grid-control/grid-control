Host Specific Config Files
==========================

This directory contains host specific config files. The hostname of the
machine running the grid-control process is used find out which config
files are loaded. This way it is possible to adapt the behaviour of
grid-control to the particular needs of any given site.

Example
-------

Hostname: ``portal.institute.university.tld``

In this case grid-control will look for config files in this order:

-  ``config/tld.conf``
-  ``config/university.tld.conf``
-  ``config/institute.university.tld.conf``
-  ``config/portal.institute.university.tld``

You are VERY welcome to submit your host specific config file for
inclusion into the grid-control package! Additionally, the following
config files are included if they exist, so they can be used to further
customize the workflow:

-  ``/etc/grid-control.conf`` - this can be installed by the admin on a
   machine to affect ALL users
-  ``~/.grid-control.conf`` - this is a user specific config file
-  ``config/default.conf`` - this config file is affecting only this
   specific grid-control installation
-  ``$GC_CONFIG`` - this environment variable can be set to point to
   some config file

grid-control will NOT stop going through this list if one of these
config files is found! They will all get added in exactly THIS order to
the included config files.

Developer Notes
---------------

Having a site-specific branch of grid-control should be the absolute
exception - since everything SHOULD be configurable, so particularities
of sites should be handled via the host specific config files. To
prevent merge conflicts due to customizations, ``config/default.conf``
will NEVER be included in the grid-control package!
