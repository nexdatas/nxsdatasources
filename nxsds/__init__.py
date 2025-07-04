#!/usr/bin/env python
#   This file is part of nxsdatasources - NeXus DataSources Collection
#
#    Copyright (C) 2025 DESY, Jan Kotanski <jkotan@mail.desy.de>
#
#    nexdatas is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    nexdatas is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with nexdatas.  If not, see <http://www.gnu.org/licenses/>.
#

"""  NeXus DataSources Collection - Tango Server """


#: package version
from .Release import __version__

__all__ = ["__version__", "run"]


def run(argv):
    """ runs the TANGO server

    :param argv: command-line arguments
    :type argv: :obj:`list` <:obj:`str`>
    """
    try:
        import tango
    except Exception:
        import PyTango as tango
    from .NXSDSources import NXSDataSources
    from .NXSDSources import NXSDataSourcesClass
    try:
        py = tango.Util(argv)
        py.add_class(NXSDataSourcesClass, NXSDataSources, 'NXSDataSources')

        U = tango.Util.instance()
        U.server_init()
        U.server_run()

    except tango.DevFailed as e:
        print('-------> Received a DevFailed exception: %s' % e)
    except Exception as e:
        print('-------> An unforeseen exception occured.... %s' % e)
