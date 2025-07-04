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


import weakref

# from .Release import __version__
from .StreamSet import StreamSet

from nxsrecconfig.Utils import TangoUtils
# from nxsrecconfig.Describer import Describer


try:
    import tango
except Exception:
    import PyTango as tango


class DataSources(object):

    def __init__(self, server=None, nxsconfigserver=None, proxy=None):
        """ contructor

        :param server: NXSDSRecSelector server
        :type server: :class:`nxsrecconfig.NXSConfig.NXSRecSelector`
        :param nxsconfigserver: nexus config server device name
        :type nxsconfigserver: :obj:`str`
        :param proxy: self device proxy
        :type proxy: :class:`tango.DeviceProxy`
        """

        #: (:class:`tango.Database`) tango database
        self.__db = tango.Database()

        #: (:class:`nxsrecconfig.NXSConfig.NXSRecSelector`) Tango server
        self.__server = server

        #: (:class:`StreamSet` or :class:`tango.LatestDeviceImpl`) stream set
        self._streams = StreamSet(weakref.ref(server) if server else None)

        #: (:obj:`str`) nexus config server device name
        self.nxsconfigserver = nxsconfigserver

        #: (:class:`tango.DeviceProxy`) self device proxy
        self.__dp = proxy

        #: (:class:`tango.DeviceProxy` \
        #: or :class:`nxsconfigserver.XMLConfigurator.XMLConfigurator`) \
        #:     configuration server proxy
        self.__configServer = None

    def getConfigServer(self):
        if self.__configServer is not None:
            return self.__configServer
        if not self.nxsconfigserver:
            self.nxsconfigserver = TangoUtils.getDeviceName(
                self.__db, "NXSConfigServer")
        if self.nxsconfigserver:
            dps = TangoUtils.getProxies(
                [self.nxsconfigserver])
            if dps:
                self.__configServer = dps[0]
                return self.__configServer

    def addDataSources(self, dss):
        return

    def removeDataSources(self, dss):
        return

    def availableDataSources(self):
        return self.getConfigServer().availableDataSources()

    def setVariables(self, vars):
        return

    def variables(self):
        return ""

    def setCommonBlock(self, vars):
        return

    def commonBlock(self):
        return ""
