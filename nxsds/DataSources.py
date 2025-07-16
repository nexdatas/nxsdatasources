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
import json
# import time
import fnmatch

# from .Release import __version__
from .StreamSet import StreamSet

from nxsrecconfig.Utils import TangoUtils
from nxsrecconfig.Describer import Describer


from nxswriter.DataSourceFactory import DataSourceFactory
from nxswriter.DataSourcePool import DataSourcePool
# from nxswriter.Element import Element
from nxswriter.EField import EField
from nxswriter.DataHolder import DataHolder
from nxswriter.Types import NTP
# from nxswriter import DataSources
# from nxswriter import ClientSource
# from nxswriter import PyEvalSource
# from nxswriter import TangoSource
# from nxswriter.Errors import DataSourceSetupError
from nxswriter.DecoderPool import DecoderPool
import xml.etree.ElementTree as et
from lxml.etree import XMLParser

try:
    import tango
except Exception:
    import PyTango as tango


class DataSources(object):

    def __init__(self, server=None, nxsconfigserver=None, proxy=None,
                 dsblacklist=None, dsprefix="ds_"):
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

        #: (:obj:`str`) datasource prefix
        self.dsprefix = dsprefix

        #: (:class:`tango.DeviceProxy`) self device proxy
        self.__dp = proxy

        #: (:obj:`list` <:obj:`str`> ) datasource blacklist
        self.dsblacklist = dsblacklist or []

        #: (:obj:`list` <:obj:`str`> ) datasource blacklist
        self.dsinterlist = ["Status", "State"]

        #: (:class:`tango.DeviceProxy` \
        #: or :class:`nxsconfigserver.XMLConfigurator.XMLConfigurator`) \
        #:     configuration server proxy
        self.__configServer = None
        #: (:obj:`dict` <:obj:`str` , :obj:`str`> ) \
        #:    map of numpy types : NEXUS
        self.__npTn = {"float32": "NX_FLOAT32", "float64": "NX_FLOAT64",
                       "float": "NX_FLOAT32", "double": "NX_FLOAT64",
                       "int": "NX_INT", "int64": "NX_INT64",
                       "int32": "NX_INT32", "int16": "NX_INT16",
                       "int8": "NX_INT8", "uint64": "NX_UINT64",
                       "uint32": "NX_UINT32", "uint16": "NX_UINT16",
                       "uint8": "NX_UINT8", "uint": "NX_UINT64",
                       "str": "NX_CHAR",
                       "string": "NX_CHAR", "bool": "NX_BOOLEAN"}

        self.__description = None
        self.__elements = {}
        self.__dsfac = {}
        self.__attr = {}

    def getConfigServer(self):
        if self.__configServer is not None:
            self.__configServer.open()
            return self.__configServer
        if not self.nxsconfigserver:
            self.nxsconfigserver = TangoUtils.getDeviceName(
                self.__db, "NXSConfigServer")
        if self.nxsconfigserver:
            dps = TangoUtils.getProxies(
                [self.nxsconfigserver])
            if dps:
                self.__configServer = dps[0]
                self.__configServer.open()
                return self.__configServer

    def addDataSources(self, dss=None):
        if not self.__description:
            self.refresh()
        if not dss:
            blist = []
            dss = self.availableDataSources()
            for flt in self.dsblacklist:
                blist.extend(fnmatch.filter(dss, flt))

            dss = list(set(dss) - set(blist))
        # print("ADD", dss)
        # print("BL", blist)
        for dsname in dss:
            dssxml = self.getConfigServer().instantiatedDataSources([dsname])
            if dssxml:
                xml = dssxml[0]
                try:
                    ds = self.storeDataSource(dsname, xml)
                    if ds:
                        self.addAttribute(dsname)
                    # self.getValue(dsname)
                except Exception as e:
                    print("ERROR1", str(e), dsname)
                    continue

    def storeDataSource(self, dsname, xml):
        des = self.__description.get(
            dsname,
            {'shape': [], 'dsname': dsname})
        if dsname not in self.__description:
            self.__description[dsname] = des
        el = EField("field", {})
        self.__elements[dsname] = el
        if "dstype" in des:
            ds = DataSourceFactory({"type": des["dstype"]}, el)
            dsp = DataSourcePool()
            dcp = DecoderPool()
            ds.setDataSources(dsp)
            ds.setDecoders(dcp)
            dset = et.fromstring(xml, parser=XMLParser(collect_ids=False))
            # print("XML", xml)
            if dset.tag != "datasource":
                dset = dset.find("datasource")
                if len(dset) == 0:
                    return
                xml = et.tostring(dset, encoding='unicode', method='xml')
            # xml should be instatiated
            ds.store([xml])
            self.__dsfac[dsname] = ds
            if "nxtype" not in des:
                try:
                    _ = self.getValue(dsname)
                    # print("TYPE", type(v))
                except Exception:
                    pass

            return dsname

    def addAttribute(self, dsname):
        # el = self.__elements[dsname]
        des = self.__description[dsname]
        # print(des)
        myAttr = None
        if "nxtype" not in des:
            des["nxtype"] = "NX_FLOAT64"
        nptype = NTP.nTnp.get(des["nxtype"], des["nxtype"])
        tntype = self.__server.pTt.get(nptype, nptype)
        if des["shape"] is None or len(des["shape"]) == 0:
            # tango.DevDouble
            myAttr = tango.Attr(dsname, tntype, tango.READ)
        elif len(des["shape"]) == 1:
            myAttr = tango.SpectrumAttr(dsname, tntype,
                                        tango.READ, 4096)
        elif len(des["shape"]) == 2:
            myAttr = tango.ImageAttr(dsname, tntype,
                                     tango.READ, 4096, 4096)
            self.__attr[dsname] = myAttr
        if myAttr is not None:
            try:
                self.__server.remove_attribute(dsname)
            except Exception:
                pass
            self.__server.add_attribute(
                myAttr, self.__server.read_DynamicAttr,
                None, None)

    def readDynamicAttr(self, attr):
        name = attr.get_name()
        dsname = name
        attr.set_value(self.getValue(dsname))

    def getValue(self, dsname):

        # t2 = time.time()
        # print("NAME: ", dsname, type(el.source), xml)
        el = self.__elements[dsname]
        des = self.__description[dsname]
        try:
            vv = el.source.getData()
            # t3 = time.time()
            if vv is not None:
                dh = DataHolder(**vv)
                vl = dh.cast(NTP.nTnp.get(
                    des["nxtype"], des["nxtype"]))
                # t4 = time.time()
                # print("VALUE: ", dsname, vl, t3 - t2, t4 - t3)
                return vl
        except Exception:
            pass
        # except Exception as e:
        #     print("XML", des)
        #     print("VAL", vv)
        #     print(str(e))

    def removeDataSources(self, dss):
        if not dss:
            dss = list(set(self.__dp.get_attribute_list())
                       - set(self.dsinterlist))
        #  print("REMOVE", dss)
        for dsname in dss:
            try:
                self.__server.remove_attribute(dsname)
                if dsname in self.__attr:
                    self.__attr.pop(dsname)

            except Exception:
                pass

        return

    def availableDataSources(self):
        return self.getConfigServer().availableDataSources()

    def setVariables(self, var):
        return

    def variables(self):
        return ""

    def setCommonBlock(self, cblock):
        return

    def commonBlock(self):
        return ""

    def details(self):
        if not self.__description:
            self.refresh()
        return json.dumps(self.__description)

    def refresh(self):
        dsdes = {}
        cs = self.getConfigServer()
        dsl = cs.availableDataSources()
        cpl = cs.availableComponents()
        de = Describer(cs, pyevalfromscript=True)
        for cp in cpl:
            try:
                des = de.components([cp])
                for dd in des:
                    dsdes[dd["dsname"]] = dd
                    #  print(dd)
            except Exception:
                # print("ERROR2", cp)
                continue
        missing = list(set(dsl) - set(dsdes.keys())
                       - set(self.dsblacklist))
        # print(missing)
        for mds in missing:
            try:
                dds = de.dataSources([mds])
                # print("EEE", mds, dds)
                for jdd in dds:
                    if jdd:
                        dd = json.loads(jdd)
                        dstype = dd.get("dstype")
                        shape = None
                        dt = "float"
                        nxtype = "NX_FLOAT64"
                        if dstype == 'TANGO':
                            source = dd["record"]
                            shape, dt, _ = TangoUtils.getShapeTypeUnit(source)
                            nxtype = self.__npTn[dt] \
                                if dt in self.__npTn.keys() else nxtype
                            # print(shape, dt, nxtype)
                        # print("dstype",dstype)
                        dd["shape"] = shape
                        dd["nxtype"] = nxtype
                        dsdes[dd["dsname"]] = dd
                        # print(dd)
            except Exception:
                # except Exception as e:
                #     print("ERROR3", mds, e)
                continue
        # print(dsdes)
        self.__description = dsdes

    def userData(self):
        return ""

    def setUserData(self, udata):
        return
