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


import os
import weakref
import json
import time
import fnmatch

# from .Release import __version__
from .StreamSet import StreamSet

from nxsrecconfig.Utils import TangoUtils, MSUtils
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
                 dsblacklist=None, dsprefix="", nxsrecselector=None,
                 metadatascript=""):
        """ contructor

        :param server: NXSDSRecSelector server
        :type server: :class:`nxsrecconfig.NXSConfig.NXSRecSelector`
        :param nxsconfigserver: nexus config server device name
        :type nxsconfigserver: :obj:`str`
        :param proxy: self device proxy
        :type proxy: :class:`tango.DeviceProxy`
        :param dsblacklist: datasource blacklist
        :type dsblacklist: :obj:`list`<:obj:`str`>
        :param dsprefix: datasource prefix
        :type dsprefix: :obj:`str`
        :param nxsrecselector: nexus recorder selector server device name
        :type nxsrecselector: :obj:`str`
        :param metadatascript: metadata python script file name
        :type metadatascript: :obj:`str`
        """
        #: (:class:`tango.Database`) tango database
        self.__db = tango.Database()

        #: (:class:`nxsds.NXSDSources.NXSDatasSources`) Tango server
        self.__server = server

        #: (:class:`StreamSet` or :class:`tango.LatestDeviceImpl`) stream set
        self._streams = StreamSet(weakref.ref(server) if server else None)

        #: (:obj:`str`) nexus config server device name
        self.nxsconfigserver = nxsconfigserver

        #: (:obj:`str`) nexus recorder selector server device name
        self.nxsrecselector = nxsrecselector

        #: (:obj:`str`) datasource prefix
        self.dsprefix = dsprefix

        #: (:obj:`str`) metadata script file name
        self.metadatascript = metadatascript

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
        #: (:class:`tango.DeviceProxy` \
        #: or :class:`nxsrecconfig.Settings.Settings`) \
        #:     configuration server proxy
        self.__recSelector = None
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

        self.__description = {}
        self.__olddescription = {}
        self.__elements = {}
        self.__dsfac = {}
        self.__attr = {}

        self.__variables = "{}"
        self.__variables_bk = "{}"

        self.__userRecord = {"data": {}}

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
                self.__configServer.set_timeout_millis(10000)
                return self.__configServer

    def getRecSelector(self):
        if self.__recSelector is not None:
            return self.__recSelector
        if not self.nxsrecselector:
            self.nxsrecselector = TangoUtils.getDeviceName(
                self.__db, "NXSRecSelector")
        if self.nxsrecselector:
            dps = TangoUtils.getProxies(
                [self.nxsrecselector])
            if dps:
                self.__recSelector = dps[0]
                self.__recSelector.set_timeout_millis(10000)
                return self.__recSelector

    def addDataSources(self, dss=None):
        ta = time.time()
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
        # t1 = time.time()
        # print("TO", tb-t1)
        pyds = [dsname for dsname in dss if (
            self.__description.get(dsname, {}).get("dstype") == "PYEVAL")]
        adss = list(set(dss)-set(pyds))
        xmls = self.getConfigServer().dataSources(adss)
        dsxmls = dict(zip(adss, xmls))
        tb = time.time()
        # print("PYEV", pyds)

        self.__variables_bk = self.getConfigServer().variables
        self.getConfigServer().variables = self.__variables
        try:
            pxmls = self.getConfigServer().instantiatedDataSources(pyds)
        except Exception:
            pxmls = [self.getConfigServer().instantiatedDataSources(pyds)[0]
                     for dsname in pyds]
        self.getConfigServer().variables = self.__variables_bk

        pdsxmls = dict(zip(pyds, pxmls))
        for dsname in dss:
            # t1 = time.time()
            if dsname in pdsxmls:
                dssxml = [pdsxmls[dsname]]
            else:
                dssxml = [dsxmls[dsname]]
            if dssxml:
                xml = dssxml[0]
                try:
                    # t2 = time.time()
                    ds = self.storeDataSource(dsname, xml)
                    # t3 = time.time()
                    if ds:
                        self.addAttribute(dsname)
                    # self.getValue(dsname)
                    # t4 = time.time()
                    # print("DS", dsname,t2-t1,t3-t2, t4-t3)
                except Exception as e:
                    print("ERROR1", str(e), dsname)
                    continue
        print("TIME:", tb - ta, time.time() - tb)

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
                    print("CHE")
                    v = self.getValue(dsname)
                    print("TYPE", type(v))
                except Exception:
                    pass

            return dsname

    def addAttribute(self, dsname):
        # el = self.__elements[dsname]

        des = self.__description[dsname]
        odes = {}
        if dsname in self.__olddescription:
            odes = self.__olddescription[dsname]
        # print(des)
        # print(odes)
        myAttr = None
        if "nxtype" not in des:
            des["nxtype"] = "NX_FLOAT64"
        if "nxtype" not in odes:
            odes["nxtype"] = "NX_FLOAT64"
        atname = self.dsprefix + dsname
        if odes and atname in self.__attr and \
                odes["nxtype"] == des["nxtype"] and \
                des.get("shape", None) == odes.get("shape", None):
            return
        nptype = NTP.nTnp.get(des["nxtype"], des["nxtype"])
        tntype = self.__server.pTt.get(nptype, nptype)
        if des["shape"] is None or len(des["shape"]) == 0:
            # tango.DevDouble
            myAttr = tango.Attr(atname, tntype, tango.READ)
        elif len(des["shape"]) == 1:
            myAttr = tango.SpectrumAttr(atname, tntype,
                                        tango.READ, 4096)
        elif len(des["shape"]) == 2:
            myAttr = tango.ImageAttr(atname, tntype,
                                     tango.READ, 4096, 4096)
        if myAttr is not None:
            self.__attr[atname] = myAttr
            try:
                self.__server.remove_attribute(atname)
            except Exception:
                pass
            self.__server.add_attribute(
                myAttr, self.__server.read_DynamicAttr,
                None, None)
            self.__olddescription[dsname] = des

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
            if hasattr(el.source, "setJSON"):
                el.source.setJSON(self.__userRecord)
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
            # throws error
            # try:
            #     atl = self.__dp.get_attribute_list()
            # except Exception as e:
            #     print(str(e))
            #     atl = self.__dp.get_attribute_list()

            # does not work
            # print([at for at in
            #    self.__server.get_device_attr().get_attribute_list()])

            atts = self.__server.get_device_attr().get_attribute_list()
            atl = [atts[i].get_name() for i in range(len(atts))]

            # print(atl)
            dss = list(set(atl) - set(self.dsinterlist))
            #  print("REMOVE", dss)
        else:
            dss = [self.dsprefix + ds for ds in dss]
        for atname in dss:
            try:
                self.__server.remove_attribute(atname)
                if atname in self.__attr:
                    self.__attr.pop(atname)

            except Exception:
                pass

        return

    def availableDataSources(self):
        return self.getConfigServer().availableDataSources()

    def setVariables(self, var):
        self.__variables = var

    def variables(self):
        return self.__variables

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
        self.__olddescription = self.__description
        self.__description = dsdes

        self.refreshUserData()

    def refreshUserData(self):

        rs = self.getRecSelector()

        self.__userRecord["data"] = {}
        msf = self.metadatascript
        if rs is not None:
            userdata = dict(json.loads(rs.userData or {}))
            self.__userRecord["data"] = userdata
            if not msf:
                ms = rs.macroServer
                try:
                    msf = MSUtils.getEnv('MetadataScript', ms)
                except Exception:
                    pass
        if msf:
            if not os.path.exists(msf):
                print("Error: %s does not exist" % msf)
            else:
                import importlib.util
                spec = importlib.util.spec_from_file_location('', msf)
                msm = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(msm)
                ms = msm.main()
                if not isinstance(ms, dict):
                    print(
                        "Error: bad output from %s" % msf)
                else:
                    self.__userRecord["data"].update(ms)

    def userData(self):
        return json.dumps(self.__userRecord["data"] or {})

    def setUserData(self, udata):
        dd = json.loads(udata)
        self.__userRecord["data"].update(dd)
