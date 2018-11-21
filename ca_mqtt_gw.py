#!/usr/bin/env python

import numpy as np
import cothread.catools as catools
import cothread
from cothread.catools import *
import signal
signal.signal(signal.SIGINT, signal.SIG_DFL)

from PyQt4 import QtCore
import json
import time
import sys
import os
import array
import struct
import paho.mqtt.client as mqtt
import paho.mqtt.publish as publish

import traceback

import logging

script_dir = os.path.dirname(__file__)

# configure logging to write both to file and stdout
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

sh = logging.StreamHandler()
sh.setLevel(logging.DEBUG)
sh.setFormatter(logging.Formatter('[%(levelname)s] %(message)s'))
logger.addHandler(sh)

fh = logging.FileHandler(os.path.join(script_dir,'info.log'))
fh.setLevel(logging.INFO)
fh.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', datefmt='%d-%m-%Y %H:%M:%S'))
logger.addHandler(fh)

chans = []
id = 0
waveforms = {}

# predefined constants
SEGMENT_SIZE = 1208 # bytes
MQTT_DELAY = 0.07 # seconds
SEGIDX_MOD = 1000 # max segment index value + 1
WF_DROP_DIFF = 3 # difference between waveform ids 
                 # that enough to drop an old incomplete ones

def moddiff(a, b, m):
    d = a - b
    if d < -m/2:
        d += m
    elif d > m/2:
        d -= m
    return d

class PvMqttChan:
    def __init__(self,connection,servers,client):
        self.chan = unicodeToStr(connection["mqtt"])
        self.pv = unicodeToStr(connection["pv"])
        if "datatype" in connection:
            self.datatype = unicodeToStr(connection["datatype"])
        else:
            self.datatype = None
        self.direction = unicodeToStr(connection["direction"])
        if "qos" in connection:
            self.qos = connection["qos"]
        else:
            self.qos = 0
        self.retain = False
        if ("retain" in connection) and connection["retain"] == "true":
            self.retain = True
        self.servers = servers
        self.client = client

        self.converter = MqttDataConverter(segment_size=SEGMENT_SIZE)
        self.wfaccums = {}
        self.wfid_cnt = 1

    def pushWfSegment(self, wfid, size, array):
        # remove distant incomplete waveforms
        for key in self.wfaccums.keys():
            d = moddiff(key, wfid, SEGIDX_MOD)
            if abs(d) >= WF_DROP_DIFF:
                del self.wfaccums[key]

        accum = self.wfaccums.get(wfid, WfAccum(wfid, size))
        assert(accum.size == size)
        
        if accum.addSegment(array):
            # waveform completed
            wf = accum.concat()
            # remove previous incomplete waveforms
            for key in self.wfaccums.keys():
                d = moddiff(key, wfid, SEGIDX_MOD)
                if d < 0:
                    del self.wfaccums[key]
            return wf
        return None

    def setConnection(self):
        try:
            catools.connect(self.pv)
            if self.direction=="mp":
                if self.datatype == "wfint":
                    self.client.subscribe(self.chan+"#")
                else:
                    self.client.subscribe(self.chan)
            elif self.direction=="pm":
                camonitor(self.pv,self.updateChan)
            logger.info(self.chan + " connection set")
        except Exception as e:
            logger.error("Trouble with connection with " + self.pv + " or " + self.chan + ": " + str(e))
            logger.info(traceback.format_exc())
            #cothread.Quit()

    def updateChan(self, value):
        try:
            if self.datatype == "wfint":
                value = (self.wfid_cnt, np.array(value))
                self.wfid_cnt += 1
            if self.datatype == "wfint1":
                value = (self.wfid_cnt, value)
                self.wfid_cnt += 1

            payload = self.converter.encode(value, self.datatype)

            if self.datatype == "wfint":
                for i, seg in enumerate(payload):
                    topic = self.chan + str(i).zfill(3)
                    self.client.publish(topic, seg, self.qos).wait_for_publish()
                    time.sleep(MQTT_DELAY)
            else:
                self.client.publish(self.chan, payload, self.qos, self.retain).wait_for_publish()
                time.sleep(MQTT_DELAY)

            logger.debug(".updateChan(%s, %s)" % (self.chan, repr(value)))
        except Exception as e:
            logger.error("Trouble when Publishing to Mqtt with " + self.chan + ": " + str(e))
            logger.info(traceback.format_exc())
            #cothread.Quit()

    def updatePv(self, payload):
        try:
            value = self.converter.decode(payload, self.datatype)
            if self.datatype == "wfint":
                wfid, size, array = value
                value = self.pushWfSegment(wfid, size, array)
            if value is not None:
                cothread.Callback(caput, self.pv, value)
            logger.debug(".updatePv(%s, %s, %s)" % (self.pv, repr(payload), value))
        except Exception as e:
            logger.error("Trouble in updatePv with " + self.pv + ": " + str(e))
            logger.info(traceback.format_exc())
            #cothread.Quit()

    def findServer(self,type,name):
        result = [x for x in self.servers if x.type == type and x.name == name]
        if len(result) != 0:
            return result[0]
    def isChannelWorkDelayed(self):
        server = self.findServer("ioc",self.pv.split("_",1)[0])
        if not server.checkServer():
            return True
        server = self.findServer("mqtt",self.chan.split("/",1)[0])
        if not server.checkServer():
            return True
        return False
    def setServerDelay(self,error):
        channelname = error.split(' ',1)[0]
        servername = channelname.split("_",1)[0]
        server = self.findServer("ioc",servername)
        if not server:
            servername = channelname.split("/",1)[0]
            server = self.findServer("mqtt",servername)
        if server:
            server.delayServer()
            return
    def sendWf(self,wf):
        global id
        waveform = WaveForm(id,wf)
        waveform.sendWfToMqtt(self.client,self.chan,self.qos,0.07)
        id+=1
    def wfToScalar(self,wf):
        return float(struct.unpack(">iii",wf)[2])
    def intToScalar(self,wf):
        return float(struct.unpack(">i",wf)[0])
    def wfToWf(self,wf):
        global waveforms
        wflen = len(wf)//4
        message = struct.unpack(">%ui"%wflen,wf)
        wfid = int(message[0])
        msgsize = int(message[1])
        if wfid in waveforms:
            #print(wfid)
            waveforms[wfid].appendMessage(message)
        else:
            waveforms[wfid] = WaveForm(id=wfid,msgsize=msgsize,first_msg=message)
        if waveforms[wfid].sendWfToPv(self.pv):
            del waveforms[wfid]

class MqttDataConverter:
    def __init__(self, segment_size):
        self.segsize = segment_size

    def encode(self, value, dtype):
        if dtype == "int":
            return struct.pack(">i", value)
        elif dtype == "string": # TODO: change when migrate to python3
            return value
        elif dtype == "wfint1": # TODO: remove
            wfid, num = value
            return struct.pack(">iii", (wfid, 0, num))
        elif dtype == "wfint":
            wfid, array = value
            size = len(array)
            output = []
            sds = self.segsize - 2 # segment data size
            for i in range((size - 1)//sds + 1):
                meta = struct.pack(">ii", (wfid, size))
                data = array[i*sds:(i+1)*sds].astype(">i4").tobytes()
                output.append(meta + data)
            return output

    def decode(self, payload, dtype):
        if dtype == "int":
            return struct.unpack(">i", payload)[0]
        elif dtype == "string": # TODO: change when migrate to python3
            return value 
        elif dtype == "wfint1": # TODO: remove
            struct.unpack(">iii",wf)[2]
        elif dtype == "wfint":
            ms = 2*4 # metainfo size
            meta, data = payload[:ms], payload[ms:]
            wfid, size = struct.unpack(">ii", meta)
            array = np.ndarray(shape=(-1,), dtype='>i4', buffer=data).astype(np.int32)
            return (wfid, size, array)

class WfAccum:
    def __init__(self, size):
        self.size = size
        self.segs = {}
        self.dc = 0 # data counter

    def addSegment(self, idx, seg):
        self.segs[idx] = seg
        self.dc += len(seg)
        if self.dc < self.size:
            return False
        elif self.dc == self.size:
            return True
        else:
            raise ValueError("Total length of segments (%d) larger than waveform size (%d)" % (self.dc, self.size))

    def concat(self):
        maxidx = max(self.segs.keys())
        seq = []
        for i in range(maxidx):
            seq.append(self.dc[i])
        return np.concatenate(seq)

class WaveForm:
    def __init__(self,id,msg=None,msgsize=None,maxsize=300,first_msg=None):
        self.maxsize = maxsize
        self.id = id
        if first_msg:
            self.msgsize = msgsize
            self.messages = [first_msg]
            self.msg = self.unpackWf()
        else:
            self.msg = msg
            self.msgsize = len(msg)
            self.messages = self.packWf()
    def appendMessage(self,message):
        self.messages.append(message)
        self.msg = self.unpackWf()
    def unpackWf(self):
        wf = []
        n_segments = (self.msgsize - 1)//self.maxsize + 1
        #print(n_segments)
        if n_segments > len(self.messages):
            return wf
        last_segment_size = self.msgsize%self.maxsize
        segment_size = self.maxsize
        for i in range(n_segments):
            if(i==n_segments-1):
                segment_size = last_segment_size
            segment = self.messages[i]
            wf = wf + list(segment[2:])#struct.unpack(">%ui"%(segment_size+2),segment)[2:]
        return wf
    def packWf(self):
        pack = []
        if self.msgsize==0:
            return pack
        n_segments = (self.msgsize - 1)//self.maxsize + 1
        last_segment_size = self.msgsize%self.maxsize
        segment_size = self.maxsize
        for i in range(n_segments):
            if(i==n_segments-1):
                segment_size = last_segment_size
            segment = []
            for j in range(i*self.maxsize,i*self.maxsize+segment_size):
                segment.append(self.msg[j])
            sendline = [id,self.msgsize]+segment
            pack.append(struct.pack(">%ui"%(segment_size+2),*sendline))
        return pack
    def sendWfToMqtt(self,client,address,qos,sleeptime):
        for i in range(len(self.messages)):
            msgaddress = address+"/"+str(i).zfill(3)
            client.publish(msgaddress,self.messages[i],qos)
            time.sleep(sleeptime)
    def sendWfToPv(self,pv_name):
        try:
            #print("[info] self.msg: %s" % self.msg)
            #print("[info] len(self.msg): %s" % len(self.msg))
            if len(self.msg)!=0:
                cothread.Callback(caput,pv_name,self.msg)
                return True
            return False
        except Exception as e:
            logger.error("Trouble when Publishing to PV with " + pv_name + ": " + str(e))
            logger.info(traceback.format_exc())


class Server:
    def __init__(self,type,name,timestamp=None):
        self.type = type
        self.name = name
        self.timestamp = timestamp
    def delayServer(self):
        self.timestamp = time.time()
    def checkServer(self):
        if not self.timestamp:
            return True
        if time.time()-self.timestamp>10:
            return True
        return False


def openConfigFile(filename):
    with open(filename) as data_file:
        data = json.load(data_file)
    return data

def unicodeToStr(name):
    string = name.encode('ascii', 'ignore')
    return string


def getChannel(channame):
    global chans
    for chan in chans:
        if channame.startswith(chan.chan):
            return chan

def on_connect(client, userdata, flags, rc):
    global chans
    logger.info("Connected with result code " + str(rc))
    #for channel in chans:
    #    channel.setConnection()


def on_message(client, userdata, msg):
    logger.debug(".on_message(topic=%s)" % repr(msg.topic))
    getChannel(msg.topic).updatePv(msg.payload)

try:
    config_path = os.path.join(script_dir, "gateway_config.json") # default config file
    if len(sys.argv) > 1:
        # config path given as cmdline argument
        config_path = sys.argv[1]

    config_info = openConfigFile(config_path)
    qapp = QtCore.QCoreApplication(sys.argv)
    #app = cothread.iqt()#run_exec=False)

    logger.info("Start")

    servers = []
    servers.extend((Server("ioc","VEPP3"),Server("mqtt","VEPP3")))


    client = mqtt.Client()

    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(config_info["mqtt_broker_address"])

    for connection in config_info["connections"]:
        channel = PvMqttChan(connection,servers,client)
        channel.setConnection()
        chans.append(channel)

    client.loop_start()
except Exception as e:
    logger.error("Initialization error: " + str(e))
    logger.error(traceback.format_exc())
    exit(1)

try:
    cothread.WaitForQuit()
finally:
    client.loop_stop()
