#!/usr/bin/env python

import numpy as np
from multiprocessing import Queue
from threading import Thread

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
import paho.mqtt.client as mqtt
import paho.mqtt.publish as publish

import traceback
import logging

import mqttconv


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

# predefined constants
CONV_CFG = {
    "segment_size_max": 1208, # bytes
    "segment_index_digits": 3, # decimal digits count in segment index
    "waveform_queue_size": 3, # difference between waveform ids 
                              # that enough to drop an old incomplete ones
}
MQTT_DELAY = 0.07 # seconds

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

        self.conv = mqttconv.get(self.datatype, CONV_CFG)

        self.queue = Queue()
        self.thread = Thread(target=self.updateChanLoop)

    def setConnection(self):
        try:
            logger.debug(".setConnection(%s, %s)" % (self.pv, self.chan))
            
            catools.connect(self.pv)
            if self.direction=="mp":
                if self.datatype == "wfint":
                    self.client.subscribe(self.chan+"#")
                else:
                    self.client.subscribe(self.chan)
            elif self.direction=="pm":
                self.thread.start()
                camonitor(self.pv, self.pushValue)

            logger.info(self.chan + " connection set")

        except Exception as e:
            logger.error("Trouble with connection with " + self.pv + " or " + self.chan + ": " + str(e))
            logger.debug(traceback.format_exc())
            #cothread.Quit()

    def pushValue(self, value):
        self.queue.put(value)

    def updateChanLoop(self):
        while True:
            self.updateChan(self.queue.get())

    def updateChan(self, value):
        try:
            logger.debug(".updateChan(%s, %s)" % (self.chan, repr(value)))

            for topic, payload in self.conv.encode(self.chan, value):
                self.client.publish(topic, payload, self.qos, self.retain).wait_for_publish()
                time.sleep(MQTT_DELAY)

        except Exception as e:
            logger.error("Trouble when Publishing to Mqtt with " + self.chan + ": " + str(e))
            logger.debug(traceback.format_exc())
            #cothread.Quit()

    def updatePv(self, topic, payload):
        try:
            logger.debug(".updatePv(%s, %s, %s)" % (self.pv, repr(topic), repr(payload)))

            value = self.conv.decode(topic, payload)
            if value is not None:
                cothread.CallbackResult(caput, self.pv, value)

        except Exception as e:
            logger.error("Trouble in updatePv with " + self.pv + ": " + str(e))
            logger.debug(traceback.format_exc())
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
        if channame == chan.chan or channame.rstrip("0123456789") == chan.chan:
            return chan
    return None

def on_connect(client, userdata, flags, rc):
    global chans
    logger.info("Connected with result code " + str(rc))
    #for channel in chans:
    #    channel.setConnection()


def on_message(client, userdata, msg):
    logger.debug(msg.topic)
    chan = getChannel(msg.topic)
    if chan is not None:
        chan.updatePv(msg.topic, msg.payload)

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
